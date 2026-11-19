use std::future::{Future, poll_fn};
use std::task::{Context, Poll, Wake, Waker};

use super::Tiers;
use crate::cache::policy::Tier;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::thread::{self, JoinHandle, Thread};
use crate::io::sync::{Arc, Condvar, Mutex};

mod landed_block;

pub(in crate::cache) use landed_block::LandedBlock;

pub(super) struct PendingDisk {
    batch_size: usize,
    queue: Mutex<DiskQueue>,
    changed: Condvar,
}

#[derive(Default)]
struct DiskQueue {
    blocks: Vec<PendingWrite>,
    active: bool,
    stopping: bool,
    /// Landings waiting for room in the queue.
    waiting_landings: Vec<Waker>,
}

pub(super) enum PendingWrite {
    /// Picked by the policy, read from its memory chunk.
    Victim {
        chunk: ChunkId,
        block: BlockKey,
    },
    Landed(LandedBlock),
}

impl PendingWrite {
    fn is_landed(&self) -> bool {
        matches!(self, Self::Landed(_))
    }
}

impl PendingDisk {
    /// A batch, and the most blocks that wait behind the one being written.
    const BATCH: usize = 16;

    /// A memory tier smaller than a batch keeps its budget and writes its victims a block at a
    /// time.
    pub(super) fn new(memory_chunks: u32) -> Self {
        let batch_size = if (memory_chunks as usize) < Self::BATCH {
            1
        } else {
            Self::BATCH
        };
        Self {
            batch_size,
            queue: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    /// A victim is queued once: the policy that picked it forgets it until it is admitted again.
    pub(super) fn enqueue(&self, chunk: ChunkId, block: BlockKey) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if queue.stopping || queue.blocks.len() == Self::BATCH {
            return false;
        }
        queue.blocks.push(PendingWrite::Victim { chunk, block });
        self.changed.notify_all();
        true
    }

    /// Waits while the queue is full, as the writer always empties it a batch at a time; gives
    /// the block back if the writer stops.
    pub(super) async fn enqueue_landed(&self, landed: LandedBlock) -> Result<(), LandedBlock> {
        let mut landed = Some(landed);
        poll_fn(|context| {
            let mut queue = self.queue.lock().unwrap();
            let Some(block) = landed.take() else {
                unreachable!("polled until ready, once");
            };
            if queue.stopping {
                return Poll::Ready(Err(block));
            }
            if queue.blocks.len() < Self::BATCH {
                queue.blocks.push(PendingWrite::Landed(block));
                self.changed.notify_all();
                return Poll::Ready(Ok(()));
            }
            landed = Some(block);
            queue.waiting_landings.push(context.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// A landed block goes at once, with whatever else waits: reads find it meanwhile, but a kill
    /// loses it. Victims wait for a full batch, or for the writer to stop: a drop writes all.
    fn take_batch(&self) -> Option<Vec<PendingWrite>> {
        let mut queue = self.queue.lock().unwrap();
        loop {
            if queue.stopping && queue.blocks.is_empty() {
                return None;
            }
            let landed = queue.blocks.iter().any(PendingWrite::is_landed);
            if queue.stopping || queue.blocks.len() >= self.batch_size || landed {
                queue.active = true;
                let most = if landed || queue.stopping {
                    Self::BATCH
                } else {
                    self.batch_size
                };
                let size = queue.blocks.len().min(most);
                let batch = queue.blocks.drain(..size).collect();
                let waiting = std::mem::take(&mut queue.waiting_landings);
                drop(queue);
                waiting.into_iter().for_each(Waker::wake);
                return Some(batch);
            }
            queue = self.changed.wait(queue).unwrap();
        }
    }

    fn completed(&self) {
        let mut queue = self.queue.lock().unwrap();
        queue.active = false;
        self.changed.notify_all();
    }

    fn abandon(&self) -> Vec<PendingWrite> {
        let (blocks, waiting) = {
            let mut queue = self.queue.lock().unwrap();
            self.changed.notify_all();
            let blocks = std::mem::take(&mut queue.blocks);
            (blocks, std::mem::take(&mut queue.waiting_landings))
        };
        waiting.into_iter().for_each(Waker::wake);
        blocks
    }

    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn wait_for_idle(&self) {
        let mut queue = self.queue.lock().unwrap();
        while queue.active
            || queue.blocks.len() >= self.batch_size
            || queue.blocks.iter().any(PendingWrite::is_landed)
        {
            queue = self.changed.wait(queue).unwrap();
        }
    }
}

pub(super) struct DiskWriter {
    tiers: Arc<Tiers>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl DiskWriter {
    pub(super) fn start(tiers: Arc<Tiers>) -> std::io::Result<Self> {
        let writing = Arc::clone(&tiers);
        let thread = thread::Builder::new()
            .name("t4-disk-writer".into())
            .spawn(move || writing.write_batches_until_stopped())?;
        Ok(Self {
            tiers,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(super) fn stop(&self) {
        // Keep the handle locked through join: concurrent shutdowns must all wait for the writer.
        let mut thread = self.thread.lock().unwrap();
        {
            let pending = &self.tiers.pending;
            let mut queue = pending.queue.lock().unwrap();
            queue.stopping = true;
            pending.changed.notify_all();
        }
        if let Some(thread) = thread.take() {
            thread.join().expect("the disk writer completes");
        }
    }
}

impl Tiers {
    fn write_batches_until_stopped(&self) {
        let wake = Waker::from(std::sync::Arc::new(WriterWake(thread::current())));
        while let Some(batch) = self.pending.take_batch() {
            let mut flush = std::pin::pin!(self.write_disk_batch(batch));
            let mut context = Context::from_waker(&wake);
            let kept = loop {
                match flush.as_mut().poll(&mut context) {
                    Poll::Ready(kept) => break kept,
                    Poll::Pending => thread::park(),
                }
            };
            for (chunk, block) in kept {
                self.keep_in_memory(chunk, block);
            }
            self.pending.completed();
        }
        for write in self.pending.abandon() {
            match write {
                PendingWrite::Victim { chunk, block } => self.keep_in_memory(chunk, block),
                PendingWrite::Landed(landed) => self.leave_uncached(landed),
            }
        }
    }

    /// The policy forgot the chunk when it picked it as a victim: give it back.
    pub(super) fn keep_in_memory(&self, chunk: ChunkId, block: BlockKey) {
        if self.memory.read(chunk, block).is_some() {
            self.placement.on_admit(Tier::Memory, chunk, block);
        }
    }

    /// Unwritten: its fetch ends where another fetch cached it, if one did.
    pub(super) fn leave_uncached(&self, landed: LandedBlock) {
        let location = self.blocks.get(landed.block());
        landed.ended(location);
    }
}

struct WriterWake(Thread);

impl Wake for WriterWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.unpark();
    }
}
