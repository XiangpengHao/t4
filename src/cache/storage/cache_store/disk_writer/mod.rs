use std::future::{Future, poll_fn};
use std::task::{Context, Poll, Wake, Waker};

use super::Tiers;
use crate::cache::policy::Tier;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::thread::{self, JoinHandle, Thread};
use crate::io::sync::{Arc, Condvar, Mutex};

pub(super) struct PendingDisk {
    batch_size: usize,
    queue: Mutex<DiskQueue>,
    changed: Condvar,
}

#[derive(Default)]
struct DiskQueue {
    blocks: Vec<(ChunkId, BlockKey)>,
    active: bool,
    stopping: bool,
    batches_written: u64,
    blocks_moved: u64,
    /// Fills waiting for a batch, which goes even if not full while they wait.
    waiting_fills: Vec<Waker>,
}

impl PendingDisk {
    /// A batch, and the most blocks that wait behind the one being written.
    const BATCH: usize = 16;

    /// A memory tier smaller than a batch keeps its budget and writes a block at a time.
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

    /// A block is queued once: the fill that published it, or the policy that picked it, which
    /// forgets a victim until it is admitted again.
    pub(super) fn enqueue(&self, chunk: ChunkId, block: BlockKey) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if queue.stopping || queue.blocks.len() == Self::BATCH {
            return false;
        }
        queue.blocks.push((chunk, block));
        self.changed.notify_all();
        true
    }

    /// For a block bound for disk that found memory full: true once the writer has written a
    /// batch, the one in flight or the next, and taken blocks out of memory since; false at
    /// once if it has nothing to write or is stopping, or once its batch moved nothing, as a
    /// failed write does, so that a fill never waits on a writer making no room.
    pub(super) async fn moved_blocks(&self) -> bool {
        let mut waited_for = None;
        poll_fn(|context| {
            let mut queue = self.queue.lock().unwrap();
            if queue.stopping {
                return Poll::Ready(false);
            }
            let (batch, moved_before) = match waited_for {
                Some(waited_for) => waited_for,
                None if !queue.active && queue.blocks.is_empty() => return Poll::Ready(false),
                None => *waited_for.insert((queue.batches_written + 1, queue.blocks_moved)),
            };
            if queue.batches_written >= batch {
                return Poll::Ready(queue.blocks_moved > moved_before);
            }
            queue.waiting_fills.push(context.waker().clone());
            self.changed.notify_all();
            Poll::Pending
        })
        .await
    }

    fn take_batch(&self) -> Option<Vec<(ChunkId, BlockKey)>> {
        let mut queue = self.queue.lock().unwrap();
        loop {
            if queue.stopping {
                return None;
            }
            let fills_wait = !queue.waiting_fills.is_empty() && !queue.blocks.is_empty();
            if queue.blocks.len() >= self.batch_size || fills_wait {
                queue.active = true;
                let size = queue.blocks.len().min(self.batch_size);
                return Some(queue.blocks.drain(..size).collect());
            }
            queue = self.changed.wait(queue).unwrap();
        }
    }

    fn completed(&self, moved: usize) {
        let waiting_fills = {
            let mut queue = self.queue.lock().unwrap();
            queue.active = false;
            queue.batches_written += 1;
            queue.blocks_moved += moved as u64;
            self.changed.notify_all();
            std::mem::take(&mut queue.waiting_fills)
        };
        waiting_fills.into_iter().for_each(Waker::wake);
    }

    fn abandon(&self) -> Vec<(ChunkId, BlockKey)> {
        let (blocks, waiting_fills) = {
            let mut queue = self.queue.lock().unwrap();
            self.changed.notify_all();
            let blocks = std::mem::take(&mut queue.blocks);
            (blocks, std::mem::take(&mut queue.waiting_fills))
        };
        waiting_fills.into_iter().for_each(Waker::wake);
        blocks
    }

    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn wait_for_idle(&self) {
        let mut queue = self.queue.lock().unwrap();
        while queue.active || queue.blocks.len() >= self.batch_size {
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
            let mut flush = std::pin::pin!(self.write_disk_batch(&batch));
            let mut context = Context::from_waker(&wake);
            let retry = loop {
                match flush.as_mut().poll(&mut context) {
                    Poll::Ready(retry) => break retry,
                    Poll::Pending => thread::park(),
                }
            };
            let moved = batch.len() - retry.len();
            for (chunk, block) in retry {
                self.keep_in_memory(chunk, block);
            }
            self.pending.completed(moved);
        }
        for (chunk, block) in self.pending.abandon() {
            self.keep_in_memory(chunk, block);
        }
    }

    /// The policy forgot the chunk when it picked it as a victim: give it back.
    fn keep_in_memory(&self, chunk: ChunkId, block: BlockKey) {
        if self.memory.read(chunk, block).is_some() {
            self.placement.on_admit(Tier::Memory, chunk, block);
        }
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
