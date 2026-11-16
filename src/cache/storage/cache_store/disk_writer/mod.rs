use std::future::Future;
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

    fn take_batch(&self) -> Option<Vec<(ChunkId, BlockKey)>> {
        let mut queue = self.queue.lock().unwrap();
        loop {
            if queue.stopping {
                return None;
            }
            if queue.blocks.len() >= self.batch_size {
                queue.active = true;
                return Some(queue.blocks.drain(..self.batch_size).collect());
            }
            queue = self.changed.wait(queue).unwrap();
        }
    }

    fn completed(&self) {
        let mut queue = self.queue.lock().unwrap();
        queue.active = false;
        self.changed.notify_all();
    }

    fn abandon(&self) -> Vec<(ChunkId, BlockKey)> {
        let mut queue = self.queue.lock().unwrap();
        self.changed.notify_all();
        std::mem::take(&mut queue.blocks)
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
            for (chunk, block) in retry {
                self.keep_in_memory(chunk, block);
            }
            self.pending.completed();
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
