use std::collections::BTreeSet;
use std::future::Future;
use std::task::{Context, Poll, Wake, Waker};

use super::{CacheState, ObjectCache};
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
    held: BTreeSet<(ChunkId, BlockKey)>,
    active: bool,
    stopping: bool,
}

impl PendingDisk {
    pub(super) fn new(memory_chunks: u32) -> Self {
        Self {
            batch_size: if memory_chunks < 16 { 1 } else { 16 },
            queue: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    pub(super) fn enqueue(&self, chunk: ChunkId, block: BlockKey) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if queue.stopping || queue.blocks.len() == 16 {
            return false;
        }
        if queue.held.insert((chunk, block)) {
            queue.blocks.push((chunk, block));
            self.changed.notify_all();
        }
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

    fn release(&self, batch: &[(ChunkId, BlockKey)]) {
        let mut queue = self.queue.lock().unwrap();
        for block in batch {
            queue.held.remove(block);
        }
    }

    fn completed(&self) {
        let mut queue = self.queue.lock().unwrap();
        queue.active = false;
        self.changed.notify_all();
    }

    fn abandon(&self) -> Vec<(ChunkId, BlockKey)> {
        let mut queue = self.queue.lock().unwrap();
        queue.held.clear();
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
    pending: Arc<PendingDisk>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl DiskWriter {
    pub(super) fn start(state: Arc<CacheState>) -> std::io::Result<Self> {
        let pending = Arc::clone(&state.pending);
        let thread = thread::Builder::new()
            .name("t4-disk-writer".into())
            .spawn(move || {
                let cache = ObjectCache {
                    clock: Default::default(),
                    state,
                    writer: None,
                };
                let wake = Waker::from(std::sync::Arc::new(WriterWake(thread::current())));
                while let Some(batch) = cache.pending.take_batch() {
                    let mut flush = std::pin::pin!(cache.write_disk_batch(&batch));
                    let mut context = Context::from_waker(&wake);
                    let retry = loop {
                        match flush.as_mut().poll(&mut context) {
                            Poll::Ready(retry) => break retry,
                            Poll::Pending => thread::park(),
                        }
                    };
                    cache.pending.release(&batch);
                    for (chunk, block) in retry {
                        if cache.memory.read(chunk, block).is_some() {
                            cache.placement.on_admit(
                                crate::cache::policy::Tier::Memory,
                                chunk,
                                block,
                            );
                        }
                    }
                    cache.pending.completed();
                }
                for (chunk, block) in cache.pending.abandon() {
                    if cache.memory.read(chunk, block).is_some() {
                        cache
                            .placement
                            .on_admit(crate::cache::policy::Tier::Memory, chunk, block);
                    }
                }
            })?;
        Ok(Self {
            pending,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(super) fn stop(&self) {
        // Keep the handle locked through join: concurrent shutdowns must all wait for the writer.
        let mut thread = self.thread.lock().unwrap();
        {
            let mut queue = self.pending.queue.lock().unwrap();
            queue.stopping = true;
            self.pending.changed.notify_all();
        }
        if let Some(thread) = thread.take() {
            thread.join().expect("the disk writer completes");
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
