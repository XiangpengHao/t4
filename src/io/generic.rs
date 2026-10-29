use std::fs::File;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Select, Sender, bounded, unbounded};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

struct IoJob {
    files: Arc<[File]>,
    entry: SubmissionEntry,
}

unsafe impl Send for IoJob {}

pub(crate) struct GenericIoDriver {
    files: Arc<[File]>,
    queue_size: usize,
    inflight: usize,
    // Declared before `_pool`, so it drops first: the pool's threads see the
    // job channel close and exit before the pool joins them.
    job_tx: Sender<IoJob>,
    completion_rx: Receiver<CompletionEvent>,
    _pool: IoThreadPool,
}

impl GenericIoDriver {
    fn new(files: Vec<File>, queue_size: usize) -> Result<Self> {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())?
            .min(queue_size.max(1));
        let (job_tx, job_rx) = bounded::<IoJob>(queue_size);
        let (completion_tx, completion_rx) = unbounded::<CompletionEvent>();

        let threads = (0..num_threads)
            .map(|_| {
                let job_rx = job_rx.clone();
                let completion_tx = completion_tx.clone();
                // The pool stands in for the kernel's asynchronous I/O.
                std::thread::spawn(move || worker_loop(job_rx, completion_tx))
            })
            .collect();

        Ok(Self {
            files: files.into(),
            queue_size,
            inflight: 0,
            job_tx,
            completion_rx,
            _pool: IoThreadPool(threads),
        })
    }
}

/// The threads that run the jobs, joined when dropped: a thread holds the
/// files a moment past reporting its last job done, so only the join makes
/// sure no thread still holds them, and their locks, once the driver is
/// gone.
struct IoThreadPool(Vec<JoinHandle<()>>);

impl Drop for IoThreadPool {
    fn drop(&mut self) {
        for thread in self.0.drain(..) {
            // A panic on a pool thread was reported where it happened.
            let _ = thread.join();
        }
    }
}

impl IoDriver for GenericIoDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.queue_size.saturating_sub(self.inflight)
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let job = IoJob {
            files: Arc::clone(&self.files),
            entry,
        };
        self.job_tx.try_send(job).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "submission queue is full",
            ))
        })?;
        self.inflight += 1;
        Ok(())
    }

    fn submit(&mut self) -> Result<()> {
        Ok(())
    }

    fn unsubmitted(&mut self) -> usize {
        0
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        match self.completion_rx.try_recv() {
            Ok(event) => {
                self.inflight = self.inflight.saturating_sub(1);
                Some(event)
            }
            Err(_) => None,
        }
    }

    fn wait_for_progress(&mut self, request_rx: &mpsc::Receiver<WorkerRequest>) {
        let mut sel = Select::new();
        sel.recv(request_rx);
        sel.recv(&self.completion_rx);
        sel.ready();
    }
}

fn worker_loop(job_rx: Receiver<IoJob>, completion_tx: Sender<CompletionEvent>) {
    while let Ok(job) = job_rx.recv() {
        if completion_tx.send(job.entry.execute(&job.files)).is_err() {
            return;
        }
    }
}

pub(crate) fn new_backend(
    files: Vec<File>,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<GenericIoDriver>> {
    let queue_depth = queue_depth as usize;
    let driver = GenericIoDriver::new(files, queue_depth)?;
    Ok(BackendLoop::new(queue_depth, rx, driver))
}
