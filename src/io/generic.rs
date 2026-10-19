use std::fs::File;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Select, Sender, bounded, unbounded};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

struct IoJob {
    file: Arc<File>,
    entry: SubmissionEntry,
}

unsafe impl Send for IoJob {}

pub(crate) struct GenericIoDriver {
    file: Arc<File>,
    queue_size: usize,
    inflight: usize,
    job_tx: Sender<IoJob>,
    completion_rx: Receiver<CompletionEvent>,
}

impl GenericIoDriver {
    fn new(file: File, queue_size: usize) -> Result<Self> {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())?
            .min(queue_size.max(1));
        let (job_tx, job_rx) = bounded::<IoJob>(queue_size);
        let (completion_tx, completion_rx) = unbounded::<CompletionEvent>();

        for _ in 0..num_threads {
            let job_rx = job_rx.clone();
            let completion_tx = completion_tx.clone();
            // The pool stands in for the kernel's asynchronous I/O.
            std::thread::spawn(move || worker_loop(job_rx, completion_tx));
        }

        Ok(Self {
            file: Arc::new(file),
            queue_size,
            inflight: 0,
            job_tx,
            completion_rx,
        })
    }
}

impl IoDriver for GenericIoDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.queue_size.saturating_sub(self.inflight)
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let job = IoJob {
            file: Arc::clone(&self.file),
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
        if completion_tx.send(job.entry.execute(&job.file)).is_err() {
            return;
        }
    }
}

pub(crate) fn new_backend(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<GenericIoDriver>> {
    let queue_depth = queue_depth as usize;
    let driver = GenericIoDriver::new(file, queue_depth)?;
    Ok(BackendLoop::new(queue_depth, rx, driver))
}
