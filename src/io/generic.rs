use std::fs::File;

use crossbeam_channel::{Receiver, Select, Sender, bounded, unbounded};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::{mpsc, thread};

use super::common::FileType;

struct IoJob {
    file: FileType,
    user_data: u64,
    kind: IoJobKind,
}

enum IoJobKind {
    Read {
        buf: *mut u8,
        len: usize,
        offset: u64,
    },
    Write {
        buf: *const u8,
        len: usize,
        offset: u64,
    },
    Fsync,
}

unsafe impl Send for IoJob {}

impl IoJob {
    fn execute(&self) -> i32 {
        let file = match &self.file {
            FileType::File(file) => file.as_ref(),
            FileType::RawFd(_) => unimplemented!("generic io driver uses normal file types."),
        };

        let result: std::io::Result<usize> = match self.kind {
            IoJobKind::Read { buf, len, offset } => {
                let slice = unsafe { std::slice::from_raw_parts_mut(buf, len) };
                read_at(file, slice, offset)
            }
            IoJobKind::Write { buf, len, offset } => {
                let slice = unsafe { std::slice::from_raw_parts(buf, len) };
                write_at(file, slice, offset)
            }
            IoJobKind::Fsync => file.sync_all().map(|_| 0),
        };

        match result {
            Ok(n) => n as i32,
            // Fallback to EIO(5) if no raw_os_error
            Err(err) => -err.raw_os_error().unwrap_or(5),
        }
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buf, offset)
}

pub(crate) struct GenericIoDriver {
    queue_size: usize,
    inflight: usize,
    job_tx: Sender<IoJob>,
    completion_rx: Receiver<CompletionEvent>,
}

impl GenericIoDriver {
    fn new(queue_size: usize) -> Result<Self> {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())?
            .min(queue_size.max(1));
        let (job_tx, job_rx) = bounded::<IoJob>(queue_size);
        let (completion_tx, completion_rx) = unbounded::<CompletionEvent>();

        for _ in 0..num_threads {
            let job_rx = job_rx.clone();
            let completion_tx = completion_tx.clone();
            thread::spawn(move || worker_loop(job_rx, completion_tx));
        }

        Ok(Self {
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
        let job = match entry {
            SubmissionEntry::Read {
                file,
                buf_ptr,
                buf_len,
                offset,
                user_data,
            } => IoJob {
                file,
                user_data,
                kind: IoJobKind::Read {
                    buf: buf_ptr,
                    len: buf_len as usize,
                    offset,
                },
            },
            SubmissionEntry::Write {
                file,
                buf_ptr,
                buf_len,
                offset,
                user_data,
                is_last_in_batch: _,
            } => IoJob {
                file,
                user_data,
                kind: IoJobKind::Write {
                    buf: buf_ptr,
                    len: buf_len as usize,
                    offset,
                },
            },
            SubmissionEntry::Fsync { file, user_data } => IoJob {
                file,
                user_data,
                kind: IoJobKind::Fsync,
            },
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

    fn use_raw_fd(&mut self) -> bool {
        false
    }
}

fn worker_loop(job_rx: Receiver<IoJob>, completion_tx: Sender<CompletionEvent>) {
    while let Ok(job) = job_rx.recv() {
        let event = CompletionEvent {
            user_data: job.user_data,
            result: job.execute(),
        };
        if completion_tx.send(event).is_err() {
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
    let driver = GenericIoDriver::new(queue_depth)?;
    Ok(BackendLoop::new(file, queue_depth, rx, driver))
}
