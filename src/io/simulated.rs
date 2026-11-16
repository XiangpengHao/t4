//! An operation reaches the disk only when it completes, so reads see writes in completion order.

use std::collections::VecDeque;

use shuttle::rand::{Rng, thread_rng};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::file_system::OpenFile;
use crate::io::io_task::{FilePosition, WorkerRequest};
use crate::io::sync::mpsc;

const MAX_WAITS: u32 = 8;

pub(crate) struct SimulatedDriver {
    files: Vec<OpenFile>,
    queue_depth: usize,
    inflight: Vec<InflightOp>,
    completions: VecDeque<CompletionEvent>,
}

struct InflightOp {
    op: SimOp,
    user_data: u64,
    waits_left: u32,
}

enum SimOp {
    Read {
        buf_ptr: *mut u8,
        buf_len: u32,
        at: FilePosition,
    },
    /// The writer's buffer, not a copy: a file opened for direct I/O refuses unaligned bytes.
    Write {
        buf_ptr: *const u8,
        buf_len: u32,
        at: FilePosition,
    },
}

impl SimOp {
    /// `push` checked the file is the worker's.
    fn complete(&self, files: &[OpenFile]) -> i32 {
        let done = match *self {
            Self::Read {
                buf_ptr,
                buf_len,
                at,
            } => {
                // SAFETY: the backend loop keeps the buffer alive and untouched until this completes.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len as usize) };
                files[at.file() as usize].read_at(buf, at.offset())
            }
            Self::Write {
                buf_ptr,
                buf_len,
                at,
            } => {
                // SAFETY: the backend loop keeps the buffer alive and unchanged until this completes.
                let bytes = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };
                files[at.file() as usize].write_at(bytes, at.offset())
            }
        };
        match done {
            Ok(len) => len as i32,
            Err(error) => -error.raw_os_error().unwrap_or(libc::EIO),
        }
    }
}

impl IoDriver for SimulatedDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.queue_depth - self.inflight.len()
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let (op, user_data) = match entry {
            SubmissionEntry::Read {
                buf_ptr,
                buf_len,
                at,
                user_data,
            } => {
                self.file(at)?;
                (
                    SimOp::Read {
                        buf_ptr,
                        buf_len,
                        at,
                    },
                    user_data,
                )
            }
            SubmissionEntry::Write {
                buf_ptr,
                buf_len,
                at,
                user_data,
                ..
            } => {
                self.file(at)?;
                (
                    SimOp::Write {
                        buf_ptr,
                        buf_len,
                        at,
                    },
                    user_data,
                )
            }
        };
        let waits_left = thread_rng().gen_range(0..=MAX_WAITS);
        self.inflight.push(InflightOp {
            op,
            user_data,
            waits_left,
        });
        Ok(())
    }

    fn submit(&mut self) -> Result<()> {
        Ok(())
    }

    fn unsubmitted(&mut self) -> usize {
        0
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completions.pop_front()
    }

    fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {
        shuttle::thread::yield_now();
        let files = &self.files;
        let completions = &mut self.completions;
        self.inflight.retain_mut(|inflight| {
            if inflight.waits_left > 0 {
                inflight.waits_left -= 1;
                return true;
            }
            completions.push_back(CompletionEvent {
                user_data: inflight.user_data,
                result: inflight.op.complete(files),
            });
            false
        });
    }
}

impl SimulatedDriver {
    fn file(&self, at: FilePosition) -> Result<&OpenFile> {
        self.files
            .get(at.file() as usize)
            .ok_or(Error::InvalidArgument(
                "an I/O names a file the worker hasn't",
            ))
    }
}

/// Files are read and written on the simulated disk at the scheduled completion.
pub(crate) fn new_backend(
    files: Vec<OpenFile>,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<SimulatedDriver>> {
    let queue_depth = queue_depth as usize;
    let driver = SimulatedDriver {
        files,
        queue_depth,
        inflight: Vec::with_capacity(queue_depth),
        completions: VecDeque::new(),
    };
    Ok(BackendLoop::new(queue_depth, rx, driver))
}
