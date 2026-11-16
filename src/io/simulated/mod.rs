//! An operation reaches the disk only when it completes, so reads see writes in completion order.

mod disk;

pub(crate) use disk::SimDisk;

use std::collections::VecDeque;
use std::fs::File;

use shuttle::rand::{Rng, thread_rng};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::{FilePosition, WorkerRequest};
use crate::io::sync::mpsc;
use disk::{Completion, WriteId};

const MAX_WAITS: u32 = 8;

pub(crate) struct SimulatedDriver {
    disks: Vec<SimDisk>,
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
    Write {
        file: u32,
        id: WriteId,
    },
}

impl SimOp {
    fn complete(&self, disks: &mut [SimDisk]) -> i32 {
        let (file, completion) = match *self {
            Self::Read {
                buf_ptr,
                buf_len,
                at,
            } => {
                // SAFETY: the backend loop keeps the buffer alive and untouched until this completes.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len as usize) };
                let offset = at.offset();
                (at.file(), Completion::Read { offset, buf })
            }
            Self::Write { file, id } => (file, Completion::Write(id)),
        };
        disks[file as usize].complete(completion)
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
                self.disk(at)?;
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
                // SAFETY: the backend loop keeps the buffer alive and unchanged until this completes.
                let bytes = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };
                let id = self.disk(at)?.submit_write(at.offset(), bytes.to_vec());
                (
                    SimOp::Write {
                        file: at.file(),
                        id,
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
        let disks = &mut self.disks;
        let completions = &mut self.completions;
        self.inflight.retain_mut(|inflight| {
            if inflight.waits_left > 0 {
                inflight.waits_left -= 1;
                return true;
            }
            completions.push_back(CompletionEvent {
                user_data: inflight.user_data,
                result: inflight.op.complete(disks),
            });
            false
        });
    }
}

impl SimulatedDriver {
    fn disk(&mut self, at: FilePosition) -> Result<&mut SimDisk> {
        self.disks
            .get_mut(at.file() as usize)
            .ok_or(Error::InvalidArgument(
                "an I/O names a file the worker hasn't",
            ))
    }
}

pub(crate) fn new_backend_on(
    disks: Vec<SimDisk>,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> BackendLoop<SimulatedDriver> {
    let queue_depth = queue_depth as usize;
    let driver = SimulatedDriver {
        disks,
        queue_depth,
        inflight: Vec::with_capacity(queue_depth),
        completions: VecDeque::new(),
    };
    BackendLoop::new(queue_depth, rx, driver)
}

/// Files are read and written at the scheduled completion: restart bytes and locks hold
/// with no OS I/O worker.
pub(crate) fn new_backend(
    files: Vec<File>,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<SimulatedDriver>> {
    Ok(new_backend_on(
        files.into_iter().map(SimDisk::on_file).collect(),
        queue_depth,
        rx,
    ))
}
