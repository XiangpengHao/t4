//! The driver shuttle tests run on, in place of the kernel and the drive.
//! Each operation stays in flight for a random number of the loop's waits,
//! drawn from shuttle's random source, and reaches the [`SimDisk`] only
//! once it completes: a read can see a write that completes before it,
//! whichever was submitted first, as with real asynchronous I/O. The
//! scheduler decides every step, so a failing schedule replays exactly.

mod disk;

pub(crate) use disk::SimDisk;

use std::collections::VecDeque;
use std::fs::File;

use shuttle::rand::{Rng, thread_rng};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;
use disk::{Completion, WriteId};

/// The most waits an operation stays in flight.
const MAX_WAITS: u32 = 8;

pub(crate) struct SimulatedDriver {
    disk: SimDisk,
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
        offset: u64,
    },
    Write(WriteId),
}

impl SimOp {
    fn complete(&self, disk: &mut SimDisk) -> i32 {
        let completion = match *self {
            Self::Read {
                buf_ptr,
                buf_len,
                offset,
            } => {
                // SAFETY: the backend loop keeps the buffer alive, and touches
                // it nowhere else, until this read completes.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len as usize) };
                Completion::Read { offset, buf }
            }
            Self::Write(id) => Completion::Write(id),
        };
        disk.complete(completion)
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
                offset,
                user_data,
            } => (
                SimOp::Read {
                    buf_ptr,
                    buf_len,
                    offset,
                },
                user_data,
            ),
            SubmissionEntry::Write {
                buf_ptr,
                buf_len,
                offset,
                user_data,
                ..
            } => {
                // SAFETY: the backend loop keeps the buffer alive, and
                // unchanged, until this write completes.
                let bytes = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };
                let id = self.disk.submit_write(offset, bytes.to_vec());
                (SimOp::Write(id), user_data)
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

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completions.pop_front()
    }

    /// One unit of I/O time: the other threads run first, then each
    /// operation in flight either completes, if its waits are up, or ages
    /// by one.
    fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {
        shuttle::thread::yield_now();
        let disk = &mut self.disk;
        let completions = &mut self.completions;
        self.inflight.retain_mut(|inflight| {
            if inflight.waits_left > 0 {
                inflight.waits_left -= 1;
                return true;
            }
            completions.push_back(CompletionEvent {
                user_data: inflight.user_data,
                result: inflight.op.complete(disk),
            });
            false
        });
    }
}

/// The backend over `disk`.
pub(crate) fn new_backend_on(
    disk: SimDisk,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> BackendLoop<SimulatedDriver> {
    let queue_depth = queue_depth as usize;
    let driver = SimulatedDriver {
        disk,
        queue_depth,
        inflight: Vec::with_capacity(queue_depth),
        completions: VecDeque::new(),
    };
    BackendLoop::new(queue_depth, rx, driver)
}

/// Shuttle tests run on a [`SimDisk`] (`IoWorker::simulated`), never on a
/// real file.
pub(crate) fn new_backend(
    _file: File,
    _queue_depth: u32,
    _rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<SimulatedDriver>> {
    Err(Error::InvalidArgument(
        "shuttle tests run on a SimDisk: IoWorker::simulated",
    ))
}
