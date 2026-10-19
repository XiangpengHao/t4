//! The driver shuttle tests run on, in place of the kernel. Each operation
//! stays in flight for a random number of the loop's waits, drawn from
//! shuttle's random source, and touches the file only once it completes:
//! a read can see a write that completes before it, whichever was
//! submitted first, as with real asynchronous I/O. The scheduler decides
//! every step, so a failing schedule replays exactly.

use std::collections::VecDeque;
use std::fs::File;

use shuttle::rand::{Rng, thread_rng};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::Result;
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

/// The most waits an operation stays in flight.
const MAX_WAITS: u32 = 8;

pub(crate) struct SimulatedDriver {
    file: File,
    queue_depth: usize,
    inflight: Vec<InflightOp>,
    completions: VecDeque<CompletionEvent>,
}

struct InflightOp {
    entry: SubmissionEntry,
    waits_left: u32,
}

impl IoDriver for SimulatedDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.queue_depth - self.inflight.len()
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let waits_left = thread_rng().gen_range(0..=MAX_WAITS);
        self.inflight.push(InflightOp { entry, waits_left });
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
        let file = &self.file;
        let completions = &mut self.completions;
        self.inflight.retain_mut(|op| {
            if op.waits_left > 0 {
                op.waits_left -= 1;
                return true;
            }
            completions.push_back(op.entry.execute(file));
            false
        });
    }
}

pub(crate) fn new_backend(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<SimulatedDriver>> {
    let queue_depth = queue_depth as usize;
    let driver = SimulatedDriver {
        file,
        queue_depth,
        inflight: Vec::with_capacity(queue_depth),
        completions: VecDeque::new(),
    };
    Ok(BackendLoop::new(queue_depth, rx, driver))
}
