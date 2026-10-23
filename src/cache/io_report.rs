//! [`IoReport`]: an I/O the cache makes, told to the read policy as it goes
//! (`doc/06-read-modes.md`, decision 9).

use std::future::Future;
use std::time::Instant;

use super::policy::{Io, IoEvent, IoPhase, ReadPolicy};

/// An I/O, reported as it goes: started when made; then answered, and
/// finished or failed; given up on if dropped before either, as a race's
/// losers are.
pub(super) struct IoReport<'r> {
    policy: &'r dyn ReadPolicy,
    io: Io<'r>,
    started: Instant,
    ended: bool,
}

impl<'r> IoReport<'r> {
    /// Reports `io` started.
    pub(super) fn start(policy: &'r dyn ReadPolicy, io: Io<'r>) -> Self {
        policy.on_io(&IoEvent::new(io.clone(), IoPhase::Started));
        Self {
            policy,
            io,
            started: Instant::now(),
            ended: false,
        }
    }

    /// Reports its answer begun: a GET's response come, its body to follow.
    pub(super) fn answered(&self) {
        self.report(IoPhase::Answered {
            after: self.started.elapsed(),
        });
    }

    /// Reports it done, `bytes` moved.
    pub(super) fn finished(mut self, bytes: u64) {
        self.ended = true;
        self.report(IoPhase::Finished {
            bytes,
            after: self.started.elapsed(),
        });
    }

    pub(super) fn failed(mut self) {
        self.ended = true;
        self.report(IoPhase::Failed {
            after: self.started.elapsed(),
        });
    }

    /// Waits for `io`, the I/O reported, and reports it finished, its
    /// bytes moved, or failed.
    pub(super) async fn awaiting<T, E>(
        self,
        io: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let done = io.await;
        match &done {
            Ok(_) => {
                let bytes = self.io.bytes();
                self.finished(bytes);
            }
            Err(_) => self.failed(),
        }
        done
    }

    fn report(&self, phase: IoPhase) {
        self.policy.on_io(&IoEvent::new(self.io.clone(), phase));
    }
}

impl Drop for IoReport<'_> {
    fn drop(&mut self) {
        if !self.ended {
            self.report(IoPhase::Cancelled {
                after: self.started.elapsed(),
            });
        }
    }
}
