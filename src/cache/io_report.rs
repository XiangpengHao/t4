//! [`IoReport`]: an I/O the cache makes, told to the read policy as it goes
//! (`doc/06-read-modes.md`, decision 9).

use std::future::Future;
use std::time::{Duration, Instant};

use super::policy::{Io, IoEvent, IoPhase, ReadPolicy};

/// An I/O, reported as it goes: started when made; then answered, and
/// finished or failed; given up on if dropped before either, as a race's
/// losers are. A GET is reported landed too, once its blocks are cached.
pub(super) struct IoReport<'r> {
    policy: &'r dyn ReadPolicy,
    io: Io<'r>,
    started: Instant,
    ended: bool,
    /// When its answer began, and when it was all in, since it started.
    answered: Duration,
    received: Duration,
}

impl<'r> IoReport<'r> {
    /// Reports `io` started.
    pub(super) fn start(policy: &'r dyn ReadPolicy, io: Io<'r>) -> Self {
        Self::start_at(policy, io, Instant::now())
    }

    /// Reports `io` started, its times counted from `started`: a late
    /// racer's from when its GET's first request went, as its GET's are.
    pub(super) fn start_at(policy: &'r dyn ReadPolicy, io: Io<'r>, started: Instant) -> Self {
        policy.on_io(&IoEvent::new(io.clone(), IoPhase::Started));
        Self {
            policy,
            io,
            started,
            ended: false,
            answered: Duration::ZERO,
            received: Duration::ZERO,
        }
    }

    /// Reports its answer begun: a GET's response come, its body to follow.
    pub(super) fn answered(&mut self) {
        self.answered = self.started.elapsed();
        self.report(IoPhase::Answered {
            after: self.answered,
        });
    }

    /// Reports it done, `bytes` moved.
    pub(super) fn finished(&mut self, bytes: u64) {
        self.ended = true;
        self.received = self.started.elapsed();
        self.report(IoPhase::Finished {
            bytes,
            after: self.received,
        });
    }

    /// Reports a GET's blocks cached, once it finished: `landing` of its
    /// time went to landing its pieces in the cache as they came, and it
    /// `waited` for its turn before it started.
    pub(super) fn landed(self, landing: Duration, waited: Duration) {
        self.report(IoPhase::Landed {
            answered: self.answered,
            received: self.received,
            landing,
            after: self.started.elapsed(),
            waited,
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
        mut self,
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
