use std::future::Future;
use std::time::{Duration, Instant};

use super::policy::{Io, IoEvent, IoPhase, ReadPolicy};

pub(super) struct IoReport<'r> {
    policy: &'r dyn ReadPolicy,
    io: Io<'r>,
    started: Instant,
    ended: bool,
    answered: Duration,
    received: Duration,
}

impl<'r> IoReport<'r> {
    pub(super) fn start(policy: &'r dyn ReadPolicy, io: Io<'r>) -> Self {
        Self::start_at(policy, io, Instant::now())
    }

    /// A late racer counts from its GET's first request, as the GET does.
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

    pub(super) fn answered(&mut self) {
        self.answered = self.started.elapsed();
        self.report(IoPhase::Answered {
            after: self.answered,
        });
    }

    pub(super) fn finished(&mut self, bytes: u64) {
        self.ended = true;
        self.received = self.started.elapsed();
        self.report(IoPhase::Finished {
            bytes,
            after: self.received,
        });
    }

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
