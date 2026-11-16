use crate::io::time::Clock;
use std::future::Future;
use std::time::Duration;

use super::policy::{Io, IoEvent, IoPhase, ReadPolicy};

pub(super) struct IoReport<'r> {
    policy: &'r dyn ReadPolicy,
    io: Io<'r>,
    started: Duration,
    clock: Clock,
    ended: bool,
    answered: Duration,
    received: Duration,
}

impl<'r> IoReport<'r> {
    pub(super) fn start(policy: &'r dyn ReadPolicy, io: Io<'r>, clock: &Clock) -> Self {
        Self::start_at(policy, io, clock.now(), clock)
    }

    /// A late racer counts from its GET's first request, as the GET does.
    pub(super) fn start_at(
        policy: &'r dyn ReadPolicy,
        io: Io<'r>,
        started: Duration,
        clock: &Clock,
    ) -> Self {
        policy.on_io(&IoEvent::new(io.clone(), IoPhase::Started));
        Self {
            policy,
            io,
            started,
            clock: clock.clone(),
            ended: false,
            answered: Duration::ZERO,
            received: Duration::ZERO,
        }
    }

    pub(super) fn answered(&mut self) {
        self.answered = self.clock.now().saturating_sub(self.started);
        self.report(IoPhase::Answered {
            after: self.answered,
        });
    }

    pub(super) fn finished(&mut self, bytes: u64) {
        self.ended = true;
        self.received = self.clock.now().saturating_sub(self.started);
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
            after: self.clock.now().saturating_sub(self.started),
            waited,
        });
    }

    pub(super) fn failed(mut self) {
        self.ended = true;
        self.report(IoPhase::Failed {
            after: self.clock.now().saturating_sub(self.started),
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
                after: self.clock.now().saturating_sub(self.started),
            });
        }
    }
}
