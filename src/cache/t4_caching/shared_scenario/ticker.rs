//! Time passes while every other thread waits, as it does on a machine: a wait that looks again
//! every second comes due. Shuttle runs this thread only then, as each yield lowers it below the
//! others. Once the script stops making progress it gives up, so that a hang ends in the
//! deadlock shuttle reports rather than in time passing for good.

use std::time::Duration;

use crate::io::sync::Arc;
use crate::io::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::io::sync::thread::{self, JoinHandle};
use crate::io::time::Clock;

pub(super) struct Ticker {
    state: Arc<TickerState>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct TickerState {
    progress: AtomicUsize,
    stopped: AtomicBool,
}

impl Ticker {
    /// `FrameSocket::RECHECK`, a waiting thread's longest sleep.
    pub(super) const TICK: Duration = Duration::from_secs(1);
    /// Rounds of every other thread waiting with no progress of the script's.
    const IDLE_ROUNDS: usize = 64;

    pub(super) fn start(clock: Clock) -> Self {
        let state = Arc::new(TickerState::default());
        let ticking = Arc::clone(&state);
        let thread = thread::spawn(move || ticking.run(&clock));
        Self {
            state,
            thread: Some(thread),
        }
    }

    pub(super) fn progressed(&self) {
        self.state.progress.fetch_add(1, Ordering::Relaxed);
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

impl TickerState {
    fn run(&self, clock: &Clock) {
        let (mut seen, mut idle) = (0, 0);
        while !self.stopped.load(Ordering::Relaxed) {
            let progress = self.progress.load(Ordering::Relaxed);
            if progress != seen {
                (seen, idle) = (progress, 0);
            }
            if idle == Ticker::IDLE_ROUNDS {
                thread::park();
                continue;
            }
            idle += 1;
            if clock.timers_waiting() > 0 {
                clock.advance(Ticker::TICK);
            }
            thread::yield_now();
        }
    }
}
