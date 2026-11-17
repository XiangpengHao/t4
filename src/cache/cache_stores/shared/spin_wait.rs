//! A thread that sleeps the moment it waits pays a wakeup when its answer comes, and the exit
//! from any deep idle state its CPU fell into meanwhile (C6: 190 µs; doc 11, Cost).

use std::time::Duration;

#[derive(Clone, Copy)]
pub(super) struct SpinWait {
    budget: Duration,
}

impl SpinWait {
    /// Past a hot 1 MiB read's whole round, 75 µs; what a disk read or a GET answers waits longer
    /// than that, and sleeps.
    pub(super) const BEFORE_SLEEP: Self = Self {
        budget: Duration::from_micros(100),
    };

    /// Asks `ready` till it holds or fails, or the budget is spent: `Ok(false)` is the caller's
    /// to sleep on.
    #[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
    pub(super) fn until<E>(self, mut ready: impl FnMut() -> Result<bool, E>) -> Result<bool, E> {
        let clock = crate::io::time::Clock::default();
        loop {
            if ready()? {
                return Ok(true);
            }
            if clock.now() >= self.budget {
                return Ok(false);
            }
            std::hint::spin_loop();
        }
    }

    /// Simulated time stands still while a thread spins: there a wait sleeps at once.
    #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
    pub(super) fn until<E>(self, _ready: impl FnMut() -> Result<bool, E>) -> Result<bool, E> {
        let _ = self.budget;
        Ok(false)
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use super::SpinWait;

    /// A budget no preemption spends: ready comes first.
    const UNSPENT: SpinWait = SpinWait {
        budget: Duration::from_secs(3600),
    };

    #[test]
    fn a_wait_ends_as_soon_as_it_is_ready() {
        let asked = Cell::new(0);
        let ready = UNSPENT.until(|| {
            asked.set(asked.get() + 1);
            Ok::<_, ()>(asked.get() == 3)
        });
        assert_eq!((ready, asked.get()), (Ok(true), 3));
    }

    #[test]
    fn a_wait_never_ready_gives_up_once_its_budget_is_spent() {
        let wait = SpinWait {
            budget: Duration::from_millis(2),
        };
        let clock = crate::io::time::Clock::default();
        assert_eq!(wait.until(|| Ok::<_, ()>(false)), Ok(false));
        assert!(clock.now() >= Duration::from_millis(2));
    }

    #[test]
    fn a_wait_stops_at_the_first_failure() {
        let asked = Cell::new(0);
        let failed = UNSPENT.until(|| {
            asked.set(asked.get() + 1);
            Err::<bool, _>("gone")
        });
        assert_eq!((failed, asked.get()), (Err("gone"), 1));
    }
}
