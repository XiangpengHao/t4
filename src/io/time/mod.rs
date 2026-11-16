//! An owned clock: clones share an epoch, and tests explicitly advance time.
use std::time::Duration;

#[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
#[derive(Clone)]
pub(crate) struct Clock(std::time::Instant);

#[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
impl Default for Clock {
    fn default() -> Self {
        Self(std::time::Instant::now())
    }
}

#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
#[derive(Clone, Default)]
pub(crate) struct Clock(std::sync::Arc<crate::io::sync::Mutex<ManualTime>>);

#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
#[derive(Default)]
struct ManualTime {
    now: Duration,
    timers: Vec<crate::io::sync::thread::Thread>,
}

impl Clock {
    pub(crate) fn now(&self) -> Duration {
        #[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
        {
            self.0.elapsed()
        }
        #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
        {
            self.0.lock().unwrap().now
        }
    }
    pub(crate) fn wait(&self, duration: Duration) {
        #[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
        std::thread::sleep(duration);
        #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
        {
            self.advance(duration);
            crate::io::sync::thread::yield_now();
        }
    }
    #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
    pub(crate) fn advance(&self, duration: Duration) {
        let timers = {
            let mut time = self.0.lock().unwrap();
            time.now += duration;
            std::mem::take(&mut time.timers)
        };
        for timer in timers {
            timer.unpark();
        }
    }
    #[cfg(all(test, feature = "shuttle"))]
    pub(crate) fn timers_waiting(&self) -> usize {
        self.0.lock().unwrap().timers.len()
    }
    #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
    pub(crate) fn register_timer(&self) {
        let thread = crate::io::sync::thread::current();
        let mut time = self.0.lock().unwrap();
        if !time.timers.iter().any(|timer| timer.id() == thread.id()) {
            time.timers.push(thread);
        }
    }
}
