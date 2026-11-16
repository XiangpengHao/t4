use std::time::Duration;

use super::read::{FetchRuns, Io, IoEvent, IoPhase, LocatedRead, ReadPlan, ReadPolicy};
use crate::io::sync::Mutex;
use crate::io::sync::atomic::{AtomicU64, Ordering};

/// Races a normal read's GET once more if it runs past a quantile of recent GET times.
/// Not a read with an intent: its GETs may be huge, and a late racer's bytes are held whole.
#[derive(Debug)]
pub struct RaceStragglers {
    fetches: FetchRuns,
    quantile: f64,
    heard: Mutex<HeardTimes>,
    trigger_us: AtomicU64,
}

impl RaceStragglers {
    pub const HEARD: usize = 64;
    /// The trigger until [`Self::HEARD`] times are heard: past almost every GET of a few MiB,
    /// yet a fresh cache still races a GET that stalls.
    pub const START: Duration = Duration::from_millis(500);

    /// Races a GET once it runs past `quantile` of the GET times heard: 0.9 for the p90.
    pub fn at(quantile: f64) -> Self {
        assert!(
            quantile > 0.0 && quantile < 1.0,
            "a quantile is strictly between 0 and 1, not {quantile}"
        );
        Self {
            fetches: FetchRuns::default(),
            quantile,
            heard: Mutex::new(HeardTimes::default()),
            trigger_us: AtomicU64::new(HeardTimes::micros(Self::START)),
        }
    }

    #[must_use]
    pub fn planning_as(self, fetches: FetchRuns) -> Self {
        Self { fetches, ..self }
    }

    pub fn trigger(&self) -> Duration {
        Duration::from_micros(self.trigger_us.load(Ordering::Relaxed))
    }
}

/// t4's default read policy: races at the p90.
impl Default for RaceStragglers {
    fn default() -> Self {
        Self::at(0.9)
    }
}

impl ReadPolicy for RaceStragglers {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let plan = self.fetches.plan(read);
        match read.intent() {
            None => ReadPlan::new(
                plan.gets()
                    .iter()
                    .map(|get| get.clone().with_late_race(self.trigger()))
                    .collect(),
            ),
            Some(_) => plan,
        }
    }

    fn on_io(&self, event: &IoEvent<'_>) {
        if let (Io::Get { .. }, IoPhase::Finished { after, .. }) = (event.io(), event.phase()) {
            let learned = self
                .heard
                .lock()
                .expect("hearing a time doesn't panic")
                .hear(after, self.quantile);
            if let Some(trigger) = learned {
                self.trigger_us
                    .store(HeardTimes::micros(trigger), Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Default)]
struct HeardTimes {
    times: Vec<u64>,
    next: usize,
}

impl HeardTimes {
    fn hear(&mut self, time: Duration, quantile: f64) -> Option<Duration> {
        let us = Self::micros(time);
        if self.times.len() < RaceStragglers::HEARD {
            self.times.push(us);
            if self.times.len() < RaceStragglers::HEARD {
                return None;
            }
        } else {
            self.times[self.next] = us;
            self.next = (self.next + 1) % RaceStragglers::HEARD;
        }
        let mut times = [0; RaceStragglers::HEARD];
        times.copy_from_slice(&self.times);
        let at = ((times.len() as f64 * quantile) as usize).min(times.len() - 1);
        let (_, nth, _) = times.select_nth_unstable(at);
        Some(Duration::from_micros(*nth))
    }

    fn micros(time: Duration) -> u64 {
        u64::try_from(time.as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BLOCK_SIZE_U64;
    use crate::cache::policy::{BlockSource, ReadIntent};

    fn finished(ms: u64) -> IoEvent<'static> {
        IoEvent::new(
            Io::Get {
                path: b"f",
                bytes: 0..BLOCK_SIZE_U64,
                racer: 0,
            },
            IoPhase::Finished {
                bytes: BLOCK_SIZE_U64,
                after: Duration::from_millis(ms),
            },
        )
    }

    fn late_races(policy: &RaceStragglers, intent: Option<ReadIntent>) -> Vec<Option<Duration>> {
        let sources = [BlockSource::Remote; 2];
        let size = 2 * BLOCK_SIZE_U64;
        let read = LocatedRead::new(b"f", size, 0..size, intent, 0, &sources);
        policy
            .plan(&read)
            .gets()
            .iter()
            .map(|get| get.late_race())
            .collect()
    }

    #[test]
    fn races_a_normal_reads_gets_at_the_start_then_the_quantile_heard() {
        let policy = RaceStragglers::at(0.9);
        for ms in 1..RaceStragglers::HEARD as u64 {
            policy.on_io(&finished(ms));
        }
        assert_eq!(
            late_races(&policy, None),
            [Some(RaceStragglers::START)],
            "not learned yet"
        );
        policy.on_io(&finished(RaceStragglers::HEARD as u64));
        // 1..=64 ms heard: their p90, the 58th.
        let trigger = Duration::from_millis(58);
        assert_eq!(policy.trigger(), trigger);
        assert_eq!(late_races(&policy, None), [Some(trigger)]);
        assert_eq!(late_races(&policy, Some(ReadIntent::Warm)), [None]);
    }

    #[test]
    fn learns_from_the_latest_times() {
        let policy = RaceStragglers::at(0.5);
        for _ in 0..RaceStragglers::HEARD {
            policy.on_io(&finished(1000));
        }
        for _ in 0..RaceStragglers::HEARD {
            policy.on_io(&finished(10));
        }
        assert_eq!(policy.trigger(), Duration::from_millis(10));
    }
}
