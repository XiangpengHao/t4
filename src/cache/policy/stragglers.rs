//! [`RaceStragglers`]: a read policy that races a GET once it runs long
//! (`doc/06-read-modes.md`, decision 8).

use std::time::Duration;

use super::read::{FetchRuns, Io, IoEvent, IoPhase, LocatedRead, ReadPlan, ReadPolicy};
use crate::io::sync::Mutex;
use crate::io::sync::atomic::{AtomicU64, Ordering};

/// Plans as [`FetchRuns`] does, the default unless [`Self::planning_as`]
/// says otherwise, and races each GET of a normal read once more if its
/// body isn't all in by a quantile of the times the last [`Self::HEARD`]
/// GETs took, from their first request to their body all in; until it has
/// heard that many, by [`Self::START`]. A read with an intent isn't raced:
/// its GETs may be of any size, and the cache holds a late racer's bytes
/// until it has them all.
#[derive(Debug)]
pub struct RaceStragglers {
    fetches: FetchRuns,
    quantile: f64,
    heard: Mutex<HeardTimes>,
    /// When a GET is raced, in µs.
    trigger_us: AtomicU64,
}

impl RaceStragglers {
    /// The GET times it learns from, the latest.
    pub const HEARD: usize = 64;
    /// When a GET is raced until [`Self::HEARD`] have been heard: past
    /// almost every GET of a few MiB, so that a fresh cache, its footers
    /// read first, races a GET that stalls before it knows the store.
    pub const START: Duration = Duration::from_millis(500);

    /// Races a GET when it runs past `quantile` of the GET times heard, 0.9
    /// for the p90, say.
    ///
    /// # Panics
    ///
    /// If `quantile` isn't strictly between 0 and 1.
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

    /// Plans as `fetches` does, before racing late.
    #[must_use]
    pub fn planning_as(self, fetches: FetchRuns) -> Self {
        Self { fetches, ..self }
    }

    /// When a GET is raced: the quantile of the latest GET times heard, or
    /// [`Self::START`] until it has heard enough.
    pub fn trigger(&self) -> Duration {
        Duration::from_micros(self.trigger_us.load(Ordering::Relaxed))
    }
}

/// t4's default read policy: races a GET past the p90 of the GET times
/// heard.
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

/// The latest GET times heard, in µs: a ring of [`RaceStragglers::HEARD`].
#[derive(Debug, Default)]
struct HeardTimes {
    times: Vec<u64>,
    /// Where the next time goes, once the ring is full.
    next: usize,
}

impl HeardTimes {
    /// Hears `time`; and, once the ring is full, returns the `quantile` of
    /// the times in it.
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

    /// `time` in whole µs, as the ring keeps it.
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

    /// Until it has heard enough GETs, it races a normal read's GETs at the
    /// start; then at the quantile of those heard; a read with an intent's
    /// never.
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

    /// The times it learns from are the latest: older ones fall out.
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
