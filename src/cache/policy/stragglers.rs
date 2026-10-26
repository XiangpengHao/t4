//! [`RaceStragglers`]: a read policy that races a GET once it runs long
//! (`doc/06-read-modes.md`, decision 8).

use std::time::Duration;

use super::read::{FetchRuns, Io, IoEvent, IoPhase, LocatedRead, ReadPlan, ReadPolicy};
use crate::io::sync::Mutex;
use crate::io::sync::atomic::{AtomicU64, Ordering};

/// Plans as [`FetchRuns`] does, and races each GET of a normal read once
/// more if its body isn't all in by a quantile of the times GETs lately
/// took, from their first request to their body all in: the last
/// [`Self::HEARD`] heard of. It races nothing until it has heard
/// [`Self::LEARNED`]. A read with an intent isn't raced: its GETs may be of
/// any size, and the cache holds a late racer's bytes until it has them all.
#[derive(Debug)]
pub struct RaceStragglers {
    quantile: f64,
    heard: Mutex<HeardTimes>,
    /// When a GET is raced, in µs; 0 until learned.
    trigger_us: AtomicU64,
}

impl RaceStragglers {
    /// The GET times it learns from, the latest.
    pub const HEARD: usize = 4096;
    /// The GET times it hears before racing any.
    pub const LEARNED: usize = 256;

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
            quantile,
            heard: Mutex::new(HeardTimes::default()),
            trigger_us: AtomicU64::new(0),
        }
    }

    /// When a GET is raced, once learned.
    pub fn trigger(&self) -> Option<Duration> {
        match self.trigger_us.load(Ordering::Relaxed) {
            0 => None,
            us => Some(Duration::from_micros(us)),
        }
    }
}

impl ReadPolicy for RaceStragglers {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let plan = FetchRuns.plan(read);
        match (self.trigger(), read.intent()) {
            (Some(after), None) => ReadPlan::new(
                plan.gets()
                    .iter()
                    .map(|get| get.clone().with_late_race(after))
                    .collect(),
            ),
            _ => plan,
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
                let us = u64::try_from(trigger.as_micros()).unwrap_or(u64::MAX);
                self.trigger_us.store(us.max(1), Ordering::Relaxed);
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
    /// Times heard since the quantile was last found.
    since: usize,
}

impl HeardTimes {
    /// Hears `time`; and, every [`RaceStragglers::LEARNED`] times, returns
    /// the `quantile` of those kept.
    fn hear(&mut self, time: Duration, quantile: f64) -> Option<Duration> {
        let us = u64::try_from(time.as_micros()).unwrap_or(u64::MAX);
        if self.times.len() < RaceStragglers::HEARD {
            self.times.push(us);
        } else {
            self.times[self.next] = us;
            self.next = (self.next + 1) % RaceStragglers::HEARD;
        }
        self.since += 1;
        if self.since < RaceStragglers::LEARNED {
            return None;
        }
        self.since = 0;
        let mut times = self.times.clone();
        let at = ((times.len() as f64 * quantile) as usize).min(times.len() - 1);
        let (_, nth, _) = times.select_nth_unstable(at);
        Some(Duration::from_micros(*nth))
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

    /// It races nothing until it has heard enough GETs; then a normal read's
    /// GETs at the quantile of those heard, and a read with an intent's
    /// never.
    #[test]
    fn races_a_normal_reads_gets_at_the_quantile_heard() {
        let policy = RaceStragglers::at(0.9);
        for ms in 1..RaceStragglers::LEARNED as u64 {
            policy.on_io(&finished(ms));
        }
        assert_eq!(late_races(&policy, None), [None], "not learned yet");
        for ms in RaceStragglers::LEARNED as u64..=1000 {
            policy.on_io(&finished(ms));
        }
        // 1..=1000 ms heard, the quantile found last when 768 had been:
        // their p90.
        let trigger = Duration::from_millis(692);
        assert_eq!(policy.trigger(), Some(trigger));
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
        assert_eq!(policy.trigger(), Some(Duration::from_millis(10)));
    }
}
