use std::fmt;
use std::io::{self, Write};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use t4::policy::{Io, IoEvent, IoPhase, LocatedRead, ReadPlan, ReadPolicy};

use crate::replay::LatencySummary;

#[derive(Debug)]
pub struct TimedReads<P> {
    plans: P,
    latencies: Arc<IoLatencies>,
}

impl<P: ReadPolicy> TimedReads<P> {
    pub fn new(plans: P, latencies: Arc<IoLatencies>) -> Self {
        Self { plans, latencies }
    }
}

impl<P: ReadPolicy> ReadPolicy for TimedReads<P> {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        self.plans.plan(read)
    }

    fn on_io(&self, event: &IoEvent<'_>) {
        self.latencies.record(event);
        self.plans.on_io(event);
    }
}

#[derive(Debug, Default)]
pub struct IoLatencies {
    get_first_byte: LatencyHistogram,
    get_done: LatencyHistogram,
    disk_read_done: LatencyHistogram,
    cancelled: AtomicU64,
    failed: AtomicU64,
    gets: Option<GetPhases>,
}

impl IoLatencies {
    pub fn keeping_gets() -> Self {
        Self {
            gets: Some(GetPhases {
                step: Mutex::new("setup".into()),
                rows: Mutex::default(),
            }),
            ..Self::default()
        }
    }

    pub fn begin_step(&self, step: &str) {
        if let Some(gets) = &self.gets {
            *gets.step.lock().unwrap_or_else(PoisonError::into_inner) = step.into();
        }
    }

    pub fn write_gets_csv(&self, out: impl Write) -> io::Result<()> {
        match &self.gets {
            Some(gets) => gets.write_csv(out),
            None => Ok(()),
        }
    }

    pub fn take(&self) -> TakenIoLatencies {
        TakenIoLatencies {
            get_first_byte: self.get_first_byte.take(),
            get_done: self.get_done.take(),
            disk_read_done: self.disk_read_done.take(),
            cancelled: self.cancelled.swap(0, Ordering::Relaxed),
            failed: self.failed.swap(0, Ordering::Relaxed),
        }
    }

    fn record(&self, event: &IoEvent<'_>) {
        let histogram = match (event.io(), event.phase()) {
            (Io::Get { .. }, IoPhase::Answered { after }) => Some((&self.get_first_byte, after)),
            (Io::Get { .. }, IoPhase::Finished { after, .. }) => Some((&self.get_done, after)),
            (Io::DiskRead { .. }, IoPhase::Finished { after, .. }) => {
                Some((&self.disk_read_done, after))
            }
            (Io::Get { path, bytes, racer }, IoPhase::Landed { .. }) => {
                if let Some(gets) = &self.gets {
                    gets.keep(path, bytes, *racer, event.phase());
                }
                None
            }
            (Io::DiskRead { .. }, IoPhase::Landed { .. }) => None,
            (_, IoPhase::Cancelled { .. }) => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
                None
            }
            (_, IoPhase::Failed { .. }) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                None
            }
            (_, IoPhase::Started | IoPhase::Answered { .. }) => None,
        };
        if let Some((histogram, after)) = histogram {
            histogram.record(after);
        }
    }
}

#[derive(Debug)]
struct GetPhases {
    step: Mutex<String>,
    rows: Mutex<Vec<String>>,
}

impl GetPhases {
    const CSV_COLUMNS: &str =
        "step,path,start,end,racer,answered_ms,received_ms,landing_ms,landed_ms,waited_ms";

    fn keep(&self, path: &[u8], bytes: &Range<u64>, racer: u8, landed: IoPhase) {
        let IoPhase::Landed {
            answered,
            received,
            landing,
            after,
            waited,
        } = landed
        else {
            return;
        };
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        let step = self
            .step
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let row = format!(
            "{step},{},{},{},{racer},{:.3},{:.3},{:.3},{:.3},{:.3}",
            String::from_utf8_lossy(path),
            bytes.start,
            bytes.end,
            ms(answered),
            ms(received),
            ms(landing),
            ms(after),
            ms(waited)
        );
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(row);
    }

    fn write_csv(&self, mut out: impl Write) -> io::Result<()> {
        writeln!(out, "{}", Self::CSV_COLUMNS)?;
        for row in self
            .rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            writeln!(out, "{row}")?;
        }
        out.flush()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TakenIoLatencies {
    get_first_byte: TakenLatencies,
    get_done: TakenLatencies,
    disk_read_done: TakenLatencies,
    cancelled: u64,
    failed: u64,
}

impl TakenIoLatencies {
    pub const CSV_COLUMNS: &str = "get_first_byte_count,get_first_byte_p50_ms,\
                                   get_first_byte_p99_ms,get_first_byte_max_ms,\
                                   get_count,get_p50_ms,get_p99_ms,get_max_ms,\
                                   disk_read_count,disk_read_p50_ms,disk_read_p99_ms,\
                                   disk_read_max_ms,io_cancelled,io_failed";

    pub fn csv_fields(taken: Option<&Self>) -> String {
        taken.map_or_else(
            || ",".repeat(13),
            |taken| {
                format!(
                    "{},{},{},{},{}",
                    taken.get_first_byte.csv_fields(),
                    taken.get_done.csv_fields(),
                    taken.disk_read_done.csv_fields(),
                    taken.cancelled,
                    taken.failed
                )
            },
        )
    }
}

impl fmt::Display for TakenIoLatencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GETs: {}, first byte {}; disk reads: {}",
            self.get_done, self.get_first_byte, self.disk_read_done
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct TakenLatencies {
    count: u64,
    summary: Option<LatencySummary>,
}

impl TakenLatencies {
    fn csv_fields(&self) -> String {
        let ms = |latency: Duration| latency.as_secs_f64() * 1e3;
        match self.summary {
            Some(summary) => format!(
                "{},{:.3},{:.3},{:.3}",
                self.count,
                ms(summary.p50()),
                ms(summary.p99()),
                ms(summary.max())
            ),
            None => format!("{},,,", self.count),
        }
    }
}

impl fmt::Display for TakenLatencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.summary {
            Some(summary) => write!(f, "{} ({summary})", self.count),
            None => write!(f, "0"),
        }
    }
}

/// Lock-free: the read path it times pays only an atomic add or two.
#[derive(Debug)]
struct LatencyHistogram {
    /// One bucket per µs below [`Self::SUB`], then [`Self::SUB`] per power of two.
    buckets: Box<[AtomicU64]>,
    max_micros: AtomicU64,
}

impl LatencyHistogram {
    const SUB_BITS: u32 = 4;
    const SUB: u64 = 1 << Self::SUB_BITS;
    /// The longest latency told apart, in microseconds: 2^41 - 1, 25 days.
    const MAX_BITS: u32 = 40;
    const BUCKETS: usize = (Self::SUB * (Self::MAX_BITS - Self::SUB_BITS + 2) as u64) as usize;

    fn record(&self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.buckets[Self::bucket(micros)].fetch_add(1, Ordering::Relaxed);
        self.max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    fn take(&self) -> TakenLatencies {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.swap(0, Ordering::Relaxed))
            .collect();
        let max = self.max_micros.swap(0, Ordering::Relaxed);
        let count: u64 = counts.iter().sum();
        let percentile = |percent: u64| {
            // The nearest rank, as `LatencySummary::of` takes it.
            let rank = (count * percent).div_ceil(100);
            let mut seen = 0;
            let bucket = counts
                .iter()
                .position(|&in_bucket| {
                    seen += in_bucket;
                    seen >= rank
                })
                .expect("the rank is within the count");
            Duration::from_micros(Self::middle(bucket).min(max))
        };
        let summary = (count > 0).then(|| {
            LatencySummary::new(percentile(50), percentile(99), Duration::from_micros(max))
        });
        TakenLatencies { count, summary }
    }

    fn bucket(micros: u64) -> usize {
        if micros < Self::SUB {
            return micros as usize;
        }
        let micros = micros.min((1 << (Self::MAX_BITS + 1)) - 1);
        let log = micros.ilog2();
        let within = (micros >> (log - Self::SUB_BITS)) - Self::SUB;
        (Self::SUB * u64::from(log - Self::SUB_BITS + 1) + within) as usize
    }

    fn middle(bucket: usize) -> u64 {
        let bucket = bucket as u64;
        if bucket < Self::SUB {
            return bucket;
        }
        let (power, within) = (bucket / Self::SUB - 1, bucket % Self::SUB);
        ((Self::SUB + within) << power) + ((1 << power) >> 1)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: (0..Self::BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            max_micros: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_holds_its_latencies_within_three_percent() {
        for micros in (0..100_000).chain([1 << 30, (1 << 41) - 1]) {
            let middle = LatencyHistogram::middle(LatencyHistogram::bucket(micros));
            let off = middle.abs_diff(micros) as f64;
            assert!(off <= micros as f64 / 32.0, "{micros} µs read as {middle}");
        }
        assert_eq!(
            LatencyHistogram::bucket(u64::MAX),
            LatencyHistogram::BUCKETS - 1
        );
    }

    #[test]
    fn take_sums_up_and_empties() {
        let histogram = LatencyHistogram::default();
        for ms in 1..=100 {
            histogram.record(Duration::from_millis(ms));
        }
        let taken = histogram.take();
        assert_eq!(taken.count, 100);
        let summary = taken.summary.unwrap();
        let near =
            |latency: Duration, ms: f64| (latency.as_secs_f64() * 1e3 - ms).abs() <= ms / 32.0;
        assert!(near(summary.p50(), 50.0), "{summary}");
        assert!(near(summary.p99(), 99.0), "{summary}");
        assert_eq!(summary.max(), Duration::from_millis(100));
        let again = histogram.take();
        assert_eq!((again.count, again.summary), (0, None));
    }
}
