//! [`TimedReads`]: a read policy over another that times every I/O t4
//! makes, as t4 reports it (`doc/06-read-modes.md`, decision 9), in
//! [`IoLatencies`]: a GET's first byte and its last, a disk read's end.
//! A benchmark takes them once a query is done.
//!
//! The times are t4's: a GET's last byte is when its body has landed in the
//! read's output and in the chunks its blocks go to, the evictions it made
//! for room included (a block bound for disk is written after); a disk
//! read's end is when its bytes are in the read's output, or in memory for a
//! block it moves there.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use t4::policy::{Io, IoEvent, IoPhase, LocatedRead, ReadPlan, ReadPolicy};

use crate::replay::LatencySummary;

/// A read policy that plans as `plans` does, and times every I/O in
/// [`IoLatencies`].
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

/// How long the I/Os t4 made took, since they were last taken.
#[derive(Debug, Default)]
pub struct IoLatencies {
    get_first_byte: LatencyHistogram,
    get_done: LatencyHistogram,
    disk_read_done: LatencyHistogram,
    /// Given up on before they ended, as a race's losers are.
    cancelled: AtomicU64,
    failed: AtomicU64,
}

impl IoLatencies {
    /// The latencies since they were last taken, and every count back to 0.
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

/// The I/O latencies of a stretch of time, as [`IoLatencies::take`] took
/// them.
#[derive(Clone, Copy, Debug)]
pub struct TakenIoLatencies {
    get_first_byte: TakenLatencies,
    get_done: TakenLatencies,
    disk_read_done: TakenLatencies,
    cancelled: u64,
    failed: u64,
}

impl TakenIoLatencies {
    /// The CSV columns [`Self::csv_fields`] fills: for a GET's first byte,
    /// its last, and a disk read's end, how many and their p50, p99 and
    /// largest, in ms; and the I/Os given up on and failed.
    pub const CSV_COLUMNS: &str = "get_first_byte_count,get_first_byte_p50_ms,\
                                   get_first_byte_p99_ms,get_first_byte_max_ms,\
                                   get_count,get_p50_ms,get_p99_ms,get_max_ms,\
                                   disk_read_count,disk_read_p50_ms,disk_read_p99_ms,\
                                   disk_read_max_ms,io_cancelled,io_failed";

    /// `taken` as the fields of [`Self::CSV_COLUMNS`], empty where nothing
    /// was taken, as when t4 isn't read through.
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

/// The latencies one histogram took: how many, and their summary if any.
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

/// Latencies, counted in buckets a sixteenth of a power of two wide, so a
/// percentile read from them is within 3% of the latency it stands for; the
/// largest kept exactly. Many threads record at once, without a lock: the
/// read path it times pays an atomic add or two.
#[derive(Debug)]
struct LatencyHistogram {
    /// How many latencies fell in each bucket, in microseconds: one a value
    /// below [`Self::SUB`], then [`Self::SUB`] a power of two.
    buckets: Box<[AtomicU64]>,
    max_micros: AtomicU64,
}

impl LatencyHistogram {
    /// A power of two splits into 2^`SUB_BITS` buckets.
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

    /// The latencies recorded since the last take, and the histogram
    /// emptied.
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

    /// The bucket that counts a latency of `micros`.
    fn bucket(micros: u64) -> usize {
        if micros < Self::SUB {
            return micros as usize;
        }
        let micros = micros.min((1 << (Self::MAX_BITS + 1)) - 1);
        let log = micros.ilog2();
        let within = (micros >> (log - Self::SUB_BITS)) - Self::SUB;
        (Self::SUB * u64::from(log - Self::SUB_BITS + 1) + within) as usize
    }

    /// The middle of the latencies `bucket` counts, in microseconds.
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
