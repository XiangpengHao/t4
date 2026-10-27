//! Fetches a table's files from their store in ranges, many at once, as
//! fast as it can, straight from the store or through t4 reading every
//! block from the remote: how fast those come in, how long a fetch takes
//! (p50, p99, max),
//! and the CPU time a GiB costs, for each HTTP transport (`--http`).
//!
//! The files are cut into `--range-mib` pieces, ordered by a hash of each
//! (the same order every run), and fetched `--in-flight` at a time until
//! `--gib` are in. Nothing is fetched twice, so t4 reads every block from
//! the remote: its
//! memory tier fills, then evicts. The table must hold `--gib`.
//!
//! ```text
//! cargo run --release -p t4-bench --bin fetch_ranges -- \
//!     --location s3://t4-bench-xiangpeng-use1/clickbench/copies/ --http ktls \
//!     --in-flight 128 --range-mib 8 --gib 64 \
//!     --cache-policy memory --cache-memory-mib 32768 \
//!     --cache-path /mnt/t4/0/t4.cache --cache-disk-gib 1
//! ```

use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use t4_bench::Error;
use t4_bench::host_counters::HostProbe;
use t4_bench::replay::LatencySummary;
use t4_bench::table_store::{CacheOptions, HttpOptions, TableLocation};

/// The allocator: mimalloc unless built `--no-default-features`, as the
/// `mimalloc` feature in `bench/Cargo.toml` explains. A static, as Rust takes
/// a global allocator only so.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// The table: a file, or a directory of files, a URL like
    /// `s3://bucket/path` or `gs://bucket/path`, a directory's ending in `/`.
    #[arg(long, value_parser = TableLocation::parse)]
    location: TableLocation,
    #[command(flatten)]
    http: HttpOptions,
    /// Fetches in flight at once.
    #[arg(long, default_value_t = 64)]
    in_flight: usize,
    /// Each fetch's bytes, in MiB.
    #[arg(long, default_value_t = 8)]
    range_mib: u64,
    /// The GiB to fetch in all.
    #[arg(long, default_value_t = 32)]
    gib: u64,
    #[command(flatten)]
    cache: CacheOptions,
}

/// The ranges to fetch, in the order they're fetched.
struct FetchPlan {
    ranges: Vec<(Path, Range<u64>)>,
}

impl FetchPlan {
    /// `location`'s files cut into ranges of `range_bytes`, in the order of
    /// a hash of each, enough of them for `total_bytes`.
    async fn new(
        origin: &dyn ObjectStore,
        location: &TableLocation,
        range_bytes: u64,
        total_bytes: u64,
    ) -> Result<Self, Error> {
        let path = location.path()?;
        let files = if location.url().path().ends_with('/') {
            origin.list(Some(&path)).try_collect::<Vec<_>>().await?
        } else {
            vec![origin.head(&path).await?]
        };
        let mut ranges: Vec<(Path, Range<u64>)> = files
            .iter()
            .flat_map(|file| {
                (0..file.size)
                    .step_by(usize::try_from(range_bytes).expect("a range's size fits a usize"))
                    .map(|start| {
                        (
                            file.location.clone(),
                            start..(start + range_bytes).min(file.size),
                        )
                    })
            })
            .collect();
        ranges.sort_by_cached_key(|(path, range)| {
            let mut hasher = DefaultHasher::new();
            (path.as_ref(), range.start).hash(&mut hasher);
            hasher.finish()
        });
        let mut taken = 0;
        let count = ranges
            .iter()
            .position(|(_, range)| {
                taken += range.end - range.start;
                taken >= total_bytes
            })
            .ok_or_else(|| {
                format!(
                    "{}: holds {:.1} GiB, less than asked",
                    location.url(),
                    taken as f64 / f64::from(1 << 30)
                )
            })?;
        ranges.truncate(count + 1);
        Ok(Self { ranges })
    }

    fn bytes(&self) -> u64 {
        self.ranges
            .iter()
            .map(|(_, range)| range.end - range.start)
            .sum()
    }

    /// Fetches every range from `store`, `in_flight` at a time; how long
    /// each took.
    async fn run(
        self,
        store: Arc<dyn ObjectStore>,
        in_flight: usize,
    ) -> Result<Vec<Duration>, Error> {
        let ranges = Arc::new(self.ranges);
        let next = Arc::new(AtomicUsize::new(0));
        let fetchers = (0..in_flight).map(|_| {
            let (store, ranges, next) =
                (Arc::clone(&store), Arc::clone(&ranges), Arc::clone(&next));
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                while let Some((path, range)) = ranges.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let started = Instant::now();
                    let bytes = store.get_range(path, range.clone()).await?;
                    latencies.push(started.elapsed());
                    if bytes.len() as u64 != range.end - range.start {
                        return Err(Error::from(format!(
                            "{path} {range:?}: fetched {} bytes",
                            bytes.len()
                        )));
                    }
                }
                Ok(latencies)
            })
        });
        let mut latencies = Vec::new();
        for fetcher in futures_util::future::join_all(fetchers).await {
            latencies.extend(fetcher??);
        }
        Ok(latencies)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let http = args.http.start()?;
    let origin = args.location.origin(&http)?;
    let plan = FetchPlan::new(
        origin.as_ref(),
        &args.location,
        args.range_mib << 20,
        args.gib << 30,
    )
    .await?;
    let bytes = plan.bytes();
    // t4's own I/O goes untimed: this tool times the fetches made of it.
    let store = args.cache.in_front_of(origin, &Arc::default())?;
    let probe = HostProbe::new(&[])?;

    let before = probe.sample();
    let started = Instant::now();
    let latencies = plan.run(store, args.in_flight).await?;
    let elapsed = started.elapsed().as_secs_f64();
    let moved = probe
        .sample()
        .zip(before)
        .map(|(after, before)| after.since(&before));

    let gib = bytes as f64 / f64::from(1 << 30);
    let through = if args.cache.is_set() {
        "through t4"
    } else {
        "direct"
    };
    print!(
        "{:?} {through}, {} in flight, {} MiB ranges: {gib:.1} GiB in {elapsed:.2} s, {:.2} GiB/s",
        args.http,
        args.in_flight,
        args.range_mib,
        gib / elapsed
    );
    if let Some(summary) = LatencySummary::of(latencies) {
        print!("; fetch {summary}");
    }
    if let Some(moved) = moved {
        let seconds = |ms: u64| ms as f64 / 1e3;
        print!(
            "; network {:.1} GiB; {} page faults; process CPU {:.1} s user + {:.1} s kernel; \
             machine CPU {:.1} s ({:.1} s softirq), {:.2} CPU-s/GiB",
            moved.network_received() as f64 / f64::from(1 << 30),
            moved.minor_faults(),
            seconds(moved.user_cpu_ms()),
            seconds(moved.system_cpu_ms()),
            seconds(moved.machine_busy_ms()),
            seconds(moved.machine_softirq_ms()),
            seconds(moved.machine_busy_ms()) / gib
        );
    }
    println!();
    Ok(())
}
