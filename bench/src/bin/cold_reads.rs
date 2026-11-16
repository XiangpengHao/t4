//! Reads 1 MiB blocks cold from one disk cache: t4, foyer (as shipped or
//! tuned) or Kismet as spiraldb runs it, each on the same drives
//! (`t4_bench::disk_caches`).
//!
//! It fills the cache with `--blocks` blocks, lets go of what memory holds,
//! and then makes `--passes` passes, each reading every block once in a new
//! random order, `--in-flight` reads at once, the page cache dropped before
//! it. A pass is a CSV row: its time and throughput, the CPU the process and
//! the machine spent, the bytes the drives read, the GETs that reached the
//! source (none, unless the cache missed), its misses and wrong bytes, and
//! its read latencies, slowest first.
//!
//! Dropping the page cache takes root:
//!
//! ```text
//! cargo build --release -p t4-bench --bin cold_reads
//! sudo target/release/cold_reads --cache t4 --dir /mnt/raid/cold --out t4.csv
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use t4_bench::Error;
use t4_bench::disk_caches::{Blocks, DiskCache, DiskCacheKind};
use t4_bench::host_counters::{HostCounters, HostProbe};
use t4_bench::request_cost::{CountedStore, RequestCounter};

/// The allocator: mimalloc unless built `--no-default-features`, as the
/// `mimalloc` feature in `bench/Cargo.toml` explains. A static, as Rust takes
/// a global allocator only so.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// The cache to read.
    #[arg(long, value_enum)]
    cache: DiskCacheKind,
    /// The directory the cache keeps its files in, on the drives measured.
    #[arg(long)]
    dir: PathBuf,
    /// The 1 MiB blocks the cache holds and each pass reads: 128 GiB unless set.
    #[arg(long, default_value_t = 128 << 10)]
    blocks: u64,
    /// Reads in flight at once.
    #[arg(long, default_value_t = 128)]
    in_flight: usize,
    /// The I/Os an io_uring engine keeps in flight, t4's and tuned foyer's;
    /// each's default (64) unless set.
    #[arg(long)]
    io_depth: Option<u32>,
    /// Cold passes over every block.
    #[arg(long, default_value_t = 5)]
    passes: u64,
    /// Seeds each pass's read order.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Leave the page cache alone between passes, for a run without root
    /// (a smoke test): the passes are then not cold for foyer and Kismet.
    #[arg(long)]
    keep_page_cache: bool,
    /// The CSV to write, a row a pass.
    #[arg(long)]
    out: PathBuf,
    /// The variant's name in the CSV: the cache's, unless set (as when a run
    /// differs by more than its flags, such as a memory cap around it).
    #[arg(long)]
    label: Option<String>,
}

/// The read requests the drives under a directory completed, from
/// `/proc/diskstats`, summed over the drives under it (an md RAID's members,
/// else the device itself): how many, their bytes, and their time queued, so
/// a pass shows its mean request size and how many requests were in flight.
struct DriveRequests {
    devices: Vec<String>,
}

/// What [`DriveRequests`] counted, so far or over a pass.
#[derive(Clone, Copy)]
struct DriveSample {
    requests: u64,
    bytes: u64,
    /// Milliseconds requests spent queued or in service, summed over
    /// requests: over a span of wall time, the mean in flight.
    queued_ms: u64,
}

impl DriveRequests {
    fn new(dir: &std::path::Path) -> Result<Self, Error> {
        use std::os::unix::fs::MetadataExt;
        let dev = std::fs::metadata(dir)?.dev();
        // Linux's encoding of a device number.
        let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
        let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
        let name = std::fs::read_to_string("/proc/diskstats")
            .unwrap_or_default()
            .lines()
            .find_map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                let number = |at: usize| fields.get(at)?.parse::<u64>().ok();
                (number(0)? == major && number(1)? == minor).then(|| fields[2].to_owned())
            });
        let devices = name.map_or_else(Vec::new, |name| {
            let members: Vec<String> = std::fs::read_dir(format!("/sys/block/{name}/slaves"))
                .map(|entries| {
                    entries
                        .filter_map(|entry| {
                            Some(entry.ok()?.file_name().to_string_lossy().into_owned())
                        })
                        .collect()
                })
                .unwrap_or_default();
            if members.is_empty() {
                vec![name]
            } else {
                members
            }
        });
        Ok(Self { devices })
    }

    /// The counts so far, or `None` without `/proc/diskstats` (not Linux).
    fn sample(&self) -> Option<DriveSample> {
        if self.devices.is_empty() {
            return None;
        }
        let stats = std::fs::read_to_string("/proc/diskstats").ok()?;
        let mut sum = DriveSample {
            requests: 0,
            bytes: 0,
            queued_ms: 0,
        };
        for line in stats.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() > 13 && self.devices.iter().any(|device| device == fields[2]) {
                let number = |at: usize| fields[at].parse::<u64>().unwrap_or(0);
                sum.requests += number(3);
                sum.bytes += number(5) * 512;
                sum.queued_ms += number(13);
            }
        }
        Some(sum)
    }
}

impl DriveSample {
    fn since(&self, earlier: &Self) -> Self {
        Self {
            requests: self.requests - earlier.requests,
            bytes: self.bytes - earlier.bytes,
            queued_ms: self.queued_ms - earlier.queued_ms,
        }
    }
}

/// The page cache, emptied before each pass so that a read through it
/// starts cold.
struct PageCache;

impl PageCache {
    /// Writes dirty pages out, then drops every clean one. Takes root.
    fn drop_all() -> Result<(), Error> {
        std::process::Command::new("sync").status()?;
        std::fs::write("/proc/sys/vm/drop_caches", "3")
            .map_err(|error| format!("dropping the page cache (run as root): {error}"))?;
        Ok(())
    }
}

/// The order a pass reads the blocks in: each once, shuffled.
struct ReadOrder;

impl ReadOrder {
    /// `count` block indices in a random order fixed by `seed`: a
    /// Fisher-Yates shuffle driven by xorshift64*.
    fn shuffled(count: u64, seed: u64) -> Vec<u64> {
        let mut order: Vec<u64> = (0..count).collect();
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for i in (1..order.len()).rev() {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let pick = state.wrapping_mul(0x2545_F491_4F6C_DD1D) % (i as u64 + 1);
            order.swap(i, usize::try_from(pick).expect("an index fits a usize"));
        }
        order
    }
}

/// What one pass saw.
struct PassReport {
    pass: u64,
    elapsed: Duration,
    bytes: u64,
    moved: Option<HostCounters>,
    /// The drives' requests over the pass.
    requests: Option<DriveSample>,
    source_gets: u64,
    misses: u64,
    wrong: u64,
    /// Each read's latency in microseconds, sorted.
    latencies_us: Vec<u64>,
}

impl PassReport {
    const CSV_COLUMNS: &str = "variant,cache,pass,blocks,in_flight,io_depth,seconds,gb_per_s,source_gets,misses,wrong,\
                               p50_us,p99_us,max_us,over_10ms,over_100ms,drive_requests,mean_request_kib,mean_in_flight";

    fn percentile(&self, fraction: f64) -> u64 {
        let last = self.latencies_us.len().saturating_sub(1);
        self.latencies_us
            .get((last as f64 * fraction).round() as usize)
            .copied()
            .unwrap_or(0)
    }

    fn over(&self, micros: u64) -> usize {
        self.latencies_us.len()
            - self
                .latencies_us
                .partition_point(|&latency| latency <= micros)
    }

    fn gb_per_second(&self) -> f64 {
        self.bytes as f64 / 1e9 / self.elapsed.as_secs_f64()
    }

    fn mean_request_kib(&self) -> Option<f64> {
        let requests = self.requests?;
        (requests.requests > 0).then(|| requests.bytes as f64 / 1024.0 / requests.requests as f64)
    }

    /// The drives' requests in flight on average over the pass, all drives
    /// together.
    fn mean_in_flight(&self) -> Option<f64> {
        Some(self.requests?.queued_ms as f64 / (self.elapsed.as_secs_f64() * 1e3))
    }

    fn csv_row(
        &self,
        variant: &str,
        cache: DiskCacheKind,
        in_flight: usize,
        io_depth: Option<u32>,
    ) -> String {
        let io_depth = io_depth.map_or(String::new(), |depth| depth.to_string());
        let (requests, mean_kib, drive_in_flight) = match (
            self.requests,
            self.mean_request_kib(),
            self.mean_in_flight(),
        ) {
            (Some(sample), Some(kib), Some(queued)) => (
                sample.requests.to_string(),
                format!("{kib:.0}"),
                format!("{queued:.0}"),
            ),
            _ => (String::new(), String::new(), String::new()),
        };
        format!(
            "{variant},{cache:?},{},{},{in_flight},{io_depth},{:.3},{:.2},{},{},{},{},{},{},{},{},{requests},{mean_kib},{drive_in_flight},{}",
            self.pass,
            self.latencies_us.len(),
            self.elapsed.as_secs_f64(),
            self.gb_per_second(),
            self.source_gets,
            self.misses,
            self.wrong,
            self.percentile(0.5),
            self.percentile(0.99),
            self.percentile(1.0),
            self.over(10_000),
            self.over(100_000),
            HostCounters::csv_fields(self.moved.as_ref()),
        )
    }

    fn summary(&self) -> String {
        let cpu = self.moved.as_ref().map_or(String::from("-"), |moved| {
            let seconds = (moved.user_cpu_ms() + moved.system_cpu_ms()) as f64 / 1e3;
            format!(
                "{seconds:.1} CPU s ({:.3} s/GB), drives read {:.1} GB",
                seconds / (self.bytes as f64 / 1e9),
                moved.drive_read() as f64 / 1e9
            )
        });
        let requests = self
            .mean_request_kib()
            .zip(self.mean_in_flight())
            .map_or(String::new(), |(kib, in_flight)| {
                format!(", {kib:.0} KiB a drive request, {in_flight:.0} in flight")
            });
        format!(
            "pass {}: {:.2} GB/s in {:.1} s; p50 {} us, p99 {} us, max {} us; {cpu}{requests}; source GETs {}, misses {}, wrong {}",
            self.pass,
            self.gb_per_second(),
            self.elapsed.as_secs_f64(),
            self.percentile(0.5),
            self.percentile(0.99),
            self.percentile(1.0),
            self.source_gets,
            self.misses,
            self.wrong,
        )
    }
}

/// A cache filled with [`Blocks`] and read cold.
struct ColdReads {
    cache: Arc<DiskCache>,
    blocks: Arc<Blocks>,
    in_flight: usize,
}

impl ColdReads {
    /// Puts every block in the cache: foyer one at a time, so its flushers
    /// keep up (`FoyerBlocks::fill`), the others `in_flight` at once.
    async fn fill(&self) -> Result<Duration, Error> {
        let at_once = if matches!(*self.cache, DiskCache::Foyer(_)) {
            1
        } else {
            self.in_flight
        };
        let order = Arc::new((0..self.blocks.count()).collect::<Vec<_>>());
        let start = Instant::now();
        self.each_block(order, at_once, |cache, blocks, index| async move {
            cache.fill(index, &blocks).await.map(|()| (0, false, false))
        })
        .await?;
        Ok(start.elapsed())
    }

    /// Reads every block once in `order`: each read's latency, and whether
    /// it missed or its bytes were wrong. The bytes are checked where every
    /// block's start, middle and end are, and in full for every 97th block,
    /// so that checking costs little next to the read.
    async fn pass(&self, order: Arc<Vec<u64>>) -> Result<(Vec<u64>, u64, u64), Error> {
        let results = self
            .each_block(order, self.in_flight, |cache, blocks, index| async move {
                let start = Instant::now();
                let read = cache.read(index).await?;
                let latency = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
                Ok(match read {
                    None => (latency, true, false),
                    Some(bytes) => (latency, false, !Self::bytes_match(&blocks, index, &bytes)),
                })
            })
            .await?;
        let misses = results.iter().filter(|(_, missed, _)| *missed).count() as u64;
        let wrong = results.iter().filter(|(_, _, wrong)| *wrong).count() as u64;
        let mut latencies: Vec<u64> = results.into_iter().map(|(latency, _, _)| latency).collect();
        latencies.sort_unstable();
        Ok((latencies, misses, wrong))
    }

    fn bytes_match(blocks: &Blocks, index: u64, bytes: &[u8]) -> bool {
        let expected = blocks.bytes(index);
        if bytes.len() != expected.len() {
            return false;
        }
        if index.is_multiple_of(97) {
            return bytes == &expected[..];
        }
        let len = bytes.len();
        [0, len / 2, len - 64]
            .iter()
            .all(|&at| bytes[at..at + 64] == expected[at..at + 64])
    }

    /// Runs `visit` on every block of `order`, `at_once` at a time, and
    /// collects what each returned.
    async fn each_block<F, Fut>(
        &self,
        order: Arc<Vec<u64>>,
        at_once: usize,
        visit: F,
    ) -> Result<Vec<(u64, bool, bool)>, Error>
    where
        F: Fn(Arc<DiskCache>, Arc<Blocks>, u64) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(u64, bool, bool), Error>> + Send,
    {
        let next = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..at_once)
            .map(|_| {
                let (cache, blocks, order, next, visit) = (
                    Arc::clone(&self.cache),
                    Arc::clone(&self.blocks),
                    Arc::clone(&order),
                    Arc::clone(&next),
                    visit.clone(),
                );
                tokio::spawn(async move {
                    let mut seen = Vec::new();
                    while let Some(&index) = order.get(next.fetch_add(1, Ordering::Relaxed)) {
                        seen.push(visit(Arc::clone(&cache), Arc::clone(&blocks), index).await?);
                    }
                    Ok::<_, Error>(seen)
                })
            })
            .collect();
        let mut all = Vec::with_capacity(order.len());
        for worker in workers {
            all.extend(worker.await??);
        }
        Ok(all)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.dir)?;
    let blocks = Arc::new(Blocks::new(args.blocks));
    let source = Arc::new(RequestCounter::default());
    let counted = Arc::new(CountedStore::new(blocks.store(), Arc::clone(&source)));

    let cache = DiskCache::build(args.cache, &args.dir, &blocks, counted, args.io_depth).await?;
    let mut reads = ColdReads {
        cache: Arc::new(cache),
        blocks: Arc::clone(&blocks),
        in_flight: args.in_flight,
    };
    let fill = reads.fill().await?;
    eprintln!(
        "{:?}: filled {} blocks in {:.1} s ({:.2} GB/s)",
        args.cache,
        args.blocks,
        fill.as_secs_f64(),
        (args.blocks * Blocks::SIZE) as f64 / 1e9 / fill.as_secs_f64()
    );
    let cache = Arc::into_inner(reads.cache).expect("no read holds the cache between passes");
    reads.cache = Arc::new(cache.settled().await?);

    let probe = HostProbe::new(std::slice::from_ref(&args.dir))?;
    let drive_requests = DriveRequests::new(&args.dir)?;
    let variant = args
        .label
        .clone()
        .unwrap_or_else(|| format!("{:?}", args.cache));
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(
        out,
        "{},{}",
        PassReport::CSV_COLUMNS,
        HostCounters::CSV_COLUMNS
    )?;
    for pass in 0..args.passes {
        if !args.keep_page_cache && !reads.cache.reads_memory() {
            PageCache::drop_all()?;
        }
        let order = Arc::new(ReadOrder::shuffled(
            args.blocks,
            args.seed.wrapping_add(pass),
        ));
        let (gets_before, counters_before) = (source.counts().gets(), probe.sample());
        let requests_before = drive_requests.sample();
        let start = Instant::now();
        let (latencies_us, misses, wrong) = reads.pass(order).await?;
        let elapsed = start.elapsed();
        let requests = drive_requests
            .sample()
            .zip(requests_before)
            .map(|(after, before)| after.since(&before));
        let moved = probe
            .sample()
            .zip(counters_before)
            .map(|(after, before)| after.since(&before));
        let report = PassReport {
            pass,
            elapsed,
            bytes: (latencies_us.len() as u64 - misses) * Blocks::SIZE,
            moved,
            requests,
            source_gets: source.counts().gets() - gets_before,
            misses,
            wrong,
            latencies_us,
        };
        eprintln!("{variant} {}", report.summary());
        writeln!(
            out,
            "{}",
            report.csv_row(&variant, args.cache, args.in_flight, args.io_depth)
        )?;
        out.flush()?;
    }
    Ok(())
}
