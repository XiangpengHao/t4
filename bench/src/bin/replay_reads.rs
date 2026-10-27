//! Replays the calls a trace recorded (`run_queries --trace`) against a
//! table's store, with nothing between them: no decoding, the reads alone,
//! each query's tasks at once as DataFusion ran them. Writes each query's
//! time, bytes and call latencies as CSV, and prints each pass's latencies
//! by call size.
//!
//! A run makes `--passes` passes over the queries in one process, after the
//! trace's setup: t4's cache lives as long as the process, so the first
//! pass runs cold and the rest warm. `--copies` runs each query's tasks
//! that many times at once, for more reads in flight than DataFusion made.
//! `TOOL=replay bench/clickbench/run.sh` replays ClickBench every way the
//! benchmark compares.
//!
//! ```text
//! cargo run --release -p t4-bench --bin replay_reads -- \
//!     --trace trace.csv \
//!     --location gs://t4-bench-us-east4/clickbench/hits.parquet --passes 2 \
//!     --cache-policy disk --cache-path /mnt/t4/t4.cache \
//!     --cache-memory-mib 1024 --cache-disk-gib 32 \
//!     --drive-dir /mnt/t4 --out t4-disk.csv
//! ```

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use t4_bench::Error;
use t4_bench::host_counters::{CgroupMemory, HostCounters, HostProbe};
use t4_bench::replay::{CallSize, LatencySummary, QueryReplay, ReplayPlan, Replayer};
use t4_bench::request_trace::RecordedRequest;
use t4_bench::table_store::{CacheOptions, HttpOptions, TableLocation};

/// The allocator: mimalloc unless built `--no-default-features`, as the
/// `mimalloc` feature in `bench/Cargo.toml` explains. A static, as Rust takes
/// a global allocator only so.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// The trace to replay, as `run_queries --trace` wrote it.
    #[arg(long)]
    trace: PathBuf,
    /// The table to read: a Parquet file, or a directory of them, at a local
    /// path or a `gs://bucket/path` or `s3://bucket/path` URL, a
    /// directory's ending in `/`. The trace's files are read here: a file
    /// stands for the trace's one file, a directory holds files of the same
    /// names.
    #[arg(long, value_parser = TableLocation::parse)]
    location: TableLocation,
    #[command(flatten)]
    http: HttpOptions,
    #[command(flatten)]
    cache: CacheOptions,
    /// Passes over the queries, in one process: the first cold, the rest
    /// warm.
    #[arg(long, default_value_t = 1)]
    passes: usize,
    /// Each query's tasks run this many times at once.
    #[arg(long, default_value_t = NonZeroUsize::MIN)]
    copies: NonZeroUsize,
    /// Only these queries; all unless given.
    #[arg(long = "query")]
    only: Vec<usize>,
    /// Count the bytes read and written on the drive that holds this
    /// directory, summed over every one given (Linux only).
    #[arg(long = "drive-dir")]
    drive_dirs: Vec<PathBuf>,
    /// Where each query's time, bytes and latencies go, as CSV.
    #[arg(long)]
    out: PathBuf,
}

/// One query replayed in one pass, and what the drive and the network
/// moved meanwhile, and the cgroup's memory after.
struct QueryResult {
    pass: usize,
    query: usize,
    replay: QueryReplay,
    moved: Option<HostCounters>,
    memory: Option<CgroupMemory>,
}

impl QueryResult {
    fn latencies(&self) -> Option<LatencySummary> {
        LatencySummary::of(self.replay.calls().iter().map(|call| call.latency()))
    }

    fn write_csv(&self, out: &mut impl Write) -> std::io::Result<()> {
        let us = |latency: Duration| latency.as_micros().to_string();
        let latencies = self.latencies().map_or_else(
            || ",,".to_owned(),
            |summary| {
                format!(
                    "{},{},{}",
                    us(summary.p50()),
                    us(summary.p99()),
                    us(summary.max())
                )
            },
        );
        writeln!(
            out,
            "{},{},{:.3},{},{},{latencies},{},{}",
            self.pass,
            self.query,
            self.replay.elapsed().as_secs_f64() * 1e3,
            self.replay.calls().len(),
            self.replay.bytes(),
            HostCounters::csv_fields(self.moved.as_ref()),
            CgroupMemory::csv_fields(self.memory.as_ref())
        )
    }

    fn print(&self) {
        let latencies = self
            .latencies()
            .map_or(String::new(), |summary| format!(", calls {summary}"));
        let moved = self
            .moved
            .map_or(String::new(), |moved| format!(", {moved}"));
        let memory = self
            .memory
            .map_or(String::new(), |memory| format!(", {memory}"));
        println!(
            "p{} q{}: {:.1} ms, {} calls, {:.0} MiB{latencies}{moved}{memory}",
            self.pass,
            self.query,
            self.replay.elapsed().as_secs_f64() * 1e3,
            self.replay.calls().len(),
            self.replay.bytes() as f64 / f64::from(1 << 20),
        );
    }
}

/// A pass's queries summed up: time, bytes, and latencies by call size.
struct PassSummary<'a> {
    pass: usize,
    results: &'a [QueryResult],
}

impl PassSummary<'_> {
    fn print(&self) {
        let elapsed: Duration = self.results.iter().map(|r| r.replay.elapsed()).sum();
        let bytes: u64 = self.results.iter().map(|r| r.replay.bytes()).sum();
        let calls = || self.results.iter().flat_map(|r| r.replay.calls());
        let faults: u64 = self
            .results
            .iter()
            .filter_map(|r| r.moved.as_ref().map(HostCounters::minor_faults))
            .sum();
        println!(
            "pass {}: {:.2} s, {:.2} GiB, {:.2} GiB/s, {} calls, {faults} page faults",
            self.pass,
            elapsed.as_secs_f64(),
            bytes as f64 / f64::from(1 << 30),
            bytes as f64 / f64::from(1 << 30) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
            calls().count()
        );
        let largest = |of: fn(&CgroupMemory) -> u64| {
            self.results
                .iter()
                .filter_map(|r| r.memory.as_ref().map(of))
                .max()
        };
        if let (Some(page_cache), Some(anon)) = (
            largest(CgroupMemory::page_cache),
            largest(CgroupMemory::anon),
        ) {
            println!(
                "  cgroup memory after a query, at most: page cache {:.0} MiB, anon {:.0} MiB",
                page_cache as f64 / f64::from(1 << 20),
                anon as f64 / f64::from(1 << 20)
            );
        }
        for size in CallSize::ALL {
            let sized = calls().filter(|call| CallSize::of(call.bytes()) == size);
            let count = sized.clone().count();
            if let Some(summary) = LatencySummary::of(sized.map(|call| call.latency())) {
                println!("  {:>15}: {count:>6} calls, {summary}", size.label());
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let trace = RecordedRequest::read_csv(BufReader::new(File::open(&args.trace)?))?;
    let plan = ReplayPlan::new(&trace, &args.location)?;
    let http = args.http.start()?;
    // t4's own I/O goes untimed: this tool times the calls made of it.
    let store = args
        .cache
        .in_front_of(args.location.origin(&http)?, &Arc::default(), 0)?;
    let replayer = Replayer::new(store, args.copies);
    let probe = HostProbe::new(&args.drive_dirs)?;

    let setup = replayer.sequence(plan.setup()).await?;
    println!(
        "setup: {:.1} ms, {} calls",
        setup.elapsed().as_secs_f64() * 1e3,
        setup.calls().len()
    );
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(
        out,
        "pass,query,ms,calls,bytes,p50_us,p99_us,max_us,{},{}",
        HostCounters::CSV_COLUMNS,
        CgroupMemory::CSV_COLUMNS
    )?;
    for pass in 0..args.passes {
        let mut results = Vec::new();
        for query in plan.queries() {
            if !args.only.is_empty() && !args.only.contains(&query.query()) {
                continue;
            }
            let before = probe.sample();
            let replay = replayer.query(query).await?;
            let moved = probe
                .sample()
                .zip(before)
                .map(|(after, before)| after.since(&before));
            let result = QueryResult {
                pass,
                query: query.query(),
                replay,
                moved,
                memory: probe.cgroup_memory(),
            };
            result.print();
            result.write_csv(&mut out)?;
            results.push(result);
        }
        PassSummary {
            pass,
            results: &results,
        }
        .print();
    }
    out.flush()?;
    Ok(())
}
