//! Runs SQL queries in DataFusion over Parquet or Vortex tables, on a local
//! drive, in GCS or in S3, read directly or through t4. Writes, for each
//! query run and each session's setup, its time, the requests its stores
//! were sent and what they cost, t4's I/O latencies, and the bytes the drive
//! and the network moved meanwhile, as CSV.
//!
//! It runs one of two ways:
//!
//! - **Passes,** the default: one session makes `--passes` passes over the
//!   queries. t4's cache lives as long as the session, so the first pass
//!   runs cold and the rest warm, each query warmed by those before it too.
//!   Its trace (`--trace`) is what `replay_reads` replays.
//!   `bench/clickbench/run.sh` runs ClickBench every way that benchmark
//!   compares.
//! - **Cold runs** (`--cold-runs n`): each query in `n` sessions of its own,
//!   one after another, each starting with t4's cache and DataFusion's
//!   empty and running the query `--passes` times, the first cold. So each
//!   query's cold time, its spread over the runs, and its cold time less its
//!   warm, what it waits on the remote at most, are measured alike.
//!   `bench/end_to_end/run.sh` runs these.
//!
//! Either way, `--trace` records every call made of the tables' stores,
//! under the step it served: `setup` and `p{pass}q{query}` for passes,
//! `r{run}q{query}setup` and `r{run}p{pass}q{query}` for cold runs; and
//! `--get-phases` each GET t4 makes, in phases, under the same steps.
//!
//! A session's setup registers the tables: DataFusion lists their files and
//! reads their footers, which it keeps for the session, so a query's cold
//! run reads the remote for data alone. The requests are counted as the
//! tables' stores are sent them: under t4, when it is read through, so they
//! are the GETs its read policy plans, and their cost is t4's.
//!
//! ```text
//! cargo run --release -p t4-bench --bin run_queries -- \
//!     --table hits=s3://t4-bench-xiangpeng-use1/clickbench/partitioned/ \
//!     --queries bench/clickbench/queries.sql --cold-runs 5 --passes 2 \
//!     --cache-policy memory --cache-path /mnt/t4/0/t4.cache \
//!     --cache-memory-mib 131072 --cache-disk-gib 1 \
//!     --dollars-per-hour 4.47 --out t4.csv
//! ```

use std::collections::BTreeMap;
use std::fs::File;
use std::future::Future;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use t4_bench::host_counters::{CgroupMemory, HostCounters, HostProbe, NicCounters, NicProbe};
use t4_bench::io_latency::{IoLatencies, TakenIoLatencies};
use t4_bench::raw_vortex_reads::RawVortexFormat;
use t4_bench::replay::LatencySummary;
use t4_bench::request_cost::{CountedStore, RequestCounter, RequestCounts, RequestPrices};
use t4_bench::request_trace::{RequestTrace, TracedStore};
use t4_bench::table_store::{CacheOptions, HttpOptions, StoreHttp, TableLocation};
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

/// The allocator: mimalloc unless built `--no-default-features`, as the
/// `mimalloc` feature in `bench/Cargo.toml` explains. A static, as Rust takes
/// a global allocator only so.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// A table, `name=location`: a file, or a directory of them, at a local
    /// path or a `gs://bucket/path` or `s3://bucket/path` URL, a
    /// directory's ending in `/`.
    #[arg(long = "table", value_parser = TableArg::parse, required = true)]
    tables: Vec<TableArg>,
    /// The tables' file format.
    #[arg(long, value_enum, default_value_t)]
    format: TableFormat,
    #[command(flatten)]
    http: HttpOptions,
    /// The queries, one a line; query `n` is line `n`, from 0.
    #[arg(long)]
    queries: PathBuf,
    /// Only these queries; all unless given.
    #[arg(long = "query")]
    only: Vec<usize>,
    /// Passes over the queries in a session; with `--cold-runs`, runs of a
    /// query in each of its sessions. The first cold, the rest warm.
    #[arg(long, default_value_t = 1)]
    passes: usize,
    /// Each query in this many sessions of its own, each starting cold;
    /// unless given, one session for every query.
    #[arg(long)]
    cold_runs: Option<NonZeroUsize>,
    /// Evaluate filters while decoding, DataFusion's late materialization.
    #[arg(long)]
    pushdown_filters: bool,
    /// DataFusion's target partitions; its default, the cores, unless given.
    #[arg(long)]
    partitions: Option<usize>,
    #[command(flatten)]
    cache: CacheOptions,
    /// What the machine costs an hour, in dollars: each step's time priced
    /// at it, beside its requests.
    #[arg(long)]
    dollars_per_hour: Option<f64>,
    /// Count the bytes read and written on the drive that holds this
    /// directory, summed over every one given (Linux only).
    #[arg(long = "drive-dir")]
    drive_dirs: Vec<PathBuf>,
    /// Record every call made of the tables' stores, as CSV, here.
    #[arg(long)]
    trace: Option<PathBuf>,
    /// Record each GET t4 makes in phases, as CSV, here: its first byte, its
    /// body in, the part of that spent landing its pieces, and its blocks
    /// cached.
    #[arg(long)]
    get_phases: Option<PathBuf>,
    /// Where each step's time, requests and bytes go, as CSV.
    #[arg(long)]
    out: PathBuf,
}

/// A table: its name, and where its files are.
#[derive(Clone)]
struct TableArg {
    name: String,
    location: TableLocation,
}

impl TableArg {
    fn parse(arg: &str) -> std::result::Result<Self, String> {
        let (name, location) = arg
            .split_once('=')
            .ok_or_else(|| format!("expected name=location, got {arg}"))?;
        Ok(Self {
            name: name.to_owned(),
            location: TableLocation::parse(location)?,
        })
    }
}

/// How a table's files are written.
#[derive(Clone, Copy, Default, ValueEnum)]
enum TableFormat {
    #[default]
    Parquet,
    Vortex,
}

/// A DataFusion session whose tables are read through each one's store: its
/// origin, counted, then t4 if asked, then a trace if asked. Each session
/// has a t4 cache of its own, which starts cold.
struct QuerySession {
    context: SessionContext,
    vortex: VortexSession,
}

impl QuerySession {
    fn new(
        args: &Args,
        http: &StoreHttp,
        requests: &Arc<RequestCounter>,
        latencies: &Arc<IoLatencies>,
        trace: Option<&Arc<RequestTrace>>,
    ) -> Result<Self> {
        let mut config = SessionConfig::new();
        let options = config.options_mut();
        // ClickBench's strings are Parquet binaries.
        options.execution.parquet.binary_as_string = true;
        options.execution.parquet.pushdown_filters = args.pushdown_filters;
        if let Some(partitions) = args.partitions {
            options.execution.target_partitions = partitions;
        }
        let context = SessionContext::new_with_config(config);

        // One store for each bucket, or the local file system, that the
        // tables are in.
        let mut stores = BTreeMap::new();
        for table in &args.tables {
            stores
                .entry(table.location.store_url())
                .or_insert(&table.location);
        }
        if args.cache.is_set() && stores.len() > 1 {
            return Err(DataFusionError::Configuration(
                "t4 caches one store: put every table in one bucket".into(),
            ));
        }
        for (url, location) in stores {
            let origin = Arc::new(CountedStore::new(
                location.origin(http)?,
                Arc::clone(requests),
            ));
            let mut store = args
                .cache
                .in_front_of(origin, latencies)
                .map_err(DataFusionError::External)?;
            if let Some(trace) = trace {
                store = Arc::new(TracedStore::new(store, Arc::clone(trace)));
            }
            context.register_object_store(&url, store);
        }
        Ok(Self {
            context,
            vortex: VortexSession::default(),
        })
    }

    /// Registers every table, as `format` writes them.
    async fn register_tables(&self, tables: &[TableArg], format: TableFormat) -> Result<()> {
        for table in tables {
            let url = table.location.url().as_str();
            match format {
                TableFormat::Parquet => {
                    self.context
                        .register_parquet(&table.name, url, ParquetReadOptions::default())
                        .await?;
                }
                TableFormat::Vortex => {
                    let store = self
                        .context
                        .runtime_env()
                        .object_store(ObjectStoreUrl::parse(table.location.store_url())?)?;
                    let format = RawVortexFormat::new(self.vortex.clone(), store);
                    let options = ListingOptions::new(Arc::new(format));
                    self.context
                        .register_listing_table(&table.name, url, options, None, None)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Runs `sql`, returning the rows it answered with.
    async fn run(&self, sql: &str) -> Result<usize> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }
}

/// What the benchmark measures each step by, shared by its sessions: the
/// host's counters, the requests the stores are sent, and t4's I/O.
struct Meters {
    probe: HostProbe,
    nic: NicProbe,
    requests: Arc<RequestCounter>,
    latencies: Arc<IoLatencies>,
    /// Whether t4 is read through, so that its I/O is timed.
    cached: bool,
}

impl Meters {
    /// Measures `step` as it runs.
    async fn measure<T>(&self, step: impl Future<Output = Result<T>>) -> Result<(T, Measured)> {
        // ethtool runs before the CPU counters are read, and after: its own
        // CPU time stays out of the step's.
        let nic_before = self.nic.sample();
        let before = (self.probe.sample(), self.requests.counts());
        self.latencies.take();
        let start = Instant::now();
        let done = step.await?;
        let elapsed = start.elapsed();
        let moved = self
            .probe
            .sample()
            .zip(before.0)
            .map(|(after, before)| after.since(&before));
        let nic = self
            .nic
            .sample()
            .zip(nic_before)
            .map(|(after, before)| after.since(&before));
        let measured = Measured {
            elapsed,
            moved,
            nic,
            memory: self.probe.cgroup_memory(),
            requests: self.requests.counts().since(&before.1),
            io: self.cached.then(|| self.latencies.take()),
        };
        Ok((done, measured))
    }
}

/// A step's time, the bytes the drive and the network moved meanwhile, the
/// cgroup's memory after, where they are counted, the requests the stores
/// were sent, and t4's I/O latencies, if it is read through.
struct Measured {
    elapsed: Duration,
    moved: Option<HostCounters>,
    nic: Option<NicCounters>,
    memory: Option<CgroupMemory>,
    requests: RequestCounts,
    io: Option<TakenIoLatencies>,
}

/// What a step of a session is.
#[derive(Clone, Copy)]
enum Step {
    /// The tables registered.
    Setup,
    /// Query `query`'s run in pass `pass`.
    Query { pass: usize, query: usize },
}

/// One step of a session's, measured: a row of the CSV.
struct StepRow {
    /// The session's number: with `--cold-runs`, the run of its query.
    run: usize,
    step: Step,
    rows: Option<usize>,
    measured: Measured,
}

impl StepRow {
    fn csv_header() -> String {
        format!(
            "pass,query,run,step,ms,rows,{},{},{},machine_dollars,{},{}",
            HostCounters::CSV_COLUMNS,
            CgroupMemory::CSV_COLUMNS,
            RequestCounts::CSV_COLUMNS,
            TakenIoLatencies::CSV_COLUMNS,
            NicCounters::CSV_COLUMNS
        )
    }

    fn write_csv(
        &self,
        out: &mut impl Write,
        dollars_per_hour: Option<f64>,
    ) -> std::io::Result<()> {
        let (pass, query, step) = match self.step {
            Step::Setup => (String::new(), String::new(), "setup"),
            Step::Query { pass, query } => (pass.to_string(), query.to_string(), "query"),
        };
        let machine_dollars = dollars_per_hour.map_or(String::new(), |dollars| {
            format!(
                "{:.9}",
                self.measured.elapsed.as_secs_f64() / 3600.0 * dollars
            )
        });
        writeln!(
            out,
            "{pass},{query},{},{step},{:.3},{},{},{},{},{machine_dollars},{},{}",
            self.run,
            self.measured.elapsed.as_secs_f64() * 1e3,
            self.rows.map_or(String::new(), |rows| rows.to_string()),
            HostCounters::csv_fields(self.measured.moved.as_ref()),
            CgroupMemory::csv_fields(self.measured.memory.as_ref()),
            self.measured.requests.csv_fields(&RequestPrices::STANDARD),
            TakenIoLatencies::csv_fields(self.measured.io.as_ref()),
            NicCounters::csv_fields(self.measured.nic.as_ref())
        )
    }

    fn print(&self) {
        let step = match self.step {
            Step::Setup => "setup".to_owned(),
            Step::Query { pass, query } => format!("q{query} p{pass}"),
        };
        let rows = self
            .rows
            .map_or(String::new(), |rows| format!(", {rows} rows"));
        let io = self
            .measured
            .io
            .map_or(String::new(), |io| format!("; t4's {io}"));
        println!(
            "r{} {step}: {:.1} ms{rows}, {}{io}",
            self.run,
            self.measured.elapsed.as_secs_f64() * 1e3,
            self.measured.requests
        );
    }
}

/// The benchmark: its arguments, the HTTP its stores share, the queries,
/// its meters, and the CSV.
struct Benchmark {
    args: Args,
    http: StoreHttp,
    queries: Vec<String>,
    meters: Meters,
    out: BufWriter<File>,
}

impl Benchmark {
    fn new(args: Args) -> Result<Self> {
        let queries = std::fs::read_to_string(&args.queries)?
            .lines()
            .map(str::to_owned)
            .collect();
        let meters = Meters {
            probe: HostProbe::new(&args.drive_dirs)?,
            nic: NicProbe::new(),
            requests: Arc::default(),
            latencies: Arc::new(match args.get_phases {
                Some(_) => IoLatencies::keeping_gets(),
                None => IoLatencies::default(),
            }),
            cached: args.cache.is_set(),
        };
        let mut out = BufWriter::new(File::create(&args.out)?);
        writeln!(out, "{}", StepRow::csv_header())?;
        Ok(Self {
            http: args.http.start()?,
            args,
            queries,
            meters,
            out,
        })
    }

    /// The numbers of the queries to run.
    fn chosen(&self) -> Vec<usize> {
        (0..self.queries.len())
            .filter(|query| self.args.only.is_empty() || self.args.only.contains(query))
            .collect()
    }

    /// One session, `--passes` passes over the chosen queries; every call
    /// made of the tables' stores recorded, if `--trace` asks.
    async fn run_passes(&mut self) -> Result<()> {
        let trace = self.new_trace();
        let session = self.start_session(0, trace.as_ref()).await?;
        for pass in 0..self.args.passes {
            for query in self.chosen() {
                self.begin_step(trace.as_deref(), &format!("p{pass}q{query}"));
                self.run_query(&session, 0, pass, query).await?;
            }
        }
        self.write_records(trace.as_deref())
    }

    /// Each chosen query in `runs` sessions of its own, `--passes` runs of
    /// it in each; and its cold and warm times summed up. Every call made
    /// of the tables' stores recorded, if `--trace` asks.
    async fn run_cold(&mut self, runs: NonZeroUsize) -> Result<()> {
        let trace = self.new_trace();
        for query in self.chosen() {
            let (mut cold, mut warm) = (Vec::new(), Vec::new());
            for run in 0..runs.get() {
                self.begin_step(trace.as_deref(), &format!("r{run}q{query}setup"));
                let session = self.start_session(run, trace.as_ref()).await?;
                for pass in 0..self.args.passes {
                    self.begin_step(trace.as_deref(), &format!("r{run}p{pass}q{query}"));
                    let elapsed = self.run_query(&session, run, pass, query).await?;
                    let times = if pass == 0 { &mut cold } else { &mut warm };
                    times.push(elapsed);
                }
            }
            Self::print_spread(query, &cold, &warm);
        }
        self.write_records(trace.as_deref())
    }

    /// A trace to record the stores' calls in, if `--trace` asks for one.
    fn new_trace(&self) -> Option<Arc<RequestTrace>> {
        self.args
            .trace
            .as_ref()
            .map(|_| Arc::new(RequestTrace::new()))
    }

    /// Records what follows under `step`: the calls in `trace`, and t4's
    /// GETs, if kept.
    fn begin_step(&self, trace: Option<&RequestTrace>, step: &str) {
        if let Some(trace) = trace {
            trace.begin_query(step);
        }
        self.meters.latencies.begin_step(step);
    }

    /// Writes `trace` where `--trace` says, and t4's GETs where
    /// `--get-phases` says.
    fn write_records(&self, trace: Option<&RequestTrace>) -> Result<()> {
        if let (Some(trace), Some(path)) = (trace, &self.args.trace) {
            trace.write_csv(BufWriter::new(File::create(path)?))?;
            println!("{} requests to {}", trace.len(), path.display());
        }
        if let Some(path) = &self.args.get_phases {
            self.meters
                .latencies
                .write_gets_csv(BufWriter::new(File::create(path)?))?;
            println!("t4's GETs to {}", path.display());
        }
        Ok(())
    }

    /// A new session, its tables registered: its setup measured, as run
    /// `run`'s.
    async fn start_session(
        &mut self,
        run: usize,
        trace: Option<&Arc<RequestTrace>>,
    ) -> Result<QuerySession> {
        let meters = &self.meters;
        let session = QuerySession::new(
            &self.args,
            &self.http,
            &meters.requests,
            &meters.latencies,
            trace,
        )?;
        let ((), measured) = meters
            .measure(session.register_tables(&self.args.tables, self.args.format))
            .await?;
        self.write(StepRow {
            run,
            step: Step::Setup,
            rows: None,
            measured,
        })?;
        Ok(session)
    }

    /// Runs `query` in `session`, as pass `pass` of run `run`; returns its
    /// time.
    async fn run_query(
        &mut self,
        session: &QuerySession,
        run: usize,
        pass: usize,
        query: usize,
    ) -> Result<Duration> {
        let (rows, measured) = self
            .meters
            .measure(session.run(&self.queries[query]))
            .await?;
        let elapsed = measured.elapsed;
        self.write(StepRow {
            run,
            step: Step::Query { pass, query },
            rows: Some(rows),
            measured,
        })?;
        Ok(elapsed)
    }

    fn write(&mut self, row: StepRow) -> Result<()> {
        row.print();
        row.write_csv(&mut self.out, self.args.dollars_per_hour)?;
        Ok(())
    }

    /// Prints query `query`'s cold times over its runs, and its warm ones.
    fn print_spread(query: usize, cold: &[Duration], warm: &[Duration]) {
        let ms = |latency: Duration| latency.as_secs_f64() * 1e3;
        let Some(cold_summary) = LatencySummary::of(cold.iter().copied()) else {
            return;
        };
        let fastest = cold.iter().min().copied().unwrap_or_default();
        let warm = LatencySummary::of(warm.iter().copied()).map_or(String::new(), |warm| {
            format!(
                "; warm p50 {:.1} ms, so {:.1} ms waiting on the remote at most",
                ms(warm.p50()),
                ms(cold_summary.p50().saturating_sub(warm.p50()))
            )
        });
        println!(
            "q{query}, {} cold runs: min {:.1} ms, p50 {:.1} ms, max {:.1} ms{warm}",
            cold.len(),
            ms(fastest),
            ms(cold_summary.p50()),
            ms(cold_summary.max())
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cold_runs = args.cold_runs;
    let mut benchmark = Benchmark::new(args)?;
    match cold_runs {
        Some(runs) => benchmark.run_cold(runs).await?,
        None => benchmark.run_passes().await?,
    }
    benchmark.out.flush()?;
    Ok(())
}
