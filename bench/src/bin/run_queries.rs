//! DataFusion keeps the footers that setup reads, so a cold query run reads only data.
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
use std::num::{NonZeroU8, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use t4_bench::host_counters::{CgroupMemory, HostCounters, HostProbe, NicCounters, NicProbe};
use t4_bench::io_latency::{IoLatencies, TakenIoLatencies};
use t4_bench::replay::LatencySummary;
use t4_bench::request_cost::{CountedStore, RequestCounter, RequestCounts, RequestPrices};
use t4_bench::request_trace::{RequestTrace, TracedStore};
use t4_bench::table_store::{CacheOptions, HttpOptions, StoreHttp, TableLocation};
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// A table, `name=location`: a file, or a directory ending in `/`; a path or a URL.
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
    /// Passes over the queries in a session, or runs of a query with `--cold-runs`; first cold.
    #[arg(long, default_value_t = 1)]
    passes: usize,
    /// Each query in this many sessions of its own, each starting cold.
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
    /// The machine's price per hour, in dollars, to price each step's time.
    #[arg(long)]
    dollars_per_hour: Option<f64>,
    /// Count the bytes moved by the drives holding these directories (Linux only).
    #[arg(long = "drive-dir")]
    drive_dirs: Vec<PathBuf>,
    /// Record every call made of the tables' stores, as CSV, here.
    #[arg(long)]
    trace: Option<PathBuf>,
    /// Record each GET t4 makes, in phases, as CSV, here.
    #[arg(long)]
    get_phases: Option<PathBuf>,
    /// Where each step's time, requests and bytes go, as CSV.
    #[arg(long)]
    out: PathBuf,
}

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

#[derive(Clone, Copy, Default, ValueEnum)]
enum TableFormat {
    #[default]
    Parquet,
    Vortex,
}

struct QuerySession {
    context: SessionContext,
    vortex: VortexSession,
}

impl QuerySession {
    fn new(
        number: usize,
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
                .in_front_of(origin, latencies, number)
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
                    let format = VortexFormat::new(self.vortex.clone());
                    let options = ListingOptions::new(Arc::new(format));
                    self.context
                        .register_listing_table(&table.name, url, options, None, None)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn run(&self, sql: &str) -> Result<usize> {
        let batches = self.context.sql(sql).await?.collect().await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }
}

struct Meters {
    probe: HostProbe,
    nic: NicProbe,
    requests: Arc<RequestCounter>,
    latencies: Arc<IoLatencies>,
    cached: bool,
}

impl Meters {
    async fn measure<T>(&self, step: impl Future<Output = Result<T>>) -> Result<(T, Measured)> {
        // ethtool runs outside the CPU counters' window, so its CPU time stays out of the step's.
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

struct Measured {
    elapsed: Duration,
    moved: Option<HostCounters>,
    nic: Option<NicCounters>,
    memory: Option<CgroupMemory>,
    requests: RequestCounts,
    io: Option<TakenIoLatencies>,
}

#[derive(Clone, Copy)]
enum Step {
    Setup,
    Query { pass: usize, query: usize },
}

struct StepRow {
    run: usize,
    footer_race: Option<NonZeroU8>,
    step: Step,
    rows: Option<usize>,
    measured: Measured,
}

impl StepRow {
    fn csv_header() -> String {
        format!(
            "pass,query,run,footer_race,step,ms,rows,{},{},{},machine_dollars,{},{}",
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
            "{pass},{query},{},{},{step},{:.3},{},{},{},{},{machine_dollars},{},{}",
            self.run,
            self.footer_race
                .map_or(String::new(), |race| race.to_string()),
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
        let race = self
            .footer_race
            .map_or(String::new(), |race| format!(" footer race {race}"));
        let io = self
            .measured
            .io
            .map_or(String::new(), |io| format!("; t4's {io}"));
        println!(
            "r{}{race} {step}: {:.1} ms{rows}, {}{io}",
            self.run,
            self.measured.elapsed.as_secs_f64() * 1e3,
            self.measured.requests
        );
    }
}

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

    fn chosen(&self) -> Vec<usize> {
        (0..self.queries.len())
            .filter(|query| self.args.only.is_empty() || self.args.only.contains(query))
            .collect()
    }

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

    fn new_trace(&self) -> Option<Arc<RequestTrace>> {
        self.args
            .trace
            .as_ref()
            .map(|_| Arc::new(RequestTrace::new()))
    }

    fn begin_step(&self, trace: Option<&RequestTrace>, step: &str) {
        if let Some(trace) = trace {
            trace.begin_query(step);
        }
        self.meters.latencies.begin_step(step);
    }

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

    async fn start_session(
        &mut self,
        run: usize,
        trace: Option<&Arc<RequestTrace>>,
    ) -> Result<QuerySession> {
        let meters = &self.meters;
        let session = QuerySession::new(
            run,
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
            footer_race: self.args.cache.footer_race(run),
            step: Step::Setup,
            rows: None,
            measured,
        })?;
        Ok(session)
    }

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
            footer_race: self.args.cache.footer_race(run),
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
