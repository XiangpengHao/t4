//! Runs SQL queries in DataFusion over Parquet tables, on a local drive or
//! in GCS, read directly or through t4, and writes each query's time and
//! the bytes the drive and the network moved meanwhile as CSV.
//!
//! A run makes `--passes` passes over the queries in one process: t4's
//! cache lives as long as the process, so the first pass runs cold and the
//! rest warm. `--trace` records every call made of the tables' stores, for
//! `replay_reads`. `bench/clickbench/run.sh` runs ClickBench every way the
//! benchmark compares.
//!
//! ```text
//! cargo run --release -p t4-bench --bin run_queries -- \
//!     --table hits=gs://t4-bench-us-east4/clickbench/hits.parquet \
//!     --queries bench/clickbench/queries.sql --passes 2 \
//!     --cache-policy disk --cache-path /mnt/t4/t4.cache \
//!     --cache-memory-mib 1024 --cache-disk-gib 32 \
//!     --drive-dir /mnt/t4 --out t4-disk.csv
//! ```

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use t4_bench::host_counters::{CgroupMemory, HostCounters, HostProbe};
use t4_bench::request_trace::{RequestTrace, TracedStore};
use t4_bench::table_store::{CacheOptions, TableLocation};

#[derive(Parser)]
struct Args {
    /// A table, `name=location`: a Parquet file, or a directory of them, at
    /// a local path or a `gs://bucket/path` URL, a directory's ending in `/`.
    #[arg(long = "table", value_parser = TableArg::parse, required = true)]
    tables: Vec<TableArg>,
    /// The queries, one a line; query `n` is line `n`, from 0.
    #[arg(long)]
    queries: PathBuf,
    /// Only these queries; all unless given.
    #[arg(long = "query")]
    only: Vec<usize>,
    /// Passes over the queries, in one process: the first cold, the rest
    /// warm.
    #[arg(long, default_value_t = 1)]
    passes: usize,
    /// Evaluate filters while decoding, DataFusion's late materialization.
    #[arg(long)]
    pushdown_filters: bool,
    /// DataFusion's target partitions; its default, the cores, unless given.
    #[arg(long)]
    partitions: Option<usize>,
    #[command(flatten)]
    cache: CacheOptions,
    /// Count the bytes read and written on the drive that holds this
    /// directory (Linux only).
    #[arg(long)]
    drive_dir: Option<PathBuf>,
    /// Record every call made of the tables' stores, as CSV, here.
    #[arg(long)]
    trace: Option<PathBuf>,
    /// Where each query's time and bytes go, as CSV.
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

/// A DataFusion session whose tables are read through each one's store:
/// its origin, then t4 if asked, then a trace if asked.
struct QuerySession {
    context: SessionContext,
    trace: Option<Arc<RequestTrace>>,
    probe: HostProbe,
}

impl QuerySession {
    async fn new(args: &Args) -> Result<Self> {
        let mut config = SessionConfig::new();
        let options = config.options_mut();
        // ClickBench's strings are Parquet binaries.
        options.execution.parquet.binary_as_string = true;
        options.execution.parquet.pushdown_filters = args.pushdown_filters;
        if let Some(partitions) = args.partitions {
            options.execution.target_partitions = partitions;
        }
        let context = SessionContext::new_with_config(config);
        let trace = args.trace.as_ref().map(|_| Arc::new(RequestTrace::new()));

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
            let mut store = args
                .cache
                .in_front_of(location.origin()?)
                .map_err(DataFusionError::External)?;
            if let Some(trace) = &trace {
                store = Arc::new(TracedStore::new(store, Arc::clone(trace)));
            }
            context.register_object_store(&url, store);
        }
        for table in &args.tables {
            context
                .register_parquet(
                    &table.name,
                    table.location.url().as_str(),
                    ParquetReadOptions::default(),
                )
                .await?;
        }
        let probe = HostProbe::new(args.drive_dir.as_deref())?;
        Ok(Self {
            context,
            trace,
            probe,
        })
    }

    /// Runs `sql`, query `query` of pass `pass`.
    async fn run(&self, pass: usize, query: usize, sql: &str) -> Result<QueryRun> {
        if let Some(trace) = &self.trace {
            trace.begin_query(&format!("p{pass}q{query}"));
        }
        let before = self.probe.sample();
        let start = Instant::now();
        let batches = self.context.sql(sql).await?.collect().await?;
        let elapsed = start.elapsed();
        let moved = self
            .probe
            .sample()
            .zip(before)
            .map(|(after, before)| after.since(&before));
        Ok(QueryRun {
            pass,
            query,
            elapsed,
            rows: batches.iter().map(|batch| batch.num_rows()).sum(),
            moved,
            memory: self.probe.cgroup_memory(),
        })
    }
}

/// One query's run: its time, its rows, and the bytes the drive and the
/// network moved meanwhile, and the cgroup's memory after, where they are
/// counted.
struct QueryRun {
    pass: usize,
    query: usize,
    elapsed: Duration,
    rows: usize,
    moved: Option<HostCounters>,
    memory: Option<CgroupMemory>,
}

impl QueryRun {
    fn write_csv(&self, out: &mut impl Write) -> std::io::Result<()> {
        writeln!(
            out,
            "{},{},{:.3},{},{},{}",
            self.pass,
            self.query,
            self.elapsed.as_secs_f64() * 1e3,
            self.rows,
            HostCounters::csv_fields(self.moved.as_ref()),
            CgroupMemory::csv_fields(self.memory.as_ref())
        )
    }

    fn print(&self) {
        let moved = self
            .moved
            .map_or(String::new(), |moved| format!(", {moved}"));
        let memory = self
            .memory
            .map_or(String::new(), |memory| format!(", {memory}"));
        println!(
            "p{} q{}: {:.1} ms, {} rows{moved}{memory}",
            self.pass,
            self.query,
            self.elapsed.as_secs_f64() * 1e3,
            self.rows
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let queries = std::fs::read_to_string(&args.queries)?;
    let session = QuerySession::new(&args).await?;
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(
        out,
        "pass,query,ms,rows,{},{}",
        HostCounters::CSV_COLUMNS,
        CgroupMemory::CSV_COLUMNS
    )?;
    for pass in 0..args.passes {
        for (query, sql) in queries.lines().enumerate() {
            if args.only.is_empty() || args.only.contains(&query) {
                let run = session.run(pass, query, sql).await?;
                run.print();
                run.write_csv(&mut out)?;
            }
        }
    }
    out.flush()?;
    if let (Some(trace), Some(path)) = (&session.trace, &args.trace) {
        trace.write_csv(BufWriter::new(File::create(path)?))?;
        println!("{} requests to {}", trace.len(), path.display());
    }
    Ok(())
}
