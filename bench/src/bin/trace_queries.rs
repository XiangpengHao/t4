//! Runs SQL queries in DataFusion over local Parquet files, read through a
//! [`TracedStore`], and writes every read they made as CSV: the workload t4
//! serves when DataFusion reads through it.
//!
//! ```text
//! cargo run --release -p t4-bench --bin trace_queries -- \
//!     --table hits=/data/hits.parquet --queries bench/clickbench/queries.sql \
//!     --out clickbench.csv
//! ```

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use object_store::local::LocalFileSystem;
use t4_bench::request_trace::{RequestTrace, TracedStore};
use url::Url;

#[derive(Parser)]
struct Args {
    /// A table, `name=path`: a Parquet file, or a directory of them.
    #[arg(long = "table", value_parser = TableArg::parse, required = true)]
    tables: Vec<TableArg>,
    /// The queries, one a line; query `n` is line `n`, from 0.
    #[arg(long)]
    queries: PathBuf,
    /// Only these queries; all unless given.
    #[arg(long = "query")]
    only: Vec<usize>,
    /// Evaluate filters while decoding, DataFusion's late materialization.
    #[arg(long)]
    pushdown_filters: bool,
    /// DataFusion's target partitions; its default, the cores, unless given.
    #[arg(long)]
    partitions: Option<usize>,
    /// Where the trace goes.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Clone)]
struct TableArg {
    name: String,
    path: PathBuf,
}

impl TableArg {
    fn parse(arg: &str) -> std::result::Result<Self, String> {
        let (name, path) = arg
            .split_once('=')
            .ok_or_else(|| format!("expected name=path, got {arg}"))?;
        let path = std::fs::canonicalize(path).map_err(|error| format!("{path}: {error}"))?;
        Ok(Self {
            name: name.to_owned(),
            path,
        })
    }
}

/// A DataFusion session whose tables are read through a [`TracedStore`].
struct TracedSession {
    context: SessionContext,
    trace: Arc<RequestTrace>,
}

impl TracedSession {
    /// The URL the traced store serves the local file system at.
    const STORE_URL: &str = "traced://local";

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
        let trace = Arc::new(RequestTrace::new());
        let store = TracedStore::new(Arc::new(LocalFileSystem::new()), Arc::clone(&trace));
        let url =
            Url::parse(Self::STORE_URL).map_err(|error| DataFusionError::External(error.into()))?;
        context.register_object_store(&url, Arc::new(store));
        for table in &args.tables {
            let location = format!("{}{}", Self::STORE_URL, table.path.display());
            context
                .register_parquet(&table.name, &location, ParquetReadOptions::default())
                .await?;
        }
        Ok(Self { context, trace })
    }

    /// Runs `sql` as query `label`, its reads recorded under it, and prints
    /// what it took.
    async fn run(&self, label: &str, sql: &str) -> Result<()> {
        self.trace.begin_query(label);
        let requests_before = self.trace.len();
        let start = Instant::now();
        let batches = self.context.sql(sql).await?.collect().await?;
        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        println!(
            "{label}: {:.1} ms, {} rows, {} requests",
            start.elapsed().as_secs_f64() * 1e3,
            rows,
            self.trace.len() - requests_before,
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let queries = std::fs::read_to_string(&args.queries)?;
    let session = TracedSession::new(&args).await?;
    for (index, sql) in queries.lines().enumerate() {
        if args.only.is_empty() || args.only.contains(&index) {
            session.run(&format!("q{index}"), sql).await?;
        }
    }
    session
        .trace
        .write_csv(BufWriter::new(File::create(&args.out)?))?;
    println!("{} requests to {}", session.trace.len(), args.out.display());
    Ok(())
}
