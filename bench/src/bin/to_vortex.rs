//! Writes a Parquet table as Vortex: each Parquet file of a table on a local
//! drive, one or a directory of them, as the Vortex file of its name,
//! `.vortex` for `.parquet`, in `--out`. A file's rows keep their order, read
//! and written by one partition, so its Vortex file is laid out as the
//! Parquet one is: the same row groups' worth of rows in the same order,
//! pruned alike. `--jobs` files are written at once. Strings Parquet keeps as
//! binaries, as ClickBench's are, become strings.
//!
//! ```text
//! cargo run --release -p t4-bench --bin to_vortex -- \
//!     --table /mnt/t4/0/data/tpch/lineitem/ \
//!     --out /mnt/t4/0/data/tpch-vortex/lineitem/
//! ```

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use vortex_datafusion::VortexFormatFactory;

/// The allocator: mimalloc unless built `--no-default-features`, as the
/// `mimalloc` feature in `bench/Cargo.toml` explains. A static, as Rust takes
/// a global allocator only so.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// A Parquet file, or a directory of them.
    #[arg(long)]
    table: PathBuf,
    /// The directory the Vortex files go to, made if missing.
    #[arg(long)]
    out: PathBuf,
    /// Files written at once; the cores unless given.
    #[arg(long)]
    jobs: Option<NonZeroUsize>,
}

/// One Parquet file to write as Vortex.
struct Conversion {
    parquet: PathBuf,
    vortex: PathBuf,
}

impl Conversion {
    /// Every Parquet file of `table`, a file or a directory, each to the
    /// Vortex file of its name in `out`.
    fn of_table(table: &Path, out: &Path) -> std::io::Result<Vec<Self>> {
        let files = if table.is_dir() {
            let mut files = Vec::new();
            for entry in std::fs::read_dir(table)? {
                let path = entry?.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
                {
                    files.push(path);
                }
            }
            files.sort();
            files
        } else {
            vec![table.to_owned()]
        };
        Ok(files
            .into_iter()
            .map(|parquet| {
                let name = Path::new(parquet.file_name().expect("a file has a name"));
                let vortex = out.join(name.with_extension("vortex"));
                Self { parquet, vortex }
            })
            .collect())
    }

    /// Reads the Parquet file in one partition and writes its rows, in
    /// order, as the Vortex file.
    async fn run(&self) -> Result<()> {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().execution.parquet.binary_as_string = true;
        let factory = Arc::new(VortexFormatFactory::new());
        let mut state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if let Some(formats) = state.file_formats() {
            formats.push(factory);
        }
        let context = SessionContext::new_with_state(state.build());
        let parquet = Self::sql_path(&self.parquet)?;
        context
            .register_parquet("source", &parquet, ParquetReadOptions::default())
            .await?;
        let vortex = Self::sql_path(&self.vortex)?;
        context
            .sql(&format!(
                "COPY (SELECT * FROM source) TO '{vortex}' STORED AS VORTEX"
            ))
            .await?
            .collect()
            .await?;
        Ok(())
    }

    /// `path` as a SQL string's contents: its quotes doubled.
    fn sql_path(path: &Path) -> Result<String> {
        let path = path.to_str().ok_or_else(|| {
            DataFusionError::Configuration(format!("{}: not UTF-8", path.display()))
        })?;
        Ok(path.replace('\'', "''"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;
    let conversions = Conversion::of_table(&args.table, &args.out)?;
    let jobs = args
        .jobs
        .or_else(|| std::thread::available_parallelism().ok())
        .map_or(1, NonZeroUsize::get);
    let (count, started) = (conversions.len(), Instant::now());
    // Each file on a task of its own, so that `jobs` decode and encode on as
    // many threads.
    stream::iter(conversions)
        .map(|conversion| {
            tokio::spawn(async move {
                conversion.run().await?;
                println!("{}", conversion.vortex.display());
                Ok::<_, DataFusionError>(())
            })
        })
        .buffer_unordered(jobs)
        .map(|joined| joined.map_err(|error| DataFusionError::External(error.into()))?)
        .try_collect::<()>()
        .await?;
    println!("{count} files in {:.1} s", started.elapsed().as_secs_f64());
    Ok(())
}
