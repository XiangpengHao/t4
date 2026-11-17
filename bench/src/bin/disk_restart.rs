//! Times a restart of t4's disk tier: the cache dropped, appending its last adds, and its log
//! replayed and rebased as the next builds (doc/13-persistence.md). A last build with the log
//! removed is the cold baseline. A partial batch stays in memory.
//!
//! ```text
//! cargo build --release -p t4-bench --bin disk_restart
//! target/release/disk_restart --dir /mnt/t4/0 --blocks 20480 --out restart.csv
//! ```

use std::fs::File;
use std::io::{BufWriter, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use t4::T4Caching;
use t4::policy::{Admit, FixedPlacementPolicy};
use t4_bench::Error;
use t4_bench::request_cost::{CountedStore, RequestCounter};
use t4_bench::synthetic_objects::SyntheticObjects;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
struct Args {
    /// A directory a cache file, each on its own drive; the log goes beside the first.
    #[arg(long, required = true)]
    dir: Vec<PathBuf>,
    /// The 1 MiB blocks filled toward disk: 20 GiB unless set.
    #[arg(long, default_value_t = 20 << 10)]
    blocks: u64,
    /// Each file's blocks. The log keeps a file version's path and e_tag once for all of them.
    #[arg(long, default_value_t = 64)]
    file_blocks: u64,
    /// Restarts timed.
    #[arg(long, default_value_t = 5)]
    restarts: u64,
    /// Reads in flight while filling.
    #[arg(long, default_value_t = 64)]
    in_flight: u64,
    /// The CSV to write, a row a build.
    #[arg(long)]
    out: PathBuf,
    /// The run's name in the CSV.
    #[arg(long, default_value = "t4")]
    label: String,
    /// The memory tier: Spiral's user-machine profile runs one 1 MiB chunk.
    #[arg(long, default_value_t = 64)]
    memory_mib: u64,
}

struct RestartBench {
    cache_files: Vec<PathBuf>,
    file_capacity: u64,
    blocks: u64,
    file_blocks: u64,
    paths: Arc<Vec<Path>>,
    objects: Arc<SyntheticObjects>,
    gets: Arc<RequestCounter>,
    store: Arc<dyn ObjectStore>,
    memory: u64,
}

impl RestartBench {
    const BLOCK: u64 = 1 << 20;

    fn new(args: &Args) -> Self {
        let files = args.blocks.div_ceil(args.file_blocks);
        // Spiral names a file by a SHA-256 in hex: paths about as long as its.
        let paths: Vec<Path> = (0..files)
            .map(|file| Path::from(format!("disk_restart/{file:064x}.vortex")))
            .collect();
        let sizes = (0..files).map(|file| {
            let blocks = args.file_blocks.min(args.blocks - file * args.file_blocks);
            (paths[file as usize].clone(), blocks * Self::BLOCK)
        });
        let objects = Arc::new(SyntheticObjects::new(sizes));
        let gets = Arc::new(RequestCounter::default());
        let store = Arc::new(CountedStore::new(
            Arc::clone(&objects) as Arc<dyn ObjectStore>,
            Arc::clone(&gets),
        ));
        // Room to spare, so that no block is evicted.
        let tier_blocks = args.blocks + args.blocks / 8 + 64;
        let cache_files = args.dir.iter().map(|dir| dir.join("t4-restart.cache"));
        Self {
            cache_files: cache_files.collect(),
            file_capacity: tier_blocks.div_ceil(args.dir.len() as u64) * Self::BLOCK,
            blocks: args.blocks,
            file_blocks: args.file_blocks,
            paths: Arc::new(paths),
            objects,
            gets,
            store,
            memory: args.memory_mib << 20,
        }
    }

    fn log_path(&self) -> PathBuf {
        let mut path = self.cache_files[0].clone().into_os_string();
        path.push(".log");
        path.into()
    }

    /// Only files this bench names: a run starts cold and leaves the drives as it found them.
    fn remove_files(&self) -> Result<(), Error> {
        let log = self.log_path();
        for path in self.cache_files.iter().chain([&log]) {
            match std::fs::remove_file(path) {
                Err(error) if error.kind() != ErrorKind::NotFound => return Err(error.into()),
                _ => {}
            }
        }
        Ok(())
    }

    fn build(&self) -> Result<T4Caching, Error> {
        let mut builder = T4Caching::builder(Arc::clone(&self.store))
            .with_memory_capacity(self.memory)
            .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk));
        for path in &self.cache_files {
            builder = builder.with_disk_file(path, self.file_capacity);
        }
        Ok(builder.build()?)
    }

    fn timed_build(&self) -> Result<(T4Caching, Duration), Error> {
        let start = Instant::now();
        let cache = self.build()?;
        Ok((cache, start.elapsed()))
    }

    fn block_range(&self, block: u64) -> (Path, std::ops::Range<u64>) {
        let file = block / self.file_blocks;
        let start = (block % self.file_blocks) * Self::BLOCK;
        (
            self.paths[file as usize].clone(),
            start..start + Self::BLOCK,
        )
    }

    /// Reads return in memory; disk writes finish in the background.
    async fn fill(self: &Arc<Self>, cache: &T4Caching, in_flight: u64) -> Result<Duration, Error> {
        let start = Instant::now();
        let next = Arc::new(AtomicU64::new(0));
        let workers: Vec<_> = (0..in_flight)
            .map(|_| {
                let (bench, cache, next) = (Arc::clone(self), cache.clone(), Arc::clone(&next));
                tokio::spawn(async move {
                    loop {
                        let block = next.fetch_add(1, Ordering::Relaxed);
                        if block >= bench.blocks {
                            return Ok::<_, Error>(());
                        }
                        let (path, range) = bench.block_range(block);
                        cache.get_range(&path, range).await?;
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.await??;
        }
        Ok(start.elapsed())
    }

    /// Read every file's first and last block, checking bytes and counting refetches.
    async fn check_taken_back(&self, cache: &T4Caching) -> Result<u64, Error> {
        let gets_before = self.gets.counts().gets();
        for first in (0..self.blocks).step_by(self.file_blocks as usize) {
            let last = (first + self.file_blocks).min(self.blocks) - 1;
            for block in [first, last] {
                let (path, range) = self.block_range(block);
                let read = cache.get_range(&path, range.clone()).await?;
                if read != self.objects.bytes(&path, range) {
                    return Err(format!("block {block} came back wrong").into());
                }
            }
        }
        Ok(self.gets.counts().gets() - gets_before)
    }

    fn log_bytes(&self) -> u64 {
        std::fs::metadata(self.log_path()).map_or(0, |meta| meta.len())
    }
}

/// One build of the cache, and how the one before it ended.
struct RestartRow {
    restart: String,
    log_bytes: u64,
    drop: Option<Duration>,
    build: Duration,
    gets_after_build: u64,
}

impl RestartRow {
    const CSV_COLUMNS: &str = "label,restart,blocks,file_blocks,cache_files,\
                               log_bytes,drop_ms,build_ms,gets_after_build";

    fn csv(&self, label: &str, bench: &RestartBench) -> String {
        let millis = |time: Option<Duration>| {
            time.map_or(String::new(), |time| {
                format!("{:.3}", time.as_secs_f64() * 1e3)
            })
        };
        format!(
            "{label},{},{},{},{},{},{},{},{}",
            self.restart,
            bench.blocks,
            bench.file_blocks,
            bench.cache_files.len(),
            self.log_bytes,
            millis(self.drop),
            millis(Some(self.build)),
            self.gets_after_build,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let bench = Arc::new(RestartBench::new(&args));
    bench.remove_files()?;
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(out, "{}", RestartRow::CSV_COLUMNS)?;
    let mut write_row = |row: RestartRow| -> Result<(), Error> {
        let line = row.csv(&args.label, &bench);
        eprintln!("{line}");
        writeln!(out, "{line}")?;
        Ok(out.flush()?)
    };

    let (mut cache, cold_build) = bench.timed_build()?;
    let fill = bench.fill(&cache, args.in_flight).await?;
    let fill_gets = bench.gets.counts().gets();
    eprintln!(
        "filled {} blocks in {:.1} s, {fill_gets} GETs",
        bench.blocks,
        fill.as_secs_f64()
    );
    write_row(RestartRow {
        restart: "first".to_owned(),
        log_bytes: 0,
        drop: None,
        build: cold_build,
        gets_after_build: 0,
    })?;

    for restart in 1..=args.restarts {
        let start = Instant::now();
        drop(cache);
        let dropped = start.elapsed();
        let log_bytes = bench.log_bytes();
        let build;
        (cache, build) = bench.timed_build()?;
        let gets_after_build = bench.check_taken_back(&cache).await?;
        write_row(RestartRow {
            restart: restart.to_string(),
            log_bytes,
            drop: Some(dropped),
            build,
            gets_after_build,
        })?;
    }

    drop(cache);
    std::fs::remove_file(bench.log_path())?;
    let (cache, build) = bench.timed_build()?;
    let gets_before = bench.gets.counts().gets();
    let (path, range) = bench.block_range(0);
    cache.get_range(&path, range).await?;
    write_row(RestartRow {
        restart: "no-log".to_owned(),
        log_bytes: 0,
        drop: None,
        build,
        gets_after_build: bench.gets.counts().gets() - gets_before,
    })?;
    drop(cache);
    bench.remove_files()
}
