//! Times a restart of t4's disk tier (doc/13-persistence.md): the log finished as the cache
//! shuts down, and replayed and rebased as the next builds. A last build with the log removed
//! is the cold baseline. `--kill-at` aborts the fill as a kill under load; `--after-kill`
//! builds on what it left. Reading every block back counts the blocks the log kept.
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
use t4_bench::simulated_store::{NetworkModel, SimulatedStore};
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
    /// Paces the store as a network of this many MB/s in all, 100 MB/s a GET and 30 ms to a
    /// GET's first byte. Instant unless set: then fills outrun any drive.
    #[arg(long)]
    link_mb_per_s: Option<u64>,
    /// After the first restart, read every block back, not each file's first and last.
    #[arg(long)]
    read_all: bool,
    /// Abort once the fill has started this many blocks, as a process killed under load.
    #[arg(long)]
    kill_at: Option<u64>,
    /// Build on the files a killed run left, time it, and read every block back.
    #[arg(long)]
    after_kill: bool,
    /// The CSV to write, a row a build.
    #[arg(long)]
    out: PathBuf,
    /// The run's name in the CSV.
    #[arg(long, default_value = "t4")]
    label: String,
}

struct RestartBench {
    cache_files: Vec<PathBuf>,
    file_capacity: u64,
    blocks: u64,
    file_blocks: u64,
    kill_at: Option<u64>,
    paths: Arc<Vec<Path>>,
    objects: Arc<SyntheticObjects>,
    gets: Arc<RequestCounter>,
    store: Arc<dyn ObjectStore>,
}

impl RestartBench {
    const BLOCK: u64 = 1 << 20;
    /// Disk admissions wait in memory until their batch goes.
    const MEMORY: u64 = 64 << 20;
    const MB: u64 = 1_000_000;

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
        let mut source = Arc::clone(&objects) as Arc<dyn ObjectStore>;
        if let Some(link) = args.link_mb_per_s {
            let network =
                NetworkModel::new(Duration::from_millis(30), 100 * Self::MB, link * Self::MB);
            source = Arc::new(SimulatedStore::new(source, network));
        }
        let gets = Arc::new(RequestCounter::default());
        let store = Arc::new(CountedStore::new(source, Arc::clone(&gets)));
        // Room to spare, so that no block is evicted.
        let tier_blocks = args.blocks + args.blocks / 8 + 64;
        let cache_files = args.dir.iter().map(|dir| dir.join("t4-restart.cache"));
        Self {
            cache_files: cache_files.collect(),
            file_capacity: tier_blocks.div_ceil(args.dir.len() as u64) * Self::BLOCK,
            blocks: args.blocks,
            file_blocks: args.file_blocks,
            kill_at: args.kill_at,
            paths: Arc::new(paths),
            objects,
            gets,
            store,
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
            .with_memory_capacity(Self::MEMORY)
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
                        if bench.kill_at == Some(block) {
                            // No destructor runs: no clean mark, and writes left in flight.
                            std::process::abort();
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

    /// Every block, or each file's first and last; checks the bytes. Returns the blocks read
    /// and the GETs they took: the blocks the restart didn't keep.
    async fn read_back(&self, cache: &T4Caching, every: bool) -> Result<(u64, u64), Error> {
        let gets_before = self.gets.counts().gets();
        let mut read_blocks = 0;
        for first in (0..self.blocks).step_by(self.file_blocks as usize) {
            let last = (first + self.file_blocks).min(self.blocks) - 1;
            let blocks: Vec<u64> = if every {
                (first..=last).collect()
            } else {
                vec![first, last]
            };
            for block in blocks {
                let (path, range) = self.block_range(block);
                let read = cache.get_range(&path, range.clone()).await?;
                if read != self.objects.bytes(&path, range) {
                    return Err(format!("block {block} came back wrong").into());
                }
                read_blocks += 1;
            }
        }
        Ok((read_blocks, self.gets.counts().gets() - gets_before))
    }

    fn log_bytes(&self) -> u64 {
        std::fs::metadata(self.log_path()).map_or(0, |meta| meta.len())
    }
}

/// One build of the cache, and how the one before it ended.
struct RestartRow {
    restart: String,
    log_bytes: u64,
    shut_down: Option<Duration>,
    drop: Option<Duration>,
    build: Duration,
    blocks_read: u64,
    gets_after_build: u64,
}

impl RestartRow {
    const CSV_COLUMNS: &str = "label,restart,blocks,file_blocks,cache_files,log_bytes,\
                               shut_down_ms,drop_ms,build_ms,blocks_read,gets_after_build";

    fn csv(&self, label: &str, bench: &RestartBench) -> String {
        let millis = |time: Option<Duration>| {
            time.map_or(String::new(), |time| {
                format!("{:.3}", time.as_secs_f64() * 1e3)
            })
        };
        format!(
            "{label},{},{},{},{},{},{},{},{},{},{}",
            self.restart,
            bench.blocks,
            bench.file_blocks,
            bench.cache_files.len(),
            self.log_bytes,
            millis(self.shut_down),
            millis(self.drop),
            millis(Some(self.build)),
            self.blocks_read,
            self.gets_after_build,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let bench = Arc::new(RestartBench::new(&args));
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(out, "{}", RestartRow::CSV_COLUMNS)?;
    let mut write_row = |row: RestartRow| -> Result<(), Error> {
        let line = row.csv(&args.label, &bench);
        eprintln!("{line}");
        writeln!(out, "{line}")?;
        Ok(out.flush()?)
    };

    if args.after_kill {
        let log_bytes = bench.log_bytes();
        let (cache, build) = bench.timed_build()?;
        let (blocks_read, gets_after_build) = bench.read_back(&cache, true).await?;
        write_row(RestartRow {
            restart: "after-kill".to_owned(),
            log_bytes,
            shut_down: None,
            drop: None,
            build,
            blocks_read,
            gets_after_build,
        })?;
        drop(cache);
        return bench.remove_files();
    }

    bench.remove_files()?;
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
        shut_down: None,
        drop: None,
        build: cold_build,
        blocks_read: 0,
        gets_after_build: 0,
    })?;

    for restart in 1..=args.restarts {
        let start = Instant::now();
        cache.shut_down()?;
        let shut_down = start.elapsed();
        let log_bytes = bench.log_bytes();
        let start = Instant::now();
        drop(cache);
        let dropped = start.elapsed();
        let build;
        (cache, build) = bench.timed_build()?;
        let every = args.read_all && restart == 1;
        let (blocks_read, gets_after_build) = bench.read_back(&cache, every).await?;
        write_row(RestartRow {
            restart: restart.to_string(),
            log_bytes,
            shut_down: Some(shut_down),
            drop: Some(dropped),
            build,
            blocks_read,
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
        shut_down: None,
        drop: None,
        build,
        blocks_read: 1,
        gets_after_build: bench.gets.counts().gets() - gets_before,
    })?;
    drop(cache);
    bench.remove_files()
}
