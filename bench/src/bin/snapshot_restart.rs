//! Times a clean restart of t4's disk tier: the snapshot kept as the cache shuts down and again
//! as it drops, and taken back as the next cache builds (doc/13-persistence.md). A last build on
//! the same files with the snapshot removed is the baseline the taking back adds to.
//!
//! ```text
//! cargo build --release -p t4-bench --bin snapshot_restart
//! target/release/snapshot_restart --dir /mnt/t4/0 --blocks 20480 --out restart.csv
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
    /// A directory a cache file, each on its own drive; the snapshot goes beside the first.
    #[arg(long, required = true)]
    dir: Vec<PathBuf>,
    /// The 1 MiB blocks filled, every one on disk: 20 GiB unless set.
    #[arg(long, default_value_t = 20 << 10)]
    blocks: u64,
    /// Each file's blocks. The snapshot keeps a file's path and e_tag once for all of them.
    #[arg(long, default_value_t = 64)]
    file_blocks: u64,
    /// Cache files that grow as blocks are written, as on a desktop's shared drive.
    #[arg(long)]
    on_demand: bool,
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
}

struct RestartBench {
    cache_files: Vec<PathBuf>,
    file_capacity: u64,
    on_demand: bool,
    blocks: u64,
    file_blocks: u64,
    paths: Arc<Vec<Path>>,
    objects: Arc<SyntheticObjects>,
    gets: Arc<RequestCounter>,
    store: Arc<dyn ObjectStore>,
}

impl RestartBench {
    const BLOCK: u64 = 1 << 20;
    /// t4 needs some memory; the policy puts nothing there, so every block is in the snapshot.
    const MEMORY: u64 = 64 << 20;

    fn new(args: &Args) -> Self {
        let files = args.blocks.div_ceil(args.file_blocks);
        // Spiral names a file by a SHA-256 in hex: paths about as long as its.
        let paths: Vec<Path> = (0..files)
            .map(|file| Path::from(format!("snapshot_restart/{file:064x}.vortex")))
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
            on_demand: args.on_demand,
            blocks: args.blocks,
            file_blocks: args.file_blocks,
            paths: Arc::new(paths),
            objects,
            gets,
            store,
        }
    }

    fn snapshot_path(&self) -> PathBuf {
        let mut path = self.cache_files[0].clone().into_os_string();
        path.push(".snapshot");
        path.into()
    }

    /// Only files this bench names: a run starts cold and leaves the drives as it found them.
    fn remove_files(&self) -> Result<(), Error> {
        let snapshot = self.snapshot_path();
        for path in self.cache_files.iter().chain([&snapshot]) {
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
        if self.on_demand {
            builder = builder.with_on_demand_disk_allocation();
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

    /// A read returns once its block is written and named in the index, so each is in the
    /// snapshot.
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

    /// Every file's first and last block, from the drive and right: none fetched again.
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

    fn snapshot_bytes(&self) -> u64 {
        std::fs::metadata(self.snapshot_path()).map_or(0, |meta| meta.len())
    }
}

/// One build of the cache, and how the one before it ended.
struct RestartRow {
    restart: String,
    snapshot_bytes: u64,
    shut_down: Option<Duration>,
    drop: Option<Duration>,
    build: Duration,
    gets_after_build: u64,
}

impl RestartRow {
    const CSV_COLUMNS: &str = "label,restart,blocks,file_blocks,cache_files,on_demand,\
                               snapshot_bytes,shut_down_ms,drop_ms,build_ms,gets_after_build";

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
            bench.on_demand,
            self.snapshot_bytes,
            millis(self.shut_down),
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
        snapshot_bytes: 0,
        shut_down: None,
        drop: None,
        build: cold_build,
        gets_after_build: 0,
    })?;

    for restart in 1..=args.restarts {
        let start = Instant::now();
        cache.shut_down()?;
        let shut_down = start.elapsed();
        let snapshot_bytes = bench.snapshot_bytes();
        let start = Instant::now();
        drop(cache);
        let dropped = start.elapsed();
        let build;
        (cache, build) = bench.timed_build()?;
        let gets_after_build = bench.check_taken_back(&cache).await?;
        write_row(RestartRow {
            restart: restart.to_string(),
            snapshot_bytes,
            shut_down: Some(shut_down),
            drop: Some(dropped),
            build,
            gets_after_build,
        })?;
    }

    drop(cache);
    std::fs::remove_file(bench.snapshot_path())?;
    let (cache, build) = bench.timed_build()?;
    let gets_before = bench.gets.counts().gets();
    let (path, range) = bench.block_range(0);
    cache.get_range(&path, range).await?;
    write_row(RestartRow {
        restart: "no-snapshot".to_owned(),
        snapshot_bytes: 0,
        shut_down: None,
        drop: None,
        build,
        gets_after_build: bench.gets.counts().gets() - gets_before,
    })?;
    drop(cache);
    bench.remove_files()
}
