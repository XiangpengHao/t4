# `t4`

[![crates.io](https://img.shields.io/crates/v/t4.svg)](https://crates.io/crates/t4)
[![docs.rs](https://docs.rs/t4/badge.svg)](https://docs.rs/t4)
[![CI](https://github.com/XiangpengHao/t4/actions/workflows/rust-ci.yml/badge.svg)](https://github.com/XiangpengHao/t4/actions/workflows/rust-ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`t4` is a fast object store downloader and cache. 
- Request prefetching, splitting, coalesencing, racing read; all handled automatically.
- Customized cache policies. 
- Multi-tier memory, disk, remote. Multi-device stripping/racing/load-balancing. 
- `io_uring` yet async runtime agnostic.

**Warning**: t4 gives you higher ceilings, but requires more understanding to get the best of it. Only use t4 when you're reasonably frustrated with existing solutions.

## Usage

```bash
cargo add t4
```

```rust
use std::sync::Arc;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use t4::T4Caching;

let s3: Arc<dyn ObjectStore> = /* any ObjectStore */;
let store = T4Caching::builder(s3)
    .with_memory_capacity(64 << 30)
    .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
    .with_disk_file("/mnt/nvme1/t4.cache", 3 << 40)
    .with_log_file("log/t4.log") // optional, for persistence
    .build()?;

let footer = store
    .get_range(&Path::from("data/part-0.parquet"), 0..8192)
    .await?;
```


## Policies

`t4` has two policies, and you can roll your own of either: a `PlacementPolicy` decides
where blocks live, and a `ReadPolicy` decides how a read reaches the object store. The
built-in policies use the same public traits.

### Placement policy: where blocks live

Tiers are exclusive: a 1 MiB block is in memory or on disk, never both.

| Policy | What it does |
|---|---|
| `ClockPolicy` (default) | A CLOCK per tier. A disk block moves to memory on its second hit, so one scan doesn't push hot blocks out of memory. |
| `FixedPlacementPolicy::new(sizes, admit)` | Every fetched block goes to the one tier `admit` names, and a disk hit stays on disk. |

```rust
pub trait PlacementPolicy: Send + Sync {
    /// Records a hit; it's on the hit path, so keep it cheap.
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>);

    /// Stay on disk, or promote to memory.
    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit;

    /// Admit a fetched block to memory or disk, or discard it.
    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit;

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey);

    /// Each victim is moved to disk or dropped.
    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim>;

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId>;
}
```

### Read policy: how a read reaches the object store

A read policy plans the GETs for the blocks the cache doesn't hold. A plan may split, widen
or merge runs of blocks, and race a GET: send it several times at once and take the first
answer, or send one more copy only if it is still running after a delay.

| Policy | What it does |
|---|---|
| `RaceStragglers` (default, at the p90) | Plans as `FetchRuns`, then races a GET once more if it runs past the p90 of recent GET times. `RaceStragglers::at(q)` picks another quantile. |
| `FetchRuns` | Splits a run into GETs of 4 MiB at most, so no one GET holds a read up, and races a Parquet or Vortex footer in a GET of its own. |

```rust
pub trait ReadPolicy: Send + Sync {
    /// The GETs for the read's blocks the cache doesn't hold.
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan;

    /// Each disk and remote I/O as it starts and ends, to learn from.
    fn on_io(&self, event: &IoEvent<'_>);
}
```

A caller may say what a read is for, a `ReadIntent` (`Latency`, `Throughput` or `Warm`),
in `GetOptions::extensions`; a policy sees it in `LocatedRead::intent` and
`BlockRead::intent`.

### Configuring policies

```rust
use t4::policy::{Admit, FetchRuns, FixedPlacementPolicy, RaceStragglers};

let store = T4Caching::builder(s3)
    .with_memory_capacity(64 << 30)
    .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
    // A closure: the policy needs the tiers' sizes, known once the files open.
    .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
    .with_read_policy(RaceStragglers::at(0.99).planning_as(FetchRuns::default()))
    // t4 has no runtime of its own: without a timer, no GET is raced late.
    .with_sleep(|after| Box::pin(tokio::time::sleep(after)))
    .build()?;
```

## Tests

Run `cargo test --locked` and `cargo test --locked --features shuttle`.
The latter runs every concurrent scenario on checked-in schedules; see
[the seed and OS-coverage inventory](doc/12-seeded-concurrency-tests.md) for replay
instructions and the real-OS exceptions.

## Limitations

1. T4 caches data at 1MB granularity. It can have high amplification if files are small (< 1MB), but there's a bigger problem if your files are like that. 

2. it works on macOS, but not as tuned as on Linux.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
