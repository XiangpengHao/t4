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


## Customized policy

`t4` is fully customizable, you can roll your own `ReadPolicy` and `PlacementPolicy`.
The default policies are built with the same public interfaces.

```rust
let store = T4Caching::builder(s3)
    .with_placement_policy(...)
    .with_read_policy(...)
    .build()?;
```

```rust
pub trait ReadPolicy: Send + Sync {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan;

    fn on_io(&self, event: &IoEvent<'_>);
}
```


```rust
pub trait PlacementPolicy: Send + Sync {
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>);

    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit;

    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit;

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey);

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim>;

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId>;
}
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
