# `t4`

`t4` is a fast object store downloader and cache. 
- Request prefetching, splitting, coalesencing, racing read; all handled automatically.
- Hybrid memory and disk caching, customized read policy and placement policy.
- `io_uring` for disks, multi-device stripping/racing/load-balancing.
- Implements `object_store` trait, plug and play; async runtime agnostic. 

## Usage

```rust
use std::sync::Arc;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use t4::T4Caching;

let s3: Arc<dyn ObjectStore> = /* any ObjectStore */;
let store = T4Caching::builder(s3)
    .with_memory_capacity(64 << 30)
    .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
    .with_disk_file("/mnt/nvme1/t4.cache", 3 << 40)
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
