# `t4`

`t4` is a fast object store cache. 

## Features

- **Two tiers of fixed size:** memory, and one file on a local drive. The
  cache never grows past either.
- **`io_uring` and `O_DIRECT`** for the drive. A hit on disk reads only the
  4 KiB pages it needs.
- **1 MiB blocks.** A miss fetches whole blocks, adjacent ones in one GET,
  and all of a read's GETs are in flight at once.
- **Your eviction policy, or ours.** A `CachePolicy` hears every hit and
  miss, decides where a fetched block goes and whether a disk hit moves to
  memory, and picks the victims when a tier is full. The default is a
  lock-free CLOCK for each tier.
- **Never the wrong bytes.** A file is cached by its e_tag, and a miss
  fetches with `If-Match` on it.
- **Runtime-agnostic async**, on `object_store` 0.13, as DataFusion 55 uses.

## Usage

```rust
use std::sync::Arc;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use t4::T4Caching;

let s3: Arc<dyn ObjectStore> = /* any ObjectStore */;
let store = T4Caching::builder(s3)
    .with_memory_capacity(64 << 30)        
    .with_disk_path("/mnt/nvme/t4.cache")
    .with_disk_capacity(4 << 40)          
    .build()?;

let footer = store
    .get_range(&Path::from("data/part-0.parquet"), 0..8192)
    .await?;
```

A policy of your own gets the tiers' sizes when the cache is built:

```rust
let store = T4Caching::builder(s3)
    // ... capacities and path as above ...
    .with_policy(|tiers| MyPolicy::new(tiers.memory_chunks(), tiers.disk_chunks()))
    .build()?;
```

`t4::policy` holds the `CachePolicy` trait, the default `ClockPolicy`, and
the types a policy works with.

## How it works

```text
 get_range(path, range)
      |
      v
 paths:   path -> (file version, e_tag, size)            bbtree, in memory
 blocks:  (file version, block) -> memory or disk chunk  bbtree, in memory
      |                     |                    |
      v                     v                    v
 memory pool            cache file           inner store
 1 MiB chunks           1 MiB chunks         GET with If-Match, then cache
```

- **A path's first read** asks the inner store what the file is (a HEAD).
  The cache trusts that answer from then on.
- **Each block of a read** is found in memory, on disk, or missing. Missing
  blocks are fetched together and cached where the policy says.
- **A full tier evicts the policy's victims:** memory moves them to disk or
  drops them, and the disk drops them.
- **Writes, copies, renames and deletes** go to the inner store, and the
  cache forgets the paths they change.

The design and every decision behind it: [doc/03-cache.md](doc/03-cache.md).

## Limitations

- **Cold after a restart.** The index lives in memory only.
- **The cache trusts the store's first answer for a path.** A file changed
  behind it reads as it was until a miss notices. That's right for files
  that don't change in place, like a data lake's Parquet files.
- **Not yet:**
  - request coalescing: two readers missing one block both fetch it;
  - splitting a long miss into parallel GETs.
- **A read returns its whole range as one buffer,** and a cached read
  doesn't keep the object's attributes.

## Development

```bash
cargo test
```

```bash
cargo test --features shuttle shuttle
```

```bash
cargo verus focus
```

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10
```

These run the unit tests, the concurrency tests under
[shuttle](https://github.com/awslabs/shuttle), the Verus proofs of the
alignment and range arithmetic, and the fuzz target
([fuzz/README.md](fuzz/README.md)). Where the docs start:
[doc/00-meta.md](doc/00-meta.md).

## Vision

`t4` will be the ultimate and only file system you need.
