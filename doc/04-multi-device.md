# 04 — Several devices

Status: built and measured (2026-10-22): chunks striped over the cache
files by their number, one I/O worker for all of them, reading three
SSDs at 98% of their cap ("Measured").

t4's disk tier was one cache file on one drive. A machine with local SSDs
has several, and each is capped: a Z3's 3 TB Titanium SSD reads at 3,000
MiB/s. Replaying ClickBench with every block on disk (`bench/`, 22 Oct
2026), t4 read at 3,063 MiB/s, one drive's cap. More bandwidth needs more
drives, and t4 drives them itself, not through md RAID (`00-meta.md`:
"io_uring and multi-device read").

```text
 index:  block -> Disk(chunk)                    unchanged
 disk tier: chunks 0 .. n * k, one chunk table   unchanged
      |
      v  chunk c -> file c mod n, chunk c / n    new
 file 0     file 1     ...     file n-1          one cache file a drive
      \        |                  /
       one I/O worker, one io_uring ring
```

## Decisions

1. **The user gives the cache files and the bytes each may take,** one
   `with_disk_file(path, capacity)` a file, nothing about how t4 lays
   chunks over them. One file is the case of one.

   ```rust
   let store = T4Caching::builder(gcs)
       .with_memory_capacity(64 << 30)
       .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
       .with_disk_file("/mnt/nvme1/t4.cache", 3 << 40)
       .with_disk_file("/mnt/nvme2/t4.cache", 3 << 40)
       .build()?;
   ```

2. **The tier keeps one chunk space, striped over the files by chunk
   number:** chunk `c` is file `c mod n`'s chunk `c / n`, at byte `(c / n)
   MiB` of it. The tier stays one array of chunks (`03-cache.md`, decision
   2), so the index (`Disk(chunk)`), the policy, eviction and the check at
   rest don't change; only where a chunk's bytes are does.

   A chunk is a block, 1 MiB: a block never spans two files, so reading
   it is one I/O on one drive. Adjacent chunk numbers sit on different
   files, and the free bitmap hands out adjacent numbers: it allocates the
   lowest clear bit of the 64-chunk word it last touched, and the CLOCK
   takes a batch of victims from the word its hand sweeps. So a miss of
   several blocks fills chunks on several drives, and a later read of
   them reads from all of them at once.

3. **Every file holds as many chunks as the smallest can:** the tier is
   `n` times that. Drives are taken to be equally fast, and the stripe
   spreads load by spreading chunks evenly. A larger file's extra bytes go
   unused, and aren't allocated.

4. **One I/O worker drives every file:** one thread, one io_uring ring,
   each request naming its file. It stays one until a measurement shows a
   thread can't keep the drives busy; on three drives it keeps them at
   their cap ("Measured").

## The plan

1. **I/O to several files:** the worker opens every file, and a read or a
   write names its file and its offset in it (`FilePosition`). io_uring,
   the thread pool and the simulated driver each hold the list.
2. **The disk tier over them:** `CacheFile` becomes `CacheFiles`; a chunk's
   position is `(c mod n, (c / n) MiB)`; readers and writers carry it.
3. **The front end:** `with_disk_file`, replacing `with_disk_path` and
   `with_disk_capacity`; the README; `03-cache.md` decision 10.
4. **Tests:** placement; the cache over several files, unit and shuttle;
   the fuzz target over one to three files.
5. **Measure** on a Z3 with three SSDs (`z3-highmem-22-highlssd`; the dev
   box's boot disk moves to it), replaying ClickBench with every block on
   disk: toward 9,000 MiB/s, from 3,063 on one drive; and whether one
   worker keeps up.

## Measured

ClickBench's single file, its traced reads replayed with nothing between
them (`bench/`, `replay_reads`), every block pinned to disk, on a
`z3-highmem-22-highlssd` (three 3 TB SSDs, 3,000 MiB/s read each), 22 Oct
2026. The warm pass reads 45.7 GiB, all of it from the SSDs:

| Cache files | Warm pass | Read from the SSDs | Calls 64 KiB – 1 MiB, p50 / p99 / max | Calls ≥ 4 MiB, p50 / p99 / max |
|---|---|---|---|---|
| 1 | 15.21 s | 3,076 MiB/s | 4.45 / 42.4 / 78.5 ms | 41.6 / 316 / 431 ms |
| 3 | 5.28 s | 8,862 MiB/s | 0.81 / 6.4 / 13.7 ms | 12.6 / 153 / 274 ms |

Three drives read 2.9 times what one does, 98% of their cap, with one I/O
worker: one thread keeps three drives busy at 1 MiB I/Os. A call's
latency falls with the queue it waits in: most of a call's time on one
drive is waiting behind the others'.

## Rejected

- **A layout the user composes,** stripes and mirrors as a tree: the user
  shouldn't have to reason about layout. They give files and sizes.
- **A worker per drive** before a measurement says one can't keep up.
- **md RAID0 under one cache file:** a failed drive takes the whole array,
  no copy to read from instead, and root to set it up before t4 runs.
- **Stripe zones for files of unequal size,** as md raid0 does: chunks in
  the later zones sit on fewer drives, so the larger drives take more of
  the load.

## Later

- **Mirrored chunks,** a block on two drives: a read goes to the drive
  less busy, and a latency-critical one races both, the first answer
  taken (`00-meta.md`: "racing read for latency critical request").
- **A worker per drive,** if one can't keep up.
- **Drives of unequal size or speed:** placement weighted by each, chosen
  at fill time.
- **A failed drive dropped alone:** its chunks forgotten, the rest of the
  tier serving on.
- **Keeping the disk tier across a clean restart** (`03-cache.md`,
  "Later") records the files with the snapshot.

## Code map

| Path | What it is | Decisions |
|---|---|---|
| `src/io/` | the worker and its drivers over several files, an I/O naming its file (`FilePosition`) | 4 |
| `src/cache/cache_files.rs` | `CacheFiles`: the tier's files, each sized to the smallest, and the worker | 2, 3 |
| `src/cache/chunk_pool/disk_chunks.rs` | a chunk's position: file `c mod n`, offset `(c / n) MiB` | 2 |
| `src/cache/t4_caching.rs` | `with_disk_file` | 1 |
