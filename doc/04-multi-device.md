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
drive is waiting behind the others'. A range inside a block is one I/O on
one drive still; a call of several blocks reads from several drives, as
blocks filled together take adjacent chunks.

Against memory, on the same machine and replay, the warm pass all in RAM:

| Where the blocks are | Warm pass | Throughput | Calls 64 KiB – 1 MiB, p50 / p99 | Calls ≥ 4 MiB, p50 / p99 |
|---|---|---|---|---|
| the page cache, a local copy of the file | 2.63 s | 17.1 GiB/s | 0.18 / 0.61 ms | 4.9 / 77 ms |
| t4's memory tier | 2.59 s | 17.4 GiB/s | 0.09 / 0.52 ms | 3.5 / 101 ms |
| t4's disk tier, three drives | 5.28 s | 8.5 GiB/s | 0.81 / 6.4 ms | 12.6 / 153 ms |

Three drives reach half of what memory serves here; at about 3 GB/s a
drive, six would match it. A small read stays a drive's latency behind
memory's, however many drives there are.

### Twelve drives

The same replay on a `z3-highmem-88-highlssd` (twelve SSDs, 88 vCPUs;
`bench/clickbench/stripe_sweep.sh`), t4 striped over the first `n`, every
block on disk, filled from a local copy of the file; the page cache holding
all of that copy. Warm pass:

| Read through | Drives | 14 streams | 56 streams |
|---|---|---|---|
| the page cache | — | 22.7 GiB/s | 41.1 GiB/s |
| t4 | 1 | 3.0 GiB/s | |
| t4 | 2 | 5.9 GiB/s | |
| t4 | 3 | 8.7 GiB/s | |
| t4 | 4 | 11.3 GiB/s | |
| t4 | 6 | 12.6–13.1 GiB/s | |
| t4 | 8 | 14.0–14.1 GiB/s | |
| t4 | 12 | 12.5–13.3 GiB/s | 17.8 GiB/s |

(Ranges: queue depth 64 and 256, which change nothing.) t4 reads each
drive at its cap up to four, then stops at about 14 GiB/s, 62% of the page
cache, with twelve drives' 36 GiB/s to spare. Neither the drives, the queue
depth nor the one I/O worker hold it: a profile of the warm passes on
twelve finds the tokio workers busy, and the I/O worker at about 3% of the
samples. Of the workers' time, zeroing buffers (`memset`) takes about a
third, copying the pages read into the output (`memmove`) more than a
quarter, and faulting in freshly allocated buffers about a fifth: the read
path's own copies (`03-cache.md`, "Later": "Fewer copies on the I/O
path"), each paid on the calling task, so a stream's throughput is capped
by them. The page cache pays one copy a byte.

### Reading straight into the output

Since, a disk hit reads into the buffer the caller gets (`ReadOutput`,
`src/cache/read_output.rs`): no zeroing, no copy. Two things then decide
the result:

- **Where fresh pages fault in.** A direct read pins the pages it fills on
  the thread that submits it, the one I/O worker. Into pages never touched,
  that worker faults in, and the kernel zeroes, every read's pages alone,
  at 98% of a CPU: t4 fell to 8.5 GiB/s. Faulting them in on the reading
  task first (`MADV_POPULATE_WRITE`) takes it to 10.4.
- **The allocator.** glibc's hands each read fresh pages to fault in;
  mimalloc reuses what it freed, still mapped. The bench binaries run on
  mimalloc, a `#[global_allocator]` (`bench/Cargo.toml`, feature
  `mimalloc`); `--no-default-features` builds them on glibc. It speeds the
  page cache too: the copy into its fresh output faults less.

Twelve drives, warm pass, 14 streams; page faults are the process's minor
faults over the pass (`/proc/self/stat`):

| Read through | glibc | page faults | mimalloc | page faults |
|---|---|---|---|---|
| the page cache | 24.2 GiB/s | 1,142,973 | 30.1 GiB/s | 47,177 |
| t4 | 11.1 GiB/s | 4,078,340 | 34.6 GiB/s | 49,088 |

On mimalloc t4 takes 83 times fewer faults, and passes the page cache:
34.6 GiB/s is 98% of the twelve drives' cap. (Ubuntu's mimalloc 3,
preloaded, did worse, 24.8 GiB/s; unexplored.) At 56 streams the page
cache passes it again ("At 56 streams").

The one I/O worker shows a whole CPU at 56 streams, but it spins while I/O
is in flight. A flamegraph of it splits its time: `io_uring_enter` about
half (the NVMe driver's queueing 16%, pinning each read's pages, `gup`,
15%, the block layer 11%, iomap's direct I/O 5%, marking pages dirty 2%);
other kernel time 8%; the request channel 4%; completing tasks 1%; and
about a third spinning, to spare. At twelve drives' cap one worker still
has room; decision 4 holds.

### At 56 streams

`--copies 4` runs each query's tasks four times at once: 56 streams, 180
GiB a warm pass. Twelve drives, mimalloc; each drive's reads and busy time
over the warm pass from `/sys/block/*/stat`:

| Read through | 14 streams | 56 streams | The drives, 56 streams |
|---|---|---|---|
| the page cache | 25.2 GiB/s | 43.4 GiB/s | — |
| t4, queue depth 64 | 31.4 GiB/s | 32.0 GiB/s | 32,836 MiB/s, eleven 86–92% busy |
| t4, queue depth 256 | | 33.0 GiB/s | 33,925 MiB/s, eleven 92–95% busy |

At 56 streams t4 is held by the drives: they read 94% of their 36,000
MiB/s cap, eleven of them at 2,860–2,940 MiB/s. The page cache serves from
memory, its rate set by the threads copying out of it, and at 56 it passes
what twelve drives can read; matching it takes fifteen or sixteen. At 14
streams it is under the drives' cap, and t4 passes it.

The page cache is read through object_store's `LocalFileSystem`: a call
runs on a thread of tokio's blocking pool, which zeroes a buffer for each
range and `pread`s into it, the ranges one after another. A stream keeps
one call in flight, so 14 streams copy on 14 threads, of 88 vCPUs, each
writing every byte twice. A t4 call costs its task no work a byte: the
drives write the output, a call's blocks from several drives at once. A deeper queue
fills the drives a little more: 64 in flight is about five a drive.

(The 14-stream runs here are lower than in "Reading straight into the
output", 25.2 against 30.1 and 31.4 against 34.6, on a box five minutes up;
not yet explained.)

One drive reads less, and at 56 streams only: file 0's, about 2,000 MiB/s
and 70% busy, 6% of the bytes where the stripe would give it 8%. The
others then hold the pass about 3% longer than balanced drives would. A
guess, not yet checked by counting each file's chunks: four copies miss
each block at once in the cold pass, and each fetches it and writes it to
a chunk of its own (`03-cache.md`, "Later": "Two readers missing one
block"); the first write done takes the block's entry, and the others'
chunks go free. File 0's drive also holds the local copy the cold pass
fetches from, so its writes finish later, and it wins fewer blocks.

### Twelve faster drives, on AWS

An `i7i.48xlarge` (us-east-1, Spot; `bench/devbox/aws-create.sh`): 192
vCPUs, 1.5 TiB, twelve 3.75 TB instance-store SSDs, 23 Oct 2026. fio, 1
MiB direct reads at queue depth 32 on every drive at once: 4.48 GB/s a
drive, 53.8 GB/s (50.1 GiB/s) together, sequential or random. The replay,
warm pass:

| Read through | 14 streams | 56 streams | 112 streams |
|---|---|---|---|
| the page cache | 30.8 GiB/s | 67.8 GiB/s | 78.2 GiB/s |
| t4, queue depth 256 | 40.1 GiB/s | 47.8 GiB/s | 47.5 GiB/s |
| the drives under t4 | 39,004 MiB/s | 48,475 MiB/s | 48,417 MiB/s |

At 14 streams t4 passes the page cache by 30%. From 56 it stops at 94% of
the drives' fio rate, while the page cache, with 192 vCPUs to copy on,
goes on to 78 GiB/s.

The one I/O worker has run out of room: 93% of its samples are in
`io_uring_enter`, about 1% spinning, where on the Z3's slower drives a
third was spare. A queue of 512 reads slower than one of 256, 45.3 GiB/s.
Inside `io_uring_enter`: pinning the output's pages (`gup`) 31%, the
block layer 27%, the NVMe driver 21%, iomap's direct I/O 10%, marking
pages dirty 3%. One worker submits about 48 GiB/s of these reads and no
more: the measurement decision 4 waits for. File 0's drive, which holds
the origin copy, again reads least at 112 streams ("At 56 streams").

End to end (ClickBench, 192 partitions, as below), the warm pass takes
11.99 s from the page cache and 12.19 s from t4's disk tier.

### End to end

ClickBench's 43 queries in DataFusion (`run_queries`, 88 partitions) over
the single file, its origin a local copy, two passes in one process; the
page cache dropped before each run:

| The warm pass reads from | Warm pass | Cold pass |
|---|---|---|
| the page cache | 15.27 s | 17.48 s |
| t4's memory tier | 15.31 s | 17.92 s |
| t4's disk tier, one SSD | 21.95 s | 24.17 s |
| t4's disk tier, twelve SSDs | 14.93 s | 18.10 s |

On twelve SSDs the queries run as fast as from memory. On one they take
44% longer, all of it in the scans of many columns: query 23 reads 13.8
GiB off the drives, in 5.03 s on one (2.7 GiB/s, the drive's cap) and 2.40
s on twelve (5.7 GiB/s; decoding is then the limit). Over the warm pass
DataFusion asks for 3 GiB/s on average, far under the replay's 35: the
drives matter at the large scans only. Decoding bytes the drives wrote,
which no CPU copied, costs nothing measurable. One run each: queries
reading nothing off the drives differ by up to a third between runs (7,
29, 37), so the totals and the large scans are what to read.

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
