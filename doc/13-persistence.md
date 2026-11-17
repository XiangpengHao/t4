# 13 — Persistence

Status: build steps 1 to 3 built (2026-11-16): the log format, strict
replay, disk writes off the read path, and live appends across a kill.
Step 4, measurement on Linux, is left. Stage 1, a clean restart, is built
(`10-spiral-integration.md`, decision 7, 2 Nov 2026) and measured here.
Stage 2, a log that keeps the disk tier across a killed process, is the
design below, now built. Revised 17 Nov 2026: the disk tier lasts one boot,
so nothing syncs and there is no clean mark or `T4Caching::shut_down`
(decision 5). Revised again 17 Nov 2026: a build claims the files whose
log it doesn't take back, so a log of other files can't name their chunks
(decision 13).

t4 is a cache: the object store holds the true data, so the cache may lose
any block, but never returns wrong bytes (`03-cache.md`). Persistence keeps
the disk tier across a restart, so that a restarted process doesn't fetch
again what it held. It promises warmth, never durability: a block lost is
fetched again. What it must never do is name a chunk that holds other
bytes.

| The process ends by | stage 1, built | stage 2 |
|---|---|---|
| a drop | warm | warm |
| a kill or a crash | cold | warm, but for 32 MiB at most |
| either, then a reboot, planned or not | warm after a drop, else cold | cold |

## Terms

| Term | What it is |
|---|---|
| **Log** | `<first cache file>.log`, beside the cache files: a header, a base, then records |
| **Base** | the index's disk half, whole, in stage 1's snapshot format: each file version with blocks on disk, each of those blocks' chunk, and the next file version id |
| **Record** | entries appended together: its length, a CRC32C, its entries |
| **Entry** | a path's version (path, e_tag, size, id), a block added (version id, block, chunk), or a chunk freed (chunk) |
| **Batch** | 16 blocks written to disk together, one batch at a time |
| **Owner** | a cache file's first page: 16 bytes naming the build that last claimed the file; its chunks follow |

## Decisions

1. **Coherence, not durability** (the user, 16 Nov 2026). The log may
   lose its last entries; it may never name a chunk that holds other
   bytes than the block it names. It could in two ways: naming a chunk
   before its block is written, or still naming it once the chunk is
   written again. Each decision below closes one of the two. Nothing is
   ever synced (decision 5).

2. **A log replaces the snapshot.** Stage 1 writes a snapshot as the cache
   ends and removes it as the next builds, so a crash finds none. The log
   is kept as the cache runs: a base, then a record each batch (decision
   3). Replay applies the entries to the base in order. The next file
   version id is the base's, raised past each path entry's id, so that no
   id is given twice (`03-cache.md`, decision 5).

3. **Blocks go to disk in batches of 16, one batch at a time.** A block
   bound for disk, a memory victim or a block the policy admits there,
   waits in a memory chunk, read from there, until its batch goes. A batch:
   - takes its 16 chunks, evicting if it must;
   - appends a record of the chunks freed since the last record, by
     eviction or promotion, and the paths named since;
   - writes its 16 chunks, all at once;
   - once all are written, appends their adds, in the next batch's record
     if that batch is waiting, so that a busy cache makes one `write()` a
     batch.

   So the log names a chunk only once its block is written, and stops
   naming it before the chunk is written again. A failed write, or a block
   another fetch cached first, is never added, so needs no free. A path
   entry is queued as the cache names a new version, under the log's lock
   that adds take too, so no add names a version before its path.

4. **No sync.** A process's death loses nothing it wrote: the kernel
   holds the log's appended bytes, and a direct-I/O write, once done, is on
   the drive. A kill loses the batch in flight and the blocks waiting, 32
   MiB at most to fetch again.

5. **The disk tier lasts one boot** (the user, 17 Nov 2026). A machine
   crash may lose pages the kernel had not written, or the drive's
   reordered writes, so neither order of decision 3 holds after one. The
   log's header holds the boot id (`/proc/sys/kernel/random/boot_id`;
   `kern.bootsessionuuid` on macOS). The next cache takes the log back only
   if it was written in this boot; else it starts cold. A planned reboot
   starts cold too: telling it from a crash would take a clean exit that
   syncs the cache files and marks the log, and Studio, the desktop app
   this is for, never exits cleanly: Tauri ends its process without
   dropping the cache, which sits in libspiral behind a C ABI with no call
   to end it (`10-spiral-integration.md`, decision 7).

6. **A drop appends what it holds; the next build rebases.**
   - As the cache drops, its tier closed and its writer joined, it appends
     the entries it holds: the last batch's adds, which a kill loses.
   - As the next cache builds, it takes the log back, then writes it again
     as a base alone with its own boot id, renamed into place, before it
     writes a chunk, so that each record appended after applies to it.
     This is the log's only compaction. A build that can't take the log
     back claims its files first (decision 13), then writes an empty base.

7. **Replay checks every field, as stage 1 checks the snapshot.** The
   header must match: format, block size, chunks per file, and each cache
   file, in order, by device, inode, creation time and owner (decision
   13). An add must find its
   chunk free, a free its chunk held, a path a version id never given
   before. No base is written while the cache runs (decision 8), so no
   entry races one. A record whose 8-byte header or declared body runs
   past the file's end ends replay there: a kill can cut the last write.
   A complete record failing its CRC starts cold, even the last one:
   records after corruption may hold frees. Any other failed check starts
   cold too: a cold start costs refills, never wrong bytes.

8. **The log grows until the next build** (the user, 16 Nov 2026: keep
   it simple). Nothing compacts it while the cache runs; the next build
   does (decision 6). It grows about 25 bytes a MiB written to disk (an
   add, a later free, a share of its record) and some 150 bytes a version
   the cache names, cached on disk or not. That is 5 MB
   for a heavy desktop session that writes its 20 GiB tier ten times
   over, and 6 GB for a server filling at 100 MB/s for a month. The next
   build replays all of it, after a clean exit as after a kill: if an
   entry costs what taking back a base block does (70 to 90 ns,
   Measured), some 30 ms and 40 s.

9. **Disk writes leave the read path.** Today a read that fills a block
   returns once the block is on disk (`cache_landed`, in
   `cache_store/fill.rs`); with batches, once it is in memory, and a writer
   flushes the batches behind it: `03-cache.md`'s Later, eviction off the
   insertion path. A memory capacity under 16 chunks keeps its budget
   and uses one-block batches; larger capacities use 16. Raising a caller's
   memory budget silently would break the fixed-capacity contract.
   A batch not full waits for its next block; its
   blocks are memory's meanwhile and, as memory's, not written at a drop.
   One owned writer thread waits on a condition variable, submits a whole
   batch, and parks until its I/O completes. It holds the index and the
   tiers (`Tiers`), which reads share, not the cache; a drop closes the
   tier and joins it.
   At most 16 blocks wait behind the batch in flight. With no free memory,
   a fill may go uncached; it never waits for the writer to make room.
   Giving up a read does not cancel its queued batch. A batch's chunks
   stay pinned until its I/O completes, even if its future is dropped.

10. **A shared cache's owner keeps the log.** It holds the tier; a client
    keeps nothing, and its next owner takes the log back as any next cache
    builds (`11-shared-mode.md`, decision 7).

11. **The base is checked too.** The header is a magic, format and a
    16-byte boot UUID, then the base's 64-bit byte length, CRC32C and
    bytes. The base reuses stage 1's complete encoding and decoder.
    Records have a 32-bit byte length and CRC32C before their entries;
    integers are little endian. A bad base or complete record starts cold;
    a record cut short ends replay before any of its entries. Unknown
    entries start cold.
    A path may replace a path, in record order, but never reuse an id,
    including any id below the base's next id. An id a racing resolution
    wasted, or one whose path a kill lost, may be given again after a
    restart: an id names a version only in the cache that gave it
    (`11-shared-mode.md`, decision 7).

12. **Failure stops reuse.** A failed log append closes the disk tier
    before its batch writes; nothing appends past a possibly torn record.
    Reads keep using memory, disk and the remote. A failed data write is
    never added. An unreadable boot UUID or missing creation times discard any old log
    and disable persistence. The cache builds and serves normally;
    a restart is cold.
    The file is `<first cache file>.log`; old `.snapshot` files are ignored.

13. **A build claims the files whose log it doesn't take back** (17 Nov
    2026). A log lies beside its first file only, so a cache built on other
    files writes their chunks without touching it. Found 17 Nov 2026, in
    one boot: a cache on files `a` and `b` logs to `a.log`; a cache on `b`
    alone finds no `b.log`, starts cold and writes over `b`'s chunks; a
    third on `a` and `b` again found `a.log` matching and served `b`'s new
    bytes as its old blocks, with no GET. So each cache file starts with
    a page naming its owner, 16 random bytes, and its chunks follow. A
    build that takes back no log writes a fresh owner into each of its
    files before it writes a chunk, and its base records each file's owner
    (decision 7). A log then matches only files that no other build has
    claimed since. A kill between the claims and the new base leaves the
    old log naming owners the files no longer hold: the next build starts
    cold. Base format 2 moved chunks a page in, so a format 1 log starts
    cold. The fuzz target checks this after every step: what a kill there
    would keep is read back and compared (`fuzz/README.md`).

## Measured: stage 1's restart

Stage 1 synced at each clean exit; stage 2 never syncs, so the shut-down
times below, the cache files' `fdatasync`, are gone. Stage 2's restart is
build order step 4, not yet measured.

On the dev box (16 Nov 2026): a z3-highmem-22-highlssd, XFS on local SSDs
that report no volatile write cache. `bench/src/bin/disk_restart.rs`
fills the disk tier through `T4Caching`, then restarts it five times: shut
down, drop, build, and read each file's first and last block back. A last
build with the snapshot removed is the baseline.

| Run | Snapshot | Shut down after a fill | Shut down at rest | Build | Build, no snapshot |
|---|---|---|---|---|---|
| 20 GiB, 64 MiB files, one on-demand cache file | 206 KB | 15.3 ms | 0.65 to 0.68 ms | 1.7 to 2.1 ms | 0.24 ms |
| 20 GiB, 1 MiB files, one on-demand cache file | 2.8 MB | 17.4 ms | 8.7 to 9.6 ms | 7.3 to 8.1 ms | 0.25 ms |
| 256 GiB, 64 MiB files, three eager cache files | 2.6 MB | 18.0 ms | 7.7 to 9.4 ms | 18.5 to 19.2 ms | 0.79 ms |

- **Every restart read every file back right, with no GET;** the build
  with no snapshot fetched again.
- **The snapshot's size is its format's:** 8 bytes a block, some 131 a file
  version.
- **After a fill, the time is the cache files' `fdatasync`** (strace): 11.1
  ms on the on-demand file, 2.1 to 2.6 ms on each eager one. The drives
  report no volatile write cache, so it is XFS writing the extents the fill
  changed. At rest each sync takes 0.15 ms at most; the snapshot's own,
  0.14 to 0.67 ms.
- **The rest is CPU:** some 70 to 90 ns a block taken back, 350 to 380 ns
  a one-block file version.
- **Not measured:** a drive with a volatile write cache (a laptop's, and
  `F_FULLFSYNC` on macOS), and a snapshot read cold after a reboot.

## Rejected

- **A WAL with `O_DSYNC`** (`03-cache.md`, Rejected): a sync each batch,
  for durability a cache doesn't need.
- **A snapshot every minute or so, each freed chunk held back until the
  next is durable** (`10-spiral-integration.md`'s stage 2 until 16 Nov
  2026). It survives a machine crash too, but costs a sync each time and a
  reserve of free chunks: a full tier evicts only when an allocation finds
  none, so with freed chunks held back it would evict for nothing.
- **Compaction while the cache runs,** writing the log again as a base
  once it passes twice its base's size. It bounds the log and the next
  build's replay, but the base is captured while promotions and path
  changes go on, so replay must let a free find its chunk free already,
  and a path come twice. A log that only grows, compacted as the next
  cache builds, is simpler (the user, 16 Nov 2026), and grows slowly
  (decision 8).
- **Writing the log again as a base at a drop.** Its next build does it
  anyway.
- **A clean mark, keeping the disk tier across a planned reboot** (built
  16 Nov 2026, removed 17 Nov 2026, the user). A drop or
  `T4Caching::shut_down` synced the cache files, then appended a mark that
  let a later boot take the log back. It cost a sync at each exit and an
  API for processes that end without a drop, and Studio could make
  neither (decision 5).
- **A log of adds alone, the next record confirming the batch before it.**
  A freed chunk keeps its bytes until written again, so a free needs no
  entry; but a batch's adds must then go before its writes and count only
  once the next record confirms them, with a mask for writes that failed.
  Explicit frees are simpler (the user, 16 Nov 2026).
- **A checksum on each block, as Kismet's CRC32.** Checked on a block's
  first read after a restart, it catches both ways of decision 1, so it
  would keep the disk tier across any reboot with no order and no boot id.
  It costs a CRC32C of each block written, 4 bytes in its add, and a whole
  block read at its first read after a restart, where t4 otherwise reads
  the pages asked for. Considered again 17 Nov 2026 and left (the user):
  a cold start after a reboot is accepted.
- **Making a cold build's files anew,** so no old log matches them
  (decision 13; the user, 17 Nov 2026, for the owner page). It needs write
  access to each cache file's directory, replaces a path that is a
  symlink with a plain file, and unlinks large files at each cold start,
  which takes seconds on ext4.
- **The owner in an extended attribute,** so that chunks don't move. Not
  every file system keeps user attributes (tmpfs only since Linux 6.6),
  and where none is kept there would be no log.

## Later

- **A checksum of each block in its add,** if cold starts after reboots
  turn out costly (Rejected).
- **A bound on the log while the cache runs,** if a long-lived server's
  log grows too large or its replay at the next build too slow (decision
  8).
  Either compaction (Rejected), or a cap past which the log is removed
  and the process keeps nothing across a kill, as in stage 1.

## Build order

1. **The log's format:** header, base, records and their CRC, replay and
   its checks. Tests: each entry's checks, as the snapshot's
   (`storage/persistence/tests.rs`); a record cut short is dropped, a
   complete corrupted record starts cold; a log from another boot starts
   the cache cold.
2. **Batched disk writes off the read path** (decision 9), with no log
   yet. Tests: a read returns once its block is in memory; a batch goes at
   16 blocks; the shuttle suite's eviction and promotion races.
3. **The log written as batches go, at a drop and as the next builds**
   (decisions 3 to 8). Tests: a process killed at random through fills, promotions and
   evictions keeps the blocks whose adds it appended and no block whose
   chunk was written after, each read back right with no GET (real
   processes); the fuzz target appends and replays between operations.
4. **Measured** as stage 1 is above, and a process killed under load.

## Code map

| Where | What it is | Decisions |
|---|---|---|
| `src/cache/storage/persistence/` | the base, `LogBase`, and its checks; `log/` frames and replays entries; `log_file.rs` appends, finishes and rebases the file | 2, 6, 7 |
| `src/cache/storage/disk/owner.rs`, `cache_files.rs` | each file's owner page; the log taken back, or the files claimed, as the cache files open | 6, 13 |
| `src/cache/storage/cache_store/restart.rs` | the disk tier closed and its log finished, and restored as the next builds | 6 |
| `src/cache/storage/cache_store/disk_writer/`, `eviction.rs` | one writer on `Tiers`, batches, and adds and frees ordered with chunk ownership; a version's path logged where `Tiers::resolve` names it | 3, 9 |
| `bench/src/bin/disk_restart.rs`, `bench/disk_restart/` | the restart's cost | Measured |
