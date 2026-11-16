# 13 — Persistence

Status: build steps 1 to 3 built (2026-11-16): the log format, strict
replay, disk writes off the read path, and live appends across a kill.
Step 4, measurement on Linux, is left. Stage 1, a clean restart, is built
(`10-spiral-integration.md`, decision 7, 2 Nov 2026) and measured here.
Stage 2, a log that keeps the disk tier across a killed process, is the
design below, now built.

t4 is a cache: the object store holds the true data, so the cache may lose
any block, but never returns wrong bytes (`03-cache.md`). Persistence keeps
the disk tier across a restart, so that a restarted process doesn't fetch
again what it held. It promises warmth, never durability: a block lost is
fetched again. What it must never do is name a chunk that holds other
bytes.

| The process ends by | stage 1, built | stage 2 |
|---|---|---|
| a clean exit: a drop, or `T4Caching::shut_down` | warm | warm |
| a kill or a crash | cold | warm, but for 32 MiB at most |
| a machine crash: power lost, a kernel panic | cold | cold |

## Terms

| Term | What it is |
|---|---|
| **Log** | `<first cache file>.log`, beside the cache files: a header, a base, then records |
| **Base** | the index's disk half, whole, in stage 1's snapshot format: each file version with blocks on disk, each of those blocks' chunk, and the next file version id |
| **Record** | entries appended together: its length, a CRC32C, its entries |
| **Entry** | a path's version (path, e_tag, size, id), a block added (version id, block, chunk), a chunk freed (chunk), or the clean mark a clean exit appends |
| **Batch** | 16 blocks written to disk together, one batch at a time |

## Decisions

1. **Coherence, not durability** (the user, 16 Nov 2026). The log may
   lose its last entries; it may never name a chunk that holds other
   bytes than the block it names. It could in two ways: naming a chunk
   before its block is written, or still naming it once the chunk is
   written again. Each decision below closes one of the two. Nothing is
   synced while the cache runs.

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
     eviction or promotion, and the paths its blocks need;
   - writes its 16 chunks, all at once;
   - once all are written, appends their adds, in the next batch's record
     if that batch is waiting, so that a busy cache makes one `write()` a
     batch.

   So the log names a chunk only once its block is written, and stops
   naming it before the chunk is written again. A failed write, or a block
   another fetch cached first, is never added, so needs no free.

4. **No sync while the cache runs.** A process's death loses nothing it
   wrote: the kernel holds the log's appended bytes, and a direct-I/O write,
   once done, is on the drive. A kill loses the batch in flight and the
   blocks waiting, 32 MiB at most to fetch again.

5. **A machine crash starts cold.** It may lose pages the kernel had not
   written, or the drive's reordered writes, so neither order of decision 3
   holds after one. The log's header holds the boot id
   (`/proc/sys/kernel/random/boot_id`; `kern.bootsessionuuid` on macOS).
   The next cache takes the log back if it ends in a clean mark, or was
   written in this boot; else it starts cold.

6. **The only syncs: as the cache ends, and as the next builds.**
   - As the cache drops or shuts down, its tier closed and its cache files
     flushed as in stage 1, it appends the adds it holds and a clean mark,
     and flushes the log. The log isn't written again: it only grows.
   - As the next cache builds, it takes the log back, then writes it again
     as a base alone with its own boot id, renamed into place and flushed,
     before it writes a chunk. This is the log's only compaction, and it
     must be durable: the old log, ending clean, would outlive a machine
     crash and name chunks reused since.

7. **Replay checks every field, as stage 1 checks the snapshot.** The
   header must match: format, block size, chunks per file, and each cache
   file, in order, by device, inode and creation time. An add must find its
   chunk free, a free its chunk held, a path a version id never given
   before. No base is written while the cache runs (decision 8), so no
   entry races one. A record whose 8-byte header or declared body runs
   past the file's end ends replay there: a kill can cut the last write.
   A complete record failing its CRC starts cold, even the last one:
   records after corruption may hold frees. Any other failed check starts
   cold too: a cold start costs refills, never wrong bytes.

8. **The log grows until the next build** (the user, 16 Nov 2026: keep
   it simple). Nothing compacts it while the cache runs; the next build
   does (decision 6). It grows about 25 bytes a MiB written to disk: an
   add, a later free, a share of its record and its path. That is 5 MB
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
   blocks are memory's meanwhile and, as memory's, not written at shutdown.
   One owned writer thread waits on a condition variable, submits a whole
   batch, and parks until its I/O completes. It holds no cache handle
   that keeps its own owner alive; shutdown closes the tier and joins it.
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
    including any id below the base's next id. The clean mark carries the
    next id too, including ids a racing resolution allocated and wasted.

12. **Failure stops reuse.** A failed log append closes the disk tier
    before its batch writes; nothing appends past a possibly torn record.
    Reads keep using memory, disk and the remote. A failed data write is
    never added. A memory victim whose path was replaced before its batch
    is dropped; a batch scans paths once for the versions it needs.
    An unreadable boot UUID or missing creation times discard any old log
    durably and disable persistence. The cache builds and serves normally;
    a restart is cold.
    The file is `<first cache file>.log`; old `.snapshot` files are ignored.

## Measured: stage 1's restart

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
- **Writing the log again as a base at a clean exit.** Its next build
  does it anyway; a clean mark appended does what a clean flag in a
  rewritten header would.
- **A log of adds alone, the next record confirming the batch before it.**
  A freed chunk keeps its bytes until written again, so a free needs no
  entry; but a batch's adds must then go before its writes and count only
  once the next record confirms them, with a mask for writes that failed.
  Explicit frees are simpler (the user, 16 Nov 2026).
- **A checksum on each block, as Kismet's CRC32.** Checked on a block's
  first read after a restart, it catches both ways of decision 1, so it
  would survive a machine crash with no order and no boot id, at a hash of
  each block written and each taken back. Later, if machine crashes turn
  out to matter.

## Later

- **A 64-bit hash of each block in its add,** if laptops losing power
  make cold starts costly (Rejected).
- **A bound on the log while the cache runs,** if a long-lived server's
  log grows too large or its replay at the next build too slow (decision
  8).
  Either compaction (Rejected), or a cap past which the log is removed
  and the process keeps nothing across a kill, as in stage 1.

## Build order

1. **The log's format:** header, base, records and their CRC, replay and
   its checks. Tests: each entry's checks, as the snapshot's
   (`storage/persistence/tests.rs`); a record cut short is dropped, a
   complete corrupted record starts cold; a log from another boot, not
   clean, starts the cache cold.
2. **Batched disk writes off the read path** (decision 9), with no log
   yet. Tests: a read returns once its block is in memory; a batch goes at
   16 blocks; the shuttle suite's eviction and promotion races.
3. **The log written as batches go, shutdown and build** (decisions 3 to
   8). Tests: a process killed at random through fills, promotions and
   evictions keeps the blocks whose adds it appended and no block whose
   chunk was written after, each read back right with no GET (real
   processes); the fuzz target appends and replays between operations.
4. **Measured** as stage 1 is above, and a process killed under load.

## Code map

| Where | What it is | Decisions |
|---|---|---|
| `src/cache/storage/persistence/` | the base, `LogBase`, and its checks; `log/` frames and replays entries; `log_file.rs` appends, finishes and rebases the file | 2, 6, 7 |
| `src/cache/storage/cache_store/restart.rs` | the disk tier closed and its log finished, and restored as the next builds | 6 |
| `src/cache/storage/cache_store/disk_writer/`, `eviction.rs` | one writer, batches, and adds and frees ordered with chunk ownership | 3, 9 |
| `bench/src/bin/disk_restart.rs`, `bench/disk_restart/` | the restart's cost | Measured |
