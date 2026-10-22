# 03 — Object store cache

Status: design (2026-10-21). Built: the chunk pool
(`src/cache/chunk_pool/`), the block index (`src/cache/block_index/`), the
disk tier (`src/cache/cache_file.rs`), the policy and its default
(`src/cache/policy/`), and reads, fills, promotions and evictions over them
(`src/cache/object_cache.rs`). Not yet: request coalescing, the front
end.
The decisions behind the pivot in `00-meta.md`, worked out from the notes
in `99-brainstorm.md`.

t4 caches the bytes of files from an object store in two tiers, memory and
disk, each of a fixed size it never exceeds. The object store holds the true
data, so the cache may lose anything, but never returns wrong bytes. A crash
loses all of it.

```text
 read(path, range)
      |
      v
 paths:   path -> (file_version_id, e_tag, size)                        bbtree
 blocks:  (file_version_id, block) -> Memory(chunk) | Disk(chunk) | Fetching
      |                    |                   |
      v                    v                   v
 memory pool           cache file          object store
 1 MB chunks           1 MB chunks         GET, then cache
```

## Terms

Two terms say where bytes are in a file, two where they are stored.

| Term | What it is | Size | Named by | In code |
|---|---|---|---|---|
| **File** | what the object store holds at a path: an object | any | path | — |
| **File version** | a file at one e_tag | any | file version id | `FileVersionId`, `PathTable` |
| **Block** | a piece of a file version: block `k` is bytes `[k MiB, (k+1) MiB)` | 1 MiB, a file's last shorter | file version id, block number | `BlockKey`, `BlockTable` |
| **Chunk** | a slot of storage that holds one block | 1 MiB | tier, number | `ChunkId`, `Location` |
| **Page** | the unit `O_DIRECT` reads and writes | 4 KiB | offset | `PAGE_SIZE`, `BlockWindow` |
| **Tier** | memory or disk, each an array of chunks | its budget | — | `MemoryPool`, `DiskChunks` |
| **Cache file** | t4's file on disk, which holds the disk chunks | the disk budget | its path | `DiskChunks` |

A block is what is cached, a chunk where: a block moves between chunks,
and a chunk holds one block after another. As in virtual memory, a block is
a virtual page, a chunk a physical frame, `blocks` the page table and the
chunk table the frame table.

## Decisions

### Blocks and chunks

1. **The cache unit is an aligned 1 MB block of an object:** block `k` is
   bytes `[k MB, (k+1) MB)`. A miss fetches whole blocks, so a 100 KB read
   fetches 1 MB: on an object store the time to first byte dominates, and a
   1 MB GET costs about what a small one does. Blocks never overlap, so no
   ranges merge. 512 KB is the smallest block considered; the size is to be
   confirmed against the I/O size where the drive's bandwidth saturates,
   measured at a few sizes on the target drive.
2. **Memory and disk are both arrays of 1 MB chunks,** a block in one chunk.
   A 1 MB I/O already reaches the drive's bandwidth, so contiguity past a
   chunk buys nothing. Free chunks are a bitmap: any free chunk serves any
   block, so nothing fragments and nothing merges. The memory pool is taken
   once, at startup, and never given back; the cache file is preallocated to
   the disk budget. The block is the smallest unit: an object's last
   block, and an object smaller than a block, still take a whole chunk,
   and a block moves to disk as its whole chunk.
3. **Reads can be partial.** A read inside a block takes only the 4 KB
   pages it needs: a copy from a memory chunk, an `O_DIRECT` read from a
   disk chunk.

### Index

4. **bbtree is the page table:** two stock `BBTreeMap`s, not bbtree-store.
   - `paths`: path → `file_version_id`, e_tag, size.
   - `blocks`: (`file_version_id`, block) → a memory chunk, a disk chunk, or
     `Fetching`.

   A read looks up its path, then scans its blocks in order. Keys and
   values are fixed size, inline in the leaves: a sparse file and a fully
   cached one cost the same per cached block, and no file has a size cap.
   btrfs keys file data the same way, by (inode, offset).

   A block's key is one `u64`: 40 bits of file version id, then 24 of
   block number. So `blocks` keys are plain `u64`s, a chunk's owner one
   atomic word, and a file is at most 16 TiB less a block; a larger one
   isn't cached.

   A path is up to 1,024 bytes, object stores' limit. Only `paths` holds
   paths, once a file version. Its keys are `Box<[u8]>`, whose bytes
   bbtree keeps outside its nodes, so long keys change no node size.
5. **A new e_tag is a new file version.** The old version's blocks are
   never read again, and leave through eviction. File version ids are
   never reused: an id reused while a reader still holds it from an old
   lookup would pass that reader's owner check with another file's bytes
   (decision 8). So ids count every version the cache ever meets, not the
   versions it holds: 2^40 of them last three years at 10,000 new versions
   a second. A file with no e_tag isn't cached: nothing would tell a new
   version of it from the cached one.
6. **The index is outside the cache budget.** Its nodes come from the
   global allocator, a cost of having a cache at all. It is bounded: at
   most one block entry per chunk, memory and disk together.

### Eviction

7. **The policy is a trait the user can supply** (`src/cache/policy/`),
   one for both tiers, every hook `&self`: the cache keeps no access state
   of its own, so a policy keeps what it wants, with what concurrency it
   wants. Its hooks:
   - `on_memory_hit(chunk, read)`;
   - `on_disk_hit(chunk, read)`, which says whether the block stays on disk
     or moves to memory: asked before the read, so a move reads the block's
     pages straight into a memory chunk, one I/O for both;
   - `on_miss(read)`, which says where the block goes once fetched: memory,
     disk, or nowhere. Asked as the read plans its fetches, so a policy
     hears every miss, those it doesn't cache too, and the read carries the
     answer to the fill;
   - `on_admit(tier, chunk, block)`: a chunk took a block;
   - `pick_memory_victims(count)`, each moved to disk or dropped, and
     `pick_disk_victims(count)`, dropped.

   Every hook on the read path gets the block, the file's size and path,
   and the range the read asked for. A victim the cache gives back, for
   want of room on disk or a failed write, goes back through `on_admit`;
   so a policy that forgets a chunk as it picks it never loses one. The
   default is a lock-free CLOCK for each tier: misses go to memory, memory
   victims to disk, and a disk hit moves to memory on its second hit since
   the hand passed.
8. **Tiers are exclusive, and eviction runs at insertion.** A block is in
   one tier, which the index names. A fill, a move or a promotion that
   finds its tier full evicts a batch of the policy's victims first (up to
   eight, never more than a sixteenth of the tier, so a small tier isn't
   emptied for one block; up to four rounds, as a victim's chunk goes free
   only once its readers leave); a block that still finds no room isn't
   cached.
   Memory's moves to disk write at once, and may make the disk evict; disk
   victims are dropped, so it ends there. A chunk table, indexed by chunk
   number, holds each chunk's owner (its block's key) and a count of its
   holders: readers copying from a memory chunk or reading a disk chunk,
   a writer, an I/O in flight. A chunk goes free only once nothing holds
   it. Evicting a chunk changes its block's entry only if the entry still
   names that chunk (`update`, `remove_if`).

### Persistence

9. **A restart loses the whole cache,** after a crash or a clean
   shutdown alike. The index lives only in memory, so the cache starts
   cold, every chunk free, and refills from the object store: that costs
   time and egress, never correctness. So nothing is logged while the
   cache runs: no WAL, no tombstones, no `O_DSYNC`. A block reaches disk
   only when memory evicts it, as a plain `O_DIRECT` write, and a freed
   disk chunk can be reused at once. Keeping the disk tier across restarts
   comes later ("Later").

### Front end

10. **t4 fetches from the object store itself.** It wraps an
    `object_store::ObjectStore`, and its API is that store's: a read names
    a path and a range, and t4 turns the blocks it misses into GETs. So t4
    decides how misses are split and merged into requests, and can keep the
    NIC busy (`00-meta.md`). A file with no e_tag is read from the store
    each time, never cached (decision 5).

## Rejected

- **bbtree-store for the bytes.** After splits a heap page holds other
  leaves' records, so it can't be evicted alone; a long value lives in a
  chain of overflow pages, one dependent read each, whose 8-byte links
  leave the bytes unaligned for `O_DIRECT`; and a get copies the whole
  value.
- **Variable-size extents with a free-space allocator:** free extents
  indexed by address and by size, merged on free (XFS), or a bitmap with
  group summaries (ext4). It works, but a cache is always full, the worst
  case for fragmentation, and eviction must then choose between the
  coldest data and the data next to free space. Fixed chunks remove both.
- **One key per path, its value a list of ranges.** Ranges of any size
  overlap and must merge.
- **A per-file header: a bitmap of cached blocks and an array of their
  chunks.** Every block cached or evicted shifts the array, and lookups
  count bits across the whole bitmap. Kept in the file's first data chunk,
  it also costs a chunk per file, adds a disk read to every lookup, and
  caps the file size.
- **32-bit file ids, so that a block key is a `u32` pair.** 4.3 billion
  ids, never reused, run out in 50 days at 1,000 new versions a second;
  24 bits of block number are enough.
- **Recording each chunk's block length, so that a move to disk writes only
  the pages a short block uses.** Only a file's last block is short: for
  files of 100 MB and more it saves a fraction of a percent of the writes.
  It would save more on many objects under a block.
- **An access bit in each chunk's state word, the cache's own.** It fits
  one policy, CLOCK; a policy that counts, or keeps queues, needs its own
  state anyway. So the policy hears every hit and keeps what it wants.
- **Inclusive tiers: a block in memory and on disk at once.** Moving a
  memory victim to disk would cost nothing when a disk copy exists, and a
  promotion would leave the disk copy; but the index would name two chunks
  a block, and every eviction change half an entry. Naming only the fast
  copy, the slow one remembered elsewhere, lets the two disagree.
- **A WAL, so that the disk tier survives a crash.** Every eviction batch
  logs its blocks, with `O_DSYNC`. A freed disk chunk can't be reused until
  a tombstone naming its old block is durable, else replay maps the old
  block to the new bytes: a pending list, and tombstones carried by the
  next batch's WAL write. And in a fixed-size file, the log needs a region
  of its own and compaction. All of it to save one refill after a crash.

## Later

- **Two readers missing one block:** the second waits on the first's GET
  through `Fetching` (request coalescing, `00-meta.md`).
- **Eviction off the insertion path:** a background thread keeping free
  chunks between two watermarks, if evicting at insertion costs too much.
- **The drive's own garbage collection:** a file kept full leaves the SSD
  no spare room. Leave part of the drive unwritten, or discard freed
  chunks.
- **Keeping the disk tier across a clean restart:** a snapshot of the
  index's disk half, written at shutdown once the writes in flight are
  done and the drive flushed: a file record (file version id, path, e_tag,
  size) for each path with blocks on disk, and a block entry (block key,
  chunk), 12 bytes, for each of those blocks. About 50 MB of block entries
  at 4 TB. Best in a file of its own next to the cache file, written as a
  temporary file and renamed: the rename is atomic, so the file being
  there says the snapshot is whole. Startup loads it, then deletes it
  before it writes any chunk, so that a later crash can't leave it naming
  chunks reused since. Open then: whether blocks in memory are written to
  disk first, so that the snapshot keeps them.
- **Then across a crash, if a refill proves too costly:** write the
  snapshot every minute or so, and keep each freed disk chunk unused until
  the next snapshot is durable. After a crash the last snapshot is still
  true, as no chunk it lists was reused since: a crash loses only the
  blocks written since, for about 150 MB written a minute at 4 TB and a
  reserve of the chunks freed meanwhile.

## What changes in today's code

- **Free space:** `FileHoles` (`src/input_kv.rs`), holes by size, becomes a
  bitmap of chunks, all free at startup.
- **Index:** `Index` (`src/index.rs`) becomes `paths` and `blocks`.
- **The WAL** (`src/wal/`) goes, with replay. A later snapshot may reuse
  its page and entry code, a checksum on each entry.
- **Writes:** no `O_DSYNC`. A put no longer goes to disk as it arrives;
  only evictions write.
- **Verus:** proofs of the WAL format, replay and `FileHoles` go with that
  code, and come back once the design settles.
- **Partial reads:** the windows of `src/disk.rs` keep reading 4 KB pages,
  now inside a chunk.
