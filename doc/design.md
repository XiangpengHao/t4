# t4 Design Summary

## What It Is

`t4` is a single-file local object store with a key/value API optimized for larger values (roughly `>4 KB`).

Core design goals:

- Use `io_uring` for all reads/writes from day one
- Keep the on-disk format simple and easy to rebuild
- Optimize for append-heavy workloads and point lookups

## What It Is Not (v1)

- No on-disk hash/tree index
- No space reuse / compaction
- No checksums
- No key iteration / scans
- No synchronous `pread`/`pwrite` fallback path

## File Layout (Single File)

The store is one file split into:

1. `Index pages` (4 KB each, linked list)
2. `Data region` (value bytes, padded to 4 KB)

Conceptually:

```text
offset 0
+-------------------+
| index page 0      |
+-------------------+
| index page N      |  (linked by next_page offsets)
+-------------------+
| data region       |  (append-only values, 4 KB aligned)
+-------------------+
```

Important details:

- Page size is fixed at `4096` bytes
- The first index page is always at offset `0`
- New index pages are appended when the current page is full
- Values are appended and padded to a 4 KB boundary for direct I/O alignment

## Index Page Format

Each index page stores:

- `magic`
- `version`
- `next_page` (offset of next index page, `0` if none)
- `entry_count`
- variable-length entries

Each entry stores:

- `key_len`
- `flags` (`live` or `tombstone`)
- `offset`
- `length`
- `key bytes`

This acts like a durable append log for metadata.

## In-Memory State (Built at Mount)

On mount, `t4` walks all index pages and rebuilds:

- `HashMap<Vec<u8>, ValueRef>` for point lookups
- `bump pointer` (next append offset)
- current/last index page state

Tombstones remove keys from the in-memory map during rebuild.

## I/O Model (`io_uring` First)

All disk I/O goes through raw `io-uring` operations (`Read`, `Write`, `Fsync`).

Why this matters:

- No split implementation between sync I/O and `io_uring`
- Direct control over queue depth and submission/completion flow
- Better fit for a pinned worker / thread-per-core execution model

## Core Operations

### `mount`

- Open/create store file (targeting `O_DIRECT` + `O_DSYNC` in production)
- If empty: write an empty index page at offset `0`
- If existing: scan linked index pages and rebuild the in-memory map

### `put(key, value)`

1. Append value bytes to the data region (4 KB padded)
2. Append a live index entry `(key, offset, length)`
3. Update in-memory `HashMap`

If the current index page is full:

- Allocate and write a new index page at end-of-file
- Update previous page’s `next_page`

### `get(key)`

1. Lookup `(offset, length)` in memory
2. Read aligned data window from disk
3. Return exactly `length` bytes (strip padding)

### `get_range(key, start, len)`

- Reads the minimal aligned disk window covering the requested range
- Returns only the requested slice

### `remove(key)`

- Append a tombstone index entry for the key
- Remove key from in-memory `HashMap`
- Old value bytes remain on disk (no reclaim in v1)

## Human-Readable Examples

### Example: Store and read a value

```rust
let store = t4::Store::mount("demo.t4").await?;

store.put(b"user:42".to_vec(), b"Alice".to_vec()).await?;

let value = store.get(b"user:42".to_vec()).await?;
assert_eq!(value, b"Alice");
```

What happens internally:

- `"Alice"` is appended to the data region (4 KB padded on disk)
- A metadata entry for `user:42` is appended to the current index page
- The in-memory `HashMap` is updated to point to the new value

### Example: Range read

```rust
store
    .put(b"blob".to_vec(), b"hello-0123456789-world".to_vec())
    .await?;

let part = store.get_range(b"blob".to_vec(), 6, 10).await?;
assert_eq!(part, b"0123456789");
```

Internally, `t4` may read a larger aligned window (4 KB boundaries) and return only the requested slice.

### Example: Delete (tombstone)

```rust
let removed = store.remove(b"user:42".to_vec()).await?;
assert!(removed);
```

What persists on disk:

- A tombstone entry for `user:42`
- The old value bytes are still present but no longer visible

After remount, scanning the index pages rebuilds the `HashMap` and keeps the key deleted.

## Important Constraints / Tradeoffs

- **Append-only growth**: file size only increases in v1
- **Mount cost grows with metadata history**: full index-page scan is required
- **Deletes do not reclaim space**: tombstones only affect visibility
- **Point lookups are fast** after mount because they hit the in-memory `HashMap`
- **Range reads must honor alignment** because of direct I/O constraints

## Concurrency Model (Target Direction)

The intended model is pinned worker threads (thread-per-core):

- Each worker owns its own `IoUring`
- Avoid shared-ring contention in v1
- Define routing/ownership strategy before multi-worker access to one store file

## Future Work (Expected)

- Compaction / space reclamation
- Checksums and corruption detection
- Iteration / scans
- Stronger concurrency coordination across workers
- Optional async runtime integrations (without changing on-disk format)
