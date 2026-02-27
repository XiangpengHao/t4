# t4 Design Summary

## What It Is

`t4` is a single-file local object store with a key/value API optimized for larger values (roughly `>4 KB`).

Core design goals:

- Use `io_uring` for all reads/writes from day one
- Keep the on-disk format simple and easy to rebuild
- Optimize for append-heavy workloads and point lookups


## File Layout (Single File)

The store is one file. On disk, there is no separate index — only a write-ahead log (WAL) and data blocks. The WAL is a linked list of 4 KB pages that record every mutation (puts and deletes). On startup, the WAL is replayed to build an in-memory `HashMap` for point lookups.

```text
offset 0
+-------------------+
| WAL page 0        |
+-------------------+
| data page(s)      |
+-------------------+
| WAL page N        |  (linked by next_page offsets)
+-------------------+
| data page(s)      |
+-------------------+
```

WAL is one logical linked space. WAL pages may be physically interleaved with data pages as the file grows.

Important details:

- Page size is fixed at `4096` bytes
- The first WAL page is always at offset `0`
- New WAL pages are appended when the current page is full
- Values are appended and padded to a 4 KB boundary for direct I/O alignment
- WAL and value allocation both come from WAL manager-owned file tail state
- WAL pages stay WAL-only (metadata never spills into value pages)

## WAL Page Format

Each WAL page stores:

- `magic`
- `version`
- `next_page` (offset of next WAL page, `0` if none)
- `entry_count`
- variable-length entries

Each entry stores:

- `key_len`
- `flags` (`live` or `tombstone`)
- `offset`
- `length`
- `lsn` (monotonic log sequence number carried by each entry)
- `key bytes`

The WAL is a durable append log for metadata.

## In-Memory State (Built at Mount)

On mount, `t4` replays all WAL pages and rebuilds:

- `HashMap<Vec<u8>, ValueRef>` for point lookups
- WAL manager state: file tail (next free page-aligned offset), current WAL tail page, and latest seen LSN

Tombstones remove keys from the in-memory map during replay.

## I/O Model (`io_uring` First)

All disk I/O goes through raw `io_uring` operations (`Read`, `Write`, `Fsync`).

Why this matters:

- No split implementation between sync I/O and `io_uring`
- Direct control over queue depth and submission/completion flow
- Better fit for a pinned worker / thread-per-core execution model
- Linux-only implementation

## Core Operations

### `mount`

- Open/create store file (targeting `O_DIRECT` + `O_DSYNC` in production)
- If empty: write an empty WAL page at offset `0`
- If existing: replay WAL pages and rebuild the in-memory map

### `put(key, value)`

1. WAL manager allocates value space from file tail and appends value bytes (4 KB padded)
2. WAL manager appends a live WAL entry `(key, offset, length, lsn)`
3. Update in-memory `HashMap`

If the current WAL page is full:

- Allocate and write a new WAL page from WAL manager file tail
- Update previous page's `next_page`

### `get(key)`

1. Lookup `(offset, length)` in memory
2. Read aligned data window from disk
3. Return exactly `length` bytes (strip padding)

### `get_range(key, start, len)`

- Reads the minimal aligned disk window covering the requested range
- Returns only the requested slice

### `remove(key)`

- WAL manager appends a tombstone WAL entry for the key
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
- A WAL entry for `user:42` is appended to the current WAL page
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

- A tombstone WAL entry for `user:42`
- The old value bytes are still present but no longer visible

After remount, replaying the WAL rebuilds the `HashMap` and keeps the key deleted.

## Important Constraints / Tradeoffs

- **Append-only growth**: file size only increases in v1
- **Mount cost grows with WAL history**: full WAL replay is required
- **Deletes do not reclaim space**: tombstones only affect visibility
- **Point lookups are fast** after mount because they hit the in-memory `HashMap`
- **Range reads must honor alignment** because of direct I/O constraints

## Concurrency Model (Target Direction)

The intended model is pinned worker threads (thread-per-core):

- Each worker owns its own `io_uring` backend
- Avoid shared-ring contention in v1
- Define routing/ownership strategy before multi-worker access to one store file
