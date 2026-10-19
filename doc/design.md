# t4 Design Summary

## I/O Model (`io_uring` First)

All disk I/O goes through raw `io_uring` operations (`Read`, `Write`).

Why this matters:

- No split implementation between sync I/O and `io_uring`
- Direct control over queue depth and submission/completion flow
- Better fit for a pinned worker / thread-per-core execution model
- Linux-only implementation

## Core Operations

### `mount`

- Open/create store file with `O_DSYNC`, and `O_DIRECT` unless `direct_io` is off
- If empty: write an empty WAL page at offset `0`
- If existing: replay WAL pages and rebuild the in-memory map

### `put(key, value)`

1. WAL manager allocates value space from file tail and appends value bytes (4 KB padded)
2. WAL manager appends a live WAL entry `(key, offset, length, lsn)`
3. Update the in-memory index, unless a record with a higher LSN for the key got there first

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
- Mark the key removed in the in-memory index (a tombstone entry, so an older concurrent put can't resurrect it)
- Old value bytes remain on disk (no reclaim in v1)
