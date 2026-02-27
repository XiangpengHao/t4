# `t4`

`t4` is a local, embedded, high-performance object store.

## Features

- `io_uring` for all I/O, scale to modern SSDs.
- Deterministic, predictable performance, one request is one I/O.
- Runtime-agnostic async API.

## Usage

Values are written and read by key. Reads support full-value and range access.

```rust
let store = t4::mount("your-data.t4").await?;

store.put(b"a.txt", b"Hello, world!").await?;

let content = store.get(b"a.txt").await?;
assert_eq!(content, b"Hello, world!");

let slice = store.get_range(b"a.txt", 7, 5).await?;
assert_eq!(slice, b"world");

let removed = store.remove(b"a.txt").await?;
assert!(removed);
```

## Notes

1. `t4` targets `io_uring` for all reads and writes.
2. `t4` supports Linux hosts only.
3. Production mount defaults use `O_DIRECT` and `O_DSYNC`.
4. Deletes append tombstones; space is not reclaimed in v1.
5. Metadata lives in linked 4 KB WAL pages and is rebuilt into memory on mount.
6. WAL manager is the single allocator for both WAL-page and value-page growth.

## Limitations

Currently it only supports files up to 4 GB.
