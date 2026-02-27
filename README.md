# `t4`

`t4` is a local, embedded, high-performance object store.

## Features

- `io_uring` for all I/O, scale to modern SSDs.
- Deterministic, predictable performance, one request is one I/O.

## Usage

Values are written and read by key. Reads support full-value and range access.

```rust
let store = t4::mount("your-data.t4").await?;

store
    .put(b"greeting".to_vec(), b"Hello, world!".to_vec())
    .await?;

let content = store.get(b"greeting".to_vec()).await?;
assert_eq!(content, b"Hello, world!");

let slice = store.get_range(b"greeting".to_vec(), 7, 5).await?;
assert_eq!(slice, b"world");

let removed = store.remove(b"greeting".to_vec()).await?;
assert!(removed);
```

To tune I/O behavior, use mount options:

```rust
let store = t4::mount_with_options(
    "your-data.t4",
    t4::MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    },
)
.await?;
```

## Notes

1. `t4` targets `io_uring` for all reads and writes.
2. `t4` supports Linux hosts only.
3. Production mount defaults use `O_DIRECT` and `O_DSYNC`.
4. Deletes append tombstones; space is not reclaimed in v1.
5. Metadata lives in linked 4 KB WAL pages and is rebuilt into memory on mount.
6. WAL manager is the single allocator for both WAL-page and value-page growth.
