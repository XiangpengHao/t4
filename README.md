# `t4`

`t4` is a local, single-file object store implemented in user space.

It behaves like a key-value store and is optimized for larger payloads (roughly `>4 KB`).

## Features

- Single-file storage layout
- `io_uring` for all I/O

## Usage

Values are written and read by key. Reads support full-value and range access.

```rust
// async example (runtime-agnostic futures; poll with your executor)
let store = t4::Store::mount("your-data.t4").await?;

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

For tests or environments where `O_DIRECT` / `O_DSYNC` are not available, use mount options:

```rust
let store = t4::Store::mount_with_options(
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
2. Production mount defaults use `O_DIRECT` and `O_DSYNC`.
3. Deletes append tombstones; space is not reclaimed in v1.
4. Metadata lives in linked 4 KB index pages and is rebuilt into memory on mount.

