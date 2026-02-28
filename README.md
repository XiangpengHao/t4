# `t4`

`t4` is a local, embedded, high-performance object store. 

## Features

- Performance, correctness, and ergonomics, pick three. 
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

## Limitations

Currently it only supports files up to 4 GB.

## Vision

`t4` will be the ultimate and only file system you need.