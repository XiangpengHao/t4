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


## I/O backends

`t4` ships with two interchangeable I/O backends, selected at compile time:

- **Generic** (default, BSD/Linux): a thread pool dispatching POSIX `pread`/`pwrite`/`fsync`. 
- **`io_uring`** (Linux 6.x+ only): opt in with the `io-uring` Cargo feature. 

```sh
cargo build --features io-uring
```

Adding additional backends is a matter of implementing the `IoDriver` trait in `src/io/common.rs`

## Benchmarks

```sh
# Generic backend
cargo bench --features __bench --bench io_worker_throughput

# io_uring backend
cargo bench --features "__bench io-uring" --bench io_worker_throughput
```

```sh
cargo bench --features __bench --bench io_worker_throughput -- --sample-count 50
```

## Limitations

File name is up to 256 bytes, file size is up to 4 GB.

## Vision

`t4` will be the ultimate and only file system you need.
