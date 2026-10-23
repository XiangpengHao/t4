# 00 — Meta

## Pivot Oct-20

I want to pivot `t4` into a object store cache that holistically optimize two goals: 1. saturate NIC bandwidth when there's a cache miss, 2. saturate disk bandwidth when there's a cache hit. Or, use minimal threads to saturate bandwidth.

The interface of this caching will be very similar to object_store, but internally it handles: 1. cache replacement, 2. request coalescing and splits, 3. io_uring and multi-device read, 4. racing read for latency critical request (like metadata).

But so far, we only focus on the second part.

Built so far (2026-10-22, `03-cache.md`, `04-multi-device.md`):
`t4::T4Caching`, an `object_store::ObjectStore` that caches another in
memory and on local drives, with cache replacement (a pluggable policy, a
lock-free CLOCK by default) and io_uring reads and writes, chunks striped
over a file on each drive. Not yet: request coalescing and splits, racing
reads.

## What It Is

`t4` is a cache for object stores. `T4Caching` wraps an
`object_store::ObjectStore` (S3, GCS, Azure, or any other) and keeps what is
read in two tiers of fixed size: memory, and a file on each local NVMe
drive it is given, chunks striped over them, read and written with
`O_DIRECT` through io_uring (`04-multi-device.md`). It fetches what
only the remote holds itself. The design: `03-cache.md`.

## The docs

- `00-meta.md`: this.
- `03-cache.md`: the cache: its design, the decisions behind it, what's
  built, and what comes later.
- `04-multi-device.md`: the disk tier over several devices: chunks
  striped over the cache files by number, one I/O worker for all.
- `05-network.md`: what a remote block's bytes cost over HTTPS, object_store's
  HTTP transport against one over kTLS, measured against S3.
- `06-read-modes.md`: cost, throughput and latency reads; what t4
  guarantees (mechanism) and what a placement policy and a read policy
  decide.
- `98-cachefs-vs-fs.md`: how a cache's storage differs from a file
  system's.
- `99-brainstorm.md`: open notes.

## Platforms

**Linux is the only intended platform.** Design decisions and performance
work target Linux: io_uring and `O_DIRECT`. The cache makes no durability
guarantee: a restart starts it cold.

**macOS is for development only**: to build t4 and test that it works on a
developer's machine, never to run it for real. CI's macOS job exists to keep
that working. On macOS:

- I/O goes through the fallback backend (a thread pool doing `pread` and
  `pwrite`), not io_uring, so its performance says nothing about Linux's.
- Direct I/O is `F_NOCACHE`, not `O_DIRECT`.

Don't design, tune or benchmark for macOS, and don't let it constrain a Linux
design: a macOS fallback only has to work.

## Verification

Parts of the crate are written inside `verus!` and proven with
[Verus](https://github.com/verus-lang/verus): alignment (`src/buffer.rs`)
and range requests (`src/range.rs`). The proofs cover that arithmetic only,
not the OS, filesystem or hardware. Outside `verus!`, Verus compiles the
code but checks nothing.

```bash
cargo verus focus    # this crate's proofs only, as CI runs them
```

`cargo verus verify` also verifies every dependency that carries Verus
metadata, bbtree included; `focus` leaves those to their own CI.

The cache's logic has no proofs: its concurrency, in the chunk table's
atomics, the CLOCK's, and reads, fills and evictions racing on a simulated
disk whose I/O completes in any order, is tested under
[shuttle](https://github.com/awslabs/shuttle), which runs each test's
threads through a thousand schedules. Shuttle runs threads one at a time,
so it finds interleavings, not memory orderings too weak: those rest on the
`Acquire` and `Release` pairs `src/cache/chunk_pool/chunk_table.rs` explains.

```bash
cargo test --features shuttle shuttle    # the shuttle tests, as CI runs them
```

A fuzz target checks the cache as a whole against the object store under
it, writes behind its back included, and the cache's index and tiers
against each other after every operation (`fuzz/README.md`).

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10
```
