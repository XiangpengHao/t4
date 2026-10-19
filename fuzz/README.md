# t4 fuzzing

This directory contains `cargo-fuzz` targets for `t4`.

The main target (`model_equivalence`) runs random operation sequences against:

- the real `t4::Store`
- an in-memory `HashMap<Vec<u8>, Vec<u8>>` model

and asserts both produce the same behavior.

It also remounts periodically to check that no data is lost across a remount.

## Prerequisites

1. Linux (same as `t4` runtime requirements)
2. Rust toolchain installed
3. `cargo-fuzz` installed:

```bash
cargo install cargo-fuzz
```

## Run the fuzz target

From repository root:

```bash
TMPDIR=/dev/shm RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence
```

The store always writes with `O_DSYNC`, so each write waits for the disk.
`TMPDIR=/dev/shm` puts the target's store files on tmpfs, where that's
free: about 240 runs a second there, against 40 on an NVMe drive.

Useful options:

```bash
# run longer
TMPDIR=/dev/shm RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence --release -- -max_total_time=600

# run with multiple jobs
TMPDIR=/dev/shm RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence --release -- -jobs=4 -workers=4
```
