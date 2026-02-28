# t4 fuzzing

This directory contains `cargo-fuzz` targets for `t4`.

The main target (`model_equivalence`) runs random operation sequences against:

- the real `t4::Store`
- an in-memory `HashMap<Vec<u8>, Vec<u8>>` model

and asserts both produce the same behavior.

It also forces periodic `sync` + remount checks to ensure values are durable and no data is lost across remount.

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
RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence
```

Useful options:

```bash
# run longer
RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence --release -- -max_total_time=600

# run with multiple jobs
RUSTFLAGS="-C force-frame-pointers=yes" cargo fuzz run model_equivalence --release -- -jobs=4 -workers=4
```
