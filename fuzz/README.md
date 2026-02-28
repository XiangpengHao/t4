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
cargo fuzz run model_equivalence
```

Useful options:

```bash
# run longer
cargo fuzz run model_equivalence --release -- -max_total_time=600

# run with multiple jobs
cargo fuzz run model_equivalence --release -- -jobs=4 -workers=4
```

## Corpus and artifacts

- Seed corpus lives in `fuzz/corpus/model_equivalence/`
- Crashes and reproducers go to `fuzz/artifacts/model_equivalence/`

To run a saved crashing input:

```bash
cargo fuzz run model_equivalence fuzz/artifacts/model_equivalence/<crash-file>
```

## What is being checked

Random sequences include:

- `put(key, value)`
- `get(key)`
- `get_range(key, start, len)`
- `remove(key)`
- `sync()`
- remounting the same backing file

The target checks:

- operation results match the `HashMap` model
- `len()` and `is_empty()` match the model
- all model values remain readable after remount

This gives strong coverage for logical correctness and persistence-related regressions.
