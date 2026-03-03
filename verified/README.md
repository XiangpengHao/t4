# `t4-verified`

`t4-verified` contains formal specifications and proofs for the `t4` storage model, plus executable helper code consumed by `t4`.
The proofs are checked with [Verus](https://github.com/verus-lang/verus). They cover the modeled invariants in this crate; they do not prove behavior outside those models (for example OS, filesystem, and hardware behavior).

## Usage

```bash
cargo verus build
```

You should see:
```
cargo verus build
   Compiling t4-verified v0.1.0 (/home/hao/coding/t4/verified)
verification results:: 71 verified, 0 errors
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.95s
```

#### Formatting

```bash
verusfmt verified/src/**/*.rs
```
