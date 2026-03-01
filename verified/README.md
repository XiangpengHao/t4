`t4` uses [verus](https://github.com/verus-lang/verus) to verify that the implementation adheres to the design spec.

## Usage

```bash
cargo verus build
```

You should see:
```
cargo verus build
   Compiling verified v0.1.0 (/home/hao/coding/t4/verified)
verification results:: 71 verified, 0 errors
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.95s
```

#### Formatting

```bash
verusfmt verified/src/**/*.rs
```
