# 00 — Meta

## Platforms

**Linux is the only intended platform.** Design decisions, performance work
and durability guarantees target Linux: io_uring, `O_DIRECT` and `O_DSYNC`.

**macOS is for development only**: to build t4 and test that it works on a
developer's machine, never to run it for real. CI's macOS job exists to keep
that working. On macOS:

- I/O goes through the fallback backend (a thread pool doing `pread` and
  `pwrite`), not io_uring, so its performance says nothing about Linux's.
- Direct I/O is `F_NOCACHE`, not `O_DIRECT`.
- `O_DSYNC` writes don't flush the drive's cache, so a put isn't durable
  against power loss until `Store::sync()` (`F_FULLFSYNC`). The crash
  guarantees in these docs hold on Linux only.

Don't design, tune or benchmark for macOS, and don't let it constrain a Linux
design: a macOS fallback only has to work.

## Verification

Parts of the main crate are written inside `verus!` and proven with
[Verus](https://github.com/verus-lang/verus): the WAL page format and replay
(`src/wal/page.rs`, `src/wal/replay.rs`), keys, values and holes
(`src/input_kv.rs`), alignment (`src/buffer.rs`), range requests
(`src/range.rs`) and file-tail reservation (`src/disk.rs`). The proofs cover
those models only, not the OS, filesystem or hardware. Outside `verus!`, Verus
compiles the code but checks nothing.

```bash
cargo verus focus    # this crate's proofs only, as CI runs them
```

`cargo verus verify` also verifies every dependency that carries Verus
metadata, bbtree included; `focus` leaves those to their own CI.
