# t4 fuzzing

`cache_equivalence` runs random sequences of operations on `T4Caching` over
an in-memory `object_store`:

- writes through the cache: puts, multipart puts, deletes, copies and
  renames;
- writes behind its back, straight to the store;
- heads, and reads of every shape: whole, `GetRange::Bounded`, `Offset` and
  `Suffix`, some joined so they interleave at each I/O, and some given up
  on after a few polls, their futures dropped midway through a fetch, a
  fill, an eviction or a promotion.

Its tiers are 1 to 4 blocks of memory and 1 to 6 on disk, and its files up
to four blocks, so eviction, moves to disk and promotions start within a few
operations. Its policy is the default CLOCK, or one the input scripts, which
also picks victims that hold nothing.

It checks that:

- every answer is one the cache may give: the file as the store holds it,
  or, after a write behind the cache's back, a version the cache may still
  trust (`doc/03-cache.md`, decision 11);
- after every operation, once the I/O a given-up read left in flight is
  done, the cache's index and tiers agree: each entry names a chunk that
  holds its block, each chunk that holds a block has an entry, and every
  other chunk is free (`T4Caching::check_at_rest`, behind the `__fuzz`
  feature);
- the scripted policy can still pick every chunk that holds a block: the
  cache frees each victim, or gives it back;
- the cache never names a chunk past its tier's to the policy.

## Run it

From the repository root, in the dev shell, which has `cargo-fuzz` and a
nightly toolchain:

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10
```

`TMPDIR=/dev/shm` puts each run's cache file on tmpfs, which the target can
use as it turns direct I/O off. There io_uring completes writes later, in
its worker threads, as a drive completes direct I/O, not inline as on ext4:
more I/O is in flight at once. A scenario takes milliseconds, so
`-timeout=10` reports a hang in seconds, not libFuzzer's default 20 minutes.
It runs about 100 scenarios a second, of up to 32 operations each.

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10 -max_total_time=600
```

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10 -jobs=8 -workers=8
```

A failure prints the scenario. To print a saved one again:

```bash
cargo fuzz fmt cache_equivalence fuzz/artifacts/cache_equivalence/<file>
```

## What it leaves to others

- Threads racing: the shuttle tests (`cargo test --features shuttle
  shuttle`). The target's concurrent reads share one thread.
- Writes given up on: its store applies a write in the call's first poll,
  so there's no point to give one up at. The unit test
  `a_write_given_up_on_after_it_lands_is_read_back` covers them.
- Direct I/O, and I/O or a store that fails.
