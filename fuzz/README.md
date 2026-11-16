# t4 fuzzing

Three targets, each run for an hour every Monday by
`.github/workflows/fuzz.yml`, from an empty corpus: nothing is kept between
runs.

## `cache_equivalence`

Random sequences of operations on `T4Caching` over an in-memory
`object_store`:

- writes through the cache: puts, multipart puts, deletes, copies and
  renames;
- writes behind its back, straight to the store;
- heads, and reads of every shape: whole, `GetRange::Bounded`, `Offset` and
  `Suffix`, some joined so they interleave at each I/O, and some given up
  on after a few polls, their futures dropped midway through a fetch, a
  fill, an eviction or a promotion;
- shutdowns, after which the cache serves reads but writes no more to
  disk, and restarts: the cache dropped, keeping its disk tier in a
  snapshot, and built again on its files, taking it back. Where the cache
  files' file system reports no creation time, the cache keeps none, and a
  restart starts cold.

Its tiers are 1 to 4 blocks of memory and 1 to 6 on disk, and its files up
to four blocks, so eviction, moves to disk and promotions start within a few
operations. Its placement policy is the default CLOCK, or one the input
scripts, which also picks victims that hold nothing. Its read policy is the
default, or one whose plans the input scripts, many of which the cache must
refuse. Its files are allocated eagerly or on demand.

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

Where io_uring is refused (`EPERM` or `ENOSYS`), it skips every scenario and
passes. GitHub's runners allow io_uring, as CI's tests use it; a runner that
didn't would make the Monday run pass without testing anything.

## `frames`

Shared mode's frames from bytes a peer could send, as a client's frame and
as an owner's: each is refused, or decodes to a frame that encodes back to
the same bytes.

## `snapshot`

The disk tier's snapshot, after a valid header: its body as the input's
bytes, or encoded from versions and blocks the input picks, small enough
that many are valid and many are off by a little. Each is refused, or
encodes back to the same bytes and is taken back by a cache whose index and
tiers then agree, each version's size known. A snapshot has
no checksum: one whose bytes were changed into another valid snapshot is
taken back as written, so this target checks the cache's consistency, not
the bytes it would serve.

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

```bash
cargo fuzz run frames -- -timeout=10 -max_total_time=600
```

AddressSanitizer's own memory grows with every cache an input builds,
about 100 KB a scenario of `cache_equivalence` on macOS, and reaches
libFuzzer's 2 GB limit within minutes; without it (`-s none`) memory stays
flat. For a long run, fuzz a minute at a time, as the Monday run does, or
pass `-s none`.

```bash
TMPDIR=/dev/shm cargo fuzz run cache_equivalence -- -timeout=10 -jobs=8 -workers=8
```

## A failure

A failure prints the input. On a Monday run, the job's artifact
`fuzz-<target>` holds it:

```bash
gh run download <run id> -n fuzz-<target> -D fuzz/artifacts/<target>
```

```bash
cargo fuzz fmt <target> fuzz/artifacts/<target>/<file>
```

```bash
cargo fuzz run <target> fuzz/artifacts/<target>/<file>
```

The second prints the input; the third runs it again.

## What it leaves to others

- Threads racing: the shuttle tests (`cargo test --features shuttle
  shuttle`). The target's concurrent reads share one thread.
- Writes given up on: its store applies a write in the call's first poll,
  so there's no point to give one up at. The unit test
  `a_write_given_up_on_after_it_lands_is_read_back` covers them.
- Direct I/O, and I/O or a store that fails.
- Shared mode's owner and clients talking: `frames` checks the codec only.
