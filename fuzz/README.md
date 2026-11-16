# t4 fuzzing

Five targets, each run for an hour every Monday by
`.github/workflows/fuzz.yml`, from an empty corpus: nothing is kept between
runs. The same workflow runs the shuttle tests on fresh seeds (A failure,
below).

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
refuse.

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

## `shared_mode`

Shared mode's owner and two or three clients in one process, as the sharing
tests build them, on the simulated socket and clock
(`doc/12-seeded-concurrency-tests.md`), driven by a script
(`src/cache/t4_caching/shared_scenario/`). Its input is the script and a
shuttle seed: the target runs the one PCT schedule the seed picks, so an
input replays exactly. The script, each step aimed at any process:

- reads of every shape, on the script's thread, joined so they interleave,
  or on threads of their own, answered at a later step; some given up on
  after a few polls;
- writes behind every cache's back, straight to the store;
- a process dropped (an owner's files handed to the next to try them),
  killed (no disk tier kept beyond its last shutdown), dropped mid-read,
  shut down, or built again on the same files;
- a process's GETs held until released, time passing, and the script's
  thread stepping aside so that the others run till they wait.

The processes' tiers hold every block a script fetches, or a few. Blocks go
to memory by CLOCK, or all to disk. A socket's shutdown may lose the wakeup
of a thread reading its own end, as macOS does; time passes while every
thread waits, so a wait that looks again a second later comes due.

It checks that:

- every read answers bytes a cache may give: the file as the store holds
  it, or a version written over since the process was built, which its
  cache may still trust (`doc/03-cache.md`, decision 11);
- once no read is in flight: the owner's index and tiers agree, no claim
  or call waits, every client's region spans are free, and the owner runs
  a session for each client connected;
- each block of a file written once is fetched once, and once more for
  each step since that could drop it or abandon its fetch: a process
  dropped, killed or restarted, or a read given up. Unchecked for a file
  written again, as the owner keeps one version of a path and a process
  that still trusts another replaces it at each read; where more blocks
  were fetched than the tier holds; and once a cache on disk shut down;
- no thread is left waiting once every process is dropped: shuttle reports
  a deadlock.

Like `shared_owner`, it runs on shuttle only when built with
`--cfg t4_simulated`.

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

## `shared_owner`

Shared mode's owner, serving a client whose frames the input picks, while a
well-behaved client (a `T4Caching` built for sharing) reads beside it
(`doc/11-shared-mode.md`). A client is another process: an older version, a
buggy one, or one killed mid-frame. The owner and both clients run on
shuttle, over the modeled socket and region (`doc/12-seeded-concurrency-tests.md`),
each input under PCT from a seed of its own, so it replays exactly. Its
`T4Caching` and `ClientRegion` are the real ones; the owner holds two blocks
in memory and three on disk.

The misbehaving client's hello is a proper one, of another version, with a
read-only descriptor, or any bytes. Then it sends well-formed frames, most
where the protocol doesn't allow them: reads, fetches and calls it never
started, or ended twice, other reads' numbers, `Land`, `Commit` and `Abandon`
out of order, spans past the region, overlapping or reused, `EndRead` before
`Finish`; frames of any body, or a length and fewer bytes than it says, the
client killed mid-frame; writes to its own region, lent or not. `Proceed`
steps send what a well-behaved client would next for a read, as far as the
owner's answers say, so that the others come mid-read.

It checks that:

- the owner never panics, and never hangs: once the script is done, the
  owner answers two probes in turn, or has closed the connection, unless the
  script cut a frame short;
- the well-behaved client reads each file's bytes, while the other is served
  and after;
- once the misbehaving client is killed and all is quiet, the owner serves no
  session, holds none of its region, reads each file right, and its index and
  tiers agree (`T4Caching::check_at_rest`).

A client may commit any bytes under any name (decision 8), so the misbehaving
client lands only a file's true bytes for the files the other client reads:
each byte is a function of its path and offset, and it lands a fetch's bytes
only once the owner has named the fetch. For a third file, and paths the store
doesn't hold, it lands any bytes, anywhere. It stages the shared files' bytes
in fresh spans at its region's start, and names spans and writes only past its
middle.

The library runs on shuttle only when built with `--cfg t4_simulated`, which
every `cargo fuzz` command of this target takes:

```bash
RUSTFLAGS="--cfg t4_simulated" cargo fuzz run shared_owner -- -timeout=10 -rss_limit_mb=8192
```

Without it the target builds, and panics on its first input.

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

```bash
RUSTFLAGS="--cfg t4_simulated" cargo fuzz run shared_mode -- -timeout=10 -len_control=0 -max_len=1024
```

`-len_control=0` lets libFuzzer try long scripts from the start; it would
otherwise begin under 20 bytes, a seed and little more.

On macOS, AddressSanitizer's own memory grows with every scenario, past
libFuzzer's 2 GB limit within half a minute for `snapshot` and in about half
an hour for `cache_equivalence`; without it (`-s none`) memory levels off
below 100 MB. For a long run there, pass `-s none`. On Linux it grows far
slower: in an hour, `snapshot` reached 1.2 GB and `cache_equivalence` 500 MB,
while `frames` levelled off at 690 MB; the Monday run allows each 8 GB.

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

The second prints the input; the third runs it again. For `shared_owner`
and `shared_mode`, prefix both with `RUSTFLAGS="--cfg t4_simulated"`: the
input's seed picks the same schedule again.

The weekly shuttle job prints each seed before it runs the suite on it, and
the failing one in its error. The same seed and budget run it again:

```bash
T4_SHUTTLE_SEED=<seed> T4_SHUTTLE_BUDGET=8 cargo test --locked --features shuttle --lib <test>
```

## What it leaves to others

- Threads racing: the shuttle tests (`cargo test --features shuttle
  shuttle`). `cache_equivalence`'s concurrent reads share one thread;
  `shared_owner`'s and `shared_mode`'s threads run one PCT schedule an input.
- Writes given up on: its store applies a write in the call's first poll,
  so there's no point to give one up at. The unit test
  `a_write_given_up_on_after_it_lands_is_read_back` covers them.
- Direct I/O, and I/O or a store that fails.
- Shared mode across real processes, killed by `SIGKILL`, and over real
  sockets and shared memory: the process and adapter tests
  (`doc/12-seeded-concurrency-tests.md`, Real-OS coverage retained).
- Too much at once from a client that follows the protocol: many reads in
  flight, each a few MiB of the owner's memory, or claims it never lands.
  `shared_owner` caps a script at 40 steps.
