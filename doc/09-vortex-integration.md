# 09 — Vortex integration: t4's own API, and Vortex's reader over it

Status: design (2026-10-27). Built: plan step 1, t4's `open`,
`open_listed` and `CachedFile::read`, with the `ObjectStore` integration
rebuilt on them. Rewritten the same day: the first version kept t4 behind
Vortex's object store reader (Rejected). Revised the same day: the thin
adapter first, over what t4 does already; merging a batch's GETs, built as
the old step 2, is shelved until measured GET counts call for it
(decision 5).

The goal: t4 in Vortex, sent to vortex-data/vortex as a pull request with
numbers from Vortex's own benchmark, one tier at a time, for Vortex's
maintainers to decide. The pull request is a proof of concept, not meant
to merge as is (the user, 27 Oct 2026): it shows the approach and the
numbers, and what would merge, in what shape, is the maintainers' call.

The shape:

- **t4 has an API of its own:** a file opened once, then read a range at a
  time (decision 1).
- **Integrations compose that API.** `ObjectStore` is one, and stays in t4
  (decision 2). Vortex's `VortexReadAt` is another, and lives in Vortex
  (decision 3).
- **The dependency goes one way.** t4 knows nothing of Vortex, and Vortex
  depends on t4: by git for the proof of concept, a published t4 if it
  ever merges.

## What shapes it

Read against the fork's `develop` of 14 Sep 2026, which uses DataFusion 55
and object_store 0.13.2, as t4 does.

**Vortex reads through one trait.** `VortexReadAt` (vortex-io) is every
Vortex reader's source of bytes. A reader is opened for one file. Through
it, Vortex says how it reads the file:

- `read_at`: one range. In vortex-file, only the footer is read this way
  (`vortex-file/src/open.rs`).
- `read_ranges`: every segment read that is waiting, handed over at once.
  Each is answered as soon as it lands.
- `coalesce_config`: whether Vortex merges nearby reads before they reach
  the reader, and how far apart they may be.
- `concurrency`: how many reads of the file Vortex keeps in flight.
- Each answer is a `BufferHandle`, which may be the reader's own buffer as
  long as it is aligned as asked.

Two readers exist:

- `FileReadAt`, for a local file:
  - each read is a `pread` on Vortex's blocking pool;
  - reads less than 1 MiB apart are merged, up to 4 MiB;
  - 32 reads are in flight per file.

  vortex-duckdb chose it over going through tokio, whose blocking pool it
  calls "very bad for local reads on high-core machines".
- `ObjectStoreReadAt`, for any `ObjectStore`:
  - each read is a `get_opts` call, and a `read_ranges` batch is split into
    one call per range;
  - reads less than 1 MiB apart are merged, up to 16 MiB;
  - 192 reads are in flight per file, with no cap across files (a TODO in
    `ObjectStoreFileSystem` asks for one);
  - before each request it allocates an aligned buffer from the session's
    allocator, then copies the answer's body into it.

  `LocalFileSystem` answers with a file instead of a body. object_store
  opens and stats that file on tokio's blocking pool for every read, and
  Vortex then `pread`s it on a second blocking task.

**Where each integration gets its reader:**

| Integration | Local file | Remote |
|---|---|---|
| DataFusion (`VortexFormat`) | `ObjectStoreReadAt` over `LocalFileSystem` | `ObjectStoreReadAt` |
| DuckDB | `FileReadAt` | `ObjectStoreReadAt`, its store in tokio's `Compat` |
| scan API, `open_path`, random-access-bench | `FileReadAt` | `ObjectStoreReadAt`, given a store |

DataFusion takes its stores from its own registry, which the user fills.
DuckDB and the other bindings resolve a URL through vortex-cloud's
`Registry`, which builds one store for each bucket from environment
variables. `Compat` enters tokio's context while it polls; it doesn't move
the work to another thread.

`VortexSource` takes a `VortexReaderFactory`, but `VortexFormat` doesn't
pass one. Its schema and statistics inference builds an `ObjectStoreReadAt`
directly. vortex-duckdb's `resolve_filesystem` hard-codes the two readers
above.

**Behind `ObjectStoreReadAt`, t4 loses what the trait carries.** `get_opts`
asks for one range of a path. Through it, t4:

- can't have Vortex's merging turned off for it alone. Merged above the
  cache, a hit copies the gaps, and a merged GET may fetch blocks memory
  already holds;
- can't have the per-file limit of 192 reads set for it alone;
- answers with a body that Vortex copies again;
- looks the file up again on every read: its metadata, and its version in
  the index.

A thin reader of its own (decision 3) takes all four back, with nothing
new in t4. One thing it doesn't take: seeing a batch whole, so as to merge
its GETs. That waits on measurement (decision 5).

| The bytes are | Vortex today | t4 behind `ObjectStoreReadAt` | t4's own reader (decision 3) |
|---|---|---|---|
| in memory | page cache: one copy (`pread`), one or two pool hops | two copies, gaps included; no hop | one copy, no hop; no gaps with Vortex's merging off |
| on NVMe | buffered `pread`: DMA to the page cache, then one copy | `O_DIRECT` DMA, then one copy | DMA, no copy |
| in S3 | one copy, the body into Vortex's buffer | two | one |

**Vortex's segments are aligned in the file.** Vortex's writer pads each
segment to its alignment (`vortex-file/src/segments/writer.rs`). A merged
read starts aligned down to the largest alignment of its segments
(`vortex-file/src/segments/source.rs`). t4's read output is page-aligned
and starts on a page of the file (`03-cache.md`, decision 3), so each
byte's address equals its file offset mod 4 KiB. So every read Vortex
makes, merged or not, gets t4 bytes aligned as it asks, for alignments up
to 4 KiB.

**vx-bench's nvme suites read the page cache.** `vx-bench run` runs each
query in a process of its own: it drops the OS's caches, runs the query n
times (10 in CI), and records the median. From the second run on, an nvme
suite reads the page cache. t4 starts cold in each process too, since a
restart loses its cache (`03-cache.md`, decision 9).

The suites: TPC-H on NVMe and on S3 at SF 1 and 10, and SF 100 nightly;
ClickBench and TPC-DS on NVMe; FineWeb on S3. Random access has a script
of its own, which also drops the caches before each combination.

**Every crate on Vortex's `develop` has version 0.1.0.** The release
workflow sets the real version (`cargo set-version`). So a crate built
against a published vortex-io implements a different trait from the one
Vortex's workspace compiles. An integration living in t4 would have to be
compiled in Vortex's CI against `develop`, and every change to Vortex's
trait would wait on a change to t4. An integration living in Vortex
changes with the trait.

**t4 adds ten entries to Vortex's lockfile.** Every crate t4 pulls in is
in Vortex's lockfile already, except t4, bbtree, vstd, and five Verus
crates (`verus_builtin`, `verus_builtin_macros`, `verus_prettyplease`,
`verus_state_machines_macros`, `verus_syn`); and the Verus macros bring
second versions of `convert_case` and `synstructure`. No version Vortex
locks moved (the fork's `t4-read-at` branch, 27 Oct 2026). vstd is pinned exactly
(`=0.0.0-2026-08-30-0159`). Vortex's style guide asks contributors to "be
conservative with adding new dependencies". The t4 0.2.0 on crates.io is
the store from before the pivot; the cache isn't published.

## Decisions

1. **t4's own API: a file opened once, then read a range at a time.**

   - **Opening.** `T4Caching::open` asks the inner store what the file is,
     once (`03-cache.md`, decision 11). `T4Caching::open_listed` takes what
     a list already said and asks nothing, unless the cache already knows
     the path: it keeps to what it heard first.
   - **The handle.** A `CachedFile` holds what the store said of the file,
     its size and e_tag, and reads with `read`: one range, as a bounded GET
     takes it, clamped to the file's end, an error if it starts past it.
     Reads of many ranges at once are many `read`s; a batch read that
     merges their GETs is shelved (decision 5).
   - **Its reads are of the version opened.** A read whose fetch finds the
     file changed fails with the store's `Precondition` error, rather than
     mix two versions of one file.
   - **Every answer is one `Bytes`** whose address equals its file offset
     mod 4 KiB: t4's read output.
   - **Reads t4 can't serve from the cache** go to the inner store: a file
     with no e_tag, or a read whose cache-file read failed. They carry
     `If-Match` on the file's e_tag, if it has one. Their bytes are copied
     into an output too, so the alignment rule holds for every answer.
   - **Lifetimes.** `T4Caching` is a handle: a clone shares the cache. A
     `CachedFile` holds one, so it is `'static` and cheap to clone, and a
     reader makes `'static` futures by moving a clone in, as Vortex's trait
     needs.
   - **Errors** are the inner store's (`object_store::Error`), since t4's
     origin is an object store.

2. **`ObjectStore` is an integration of t4's API, and stays in t4.**
   `get_opts` opens the file and reads its range through the cache, taking
   the intent from the request's extensions (`06-read-modes.md`, decision
   6). What the cache can't serve, it asks of the inner store as the
   request was, as before: so a file changed since reads as the store has
   it now, unlike a `CachedFile`'s `read`. That is why it reads by a step
   of `read`'s that stays inside t4 (`read_cached`, nothing if the cache
   can't serve), not by `read` itself. Conditional and versioned reads
   still go to the inner store. So do lists, writes and deletes, and the
   cache still remembers and forgets what they say. Its code moved out of
   `T4Caching`'s into a file of its own. Nothing changed for anything that
   reads through it, DataFusion's Parquet included: its tests passed
   unchanged. Each read now clones the cache's handle: five reference
   counts.

3. **`VortexReadAt` is an integration of t4's API, and lives in Vortex.**
   vortex-io gains a `t4` feature beside its `object_store` one, unix only
   (Linux intended, `00-meta.md`). It holds two types.

   `T4ReadAt` reads one `CachedFile`. It is a thin adapter: it does what
   `ObjectStoreReadAt` does, but for where its bytes come from, so that
   the comparison with it measures t4 and nothing else:

   - **`coalesce_config`: Vortex's, as `ObjectStoreReadAt`'s,** reads less
     than 1 MiB apart merged up to 16 MiB; or none, set at construction.
     Off, Vortex hands over exact segment ranges and t4 decides what to
     fetch together. Which is better is measured (decision 5).
   - **`concurrency`: `ObjectStoreReadAt`'s 192 per file,** or set at
     construction. t4's cap on bytes in flight already bounds the network
     across files (06, decision 11), the cap Vortex's TODO asks for.
   - **`read_ranges`:** a `CachedFile::read` for each range, at most
     `concurrency` at once, driven by one task on Vortex's I/O runtime, as
     `ObjectStoreReadAt` drives its GETs; each answered as it lands.
   - **`size`:** the file's, known since it was opened.
   - **Each answer** is t4's `Bytes` itself, with no copy, if it is aligned
     as asked and the session uses the static allocator. Otherwise it is
     copied into the allocator's memory (CUDA's pinned memory, say), as
     `ObjectStoreReadAt` copies every answer now.
   - **No intent at first** (Later).

   `T4FileSystem` is a `FileSystem` over a `T4Caching`. It lists, heads and
   deletes through t4's `ObjectStore`, so the cache remembers what lists
   say. `open_read` opens a `CachedFile`.

   Vortex depends on t4, and changes this reader whenever it changes its
   trait.

4. **Vortex lets any reader be hooked in everywhere.** These changes are
   generic; any custom reader gains from them, not only t4's.

   - **DataFusion.** `VortexFormat` takes a `VortexReaderFactory`, as
     `VortexSource` already does, and uses it for schema and statistics
     too. A `T4ReaderFactory` opens each file with `open_listed`, from
     DataFusion's listing, so no HEAD is sent. For the proof of concept it
     lives in datafusion-bench, about 15 lines; the hook is what Vortex
     would take. The v2 scan, TPC-H's default there, opens each file from
     the reader the factory makes too (`opener.rs`), so it reads through
     t4 as well.
   - **DuckDB.** With the `t4` feature, when `VORTEX_T4_URL` names a
     prefix, vortex-duckdb's `resolve_filesystem` returns a `T4FileSystem`
     over that prefix's store. t4's inner store is the registry's store,
     wrapped in `Compat`. So t4's own work runs on the thread that polls
     it, and only its GETs enter tokio's context.
   - **Local files stay Vortex's,** read by `FileReadAt` or
     `LocalFileSystem`. t4 caches object stores; in front of a local file
     it would cache an NVMe file on NVMe.

5. **t4 merges as it does today; a batch merges only if the GET counts
   call for it.** What t4 does already:

   - **With Vortex merging (the default, decision 3),** t4 sees GETs of up
     to 16 MiB, already merged.
   - **With it off,** Vortex hands over each segment. t4 fetches each block
     once, however many reads want it (06, decision 2). It merges adjacent
     GETs while they wait under the cap (06, decision 11), if
     `with_waiting_gets_merged` is set. GETs sent while there is room go
     apart.

   The remote tier runs both and counts GETs (decision 7). If merging off
   sends many more GETs than merging on, for no lower slowest GET, then
   merging a batch's GETs comes back.

   That batch merging was built as the old step 2 and shelved the same
   day, on t4's branch `shelved-batch-gathering`:
   `CachedFile::read_ranges`, and `GetQueue`'s `Gathering`. A batch's GETs
   wait aside, room or not, then enter the queue together, so that
   adjacent ones merge, while other GETs go on. It came with unit and
   shuttle tests.

   The evidence so far: on ClickBench's q20 to q34 through t4 (06, decision
   11), Vortex's reads unmerged had a slowest GET of 0.82 s and none over
   1 s; merged, 1.13 s and 23 over 1 s. That merging was the existing
   merging of waiting GETs, under the cap. t4's bench has read Vortex's
   merged reads since 26 Oct 2026, but the user lifted that call for this
   integration on 27 Oct.

6. **Late races use the integrator's timer.** t4 races a slow GET late only
   if it is given a sleep function (06, decisions 7 and 8). vortex-duckdb
   gives it `smol::Timer`, which runs under any executor and which
   vortex-io already depends on. The DataFusion bench gives it tokio's.

7. **The benchmark is Vortex's own, one tier at a time.** It uses vx-bench
   and Vortex's own suites and files. t4 is pinned to one tier by
   `FixedPlacementPolicy`, which admits every block to that tier and never
   moves it. The policy moves from the bench into t4's public policies so
   that Vortex's benchmarks can name it. t4's origin is the same S3 files
   the s3 suites read.

   | Tier | t4 | Against | Runs compared |
   |---|---|---|---|
   | remote | keeps nothing (`Admit::Discard`): every run reads S3 through t4 | the s3 suites, `ObjectStoreReadAt` straight to S3 | all |
   | memory | memory only | the nvme suites, from the page cache | 2 to n |
   | disk | disk only, `O_DIRECT` | the nvme suites, page cache dropped before every run | 2 to n against all |

   - **The first run** warms t4's tier. It is reported, not compared: its
     reads are the remote tier's.
   - **The disk baseline** runs `--iterations 1` n times, since vx-bench
     drops the cache once per process.
   - **Vortex merging on and off,** in the remote tier: their GET counts
     and slowest GETs decide whether a batch's GETs should merge
     (decision 5).
   - **t4's defaults** (CLOCK, fetched blocks to memory, memory's victims to
     disk) are reported apart, last, as what a user would get. Their medians
     come from caching, not from reading faster.
   - **Every comparison** leads with the slowest run of each query, then
     the median.

8. **Vortex's files have dictionaries of 2 MiB.** The fork's branch is
   based on vortex-data/vortex#10766, which raises the default cap to 2 MiB.
   So vx-bench writes the kind of files t4's own bench reads.

## The interfaces

t4 (decision 1):

```rust
/// A clone shares the cache.
#[derive(Clone)]
pub struct T4Caching { /* ... */ }

impl T4Caching {
    /// The file at `location`, opened for reads: what the inner store says
    /// of it, asked once.
    pub async fn open(&self, location: &Path) -> object_store::Result<CachedFile>;

    /// The file a list named, opened for reads: nothing asked.
    pub fn open_listed(&self, listed: ObjectMeta) -> CachedFile;
}

/// A file opened for reads through t4: what the store said of it, asked
/// once. Every answer's address equals its file offset mod 4 KiB.
#[derive(Clone)]
pub struct CachedFile { /* a handle on the cache, what the store said */ }

impl CachedFile {
    pub fn meta(&self) -> &ObjectMeta;

    /// `range` of the file, of the version opened.
    pub async fn read(
        &self,
        range: Range<u64>,
        intent: Option<ReadIntent>,
    ) -> object_store::Result<Bytes>;
}
```

Vortex, `vortex-io/src/t4/` (decision 3):

```rust
/// A Vortex file read through t4.
pub struct T4ReadAt {
    file: t4::CachedFile,
    uri: Arc<str>,
    handle: Handle,
    allocator: BufferAllocatorRef,
    concurrency: usize,
    coalesce_config: Option<CoalesceConfig>,
}

impl T4ReadAt {
    pub fn new(file: CachedFile, handle: Handle) -> Self;
    pub fn new_with_allocator(file: CachedFile, handle: Handle, allocator: BufferAllocatorRef) -> Self;
    pub fn with_concurrency(self, concurrency: usize) -> Self;
    pub fn with_coalesce_config(self, config: Option<CoalesceConfig>) -> Self;
}

impl VortexReadAt for T4ReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> { self.coalesce_config }
    fn concurrency(&self) -> usize { self.concurrency }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> { /* the file's */ }
    fn read_at(&self, offset: u64, length: usize, alignment: Alignment)
        -> BoxFuture<'static, VortexResult<BufferHandle>> { /* `CachedFile::read` */ }
    fn read_ranges(&self, requests: Arc<[ReadAtRequest]>) -> ReadAtStream {
        /* a `CachedFile::read` each, `concurrency` at once */
    }
}

/// A `FileSystem` over t4: lists through its `ObjectStore`, reads through
/// `T4ReadAt`.
pub struct T4FileSystem {
    store: Arc<T4Caching>,
}
```

vortex-duckdb with the `t4` feature (decision 4). Nothing else is
configurable at first; t4's default policies apply.

```text
VORTEX_T4_URL=s3://bucket/prefix/                 the store t4 goes in front of
VORTEX_T4_MEMORY_GIB=64                           the memory tier
VORTEX_T4_DISK=/mnt/nvme0/t4.cache:3072,...       cache files, each path:GiB
```

vx-bench options (decision 7). The policy names are t4 bench's
`--cache-policy` names.

```text
--opt t4-policy=clock|memory|disk|none   (absent: no t4)
--opt t4-memory-gib=64
--opt t4-disk=/mnt/nvme0/t4.cache:3072,...
--opt t4-coalesce=on|off                 (Vortex's merging of nearby reads, decision 5)
```

t4 needs the data in S3 or GCS (`--opt remote-data-dir=...`), and is
registered as the session's store for the bucket, so that DataFusion's
lists go through it too.

## The plan

1. **t4's API:** `open`, `open_listed` and `CachedFile::read`, with the
   `ObjectStore` integration rebuilt on them. Its unit, shuttle and fuzz
   tests pass unchanged. Built 27 Oct 2026: the 136 unit tests and the 23
   shuttle tests passed unchanged, and six new ones cover the API (opening,
   listing, alignment on a fetch and a hit, a file with no e_tag, a file
   changed since it was opened, the intent). The fuzz target compiles; it
   wasn't run.
2. **The thin adapter, in the fork,** based on #10766: vortex-io's `t4`
   feature and `T4ReadAt` over `CachedFile::read`, with its tests, a
   Vortex file read through it among them. Nothing new in t4; the fork
   takes t4 by path, then by git. Started 27 Oct 2026 on the fork's branch
   `t4-read-at`: the feature, `T4ReadAt`, and unit tests in vortex-io
   (each read's bytes and alignment, a fetch then a hit; a batch; the
   size): the six pass, and clippy is clean on vortex-io with every
   feature. And a Vortex file read through it: vortex-file gains a `t4`
   feature, vortex-io re-exports t4's `T4Caching`, `T4CachingBuilder` and
   `CachedFile` so that the cache is built with the t4 Vortex is built
   against, and `vortex-file/tests/t4_read_at.rs` writes a file of over
   2 MiB to an in-memory store and scans it back through `T4ReadAt` twice,
   fetched then from t4's tiers, with Vortex's merging and without: both
   pass.
3. **DataFusion's wiring:** `VortexFormat`'s reader factory and a
   `T4ReaderFactory`; `FixedPlacementPolicy` made public in t4; the
   datafusion-bench options. Built 27 Oct 2026: `FixedPlacementPolicy`
   moved into `t4::policy`; on the fork's `t4-read-at`, `VortexFormat`'s
   `with_vortex_reader_factory`, used for schema and statistics and passed
   to every `VortexSource`, with a test that a listing table infers its
   schema and scans through the factory (vortex-datafusion's 233 format and
   source tests pass); the `vortex` crate's `t4` feature; datafusion-bench's
   `t4_cache` and options. It builds; it hasn't run against S3 yet (step
   4).
4. **Measured on DataFusion:** the three tiers on TPC-H SF 10 and on
   ClickBench's q20 to q34 (06's subset); and the remote tier with Vortex's
   merging on and off, with GET counts and slowest GETs.
5. **A batch's GETs merged, only if step 4 calls for it:** the shelved
   branch brought back (decision 5).
6. **DuckDB:** `T4FileSystem` and `resolve_filesystem`'s `t4` path, then
   the same tiers.
7. **Random access by URL:** the three tiers. These are many small reads,
   where a pool hop and a syscall for every read would show, if anywhere.
8. **t4's defaults,** reported apart.
9. **t4 by git:** the fork's path dependency on t4 becomes a git one, on
   t4's pushed branch, so that the pull request builds for others.
10. **The pull request, a proof of concept:** the hooks (decision 4),
    vortex-io's `t4` module, the bench's options, and the numbers, marked
    not to merge as is. Vortex's maintainers decide what, if anything, to
    take; the generic hooks could go first, alone, as any reader gains
    from them.

The old step 2, a batch's GETs merged, was built on 27 Oct 2026 and
shelved the same day (decision 5).

Every run takes under 5 minutes: the queries and the number of runs are
picked from one timed pass, before any comparison. The box is an
i7i.48xlarge in us-east-1, as for 06's runs: NVMe for the disk tier, S3 in
the region.

What the reasoning expects; the runs check it:

- **Remote:** the slowest runs are lower, from the footer race, which cut
  the slowest Vortex run on q24 from 945 to 731 ms (06, decision 10), and
  late races. With Vortex's merging off, the slowest GET is lower still
  (decision 5), but t4 sends more GETs: its GETs are 4 MiB at most, where
  Vortex's merged GETs reach 16 MiB, and GETs sent while there is room go
  apart. Medians are within noise.
- **Memory:** TPC-H and ClickBench tie the page cache, since decoding
  dominates; t4's end-to-end run of 22 Oct 2026 tied a local file, warm and
  cold. Random access gains, since every read skips the pool, the
  syscalls and the gap bytes.
- **Disk:** unknown. `O_DIRECT` reads only the 4 KiB pages asked for,
  while a buffered `pread` reads ahead. TPC-H likely ties again.

## Open questions

- **t4's dependencies.** bbtree, vstd and five Verus crates are new to
  Vortex's lockfile, with second versions of two crates. vstd's exact pin can't be unified with any other pin
  of vstd. The proof of concept carries them as they are; t4 has to answer
  this before anything merges: can the Verus crates stay out of a normal
  build, behind a feature, with `verus!` erased without them?
- **GET size against cost.** A request costs, its bytes don't, and 8 to 16
  MiB is where cost and rate both flatten (06, What shapes it). t4's 4 MiB
  limit keeps any one GET from holding a read up. Should t4 merge
  further?
- **One cache, several stores.** A query over two buckets gets t4 in front
  of one of them. A t4 over several stores sharing one budget would be a t4
  change.
- **The disk baseline.** vx-bench drops the page cache once per process.
  Dropping it before every run, or adding an `O_DIRECT` `FileReadAt`, is
  Vortex's call; this benchmark runs n processes of one run instead.
- **A custom allocator's reads.** With CUDA's pinned allocator, t4's bytes
  are copied in. Reading straight into the caller's buffer would need that
  buffer page-aligned, for `O_DIRECT`.
- **A memory hit copies once.** t4 copies a hit into the read's output, as
  `pread` copies from the page cache. Handing Vortex the pool's chunk
  instead would hold that chunk against eviction for as long as Vortex
  keeps the array.
- **07's `vortex` feature points the other way.** By this doc's rule, the
  Vortex-aware prefetch (`07-prefetch.md`, decision 7) belongs in Vortex: a
  `PrefetchPolicy` that Vortex gives t4, parsing Vortex's own footers. 07
  changes once this is settled.

## Rejected

- **t4 implementing `VortexReadAt`.** Every crate on Vortex's `develop` is
  version 0.1.0, so Vortex's CI would compile t4 against `develop`, and
  every change to Vortex's trait would wait on t4. An integration lives
  with what it integrates.
- **t4 behind `ObjectStoreReadAt` alone.** This doc's first version, the
  same day. Through `get_opts`, Vortex's reader can't turn its merging off
  for t4 alone. It copies every answer. It opens the file again on every
  read. Its zero-copy fix is Later.
- **t4 in front of a local file:** decision 4.
- **t4's defaults in the remote comparison:** from the second run on they
  serve from memory, which is caching, not a faster remote path
  (decision 7).

## Later

- **t4 publishable,** if Vortex wants to merge: the dependency question
  settled (Open questions), and 0.3.0 published.
- **`ObjectStoreReadAt` taking a one-chunk answer as its buffer.** When a
  store answers in one aligned chunk and the session uses the static
  allocator, that chunk becomes the buffer. A small, generic pull request:
  it helps `InMemory`, and t4 behind any integration built on object
  stores.
- **Intent from Vortex.** `read_at` is the footer read, so `T4ReadAt` can
  mark it `ReadIntent::Latency`, but only once the default read policy
  races Latency reads (06, plan step 3). Until then, a read with an intent
  is fetched in one GET with no race. That is worse than the footer race a
  normal read gets by its path's ending.
- **A batch's GETs merged,** built and shelved on
  `shelved-batch-gathering` (decision 5). Only if the GET counts call for
  it.
- **A batch planned as one read:** one `LocatedRead` over several ranges,
  so the policy could widen across gaps for cost. Only if the GET counts
  call for it, and the batch merging falls short.
- **t4's I/O worker under local files.** An io_uring `FileReadAt` would
  replace the `pread` pool Vortex's maintainers pointed at. It reads files
  rather than caching, so it would be a pull request of its own, to
  vortex-io's `std_file`.
- **Prefetch** (`07-prefetch.md`): Vortex reads ahead itself.

## Code map

| Repository | Path | What | Built |
|---|---|---|---|
| t4 | `src/cache/t4_caching.rs` | `T4Caching`, a handle; `open`, `open_listed`; reads served and fetched | yes |
| t4 | `src/cache/t4_caching/cached_file.rs` | `CachedFile`: `read` | yes |
| t4 | `src/cache/t4_caching/object_store_integration.rs` | `impl ObjectStore for T4Caching`, over `open` and `read_cached` | yes |
| t4 | `src/cache/t4_caching/cached_file.rs`, `src/cache/get_queue.rs` | `CachedFile::read_ranges`; `Gathering`: a batch's GETs merged | shelved, branch `shelved-batch-gathering` |
| t4 | `src/cache/policy/fixed_placement.rs` | `FixedPlacementPolicy`, public, from `bench/src/fixed_placement_policy.rs` | yes |
| Vortex | `vortex-io/src/t4/read_at.rs` | `T4ReadAt`, feature `t4` | yes, branch `t4-read-at` |
| Vortex | `vortex-io/src/t4/mod.rs` | t4's `T4Caching`, `T4CachingBuilder`, `CachedFile` re-exported | yes, branch `t4-read-at` |
| Vortex | `vortex-file/tests/t4_read_at.rs` | a Vortex file written to a store and scanned back through `T4ReadAt`; vortex-file's feature `t4` | yes, branch `t4-read-at` |
| Vortex | `vortex-io/src/t4/filesystem.rs` | `T4FileSystem` | no |
| Vortex | `vortex-datafusion/src/persistent/format.rs` | `VortexFormat`'s reader factory, used for schema and statistics too | yes, branch `t4-read-at` |
| Vortex | `vortex/Cargo.toml` | the `t4` feature, forwarding to vortex-io's and vortex-file's | yes, branch `t4-read-at` |
| Vortex | `vortex-duckdb/src/file_reader.rs` | `resolve_filesystem`: a `T4FileSystem` for `VORTEX_T4_URL`'s prefix | no |
| Vortex | `benchmarks/datafusion-bench/src/t4_cache.rs`, `lib.rs`, `main.rs` | t4 in front of the store per `--opt t4-*`; `T4ReaderFactory` | yes, branch `t4-read-at` |
| Vortex | `benchmarks/duckdb-bench/src/main.rs` | `VORTEX_T4_*` set per `--opt t4-*` | no |
| Vortex | `vortex-bench/src/random_access/take.rs` | files opened by URL | no |
