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
     GETs while they wait under the cap (06, decision 11), by default since
     27 Oct 2026, up to 4 MiB. GETs sent while there is room go apart.

   The memory tier runs both, and counts the GETs of its first run, from
   S3 into t4, the cold read (decision 7). If merging off sends many more
   GETs than merging on, for no lower slowest GET, then merging a batch's
   GETs comes back. Not the remote tier: there t4 keeps nothing, so with
   merging off two segment reads of one block, one after the other, each
   fetch it. The smoke run of 27 Oct 2026 (TPC-H SF 0.1, q1, two runs)
   showed it: 25 GETs and 17.2 MB with merging off against 13 and 15.0 MB
   on; a cache would have fetched the block once.

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
   | disk | disk only, `O_DIRECT` | the nvme suites, their files' pages evicted before every run, on t4's drives | 2 to n |

   - **The first run** warms t4's tier. It is reported, not compared: its
     reads are the remote tier's.
   - **The disk baseline** is one process, as t4's is, its files' pages
     evicted from the page cache before every run (`--opt
     evict-page-cache=on`, `posix_fadvise` `DONTNEED`), so every run reads
     them from the drives. It was n processes of one run at first, since
     vx-bench drops the cache once per process; but a process's first run
     costs about 400 ms beyond its reads, so that baseline measured start-up
     (step 4). Vortex's files and t4's cache files share the same drives.
   - **Vortex merging on and off,** in the memory tier: the GET counts and
     slowest GETs of its first, cold run decide whether a batch's GETs
     should merge (decision 5); its later runs are memory hits with and
     without Vortex's merging.
   - **GETs are counted** below t4, around the store itself, in every
     variant over S3 (`--opt get-log=<path>`, datafusion-bench's
     `get_counter`): their number, bytes, and slowest, from request until
     the body is all in or dropped, and those over 1 s and 2 s, one JSON
     line a run and one a stretch between runs.
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
--opt t4-disk=/mnt/t4/0:/mnt/t4/1:...    (a t4.cache in each; colons, as --opt splits on commas)
--opt t4-disk-gib=64                     (each cache file)
--opt t4-coalesce=on|off                 (Vortex's merging of nearby reads, decision 5)
--opt get-log=<path>                     (GETs counted, one JSON line a process)
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
   ClickBench's q20 to q34 (06's subset); and the memory tier with Vortex's
   merging on and off, with GET counts and slowest GETs. On a single-socket
   Spot i7i.24xlarge (48 cores, 6 SSDs, 56.25 Gbit/s), not the two-socket
   i7i.48xlarge of 06's runs. Each variant's process is a query, the OS's
   caches dropped first, as vx-bench runs them: `s3`, `t4-none`,
   `nvme-warm`, `t4-memory`, `t4-memory-off`, `nvme-cold`, `t4-disk`.

   A smoke run went first, 27 Oct 2026, from a Mac on TPC-H SF 0.1 in S3:
   every variant ran; t4's memory and disk tiers answered the second run
   from the tier (18 to 44 ms against 1.4 to 2.6 s for the first), with no
   GET; t4 sent no HEAD; and t4 keeping nothing read 1.6 times the bytes
   the plain S3 path did (15.0 MB against 9.1 MB on q1), its 1 MiB blocks
   over small files and its footer race. It found two bugs before any box:
   the GET counter timed each GET to its first byte, a closure capturing a
   copy of a field rather than the timer; and the local variants needed
   the Vortex repository as the working directory. Then a quick pass on
   the box (TPC-H q1, q6, q9, q18; ClickBench q20, q24, q33; two runs),
   then the full one.

   **Measured, 27 Oct 2026** (`~/t4-results/vortex-step4-20261027/`): a Spot
   i7i.24xlarge in us-east-1b, 30 minutes from launch to termination. Vortex
   0.1.0 on the fork's `t4-read-at`, its own writer (2 MiB dictionaries):
   TPC-H SF 10, 2.6 GB in 16 files; ClickBench partitioned, 15 GB in 100.
   Five runs a query; the warm variants a sixth first, left out. Two fixes
   on the box first: vortex-bench writes each generated file under the
   repository's `target/`, missing there; and an SSH session's 1,024 open
   files, which Vortex's reader outran, failing three queries and slowing
   others (TPC-H q1 from S3: 1,075 ms slowest before, 662 after).

   Each row the variant's geometric mean over the suite against the one it
   is compared with, below 1 faster, and on how many queries its slowest run
   was lower:

   | Tier | t4 against | TPC-H, 22 queries | ClickBench q20 to q34 |
   |---|---|---|---|
   | remote | Vortex straight to S3 | slowest x0.82, median x0.81; 19 of 22 | slowest x1.01, median x1.03; 6 of 15 |
   | memory | Vortex from the page cache | slowest x0.96, median x0.99; 9 of 22 | slowest x0.94, median x0.96; 13 of 15 |
   | disk, one SSD | Vortex from the same SSD, its pages evicted | slowest x0.89, median x0.89; 18 of 22 | slowest x0.90, median x0.91; 11 of 15 |
   | disk, six SSDs | Vortex from the same six, an md RAID0, its pages evicted | slowest x0.91, median x0.84; 14 of 22 | slowest x0.99, median x0.88; 13 of 15 |

   The disk rows are the rerun's, below; the first run's disk row (slowest
   x0.35 and x0.41) wasn't a fair comparison.

   Checked for coherence before anything was said of them:

   - **Every query of every variant returned the rows it should:**
     Vortex's harness checks each query's row count against TPC-H SF 10's
     and ClickBench's own, and fails the process on a mismatch; none did. A
     count, not the values: the values are checked only by
     `vortex-file/tests/t4_read_at.rs`, which compares the arrays read
     through `T4ReadAt` with those written.
   - **The first run's disk row wasn't a fair comparison,** twice over.
     Vortex's files were on one SSD (`/mnt/t4/0`), t4's disk tier striped
     over all six: on ClickBench's big scans t4's disk reads went at 24 to
     29 GB/s, six drives' worth, and Vortex's at about one drive's. And each
     Vortex run was a fresh process's first, which costs about 400 ms beyond
     its reads (the rerun, below), where t4's were a process's second to
     sixth.
   - **Memory and NVMe variants repeat:** the quick pass and the full run
     share 7 queries, whose medians agree within a few percent; one quick
     pass reading, t4's memory tier slower than the page cache on ClickBench
     q24 (121 against 78 ms), was noise: 69 against 80 in the full run.
   - **The variants over S3 vary from pass to pass,** 40 to 120% on some
     queries, and the slowest run is nearly always a process's first, which
     reads every footer and opens its connections. Leaving every first run
     out, the remote row holds: TPC-H slowest x0.80, median x0.82, t4 faster
     on 18 of 22; ClickBench x0.97 and x1.05, 6 of 15. Each suite's
     baseline ran before t4's, minutes apart, not interleaved.

   - **Remote: t4 removes S3's stragglers.** The slowest GET over the
     suite was 823 ms straight to S3 and 256 ms through t4 on TPC-H; 3,244
     ms and 314 ms on ClickBench, where 707 of S3's GETs took over 1 s and
     none of t4's did.
   - **But on ClickBench t4 reads 1.48 times the bytes** (58.8 GB a cold
     run against 39.7 GB; TPC-H 1.03). Vortex's GETs there average 1.94
     MB, and t4 fetches whole 1 MiB blocks around each: rounding each out
     by about a block predicts x1.54, near the x1.48 measured. On TPC-H,
     whose GETs average 1.29 MB, the same rounding would give up to x1.81
     if no two reads shared a block, against x1.03 measured: its reads, a
     table's columns in order, likely share their blocks (not checked
     against the files' layout). The bandwidth they take is why its scans aren't
     faster from S3 despite the stragglers gone (`03-cache.md`, decision
     1's block size, Open questions).
   - **A cold read through t4 is no slower than S3's**, each against its
     first run, which reads the footers too: x0.78 on TPC-H, x0.98 on
     ClickBench, into memory or onto disk alike.
   - **Memory: t4 ties the page cache.** Where its slowest run trailed, the
     recheck below found S3 reads on one query and one-off runs on the rest.
   - **Vortex's merging off** (decision 5): on the cold run, 2.05 times the
     GETs on TPC-H (10,951 against 5,332) and 1.93 on ClickBench (63,769
     against 33,067), the same bytes, and a slowest GET no lower on most
     queries (lower on 10 of 22 and 4 of 15; over the suite 268 against
     295 ms, 322 against 368); and the cold run 7% and 4% faster (off
     faster on 17 of 22 and 11 of 15 queries).
     Memory hits alike either way. But t4's merging of waiting GETs was
     off then, its default until that day: the run with Vortex's merging
     off merged nothing anywhere. So it doesn't yet say whether a batch's
     GETs need merging: that waits on a run with Vortex's merging off and
     t4's merging of waiting GETs on, its default now.

   **The disk rerun, 27 Oct 2026** (`disk-rerun/` there): the same box type,
   in us-east-1b, 50 minutes from launch to termination, t4 merging waiting
   GETs by default by then. Three passes, each a quick one first (two
   queries a suite, two runs), all queries of both suites, five runs; each
   query's variants in turn, interleaved, Vortex's files from S3 as the
   first run wrote them:

   - **one SSD:** Vortex's files and t4's cache file on `/mnt/t4/0`;
   - **six SSDs:** all six as one md RAID0 (512 KiB chunks), holding both;
   - **split:** each SSD halved, the first halves an md RAID0 holding
     Vortex's files and one t4 cache file, each second half a file system
     of its own with one of six t4 cache files, which t4 stripes itself by 1
     MiB chunk; t4 over md and t4 over its own six files, against Vortex
     over md.

   The variants: `nvme-cold`, as before, five processes of one run;
   `nvme-evict`, one process, its files' pages evicted before every run,
   the baseline; `nvme-warm`, the page cache kept, in the quick passes, to
   show eviction undoes it; `t4-disk` and, split, `t4-own`. The drives'
   read counters, `/proc/diskstats` around each process, give the bytes
   each read and from which drives. Every run's row count is checked
   against the first's, no longer the first alone, since a tier answers the
   later runs.

   | Pass | t4 against Vortex evicted | TPC-H, 22 queries | ClickBench q20 to q34 |
   |---|---|---|---|
   | one SSD | t4's tier on the SSD | slowest x0.89, median x0.89; 18 of 22 | slowest x0.90, median x0.91; 11 of 15 |
   | six SSDs | t4's file on md | slowest x0.91, median x0.84; 14 of 22 | slowest x0.99, median x0.88; 13 of 15 |
   | split | t4's file on md | slowest x0.83, median x0.86; 16 of 22 | slowest x0.86, median x0.87; 14 of 15 |
   | split | t4's own six files | slowest x0.84, median x0.84; 16 of 22 | slowest x0.89, median x0.88; 14 of 15 |
   | split | t4's own six files against t4 on md | slowest x1.02, median x0.98; 11 of 22 | slowest x1.04, median x1.01; 8 of 15 |

   - **On the same drives t4's disk tier is about 10% faster than Vortex's
     `pread`,** at the median on every pass, and on the slowest run but for
     one-off runs (below). It reads about the bytes Vortex reads, within
     10% either way on ClickBench.
   - **A fresh process's first run costs about 400 ms beyond its reads.**
     On one SSD, TPC-H q6 read 423 MB a run either way: 496 ms as a fresh
     process's run, 70 ms as a later run with the pages evicted. The quick
     pass's evicted runs read what the fresh processes read, 3.3 GB a run
     on ClickBench q20, and a run with the page cache kept 0.
   - **t4's own striping ties md's.** fio's random 1 MiB reads, 4 threads
     of 16, went at 28.1 GB/s over the six files and 26.8 GB/s over md, and
     the queries tie. md's 512 KiB chunks split each of t4's 1 MiB blocks
     over two drives, at no measured cost.
   - **The passes agree:** t4 over md against Vortex over md, whole drives
     and halved, x0.84 and x0.86 at the median on TPC-H, x0.88 and x0.87 on
     ClickBench; the slowest runs vary with one-off runs.
   - **TPC-H's queries that read a table twice in a run** (q15, q17, q18,
     q21) read 1.6 to 2.7 times Vortex's bytes from the drives through t4:
     Vortex's second read is a page cache hit, while t4, pinned to disk,
     reads its disk tier again. On one SSD that costs t4 q15 and q21 (x1.28
     and x1.13 on the slowest run); on six, with bandwidth to spare, t4 is
     faster on them. A test of the tier alone; t4's own placement would keep
     them in memory.
   - **t4's one-off slow runs aren't S3:** the split pass logs GETs a run,
     and TPC-H's sent none after the first; they come mostly right after
     the warm-up (q3 192 ms, then 89 to 150), cause unknown. Vortex has its
     own (q9 632 ms against a median of 307). ClickBench's top-10 queries
     (q23, q24, q26) still fetch after the first run, 0 to 44 MB a run on
     q24 and q26, 285 to 977 MB on q23, and their slow runs are those
     fetches': q24's 171 ms run fetched 44 MB, its others 0 to 24.
   - **Open: one SSD read faster than fio says it can.** fio read it at 4.5
     to 4.8 GB/s, sequential or random, 64 to 256 in flight; but ClickBench
     q20's evicted runs read 3.3 GB in 459 ms, 7.3 GB/s, and t4's 3.0 GB in
     457, 6.6 GB/s. The drives don't serve a re-read faster: fio over a 4 GB
     file, re-read, went as fast as over 32 GB, on the RAID0. So some of the
     counted reads may fall outside the timed runs; the six-SSD passes stay
     well under their 27 to 28 GB/s, where it wouldn't change the result.
     Not settled.

   **The memory recheck, the same session** (`disk-rerun/memory/`): t4's
   memory tier against the page cache, the files on the six-SSD RAID0, on
   the queries where t4's slowest memory run trailed in the first run,
   ClickBench q23 and TPC-H q3, q8, q11, q21, q22; a first run and 10 more,
   t4's GETs logged a run.

   - **ClickBench q23 reads S3 on every run.** It is `SELECT * ... ORDER BY
     "EventTime" LIMIT 10`. As the top 10 fill, DataFusion's `FilePruner`
     skips a file whose `EventTime` statistics can't beat the 10th, at its
     open, and ends a file's scan partway once they can't (vortex-datafusion's
     `PrunableStream`; Vortex's own scan takes no dynamic filter,
     vortex-data/vortex#4034). How far each of the 100 files gets depends on
     when the bound tightens, so each run reads a different part of them.
     Each of t4's ten later runs fetched data no earlier run had, 61 to
     2,363 MB, and the slowest run (2,331 ms against a median of 526) is the
     one that fetched the most, in 844 GETs: 2,363 MB at the first run's
     1.28 GB/s from S3 is 1,846 ms, and the run took 1,805 ms over the
     median. The page cache reads that data from the drives. Its median ties
     the page cache's (526 against 534 ms).
   - **TPC-H's slow runs aren't S3:** no GET after the first run, nor
     between runs. Over 10 runs t4's median is at or below the page cache's
     on all five (x0.89 to x0.99), its slowest at or below on q11, q21 and
     q22. q8 still trails: two runs of 286 and 287 ms against 174 to 215
     for the rest. A process's first warm run is often its slowest on both
     sides, more so on t4's (q3 207 ms, then 83 to 131); cause unknown.

   **The CPU pass, 27 Oct 2026** (`cpu/` there): how much CPU each run
   takes, as wall time can't show it on a box with CPU to spare. The same
   box type, all six SSDs as one md RAID0 holding Vortex's files and t4's
   cache file; query by query, in turn, Vortex from the page cache, t4's
   memory tier, Vortex from the SSDs (its pages evicted before every run)
   and t4's disk tier; a first run and five more. `--opt cpu-log` logs each
   run's process CPU time, exact (`CLOCK_PROCESS_CPUTIME_ID`, every
   thread), split into user and system time by the kernel's 10 ms ticks,
   and the machine's busy time from its ticks, interrupts included. The
   process's CPU and the machine's busy time agree within about 1%: no work
   hides in interrupts or kernel threads.

   | t4 against Vortex | TPC-H, 22 queries | ClickBench q20 to q34 |
   |---|---|---|
   | disk tier against Vortex from the SSDs | CPU x0.81, lower on 21 of 22; wall x0.87 | CPU x0.83, lower on 15 of 15; wall x0.86 |
   | memory tier against the page cache | CPU x0.97; wall x1.02 | CPU x0.99; wall x0.97 |

   Geometric means of each query's median run. Summed over a suite's
   medians, in CPU-seconds a run:

   | | TPC-H: CPU, of it system | ClickBench: CPU, of it system |
   |---|---|---|
   | Vortex from the page cache | 122.2, 8.5 | 298.7, 18.3 |
   | t4's memory tier | 120.0, 4.2 | 299.8, 3.5 |
   | Vortex from the SSDs | 130.5, 18.0 | 330.7, 46.1 |
   | t4's disk tier | 114.6, 4.2 | 288.3, 4.9 |

   - **From the SSDs, t4 takes 12 to 13% less CPU, all of it kernel
     time:** 15.9 CPU-seconds a TPC-H run less, 13.7 of them system time;
     42.4 on ClickBench, 41.2 of them system time. A buffered `pread` reads
     ahead, allocates page cache pages and copies them out; `O_DIRECT`
     reads land in the read's own buffer. Vortex's system time from the
     SSDs is 4 to 9 times t4's.
   - **That bounds what t4's disk reads can win on these suites:** Vortex's
     whole system time is 14% of its CPU on each. The wall-time gain, x0.87
     and x0.86, is near it, and repeats the disk rerun's x0.84 and x0.88.
   - **In memory the CPU ties, the copy moved:** t4 halves or better the
     page cache's system time (4.2 against 8.5, 3.5 against 18.3) and adds
     about as much user time (115.9 against 113.7, 296.3 against 280.3): a
     memory hit's copy into the read's output, where `pread` copies in the
     kernel (Open questions, a memory hit copies once).
   - **So t4 from the SSDs takes less CPU than from its own memory**, x0.92
     and x0.96, and less than Vortex from the page cache, x0.88 and x0.95.
   - **The box has CPU to spare:** Vortex's page cache runs keep a median 40%
     (TPC-H) and 65% (ClickBench) of the 96 vCPUs busy, 15 to 85% by query.
     A box with more SSD bandwidth a core would turn more of the saving into
     time, up to the 12 to 13% bound. AWS's current storage boxes don't
     offer one: by ScyllaDB's per-drive measurements
     (`scylla-machine-image`'s `aws_io_params.yaml`), every size of i7i,
     i7ie, i8g and i8ge reads about 0.26 GB/s a vCPU, 0.53 GB/s a core on
     x86's two threads a core; only the old i3.xlarge and i3en.2xlarge reach
     about 1 GB/s a core. Pinning the bench to fewer of the i7i.24xlarge's
     cores would.

   **The small box, 27 Oct 2026** (`cpu-i3en/` there): the same pass, but
   for t4's memory tier, on a Spot i3en.2xlarge, the fewest cores of any
   box with two SSDs: 4 cores, 8 vCPUs (Xeon 8259CL), 64 GiB, the two SSDs
   as one md RAID0. fio read them at 1.3 to 1.5 GB/s, not ScyllaDB's 4.1
   GB/s: 0.35 GB/s a core, less than the i7i.24xlarge's 0.56. An
   i7i.24xlarge built the bench (Vortex builds for baseline x86-64), as 8
   vCPUs would take most of an hour. t4's memory tier was 8 GiB, its blocks
   all on disk.

   | t4's disk tier against | TPC-H, 22 queries | ClickBench q20 to q34 |
   |---|---|---|
   | Vortex from the SSDs | wall x0.76, faster on 21 of 22; CPU x0.88 | wall x0.90, faster on 15 of 15; CPU x0.92 |
   | Vortex from the page cache | wall x1.04; CPU x0.96 | wall x1.01; CPU x0.98 |

   - **Here the box is busy:** every variant keeps a median 89 to 97% of the
     8 vCPUs busy. Summed over a suite, t4 takes 13% less time on TPC-H
     (19.0 s a run against 21.9) and 9% on ClickBench (45.3 against 50.0),
     and 7% less CPU on each, nearly all of it kernel time: Vortex's 11.5 and 25.1
     system seconds against t4's 1.3 and 1.3.
   - **The rest is bytes:** Vortex reads 1.2 to 1.3 times t4's bytes from
     the drives (t4's x0.83 and x0.77), the page cache's read-ahead, and on
     1.4 GB/s of drive that costs time. TPC-H q6, q14 and q19 read twice
     t4's bytes and sit at the drives' rate: 585, 658 and 707 ms against
     t4's 121, 172 and 180. They pull the geometric mean to x0.76; the sums
     say 13%.
   - **t4 from the SSDs is as fast as Vortex from the page cache** again,
     on a box with no CPU to spare.
   - **Some of Vortex's evicted runs ran far faster** than the rest (q6
     202 against 585 ms, q14 351 against 658). This read them first as pages
     left cached by the eviction. The second small box's run-by-run
     counters (below) say otherwise: such runs read every byte from the
     drives, in their burst. Eviction falls short only on small queries, by
     11 to 21% of 42 to 122 MB, and those runs are no faster.
   - **The drives' counters outran fio** (t4's q6, 401 MB a run at 121 ms,
     3.3 GB/s, on drives fio reads at 1.4): the drives burst, below.

   **The middle box, 27 Oct 2026** (`cpu-i7ie/` there): the CPU pass on a
   Spot i7ie.12xlarge, 24 cores, 48 vCPUs (the i7i.24xlarge's Xeon 8559C),
   384 GiB, four SSDs as one RAID0 read by fio at 12.7 GB/s, 0.53 GB/s a
   core, the big box's ratio. The CPU log now also takes the RAID's reads
   run by run.

   | Summed medians, t4's disk tier against | TPC-H | ClickBench |
   |---|---|---|
   | Vortex from the SSDs | time x0.86, CPU x0.86 | time x0.86, CPU x0.88 |
   | Vortex from the page cache | time x1.01, CPU x0.94 | time x0.94, CPU x0.98 |

   - **The same picture as the other boxes:** t4 from the SSDs is as fast
     as Vortex from memory, and Vortex from the SSDs 10 to 17% slower than
     from memory (x1.17 and x1.10). t4 is faster on every query, 22 of 22
     and 15 of 15. Vortex's system time is 12.7 and 28.1 CPU-seconds a
     suite run, t4's 1.9 and 2.4.
   - **Run by run, the drives' counters fit them:** the fastest run read at
     14.6 GB/s (t4, against fio's 12.7 to 13.8), Vortex's at 12.8; t4's
     disk runs all read from the drives, its first run only from S3 (2 MB
     from the drives on q6, then 399 MB a run). So the overruns on one SSD
     and on the small box are reads outside the runs or fio's random reads
     underrating the drives, not reads from elsewhere. On the small box it
     is fio (below).
   - **The page cache reads the drives too,** where a top-10 query reaches
     data its first run skipped (ClickBench q23, up to 1.2 GB a run).
   - **The three boxes side by side:** a dashboard, "t4 Disk Tier Across
     Boxes".

   **The small box again, twice DataFusion's partitions, 27 Oct 2026**
   (`cpu-i3en-partitions/` there): a second Spot i3en.2xlarge, the bench
   rebuilt for the run-by-run drive counters. Each query ran every variant
   at DataFusion's default target partitions, 8 (one a vCPU), then at 16
   (`DATAFUSION_EXECUTION_TARGET_PARTITIONS`), back to back, on the same
   drives. TPC-H q3's plan hashes into 8 or 16 partitions, and both suites'
   files split into 8 or 16 groups; every process logged its count.

   | Summed medians, 16 partitions over 8 | TPC-H | ClickBench |
   |---|---|---|
   | Vortex from the page cache | time x0.99, CPU x1.01 | time x1.00, CPU x1.00 |
   | Vortex from the SSDs | time x1.01, CPU x1.00 | time x1.01, CPU x1.01 |
   | t4's disk tier | time x0.99, CPU x1.00 | time x1.02, CPU x1.00 |

   - **Twice the partitions changes nothing on this box:** its 8 vCPUs are
     already 87 to 97% busy (median over the queries), so more partitions
     add parallel work with no core to run it. t4 against Vortex from the
     SSDs stays x0.86 and x0.84 on TPC-H, x0.91 and x0.91 on ClickBench.
   - **A few queries move in every variant, the same way:** the short
     TPC-H q16 and q2 lose 11% and 4 to 6%; q13 and q18 gain 4 to 5%.
     ClickBench q23, the top-10 query, reads 19% more (t4 4.1 to 4.9 GB a
     run), probably as more files are under way before its bound prunes the
     rest; t4 takes x1.27, Vortex x1.11. t4's runs at 16 also fetch 27 to
     440 MB a run from S3, as its tier holds what its first run read, so
     part of its x1.27 is remote.
   - **The drives burst:** a run reads at up to 4.2 GB/s (Vortex's q6,
     826 MB in 198 ms; ScyllaDB's 4.1 GB/s) for a run or two, then at
     1.4 GB/s, fio's ten-second rate, and recover while idle. Vortex's q6
     at 8 partitions: 1.5 (the process's first run), 4.1, 4.2, 2.0, 1.4,
     1.4 GB/s. So Vortex at 16 partitions, run right after itself at 8,
     looks 19% slower on the six drive-bound TPC-H queries (from the SSDs
     1.3 times its time from memory or more: q6, q12, q14, q15, q19, q20);
     without them it is x0.98. t4 reads fewer bytes on most of them and
     stays in the burst (q6 400 MB a run, 3.7 to 3.9 GB/s every run); on
     q15, as many bytes as Vortex, it falls to 1.3 GB/s too.
   - **The burst, measured, 28 Oct 2026** (`budget-i3en/` there): a third
     i3en.2xlarge, fio alone on the RAID. After a minute's rest the drives
     read at 6.3 GB/s for about 0.4 s, 2.3 GB above the steady rate, then
     hold at 1.30 GB/s; 4 KiB reads burst to 644k a second and hold at 170k.
     Both steady rates are AWS's published limits for the size (1.3 GB/s,
     170K IOPS). After the burst is spent, a rest of 1 to 32 s gives 1.1 GB
     of it back (0.84 GB after 0.5 s); only the minute's rest gave more.
   - **Run after run, it catches both readers:** TPC-H q6, thirty runs in one
     process after a rest. Vortex reads 825 MB a run: two runs at 5.1 GB/s
     (163 ms), then 1.4 GB/s (594 ms) to the end. t4 reads 399 MB a run:
     eight runs at about 4.2 GB/s (96 ms), then 1.3 GB/s (307 ms). q19:
     Vortex 990 MB a run, one fast run, then 714 ms; t4 510 MB, three, then
     390 ms. At the steady rate t4 takes x0.52 and x0.55 of Vortex's time,
     its share of the bytes. Five runs a process flatter t4 on these
     queries (q6 x0.26 in the partitions pass), as its runs stay in the
     burst and Vortex's leave it; over the suite it hardly matters (both
     held to the steady rate: x0.84 on TPC-H against x0.86 measured).
   - **Vortex's extra bytes are the kernel's readahead:** its preads ask for
     400 MB a run on q6 (the process's rchar), the bytes t4 reads, and the
     drives read 825. md sets the RAID's readahead to twice its stripe, 2
     MiB here (two drives, 512 KiB chunks). At none, 399 MB, but every read
     goes a page at a time: 1.2 s a run. At 128 KiB, 422 MB and 140 ms a
     run, inside the burst; 512 KiB, 499 MB; 8 MiB, 485 MB. Merging adds
     nothing against t4: t4 merges nearby reads the same way here
     (`t4-coalesce=on`, the same 1 MiB gap). Both reach the drives as reads
     of about 117 KiB. The i7ie's four SSDs would get 4 MiB by the same
     rule, unchecked; Vortex reads x1.28 of t4's bytes there. So on the
     small box most of t4's lead on drive-bound queries is the RAID's
     default readahead under Vortex. The baseline keeps it: every number
     here is on the box's defaults, as a user would run it, and changing a
     kernel setting would change every other program on the box. The
     readahead runs explain the gap; they don't replace the baseline.
   - **q15 reads lineitem twice a run:** with no page cache under it, t4
     reads it from the drives twice (864 MB a run) and leaves the burst
     after two runs, as Vortex does.
   - **The burst biases any order:** a variant run after another drive-heavy
     one starts with less of it. Every comparison here ran Vortex from the
     SSDs right after Vortex from the page cache, which reads nothing from
     the drives, and t4 after Vortex from the SSDs, behind its own first
     run from S3.
   - **Two boxes of one type differ:** this box ran the same page cache
     queries in x0.80 (TPC-H) and x0.90 (ClickBench) of the first box's CPU
     time. Ratios repeat (t4 against Vortex from the SSDs x0.87 then x0.86,
     x0.91 then x0.91); times compare only within a box.

   **The middle box again, twice DataFusion's partitions, 28 Oct 2026**
   (`cpu-i7ie-partitions/` there): a second Spot i7ie.12xlarge, the same
   binary, every variant at 48 partitions (one a vCPU) and at 96, the two
   counts' order flipped query by query against the order effect above.
   The plans and every process's log show 48 and 96.

   | Summed medians, 96 partitions over 48 | TPC-H | ClickBench |
   |---|---|---|
   | Vortex from the page cache | time x1.12, CPU x1.13 | time x1.01, CPU x1.03 |
   | Vortex from the SSDs | time x1.10, CPU x1.13 | time x1.05, CPU x1.06 |
   | t4's disk tier | time x1.11, CPU x1.15 | time x0.99, CPU x1.01 |

   - **Twice the partitions costs on TPC-H, with cores to spare:** the 48
     vCPUs are 53 to 59% busy at 48 (median over the queries), yet at 96
     every variant takes 13 to 15% more CPU and 10 to 12% more time, and 1
     to 3 of 22 queries are faster. The short join queries pay most (q2,
     q11, q16, q22: x1.15 to x1.40); the scan-heavy q1 gains (x0.90 to
     x0.93). Likely per-partition work on a small data set (SF10 is 2.6 GB
     of files; each hash repartition has inputs times outputs channels,
     9,216 at 96); not measured. ClickBench, 77 to 79% busy, barely moves,
     but for q23.
   - **t4 against Vortex holds:** from the SSDs, x0.87 at 48 and x0.88 at 96
     on TPC-H, x0.91 and x0.86 on ClickBench, where Vortex loses more at 96
     on q21, q23 and q28.
   - **ClickBench q23 reads more at twice here too:** Vortex from the SSDs
     7.2 then 10.4 GB a run, x1.30; Vortex from the page cache reads 220 MB
     a run from the drives at 96, data its first run skipped. t4's later
     runs fetch from S3 at both counts, 219 to 794 MB a run, and on q24 and
     q26 now and then, up to 72 MB.
   - **No burst, no order effect:** a drive-bound query's runs read at a
     steady rate, run after run, up to 14.8 GB/s (t4's q6) against fio's
     12.7 (random 1 MiB reads) and 13.9 (sequential). Leaving out q23, the
     counts' ratio barely depends on which ran first: x0.99 either way from
     the page cache, x1.02 and x1.03 for Vortex from the SSDs, x0.96 and
     x1.00 for t4.
   - **Two boxes of one type differ again:** this one took x1.06 (TPC-H)
     and x1.16 (ClickBench) of the first i7ie's CPU from the page cache. t4
     against Vortex from the SSDs repeats on TPC-H (x0.86, then x0.87); on
     ClickBench it moves, x0.86 then x0.91, mostly on q23 and q28.

   **One SSD, no RAID, 28 Oct 2026** (`cpu-one-ssd/` there): a second
   i3en.2xlarge and i7ie.12xlarge, the CPU pass on one instance-store SSD as
   the box mounted it, at the kernel's 128 KiB readahead (each drive
   reports no optimal I/O size), DataFusion's default partitions. Each
   drive holds AWS's rate per drive: 0.68 GB/s on the i3en (half its pair),
   3.17 on the i7ie (a quarter of its four). Both burst after a rest: the
   i3en's to 2.3 GB/s for about half a second, the i7ie's to 7.4 GB/s for
   about a second, which the RAID's runs never showed.

   | t4 against Vortex from the SSD, summed | TPC-H median | slowest | ClickBench median | slowest |
   |---|---|---|---|---|
   | i3en, one SSD | x0.92 | x1.00 | x0.94 | x0.94 |
   | i3en, RAID (first box) | x0.87 | x0.86 | x0.91 | x0.91 |
   | i7ie, one SSD | x0.87 | x0.90 | x0.83 | x0.93 |
   | i7ie, RAID (first box) | x0.86 | x0.88 | x0.86 | x0.87 |

   - **Readahead off the table, the bytes even out:** on ClickBench Vortex
     reads x1.07 to x1.09 of t4's bytes, on TPC-H x0.72 to x0.78, t4 the
     more (below). t4's CPU is x0.90 to x0.95 of Vortex's.
   - **Medians flatter t4 on one SSD:** both readers' early runs ride the
     drive's burst and the later ones its steady rate, and t4, whose first
     run reads S3, rides it for more runs. ClickBench q20 on the i7ie:
     Vortex 507 ms, then about 1,050; t4 452, 451, 551, then about 980. The
     slowest runs, both at the steady rate, compare them: x0.93, about
     t4's share of the bytes (x0.90).
   - **t4's disk tier alone reads a table again for every pass:** with its
     memory tier pinned off, a query that reads a table more than once reads
     it from the drive each time, where Vortex's later passes hit the page
     cache (evicted only between runs). q21 on the i3en: t4 1,410 MB a run
     from the drive, of the 602 MB its first run fetched (none from S3
     after); Vortex 489. On the
     i3en's one slow drive this costs t4 its TPC-H lead (q15 x2.52, q8
     x1.29); with t4's memory tier on, as t4 runs outside a benchmark, the
     repeats would come from memory, unmeasured.
   - **i7i.24xlarge, one SSD, 27 Oct** (the disk rerun above, an older
     binary without CPU logs): t4 about 10% faster at the median.

   **Initial and stable latency, 28 Oct 2026** (`latency/` there): the
   three box types again, all of each box's SSDs as one RAID0 on its
   defaults, as rented. The bench gained `--opt rest-runs=5,rest-ms=2000`:
   the drives rest before each of runs 1 to 5. Each query, one process a
   reader: a first run; five rested runs, their median the initial
   latency; twelve back to back, the median of the last five the stable
   latency. Vortex from memory rests the same as a control: it reads no
   drive. The i3en's ClickBench ran on a second i3en beside the first.

   | t4 over Vortex, SSDs | initial | stable | stable slowest | CPU | rest alone (memory) |
   |---|---|---|---|---|---|
   | i3en, TPC-H | x0.90 | x0.84 | x0.85 | x0.93 | x1.01 |
   | i3en, ClickBench | x0.91 | x0.91 | x0.92 | x0.94 | x1.01 |
   | i7ie, TPC-H | x0.88 | x0.87 | x0.85 | x0.86 | x1.01 |
   | i7ie, ClickBench | x0.88 | x0.88 | x0.90 | x0.89 | x1.00 |
   | i7i, TPC-H | x0.89 | x0.87 | x0.77 | x0.88 | x1.65 |
   | i7i, ClickBench | x0.86 | x0.86 | x0.86 | x0.85 | x1.10 |

   - **t4 leads on every box, rested and stable,** by 9 to 16%. t4 from
     the SSDs against Vortex from memory: x0.90 to x1.00 rested, x0.95 to
     x1.05 stable.
   - **The burst shows only on the i3en's TPC-H:** Vortex from the SSDs
     takes x1.07 of its time from memory rested and x1.25 stable; t4 x0.96
     and x1.05. t4's lead grows as the burst is spent (x0.90 to x0.84), as
     Vortex reads more bytes (the RAID's readahead). The rested runs read
     at the burst rate (4 to 5 GB/s on q6, q14, q19) and the last
     back-to-back runs at 1.3 to 1.4 GB/s on every drive-bound query.
     ClickBench's runs read 3 to 6 GB each, far past the 1.1 GB a rest
     gives back: rested and stable match (x0.91).
   - **The i7ie's RAID bursts too,** to 18 GB/s for about 2.5 s after a
     rest, then 12.2; its queries read slower than that, so rested and
     stable match.
   - **On the i7i a rest slows the CPU, not the drives:** Vortex from
     memory takes x1.65 on TPC-H rested, x1.10 on ClickBench, with 20 to
     25% more CPU time for the same work (q1 11.7 s against 9.3 s); the
     i7ie, the same Xeon, shows none. There initial latency is the CPU
     waking; t4 against Vortex still compares like with like. One fio job
     reads its six drives at a flat 25.8 GB/s, so a burst, if any, isn't
     seen.
   - **t4's later runs read S3 only on ClickBench q23,** the top-10 query,
     on every run (up to 561 MB on the i7ie, 779 MB on the i7i), which
     counts against t4.
   - **Reported: the stable latency alone.** The rested runs mix the drives'
     burst with the CPU waking (the i7i) and show the burst on one box
     only, so they don't read clearly; the dashboard reports the last five
     back-to-back runs, the rest here for the record.
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

Every process takes under 5 minutes: the queries and the number of runs
are picked from one timed pass, before any comparison. The boxes are
single-socket Spot boxes in us-east-1 (step 4): NVMe for the disk tier, S3
in the region.

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
  while a buffered `pread` reads ahead. TPC-H likely ties again. (Step 4
  found otherwise: t4 x0.84 to x0.91 of Vortex's time from the same SSDs,
  stable, on all three boxes.)

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
  Vortex's call; this benchmark evicts its files' pages before every run
  instead, in one process, since a fresh process's first run costs about
  400 ms beyond its reads (step 4).
- **A custom allocator's reads.** With CUDA's pinned allocator, t4's bytes
  are copied in. Reading straight into the caller's buffer would need that
  buffer page-aligned, for `O_DIRECT`.
- **A memory hit copies once.** t4 copies a hit into the read's output, as
  `pread` copies from the page cache. It costs: t4's memory tier takes more
  CPU than its disk tier, x1.09 and x1.04 by query (step 4's CPU pass).
  Handing Vortex the pool's chunk instead would hold that chunk against
  eviction for as long as Vortex keeps the array.
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
| Vortex | `benchmarks/datafusion-bench/src/get_counter.rs` | `GetCounter`: GETs counted below t4, a line a run, per `--opt get-log` | yes, uncommitted |
| Vortex | `benchmarks/datafusion-bench/src/cpu_log.rs` | `CpuLog`: each run's CPU, the process's and the machine's, per `--opt cpu-log` | yes, uncommitted |
| Vortex | `benchmarks/datafusion-bench/src/page_cache.rs` | `PageCacheEviction`: the local files' pages evicted before every run, per `--opt evict-page-cache=on` | yes, uncommitted |
| Vortex | `vortex-bench/src/runner.rs` | every run's row count checked against the first's | yes, uncommitted |
| Vortex | `benchmarks/duckdb-bench/src/main.rs` | `VORTEX_T4_*` set per `--opt t4-*` | no |
| Vortex | `vortex-bench/src/random_access/take.rs` | files opened by URL | no |
