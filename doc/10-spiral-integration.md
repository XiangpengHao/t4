# 10 — Spiral integration: t4 in Kismet's place, then in spiral-io's read path

Status: design (2026-10-28). Built: plan step 1, the adapter, on
spiraldb's branch `xiangpeng/t4-cache`, not pushed. Revised the same day:
the first version gave t4 a new core API, files opened under the caller's
key and version (Later); t4's existing API does the job, with Spiral
naming each file as a path (decision 2). macOS runs t4 for real, untuned
(decision 12). Revised 29 Oct 2026: Spiral's processes share one cache
through its owner, `11-shared-mode.md` (decision 8); the lock is built
(plan step 3).

The goal: t4 replaces spiraldb's local I/O cache, Kismet, and then
takes over the rest of the work `spiral-io` does to read fast. The rest
of spiral-io's object store stays Spiral's.

The shape:

- **Phase 1: t4 in Kismet's place** (decisions 1 to 3). Spiral names
  each file as a path t4 already takes, and t4 fetches through Spiral's
  own GET. Nothing new in t4.
- **Phase 2: t4's merging and races in place of Spiral's coalescing and
  hedging,** where measurements say so (decision 4).
- **spiral-io's store keeps** providers, credentials, pins, retries, the
  scheduler, writes and lists (decision 1).
- **What Kismet gives its hosts, t4 has to give too:** a cache that
  survives a restart (decision 7), one cache shared by Spiral's
  processes (decision 8), and a footprint a laptop accepts (decision 9).
  That is the new t4 work.

## What shapes it

Read against spiraldb's `develop` at 4d18993728 (28 Oct 2026), which uses
object_store 0.13, as t4 does, and a nightly toolchain
(`nightly-2026-10-09`).

**spiral-io's object store does much besides caching.** Every reader in
Spiral (Vortex, Parquet, Hugging Face files, Blob ranges) reads through it
(`spiraldb/spiral-io/src/store.rs`). It holds:

| Job | Where |
|---|---|
| providers: S3, GCS, SPFS, Hugging Face, HTTP, local files, memory | `registry.rs`, a file each |
| credentials: SPFS's signed GETs, Hugging Face's tokens, S3 endpoints such as MinIO's, refresh | `credentials.rs`, the providers |
| pins to one version, revalidation for hosted serving | `pinned.rs`, `store.rs` |
| retries, classified; a failed body resumed from where it stopped | `store.rs` |
| scheduler permits per resource, usage attribution | `scheduler.rs`, `usage.rs` |
| writes, deletes, copies, lists, globs | `store.rs`, `discovery.rs` |
| hedging: a second request after a delay learned per size bucket | `hedge.rs` |
| coalescing nearby ranges of a batch | `coalesce.rs` |
| the cache | `cache.rs`, `cache/` |

Hedging and coalescing work on every host, cache or not. Spiral's Vortex
reads use Vortex's own `ObjectStoreReadAt` over this store
(`IoSession::vortex_read_at`), which merges reads less than 1 MiB apart
up to 16 MiB before they reach it.

**The cache has one caller.** The store reads through it in two steps:

- `cache_plan` says whether a read may use the cache, and under what pin.
  A read may if its bytes have one version: its store's keys are
  immutable, or the read carries a version or an e_tag (`If-Match`). An
  unpinned read of a rewritable key, a conditional read, and every read
  with the cache disabled go to the provider. Under
  `CacheAccess::Revalidate` (hosted serving), a binding sends a HEAD
  before its first use of the cache.
- `cached_get` names the bytes, sizes the object, and calls
  `SharedCache::get_or_fetch_blocked_range(identity, range, size, fetch)`.

  - **The name** is a `SourceIdentity`: provider kind, store or endpoint,
    object, snapshot (version, e_tag, commit, ...), and the request's
    scope. For HTTP it takes a digest of the whole URL and of the bearer's
    credential scope too, since either can change what a URL returns.
    The cache keys on its SHA-256 (`SourceIdentity::cache_digest`).
  - **The size** comes from metadata the binding holds, a size pinned by
    the caller (`pin_object`), the size the disk cache recorded for the
    identity, or else a HEAD. Blocks are planned against it, so a file's
    last block is cached whole.
  - **`fetch`** is the managed GET of one range: a scheduler permit,
    retries, a hedge if the store allows one, and the pin applied to the
    request (`ManagedStore::fetch`, then `stream_get`).

**Which stores' reads are cached:**

| Keys | Stores | Cached when |
|---|---|---|
| immutable | SPFS, Arbor's object store, Hugging Face at a commit | always |
| versioned | S3, GCS, HTTP, local files, memory, Hugging Face at a branch | the read is pinned |
| unversioned | providers that don't say | never |

**`SharedCache` is a cache of 1 MiB blocks** (`spiraldb/spiral-io/src/cache.rs`,
`cache/block.rs`, `cache/kismet.rs`, and `shared/spiral-kismet-cache`):

- **Disk:** Kismet, a multi-process, crash-safe cache in a directory, one
  file per entry, in 64 shard directories. Eviction is Kismet's batched
  second chance, so the cache can grow past its target for a while,
  "rarely by more than a factor of 2 or 3", in Kismet's words. Writes
  aren't synced; each entry carries a CRC32, and a damaged one reads as
  absent. Reads and writes go through the page cache, 32 at a time per
  class. Before each write, the drive must keep 64 MiB plus twice the
  block free.
- **Memory:** an optional Foyer cache. Every host sets it to zero.
- **Fetch-once, in one process:** a table of the blocks being fetched,
  which a read for one of them waits on. Two processes can fetch one
  block at once.
- **Runs:** a read looks its blocks up in windows of 8, 4 windows at
  once; the absent blocks next to each other in a window are fetched in
  one GET.
- **Object sizes**, stored beside the blocks, so a later process sends no
  HEAD for a file it has read before. So a Hugging Face file cached at a
  commit reads with `HF_HUB_OFFLINE` and no credentials.
- **Two classes,** data and manifests, each a cache of its own with its
  own budget: the manifests take an eighth of it, 256 MiB at most.

**Who installs it: only the hosts on a user's own machine.**

- desktop Studio, whose local ScanDAL host binds a disk cache
  (`spiraldb/apps/spiral-studio/src-tauri/src/scandal.rs`);
- `spiral connect`, a local ScanDAL host that hosted Studio pairs with
  (`spiraldb/cli/spiral-cli-connect`);
- the SQL CLI, in the book's words. `spiral-defaults` defines
  `default_cached_client_session` for it, but nothing in spiraldb calls
  it, at 4d18993728 or at d7d2e23331 (29 Oct 2026). So in the code,
  the two hosts above are all, both long-running local servers.

Server query processes, Flight SQL, the worker engines, Python and ADBC
install none (`book/src/platform/storage/local-io-cache.md`). On the three
that do, the default is a 20 GiB target in
`~/.cache/spiral/spiral-io-cache-v6`, shared by every Spiral process of
the user, and kept across restarts. A cache that can't be made fails the
session.

**Studio ships for macOS and Linux.** Spiral's release workflow builds it
for Apple silicon and for x86-64 Linux, and for nothing else
(`.github/workflows/release-app.yml`). So t4 being unix-only costs
nothing; Kismet's non-unix fallbacks have no user.

**What t4 has, set against that:**

| Kismet's hosts need | t4 today |
|---|---|
| one cache over many stores | one inner store per `T4Caching`, files named by path; a path is up to 1,024 bytes |
| immutable and pinned reads cached | a file is cached under its e_tag; a read of a named version, or with any condition but `If-Match` on that e_tag, goes to the inner store |
| no HEAD for a file Spiral already knows | `open_listed` takes the file's size and e_tag from the caller and asks nothing |
| fetches through Spiral's own GET | t4 GETs from its inner store, carrying each read's `GetOptions::extensions` |
| fetch-once | yes, within a process |
| survives a restart | no: a restart loses everything (`03-cache.md`, decision 9); a snapshot is in 03's Later |
| several processes, one cache | a lock: a second cache on a cache file fails to build, `Error::Busy` (built 29 Oct 2026). Shared mode, one process owning the cache and the others reading through it, is `11-shared-mode.md`. The key-value store had both, and lost them with it (`doc/02-multi-process.md`, removed in 45a0009 on 21 Oct 2026) |
| a laptop's footprint | the cache files are allocated in full at build (`fallocate`); a memory capacity is required |
| a disk error served from the remote | yes: a failed cache-file read goes to the inner store |
| macOS | built and tested there, for development only until this doc (decision 12): a thread pool, `F_NOCACHE` |

**What t4 brings that Kismet doesn't:** blocks in memory and on disk
under one policy, a cache file per drive read and written with `O_DIRECT`
through io_uring, a cap on bytes in flight with waiting GETs merged, and
races: footers by two requests, stragglers late (`06-read-modes.md`).
Measured under Vortex's own benchmark (`09-vortex-integration.md`, step
4): from S3, faster on TPC-H and level on ClickBench; from one SSD, about
10% faster than Vortex's reads of it; from memory, level with the page
cache. Its I/O pays on Linux servers with NVMe drives. None of Kismet's
hosts is one.

## Decisions

1. **t4 takes Kismet's place; spiral-io's store stays.** t4 takes all
   `SharedCache` does: blocks, tiers, fetch-once, object sizes. The seam
   is `cached_get`, the one place the cache is called. `SharedCache` keeps
   its name and constructor, so the tests that build one (Arbor, Hugging
   Face, the I/O plugin, the legacy table, S3) keep working; its body
   becomes t4.

   spiral-io's store keeps the rest: providers, credentials, pins,
   revalidation, retries and resume, scheduler permits, usage
   attribution, writes, deletes and lists, and its metrics. They are
   Spiral's rules; t4 holding them would make t4 depend on Spiral. Its
   hedging and coalescing stay until phase 2 measures t4's against them
   (decision 4).

2. **Spiral names each file as a path t4 already takes.** No new t4 API:
   one `T4Caching` holds files from every store, told apart by their
   names.

   - **The name** is `<digest>/<object path>`: Spiral's identity digest in
     hex, then the object's path in its store. The digest is the key
     Kismet uses now: store, object, version and the request's scope, so
     two files Spiral keeps apart never share a name. The object's path
     comes last only so that t4 still knows a footer by a path's ending
     (`.parquet`, `.vortex`).
   - **The e_tag** is the version Spiral's identity holds: the e_tag, the
     version id, or for an immutable key its object name, as `cached_get`
     takes it now.
   - **Opening:** `open_listed`, with that name, e_tag and Spiral's size.
     t4 asks nothing.
   - **Reading:** `T4Caching::get_opts` on the name, with `If-Match` on
     the e_tag and Spiral's fetch in `GetOptions::extensions` (decision
     3). t4 serves it from the cache, as it serves any read with
     `If-Match` on the e_tag it holds.

   The digest takes 65 bytes of the name. A file whose object path is
   longer than 959 bytes isn't cached, as a path longer than 1,024 bytes
   isn't now. t4 keeps each name it is given until the process ends, one
   `ObjectMeta` each, unless a GET finds the file changed.

3. **t4 fetches through Spiral's GET, and at first races nothing.** t4's
   inner store is a router in spiral-io. Each read carries, in its
   extensions, a `SpiralFetch`: the binding, the object's own path, and
   the pin. t4 passes a read's extensions to every GET it sends for it,
   and to the inner store when it can't serve the read. The router takes
   the `SpiralFetch`, drops t4's `If-Match`, and sends the GET through the
   binding's managed GET past its cache (`ManagedStore::stream_pinned_get`),
   the pin applied as `ManagedStore::fetch` applies it: a scheduler
   permit, retries, resume, a hedge if the store allows it.
   t4 decides which blocks are fetched, in what GETs, and that each is
   fetched once; Spiral decides how a GET is sent.

   A file changed under its pin fails the GET with `Precondition`, as
   now; t4 forgets the name, and the read fails rather than mix two
   versions.

   At first, t4's read policy is `FetchRuns::racing_footers(1)` and it
   has no timer, so it races nothing: a footer is fetched by one request,
   and no straggler is raced late. Otherwise one GET could be raced
   twice: by t4's race, and each of its requests by Spiral's hedge. t4's
   cap on bytes in flight is set no lower than Spiral's own limits allow,
   so it never holds a GET Spiral's scheduler would send.

4. **Phase 2: t4's merging and races replace Spiral's coalescing and
   hedging, where they measure better.** For reads through t4, once
   phase 1 is measured:

   - **Hedging:** Spiral's hedge, with t4 racing nothing (decision 3),
     against t4's races (footers by two requests, stragglers late on a
     timer from Spiral's runtime), with Spiral's hedge off for the
     router's GETs. Stores whose descriptor forbids hedged reads get no
     race either way.
   - **Coalescing:** Spiral's and Vortex's merging of nearby ranges above
     t4, against t4's merging of waiting GETs below it, by
     `09-vortex-integration.md`'s rule (decision 5 there): the GET counts
     and the slowest GET.

   Each is judged by the slowest GET, the GETs over 1 s, and the slowest
   run. What loses goes, for reads through t4. Fetch-once already moved
   to t4 with the cache (decision 1). Hosts without a cache keep Spiral's
   hedging and coalescing: t4 on them is Later.

5. **Each host takes a profile: Kismet's behaviour on a user's machine,
   t4's defaults on a server.** No new mechanism in either.

   - **A user's machine** (Studio, `spiral connect`, the SQL CLI):
     `FixedPlacementPolicy` admitting every fetched block to disk, a disk
     hit staying there; direct I/O off, so the OS's page cache serves
     repeated reads as it serves Kismet's files; a memory tier of one
     chunk, the least t4 takes. The disk tier is what survives a restart
     (decision 7), so admitting straight to it loses nothing at exit.
   - **A server**, if one opts in (Open questions): t4's defaults: the
     CLOCK, fetched blocks to memory and memory's victims to disk, direct
     I/O, a cache file on each drive.

6. **Two caches, one for each class, as now.** Data and manifests each
   get a `T4Caching` of their own, with a cache file and budget of their
   own, divided as now. So a scan can't push the manifests out, as it
   can't now, and the comparison with Kismet measures t4 and nothing
   else. One cache for both is Later.

7. **The disk tier survives a restart.** 03's Later, needed now: a
   desktop app restarts, and Kismet's cache doesn't start cold.

   - **At a clean shutdown,** once the writes in flight are done and the
     cache file is flushed (`fdatasync`, which direct I/O off needs), the
     cache writes a snapshot: each file version with blocks on disk
     (name, e_tag, size, id), and each such block's chunk; and the next
     file version id, so that no id is handed out twice across restarts
     (`03-cache.md`, decision 5). A temporary file, then a rename. At
     20 GiB that is 20,480 block entries, about 240 KiB.
   - **At startup** the cache loads the snapshot, then deletes it before
     it writes any chunk, so that a crash can't leave it naming chunks
     reused since. A snapshot from another layout, another block size or
     another chunk count is ignored, and the cache starts cold.
   - **Every minute or so** the cache writes a snapshot too, and keeps
     each freed disk chunk unused until the next snapshot is durable. A
     desktop app is force-quit, a laptop's battery runs out: a clean
     shutdown can't be counted on. A crash then loses the blocks written
     since the last snapshot.
   - **Sizes across a restart:** `T4Caching::known_size(name)` answers
     the size of the version t4 holds under a name, from the snapshot.
     So a restarted Studio sends no HEAD for a file it cached before, and
     a Hugging Face file cached at a commit still reads offline, as with
     Kismet's size records. The one addition to t4's API.
   - **Memory's blocks aren't written to disk at shutdown.** On a user's
     machine memory holds one chunk; a server's memory tier starts cold,
     which the tiering accepts.
   - **No checksums.** A snapshot names only chunks flushed before it,
     and none of them is written again until the next snapshot is
     durable, so a crash can't leave it naming a torn chunk. Kismet's
     CRC32 covers what t4 doesn't do: writing without a sync. A failed
     read of the cache file goes to the remote, as now.

   Spiral's names are stable across processes, a SHA-256 of canonical
   bytes, so a restarted process opens the files it cached before under
   the same names.

8. **One process owns the cache; the others read through it.** Each
   cache file takes `flock(LOCK_EX | LOCK_NB)` as the cache is built,
   held for the cache's life, as the key-value store's mount lock was
   (doc/02 before 45a0009). The kernel drops it when its holder exits,
   killed or not. Built: a cache that loses it fails to build with
   `Error::Busy`.

   Kismet lets Studio and `spiral connect` share one cache, and t4 does
   too, through its owner (`11-shared-mode.md`, the user's call of
   29 Oct 2026): whichever host takes the lock owns the cache and serves
   it from a thread of its own; the other reads through it, fetching what
   the cache doesn't hold with its own Spiral connection. Until that is
   built, the host that starts second gets `Error::Busy`, and spiral-io
   runs it uncached, saying so once, rather than failing the session as a
   cache that can't be made does now.

9. **A user's machine gets a cache file that grows.** t4 allocates its
   cache files in full at build: 20 GiB the first time Studio starts, and
   a failed build, so a failed session, on a disk with less. On a user's
   machine the cache file is sparse instead, sized but not allocated, and
   before each write the drive must keep 64 MiB plus twice the block
   free, as Kismet's rule is. A block that finds too little isn't cached,
   as a block that finds no free chunk isn't now (`03-cache.md`, decision
   8). A server's cache files stay allocated in full. Unlike Kismet's
   target, t4's budget is never exceeded.

10. **Spiral's settings stay Spiral's.**

    - **`io.cache.enabled`** still bypasses the cache in `cache_plan`, for
      a connection or for all; a session that starts with it off builds
      its caches at its first read with it on, as now.
    - **`CacheAccess::Revalidate`** stays in `cache_plan`: the HEAD comes
      before t4 is asked.
    - **`IoSession::clear_caches`** has no caller in spiraldb. t4 has no
      `clear`, so it goes with Kismet (decision 11); a `clear` in t4 is
      Later.
    - **Metrics:** Spiral counts bytes from memory, from disk and from the
      remote, and its disk errors, in thin wrappers around t4's two
      policies, whose hooks hear every memory hit, disk read and GET.
      Admission skips come from decision 9.
    - **The resource settings** (`io.cache.disk_path`, capacity, block
      size, concurrency) still need a new session. Block size is t4's
      1 MiB, Kismet's default; a host that sets another is refused.

11. **Kismet goes, with `SharedCache`'s body.** Once Studio and the CLI
    run on t4 (plan step 6), these are deleted:

    - `shared/spiral-kismet-cache`, `spiral-io/src/cache/kismet.rs`,
      `cache/block.rs`, and `cache.rs`'s Foyer cache, fetch table and size
      entries;
    - the dependencies `kismet-cache`, `foyer` and `foyer-common`;
    - spiral-error's `SpiralError::Foyer` and `From<SpiralError> for
      FoyerError`. They are unused already: Foyer 0.22's `get_or_fetch`
      takes any `Into<anyhow::Error>`, and `cache.rs` turns Foyer's errors
      into spiral-io's itself;
    - spiral-o11y's `foyer=warn` log directive;
    - the settings only Kismet reads (`disk_shards`,
      `memory_capacity_bytes`);
    - `IoSession::clear_caches` (decision 10).

    The book's page on the local I/O cache is rewritten.

12. **macOS runs t4 for real, untuned.** Studio ships for macOS, so t4
    replaces Kismet there too (the user, 28 Oct 2026). `00-meta.md` says
    so now: t4 must be correct on macOS and tested there, and no worse
    for Studio than Kismet, but nothing is designed, tuned or benchmarked
    for macOS's speed, and no Linux design bends for it. On macOS t4 runs
    its thread-pool backend; a user's machine's profile turns direct I/O
    off (decision 5), so `F_NOCACHE` isn't used there.

## The interfaces

t4: nothing new for phase 1. `T4Caching::builder`, `open_listed` and
`get_opts` as they are; `known_size` comes with the snapshot (decision
7):

```rust
impl T4Caching {
    /// The size of the version the cache holds under `location`, if it
    /// holds one: known since it was opened, or from the last snapshot.
    pub fn known_size(&self, location: &Path) -> Option<u64>;
}
```

spiral-io, a module of its own (`spiraldb/spiral-io/src/cache/t4_cache.rs`;
`t4.rs` would shadow the crate's name there):

```rust
/// Where t4's GETs for one read go: carried in the read's
/// `GetOptions::extensions` (decision 3).
#[derive(Clone)]
pub(crate) struct SpiralFetch {
    /// The binding the read came through.
    store: ManagedStore,
    /// The object's path in that binding.
    path: Path,
    /// The version the read is pinned to, none for an immutable key.
    snapshot: Option<SnapshotIdentity>,
}

/// t4's inner store: each GET goes where its read's `SpiralFetch` says.
/// Every file is opened with `open_listed`, so t4 sends it nothing but
/// GETs; the rest answers `NotSupported`.
struct SpiralFetchRouter;
```

`SharedCache` keeps its API and holds one of two engines,
`SharedCacheEngine::{Kismet(KismetCache), T4(T4Cache)}`, `KismetCache` being
the old body renamed. The managed store branches once, in `managed_get`:
Kismet's reads go to `cached_get` as before, t4's to `t4_cached_get`, which
sizes the object as `cached_get` does, less Kismet's size records, and calls
`T4Cache::read`. That opens the name and reads it (decision 2):

```rust
let name = Self::file_name(identity, &fetch.path); // <digest>/<path>
self.cache.open_listed(ObjectMeta {
    location: name.clone(),
    size,
    last_modified: UNIX_EPOCH.into(),
    e_tag: Some(snapshot.value.clone()),
    version: None,
});
let mut extensions = Extensions::new();
extensions.insert(fetch);
let get = GetOptions {
    range: Some(GetRange::Bounded(range)),
    if_match: Some(snapshot.value.clone()),
    extensions,
    ..GetOptions::default()
};
self.cache.get_opts(&name, get).await
```

The setting is `io.cache.engine`, `kismet` (the default) or `t4`, which
runs as on a user's machine; `IoCacheConfig` and `CacheSettings` carry it
as `CacheEngine::{Kismet, T4(T4Profile)}`. `T4Profile::Server` is reached
from code only.

## The plan

Phase 1, t4 in Kismet's place:

1. **The adapter, in a spiraldb branch,** t4 taken by git, nothing new in
   t4: the names and `SpiralFetchRouter` (decisions 2 and 3), the two
   profiles (decision 5), two caches (decision 6), the metrics wrappers.
   A setting chooses Kismet or t4, Kismet the default. The cache tests
   named in decision 1 run on both, and new ones read one object path
   from two stores, a version id, an immutable file, a file changed
   under its pin, and a read t4 can't serve.

   Built 28 Oct 2026 on spiraldb's `xiangpeng/t4-cache`, off `develop` at
   4d18993728, t4 by git at 0cb2fbf; not committed. About 1,000 lines
   added in 17 files, not counting the lockfile: 569 in `cache/t4_cache.rs`,
   half of them its tests. Tests, all passing:
   - **New, on t4:** one path in two buckets, an immutable file fetched
     once (both profiles, and Spiral's hit and remote byte counts), a
     version-id pin, a file changed under its pin (`VersionMismatch` past
     the block t4 holds), a name past 1,024 bytes read from the source each
     time; the setting's values, and t4 refusing a block size other than
     1 MiB.
   - **Existing, now on both engines:** S3's and Hugging Face's
     "endpoints keep cached bytes apart", and the I/O plugin's live
     `io.cache.enabled` toggle and connection override tests.
   - **Existing, Kismet only:** the rest assert Kismet's own layout: 4-byte
     blocks and the GETs they make, its windows, its files on disk, a
     reopened cache keeping its blocks, its directory removed under it.
   - spiral-io, the I/O plugin, Hugging Face and Arbor: 423 tests; the
     legacy table's cache tests; workspace clippy (`-D warnings`, CI's
     exclusions) clean, and the book's check.

   Found on the way: a binding bound to an object's directory makes the
   name `<digest>/<file name>`, so names stay short. A t4 cache at the
   default 20 GiB allocates its file in full as it opens, about 2 s in a
   test on a Mac (decision 9). `clear_caches` refuses on t4 (decision 10).
   t4's counters go to the metrics of the session that installed the cache
   last; sessions sharing one cache share them.
2. **Measured on Linux: Kismet and t4 in the same seat,** a user's
   machine's profile, the same drive, OS defaults. A ClickBench subset
   from S3 through Spiral's engine: engine-bench, if it reads through
   spiral-io's managed stores (to be checked), else a harness that does.
   The first run, from the remote, and the later ones, from disk, both
   caches' files evicted from the page cache before each run; then the
   same with the page cache left warm, as a user's machine runs. The
   slowest run first, then the median, and the slowest GET and GETs over
   1 s; bytes fetched, and CPU.
3. **The lock in t4** (decision 8): `flock` on each cache file,
   `Error::Busy`, with a test that a second cache on one file fails to
   build and builds once the first is dropped. Built 29 Oct 2026
   (`CacheFiles::open_file`; `a_cache_file_has_one_owner`, which reaches
   the file through a symlink too). Shared mode follows it, by
   `11-shared-mode.md`'s build order.
4. **Restarts in t4** (decision 7): the snapshot at a clean shutdown,
   then every minute; `known_size`. Tests: a restart keeps the disk tier
   and its sizes; a process killed keeps the last snapshot's blocks and no
   others; the fuzz target restarts the cache between operations.
5. **The sparse cache file in t4** (decision 9), with a test of the free
   space rule on a small file system.
6. **Studio and the CLI on t4:** checked on a Mac against Kismet as in
   step 2, and across a restart, for no worse, not tuned (decision 12);
   then t4 the default. The first start removes Kismet's
   `spiral-io-cache-v6` (Open questions).
7. **Kismet removed** (decision 11).

Phase 2, t4 in the read path:

8. **Hedging, measured** (decision 4): Spiral's hedge against t4's races,
   on the reads of step 2, from S3 and GCS. What loses goes, for reads
   through t4.
9. **Coalescing, measured** (decision 4): Spiral's and Vortex's merging
   above t4 on and off, with t4's merging below. What loses goes, for
   reads through t4.

## Open questions

- **Servers.** Server query processes, Flight SQL and the worker engines
  cache nothing today. If they run long on machines with NVMe drives,
  t4's defaults there (decision 5), under `CacheAccess::Revalidate`, are
  where its I/O shows, and where a restart that starts cold costs least.
  Where they run, and whether caching there is wanted, is Spiral's to say.
- **Kismet's old directory.** Removing `spiral-io-cache-v6` gives a user
  20 GiB back, but an older Spiral may still be running against it. The
  book says it can be removed "while Spiral is stopped". Remove it on
  first start anyway, since Kismet treats a vanished entry as absent, or
  leave it to the user.
- **t4's dependencies.** Spiral would carry bbtree, vstd and five Verus
  crates, vstd pinned exactly: doc/09's question, for a second user.

## Rejected

- **t4 replacing spiral-io's object store.** t4 would hold Spiral's
  providers, credentials, pins, retries, scheduler, writes and lists:
  Spiral's rules, which would make t4 depend on Spiral. t4 takes the
  cache, then what the measurements give it of the read path (decisions
  1 and 4).
- **Spiral reading Vortex files through `T4ReadAt`.** Only Vortex's reads
  would be cached, not Parquet's, Hugging Face's or Blob ranges'; and
  they would skip Spiral's credentials, pins and retries. In Spiral, t4
  sits below the store every reader shares, so no reader changes.
- **t4 in front of each provider's transport,** a `T4Caching` per
  binding. Every binding a cache of its own, with files and a budget of
  its own; and t4 blind to the request's scope, so two credentials could
  share bytes Spiral keeps apart.
- **Names of a store and an object path alone,** `s3/bucket/key`. They
  leave out what else Spiral's digest holds: for HTTP, the URL's query
  and the credential, which change what a URL returns. The digest is the
  name Kismet already uses.
- **Every process opening the cache file,** with an index kept in shared
  memory or caught up from a log. doc/02's reasons hold: a process killed
  mid-split leaves a broken tree that the others go on reading, and a
  latch a dead process holds blocks everyone.
- **A real memory tier on a user's machine.** Its blocks would be lost at
  exit unless written to disk then, which makes exit wait on the drive;
  keeping them on disk as well is inclusive tiering (`03-cache.md`,
  Rejected). The page cache serves repeated reads there instead (decision
  5).
- **Checksums per chunk:** decision 7.

## Later

- **A t4 core API of files opened under the caller's key,** this doc's
  first version: a cache with no inner store, each file opened with a
  key, a version, a size and a store of its own. Only if the names fall
  short: object paths over 959 bytes left uncached, or a name kept for
  every file version opened.
- **t4 on hosts without a cache:** fetch-once, merging and races, keeping
  nothing. t4 can't run without tiers today (its builder requires a
  memory capacity and a disk file), and keeping nothing it still fetches
  whole blocks: 1.48 times the bytes on ClickBench (doc/09, step 4). Only
  if phase 2 shows t4's GETs beat Spiral's, and then with exact ranges
  when nothing is kept.
- **One cache for both classes,** a placement policy keeping chunks for
  manifests, if two budgets prove wasteful.
- **A `clear` in t4,** if Spiral wants its cache emptied while running.
- **A server's memory tier written to disk at a clean shutdown,** if a
  cold memory tier after a restart costs too much.

## Code map

| Repository | Path | What | Built |
|---|---|---|---|
| t4 | `src/cache/cache_files.rs` | the lock (decision 8) | yes |
| t4 | `src/cache/cache_files.rs` | sparse cache files (decision 9) | no |
| t4 | `11-shared-mode.md`'s code map | shared mode (decision 8) | no |
| t4 | `src/cache/snapshot.rs` | the disk tier's snapshot (decision 7) | no |
| t4 | `src/cache/t4_caching.rs` | `known_size` | no |
| spiraldb | `spiraldb/spiral-io/src/cache/t4_cache.rs` | `T4Cache`: the names, `SpiralFetch`, `SpiralFetchRouter`, the profiles, the metrics wrappers; its tests | yes, branch `xiangpeng/t4-cache` |
| spiraldb | `spiraldb/spiral-io/src/cache.rs`, `cache/settings.rs` | `SharedCache` over `SharedCacheEngine`; `CacheEngine`, `T4Profile` | yes, branch `xiangpeng/t4-cache` |
| spiraldb | `spiraldb/spiral-io/src/store.rs` | `t4_cached_get`, `stream_pinned_get` | yes, branch `xiangpeng/t4-cache` |
| spiraldb | `spiraldb/spiral-io/src/config.rs` | `io.cache.engine`; `IoCacheConfig::engine` | yes, branch `xiangpeng/t4-cache` |
| spiraldb | `shared/spiral-kismet-cache`, `spiraldb/spiral-io/src/cache/kismet.rs`, `cache/block.rs` | Kismet | to remove |
| spiraldb | `spiraldb/spiral-error/src/lib.rs` | `SpiralError::Foyer` | to remove |
