# 11 — Shared mode: one cache, several processes

Status: design (2026-10-29). Built: build step 1, the lock
(`CacheFiles::open_file`, `Error::Busy`); step 2.1, the read policy
in the fetch half; step 2.2, no `Location` held by the fetch half;
and step 2.4, no read policy held by the storage half.
The user's call of 29 Oct 2026, for Spiral
(`10-spiral-integration.md`, decision 8).

Several processes on one machine read through one cache: Spiral's
desktop Studio host and `spiral connect`, which Kismet lets share one
cache now. The process that takes the cache files' lock owns the cache
and serves it. The others are its clients: they read through it, and
fetch what it doesn't hold themselves, with their own credentials.

```text
 owner process                                    client process
 +--------------------------------+   socket     +--------------------------+
 | fetch half (T4Caching)         |  (control)   | fetch half (T4Caching)   |
 |   | direct calls               |<------------>|   | calls over the socket |
 |   v                            |              |   v                      |
 | storage half (ObjectCache):    |   region     | region: read outputs,    |
 |  index, tiers, policy,         |<============>|  fetched blocks          |
 |  eviction, blocks in flight    | (value bytes)|                          |
 +---------------+----------------+              +-------------+------------+
                 |                                              |
      cache files, flock                         GETs with its own credentials
```

doc/02 had a shared mode for the key-value store t4 was before the cache
(`doc/02-multi-process.md` at 4182a4a, removed in 45a0009): a `t4-server`
process per store, started by its clients and gone a second after the
last left, values in a shared region. Its lock, its address, its
handshake and its region carry over. Its server doesn't: it can't fetch
for Spiral, whose GETs carry credentials, pins and retries that live in
the client's process (`SpiralFetch` holds a live `ManagedStore`); and a
cache that exits when idle loses what it holds (`03-cache.md`,
decision 9).

## Terms

| Term | What it is |
|---|---|
| **Owner** | the process that holds the cache files' lock: it runs the storage half, and serves clients |
| **Client** | a process whose cache found the files locked, and reads through the owner |
| **Storage half** | what only the owner runs: `ObjectCache`, the index, both tiers, the chunk table, the placement policy, eviction, and the blocks being fetched |
| **Fetch half** | what every process runs: `T4Caching`, the `ObjectStore` front end, the files it knows, the read policy, the GET queue, races |
| **Region** | a client's shared memory, mapped by the owner too: where read outputs and fetched blocks are |
| **Claim** | a block given to one reader to fetch, so that it is fetched once, across processes |
| **Landing** | where a fetched block's bytes go as they arrive, before the storage half keeps it |

## Decisions

1. **The process that takes the lock owns the cache, and serves it.**
   Building a cache takes an exclusive `flock` on each cache file, held
   for the cache's life (built). A cache built for sharing
   (`T4CachingBuilder::with_sharing`) that finds a file locked connects
   to its owner instead of failing with `Error::Busy`, and the owner of a
   cache built for sharing listens on a thread of its own. Without
   sharing, a second cache is `Busy`, as built now. The owner keeps the
   cache in-process: its own reads cross no socket. No server binary, no
   fork, no idle timeout. Sharing is per cache: two Spiral hosts both
   set it.

2. **The cache splits in two: a storage half and a fetch half.** The
   owner runs both; a client runs the fetch half alone. The fetch half
   reaches the storage half through one type, which in the owner calls
   it directly and in a client calls it over the socket:

   - **Open** a file version: name, e_tag and size in, its file version
     id out (`ObjectCache::resolve` now). The id is the owner's, minted
     once, so the GET queue's merge keys (`get_queue.rs`) mean the same
     in every process.
   - **Read** a range into an output: for each block, served (the
     storage half copies it from memory into the output, or reads it
     from disk into it), being fetched by another reader (with how many
     requests it has, for races), or claimed by this reader, with where
     the placement policy puts it. The answer comes back first; the
     served blocks land after, while the reader's GETs are out.
   - **Claim** blocks a GET covers outside the read (a read policy's
     widening): claimed, or held by a tier or another reader already.
   - **Land** a claimed block: a buffer its bytes go into as they
     arrive. **Commit** it once all in: the storage half keeps it where
     the claim said, or nowhere, and serves it to the claim's waiters
     either way (decision 3). **Abandon** it if the GET failed.
   - **Wait** on a block another reader is fetching: the storage half
     serves it into the waiter's output once committed, or tells the
     waiter to fetch it itself once abandoned.

   The two halves interleaved in four places (`ObjectCache::start_read`
   and what it returns). All four are resolved (steps 2.1 to 2.4):

   - **The read policy planned in the storage half:** `TierWork::plan`
     located a run's blocks, then called `ReadPolicy::plan` itself.
     Resolved: `TierWork::locate_next_run` takes a run's hits and returns
     a `LocatedRun`, each block's source as the read policy needs it
     (`LocatedRead`); `T4Caching` plans it with its own read policy
     (`LocatedRun::planned_by`), and `TierWork::take_gets` takes the
     plan, claiming the blocks it widens to. Over the socket those are
     the read and claim calls: a read that widens takes two round trips.
     `take_gets` trusts the plan, which the fetch half checked
     (`LocatedRead::accepts`); the owner will check a client's where it
     arrives.
   - **A fetched block lands in its chunk on its first byte**
     (`ObjectCache::landing`): a memory chunk or a disk write buffer,
     allocated then, evicting if the tier is full. In the owner, landing
     stays that. In a client, the landing is a buffer in its region, and
     the owner allocates the chunk at commit: a copy from the region for
     memory, a direct write from it for disk. The memory pool is a
     private mapping, never shared: a client can't write a chunk. Already
     behind the storage half's interface (step 2.3): the fetch half lands
     a fetch's bytes only through `FetchedBlocks`'s `range`, `file`,
     `race`, `late_race`, `land` and `finish`, the commit; where a block
     lands, the eviction it makes and its claim are private to
     `object_cache.rs`. A client's `FetchedBlocks`, landing in its region
     and committing over the socket, comes with steps 3 and 4.
   - **The fetch half held where blocks were.** A fetch ends with
     `FetchEnd::Cached(Location)`, but the waiter that takes it is the
     read's work on the tiers (`TierWork::finish`), which pins the chunk
     and serves the output: the storage half's, which the owner runs for
     a client's read. What reached the fetch half was the blocks left to
     fetch, `PlannedBlock`s with where each was when planned, which it
     handed back to `ObjectCache::retry`. Resolved: they are a
     `BlocksToFetch`, which the fetch half can only ask whether any is
     left and hand back; the cache finds each block again, as it did.
     The fetch half names no `Location`, `PlannedBlock` or `BlockFetch`.
   - **The storage half reported disk reads to the read policy**
     (`report_disk_read`, `promote`), which it held. Resolved: each read
     carries its caller's read policy (`ObjectCache::start_read` and
     `ObjectCache::retry` take it, `TierWork` keeps it), which hears the
     read's disk reads as they happen; the storage half holds none. In
     the owner serving a client, that is a recorder whose events go back
     with the read's answer (step 3).

   The placement policy stays wholly in the storage half: every hook runs
   in the owner, `on_remote_read` at claim time, as now. A read's intent
   travels with its read call, to reach both policies as now.

3. **Each process fetches with its own inner store; the placement policy
   decides what comes back.** A claim says where the block goes: memory,
   disk, or nowhere (`on_remote_read`, `03-cache.md` decision 7). Every
   claimed block is committed, a block for nowhere too: other readers may
   wait on its claim, and the owner serves it to them and keeps nothing,
   as a fetch hands a block no tier keeps to its waiters now
   (`FetchEnd::Passed`). Through the region, a commit names the block's
   buffer, and the owner copies from it only into waiters' outputs: with
   no waiter, nothing is copied. Through the socket, the owner's answer
   to the commit of a block for nowhere asks for its bytes only if a
   reader waits. Credentials never leave the process that holds them.

4. **A block is fetched once across processes.** The blocks being
   fetched (`InFlight`) are the storage half's, in the owner. The first
   reader of an absent block claims it, wherever the placement policy
   puts it; the others wait on the claim, and are served at its commit. A
   client's claims end when its connection closes: the owner abandons
   them, and their waiters fetch for themselves. Kismet fetches a block
   once per process only (`10-spiral-integration.md`).

5. **Control over a Unix socket; value bytes in a shared region.**
   - **The socket** carries frames: a little-endian `u64` length, then
     the body, as doc/02's. Requests carry an id, many are in flight on
     one connection, and replies come back in any order. A read is one
     request with two replies: the answer, then served.
   - **The region**, on Linux, is a memfd the client makes, sizes to
     1 GiB, seals (`F_SEAL_SHRINK`, `F_SEAL_GROW`, `F_SEAL_SEAL`) and
     sends in the handshake; both map it. Pages take memory only once
     touched. The owner refuses a memfd not sealed against shrinking: a
     client that shrank it would crash the owner with `SIGBUS`, and the
     owner is a user's app.
   - **A client's read outputs and landings come from its region.** The
     owner fills an output as it fills one now: disk reads land in
     place, memory hits are copied in once. The client hands it to its
     caller as `Bytes` over the region, as `ReadOutput::into_bytes`
     hands out heap memory now (`read_output.rs`), and the space goes
     back when the last `Bytes` drops. A client's read costs the
     copies an owner's does; only control crosses the socket.
   - **Freed space is given back:** the client punches a hole over a
     large freed buffer (`fallocate`, `PUNCH_HOLE`, which the seals
     allow), so the region's memory follows what is held.
   - **A buffer that doesn't fit** goes through the socket: the owner
     sends a served block's bytes in the reply, and a commit carries its
     block. So does every buffer on macOS, which has neither memfd nor
     seals (`00-meta.md`: correct there, not fast).
   - **The owner trusts no shared byte it acts on.** It reads every
     field of a request from its frame, never from the region, so a
     client changing the region garbles only its own bytes. It keeps its
     mapping of a region until every I/O into it completes, even after
     the client has gone.

6. **A client finds the owner by the first cache file's identity.**
   - **Linux:** an abstract socket, `\0t4/<dev>/<ino>` in hex, from the
     file's `stat`: one name whatever path reaches the file; no socket
     file to clean up; gone when the owner's socket closes, however it
     exits. Per network namespace: a client in another container doesn't
     see the owner, and runs as without sharing (`Busy`).
   - **macOS:** a socket file, `$TMPDIR/t4-<dev>-<ino>.sock`. The owner
     unlinks a stale one before it binds, which is safe since it holds
     the lock: no other process can be binding.
   - **Connecting:** a client opens the file, fails the lock, connects.
     Refused means the owner is starting or leaving: it tries the lock
     again, owning the cache if it wins. It loops until a deadline of
     10 s, then fails with `Busy`.
   - **The handshake:** a magic and a version; the client's own
     read-write descriptor of the first cache file, by `SCM_RIGHTS`,
     which the owner checks names the same file (`fstat`) and is open
     read-write (`F_GETFL`), so whoever may write the cache file may use
     the cache; then the region. The owner's settings rule: its tiers,
     its placement policy, its direct I/O. A client's own tier settings
     matter only if it comes to own the cache.

7. **Every failure becomes a read from the remote.** A cache may lose
   anything, so nothing a client's caller sees depends on the owner:
   - **The owner goes,** cleanly or killed: calls in flight fail, and
     their reads go to the client's inner store, as a failed cache-file
     read does now. The next read tries the lock: the client that wins
     owns the cache, cold until the snapshot exists
     (`10-spiral-integration.md`, decision 7) and from it after; the
     others connect to it.
   - **A client goes:** the owner sees the connection close, abandons
     its claims, drops its landings not committed, and unmaps its region
     once its I/O into it completes.
   - **A protocol error** closes the connection, as if the owner went.

8. **One user's processes trust each other.** A client may commit any
   bytes under any name, as any Spiral process of the user may write
   Kismet's entries now. The owner checks only what keeps the cache
   whole: that a commit's length is its block's, and that it names a
   claim the client holds.

## Cost

One round trip, a few to tens of µs, mostly waking the thread on the
other side (to be measured on the target box, not assumed):

| a client's read of | added to an owner's |
|---|---|
| blocks the cache holds | the read call: one round trip |
| blocks it fetches | the same, plus one commit each, batched per GET; the GET takes tens of ms |
| widened blocks | one more round trip, for the claim |

It shows on small reads served from memory, a few hundred ns in the
owner. A read is one call however many blocks it spans, and calls are
pipelined. The owner's own reads cost what they cost now.

## Rejected

- **A server process apart from the apps** (doc/02's `t4-server`):
  started by a client and gone when idle, it would fetch for no one
  (credentials, decision 3), and an exit loses the cache. Kept running,
  it keeps a memory tier warm after the apps quit, which on a user's
  machine is one chunk (`10-spiral-integration.md`, decision 5), and is
  a background process, and a binary to ship and sign inside Studio.
- **Forking the app to make the server.** A child gets only the forking
  thread: a lock another thread held (the allocator's, tokio's, t4's)
  stays held in it for good, and POSIX allows only async-signal-safe
  calls until `exec`. macOS's frameworks fail in a child that doesn't
  `exec`. The child would also share the parent's io_uring ring and its
  `flock`.
- **Executing the app's own binary as the server,** told by an
  environment variable checked first in `main`: every host's `main`
  would have to hand that run to t4, to buy the warm memory tier above.
- **The owner fetching for its clients:** their credentials, pins and
  retries are objects in their processes.
- **Clients reading the cache file themselves,** told where a block is
  and pinning its chunk until done: pins held across processes, and every
  client a handle on the owner's file.
- **The memory pool shared with clients:** a client could write any
  chunk.
- **Every process opening the cache file,** with the index in shared
  memory or caught up from a log: doc/02's reasons, kept in
  `10-spiral-integration.md`'s Rejected.

## Later

- **More regions,** if reads overflow to the socket: a region added when
  one is full, sealed and sent once; buffers named by region and offset.
- **No copy in Vortex's `ObjectStoreReadAt`** when a store answers with
  one `Bytes` aligned as asked, as `T4ReadAt` does
  (`09-vortex-integration.md`, decision 3); a Vortex change, which helps
  any store. In a client, Vortex's arrays would then hold region space as
  long as they live.
- **Polling, and the owner reading sockets on its io_uring loop,** if the
  round trip measures too slow (doc/02's step 4).

## Build order

1. **The lock:** `flock` on each cache file, `Error::Busy`. Built 29 Oct
   2026: `a_cache_file_has_one_owner`.
2. **The split, in one process,** in pieces, each pinned by the unit,
   shuttle and fuzz tests, behaviour unchanged:
   1. the read policy planned in the fetch half; widened blocks claimed
      in a call of their own. Built 29 Oct 2026: `TierWork::locate_next_run`
      takes a run's hits and hands `T4Caching` a `LocatedRun`, which
      `LocatedRun::planned_by` plans with `T4Caching`'s read policy and
      `TierWork::take_gets` takes. A read's runs are located one at a time,
      each after the GETs of the one before claim what they widen to, as
      before;
   2. waiters served by the storage half: no `Location` leaves it.
      Built 29 Oct 2026: the waiters already were (`TierWork::finish`);
      the blocks left to fetch go to the fetch half as a `BlocksToFetch`,
      not as `PlannedBlock`s;
   3. landing and commit behind the storage half's interface. Already so
      (29 Oct 2026): `FetchedBlocks`, no change needed;
   4. disk reads reported to the read's caller. Built 29 Oct 2026:
      `ObjectCache::start_read` and `retry` take the caller's read policy,
      which `TierWork` reports the read's disk reads to;
   5. the one type the fetch half reaches the storage half through.
3. **The socket, bytes in its frames,** on Linux and macOS: the owner
   listens, a client connects on `Busy`, the handshake, failures to the
   remote, a client coming to own the cache. Tests in one process (a
   second cache built for sharing on the same file is a client, as the
   lock conflicts within a process too), and in real processes for
   deaths: the owner killed mid-read, a client killed holding a claim,
   eight clients starting at once.
4. **The region,** on Linux: outputs and landings in a sealed memfd; the
   socket when one doesn't fit.
5. **Polling,** only if measured (Later).

## Code map

| Module | What | Built |
|---|---|---|
| `src/cache/cache_files.rs` | the lock (decision 1) | yes |
| `src/cache/object_cache.rs` | the storage half (decision 2) | as the whole cache; the split is step 2 |
| `src/cache/t4_caching.rs` | the fetch half (decision 2) | as the whole cache's front end |
| `src/cache/sharing/` | the owner's listener and a client's connection, the frames, the address, the region (decisions 5 to 7) | no |
