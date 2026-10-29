# 11 — Shared mode: one cache, several processes

Status: design (2026-10-29). Built: build step 1, the lock
(`CacheFiles::open_file`, `Error::Busy`); step 2.1, the read policy
in the fetch half; step 2.2, no `Location` held by the fetch half;
step 2.4, no read policy held by the storage half; step 2.5,
`StorageLink`; and step 3, the socket: the frames, the owner listening
and serving a client's reads, and a client connecting (`with_sharing`)
and reading through its owner, its reads going to the remote once its
owner goes, those in flight too, until a read makes it the owner or a
client of the next; tested in one process and in real ones. Next: the
region (step 4).
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
     `TierWork` holds the run's claims until `take_gets`, which checks
     the plan where it arrives (`LocatedRead::accepts`) and carries out
     the default's instead of one it can't: a `LocatedRun` is data alone,
     and a client's plan is checked in the owner.
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
     the body, as doc/02's (`sharing/frames.rs`). Calls carry a number,
     many are in flight on one connection, and answers come back in any
     order. A client's read, named by a number of its own, goes:
     - `StartRead` (path, e_tag, size, range, intent) is answered
       `Started`: the file's version, and the read's next step, a run to
       plan or the GETs to make; or `Uncacheable`.
     - `TakeGets` (the run's plan, which the owner checks) is answered
       `Next`: the next run, or the GETs.
     - `Land` (a GET's next bytes) and `Abandon` (a GET failed) take no
       answer; `Commit` (a GET all in) is answered `Committed`.
     - `Finish`, sent with the GETs, is answered once the read's work on
       the tiers is done and its GETs are committed or abandoned:
       `Retry`, the next step for the blocks it waited for that no fetch
       cached, or `Served`, the read's bytes.
     - `EndRead` (no answer) ends the read, served or not, once the
       client is done with it: the owner keeps the read's number until
       then, and drops the frames sent to a read it ended first, served
       or failed, as the client may send them before it knows.
     - `Failed` answers any call the cache failed: the read goes to the
       remote.
     - A call the client gives up, its read dropped before the answer,
       leaves the client's table of calls waiting, as the owner may never
       answer it (it answers no call of a read it ended); an answer that
       still comes is dropped. An answer to a call never made is a
       protocol error.

     The owner runs a client's reads as tasks on the connection's thread
     (`sharing/client_reads.rs`), each taking its frames in order, and
     tells the client each read from disk it makes for one (`DiskRead`),
     for the client's read policy. A frame the protocol doesn't allow
     where it comes (GETs where none were due, a fetch or a read the
     client never started, a fetch's bytes after its commit) closes the
     connection, as a protocol error does; the owner never panics on a
     client's frame. Before the region (step
     4), a fetched block's bytes cross the socket twice: to the owner,
     which lands them as its own fetches' (`FetchedBlocks`), and back in
     the read's bytes.
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
     read does now. A read whose GET is out reads from the store again
     once the GET is in: until the region (step 4) the read's output is
     the owner's, so the bytes the GET brought went nowhere but to the
     owner, and the blocks the owner served are gone with it. Serving it
     from its own GETs would take an output in the client, a copy more
     for every read, for a read the owner's going costs one GET more.
     The next read tries the lock: the client that wins
     owns the cache, cold until the snapshot exists
     (`10-spiral-integration.md`, decision 7) and from it after; the
     others connect to it. One read of a client's tries at a time, once,
     no more than every 10 ms: the lock, then the owner, never the 10 s
     a build waits for one. The others read from the remote meanwhile,
     their calls on the dead connection failing at once
     (`CurrentLink`, `LinkMaker`). A client builds the cache from its own
     builder's settings, its placement policy made then: a cache owns at
     most once, as an owner never goes back to being a client, so the
     policy's maker is called once (it is `Send` now, for a read on
     another thread may call it).
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
      Built 29 Oct 2026, with step 3's connection: `StorageLink`, the
      owner's `ObjectCache` and its listener, or a client's
      `OwnerConnection`. A read through it is a `LinkedRead`, each of
      its fetches a `LinkedFetch`, each an enum of the owner's
      (`PartialRead` and `TierWork`, `FetchedBlocks`) and the client's
      (`ClientRead`, `ClientFetch`): `T4Caching::read_through`, `fetch`
      and `turn` serve both with one copy of their logic.
3. **The socket, bytes in its frames,** on Linux and macOS: the owner
   listens, a client connects on `Busy`, the handshake, failures to the
   remote, a client coming to own the cache. Tests in one process (a
   second cache built for sharing on the same file is a client, as the
   lock conflicts within a process too), and in real processes for
   deaths: the owner killed mid-read, a client killed holding a claim,
   eight clients starting at once. Built so far (29 Oct 2026): the
   frames (`sharing/frames.rs`); `with_sharing`; the owner listening,
   its sessions, the handshake, and a client connecting on `Busy`,
   trying the lock and the owner again until `OWNER_WAIT`
   (`a_cache_built_for_sharing_on_owned_files_is_their_owners_client`,
   `an_owner_dropped_hands_its_files_to_the_next_cache`,
   `the_owner_admits_only_a_writer_of_its_first_cache_file`); the
   owner serving a client's reads, frame by frame
   (`the_owner_serves_and_caches_a_clients_cold_read`,
   `the_owner_serves_a_clients_hits_and_tells_it_of_its_disk_reads`,
   `a_client_gone_mid_read_hands_its_claims_to_their_waiters`,
   `a_clients_misuse_closes_its_connection`); a client reading through
   its owner: calls answered by number, its read policy hearing the
   owner's reads from disk for it, its served bytes copied into an
   output of its own so their address agrees with their file offset mod
   4 KiB, a read of more than 1 GiB, whose bytes would not come back in
   one frame, going to the remote
   (`a_clients_cold_read_is_cached_for_its_owner`,
   `a_block_the_owner_holds_is_served_to_its_client`,
   `reads_of_one_block_across_owner_and_client_fetch_it_once`,
   `a_clients_read_policy_hears_its_gets_and_its_owners_disk_reads`,
   `a_client_whose_owner_went_reads_from_its_store`). A commit whose
   owner has gone is no error of the fetch's: the read finds the owner
   gone as it finishes, and goes to the remote. Reads in flight as the
   owner goes: the reader's end of the connection fails every call
   waiting and every one after, a frame that takes no answer is dropped,
   and the read goes to the remote
   (`a_clients_read_whose_owner_goes_mid_get_reads_from_its_store`,
   `a_clients_read_waiting_on_another_when_the_owner_goes_reads_from_its_store`).
   A client coming to own the cache, or to read through the client
   that did (`a_client_whose_owner_went_comes_to_own_the_cache`,
   `two_clients_whose_owner_went_one_owns_and_the_other_reads_through_it`),
   and one that finds neither the lock nor an owner reading from the
   remote with no wait
   (`a_client_that_finds_no_owner_reads_from_its_store_without_waiting`).
   A read given up leaves no call waiting, and its claims end with it
   (`a_clients_read_given_up_leaves_no_call_waiting`,
   `a_clients_read_given_up_mid_get_abandons_its_claim`,
   `an_answer_to_a_call_given_up_is_dropped`); a client's GET that fails
   abandons its fetch, so its read ends
   (`a_clients_failed_get_abandons_its_fetch`).
   In real processes, the test binary run as its own children
   (`t4_caching/tests/processes.rs`), each telling the others what it
   did through marker files: the owner killed mid-read, the client's
   read returning the right bytes and a later read making it the owner,
   as the kernel released the dead owner's lock
   (`an_owner_killed_mid_read_leaves_its_client_the_cache`); a client
   killed holding a claim, the owner's read waiting on it fetching the
   block itself (`a_client_killed_holding_a_claim_hands_it_to_its_waiter`);
   eight processes starting at once on fresh files, one owning, each
   reading right, and their stores together sending the file once
   (`eight_processes_starting_at_once_fetch_each_block_once`). They take
   about 0.3 s. Built 29 Oct 2026.
4. **The region,** on Linux: outputs and landings in a sealed memfd; the
   socket when one doesn't fit.
5. **Polling,** only if measured (Later).

## Code map

| Module | What | Built |
|---|---|---|
| `src/cache/cache_files.rs` | the lock (decision 1) | yes |
| `src/cache/object_cache.rs` | the storage half (decision 2) | yes |
| `src/cache/t4_caching.rs` | the fetch half (decision 2), and `with_sharing` (decision 1) | yes |
| `src/cache/storage_link.rs` | how the fetch half reaches the storage half: its own, or its owner's (decision 2) | yes |
| `src/cache/storage_link/current_link.rs` | the link a cache reads through now, and what makes it: at build, and anew once a client's owner goes (decisions 1, 7) | yes |
| `src/cache/sharing/frames.rs` | the frames, each checked as it arrives (decision 5) | yes |
| `src/cache/sharing/frame_socket.rs` | a socket frames cross whole, raising no `SIGPIPE` (decision 5) | yes |
| `src/cache/sharing/address.rs` | the owner's address, by the first cache file's identity (decision 6) | yes |
| `src/cache/sharing/handshake.rs` | a client's hello, its descriptor by `SCM_RIGHTS`, and the owner's check (decision 6) | yes |
| `src/cache/sharing/owner.rs` | the owner's listener and a session for each client (decisions 1, 5) | yes |
| `src/cache/sharing/client_reads.rs` | a client's reads, each a task of its session's (decision 5) | yes |
| `src/cache/sharing/client.rs` | a client's connection to its owner, its calls, and its reads through it (decisions 5 to 7) | yes |
| `src/cache/t4_caching/tests/sharing.rs`, `processes.rs` | an owner and its clients in one process, and in real ones (build step 3) | yes |
| the region | outputs and landings in a sealed memfd (decision 5) | no: step 4 |
