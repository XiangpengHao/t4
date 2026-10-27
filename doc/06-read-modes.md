# 06 — Read modes: policy over mechanism

Status: the mechanism built with default policies (2026-10-23): the two
traits, plans carried out (split, merged, widened, raced), the intent,
the events, a read's GETs overlapping its work on the drives, a block in
flight fetched once. The plan's disk choices, which come with mirroring,
are not built yet. The default read policy splits a normal read's runs
into GETs of 4 MiB at most (decision 10).

A read wants one of three things (`99-brainstorm.md`):

- **cost:** the fewest requests, as when warming the cache. In a region,
  an object store charges a request, not its bytes;
- **throughput:** the most bytes a second, as a scan does;
- **latency:** the soonest answer, as metadata does: a Parquet footer, its
  equivalent in Vortex, a HEAD.

Each wants its bytes fetched, read and placed differently. t4 separates
what it can do (mechanism, which it guarantees) from what it decides to do
(policy, which a user can replace), as the cache already does for
eviction (`03-cache.md`, decision 7).

## What shapes it

Measured on `i7i.48xlarge` against S3 (`05-network.md`), and by Durner,
Leis and Neumann on S3 from EC2 ("Exploiting Cloud Object Storage for
High-Performance Analytics", PVLDB 16(11), 2023):

- **A request is slow, many are fast.** One S3 request reads about 50
  MiB/s (ours: 47 at load; the paper: a median of 55–60, never above about
  95). Its time is a base latency and a part a byte: about 30 ms and 20 ms
  a MiB at the median (the paper's model). So a rate needs many in flight:
  in flight = rate × time of one / its size. 256 GETs of 8 MiB in flight
  read 10.3 GiB/s, 89% of a 100 Gbit/s NIC; 512 and 1,024 read less, their
  tails seconds long (the paper: 200–250 for the same NIC). On a 300
  Gbit/s NIC, 768 read 26.8 GiB/s, a GET as quick as at 256.
- **Requests cost, bytes don't.** $0.0004 a 1,000 GETs, S3 and GCS alike,
  traffic in the region free: $0.42 a TiB in 1 MiB GETs, $0.05 in 8 MiB,
  $0.007 in 64 MiB. 8 to 16 MiB is where cost and rate both flatten (the
  paper's finding 3).
- **Tails are long, and some requests never answer.** Ours: an 8 MiB GET
  at load, p50 170 ms, p99 400–550 ms, max 1–2 s. The paper: under 5% of
  16 MiB GETs past 600 ms; some get no answer at all.
- **Drives:** a block on one drive; drives striped, three at 98% of their
  cap and twelve at 94%; one I/O worker submits about 48 GiB/s
  (`04-multi-device.md`).
- **Racing needs independent copies.** Two reads of one drive wait in one
  queue, behind the same garbage collection: their delays go together. A
  memory copy never races a disk copy, as tiers are exclusive. The object
  store holds every cached block, so it is a copy too, but a slow one.

| Mode | The object store | Drives | Memory |
|---|---|---|---|
| cost | few large GETs, widened past what was asked | — | — |
| throughput | runs split into GETs of 8 MiB or more while the NIC has room; merged once it hasn't | striped | a read waits for its slowest part |
| latency | several GETs at once, the first answer taken | copies on several drives, all read at once | kept there |

## Decisions

1. **Two policies, beside the mechanism.** `PlacementPolicy` says where
   blocks live: a fetched block's tier and copies, promotion, eviction; it
   is today's `CachePolicy`, grown. `ReadPolicy` says how a read is served:
   which GETs fetch its remote blocks, which copy of a block on disk it
   reads, how many it races. One replaced, the other stays.

2. **The mechanism guarantees, and decides none of it:** blocks of a size,
   from a block's start; exclusive tiers; chunk lifetimes; `If-Match` on
   every fetch; a plan carried out as given, or refused if it doesn't cover
   the read's remote blocks; every GET and disk read reported to the
   policy. And a read's GETs sent before its work on the drives is waited
   on: its page reads, its promotions and the evictions they make overlap
   its time on the network, not add to it (`ObjectCache::start_read`
   returns the read and its `TierWork`, which the front end awaits beside
   the GETs).

   And a block in flight is fetched once (`src/cache/in_flight.rs`). A
   read claims each remote block it will fetch before its GETs go out; a
   read that finds one claimed waits for that fetch, beside its own GETs,
   and takes the block from where it lands. A racing read may add requests
   for it instead, while fewer are in flight than its GET races; the first
   to land caches it, and the others' copies go back. A fetch that ends
   without caching the block, failed, given up on, or finding no room,
   leaves its waiters to fetch it themselves. A block the placement policy
   discards is claimed too: its fetch collects its bytes, which no tier
   keeps, and its waiters take theirs from them once it has them all. It
   matters when a reader sends its ranges as they are: Vortex, its own
   merging off, read ClickBench's 43 queries in one cold session through
   42.8 GB of GETs from t4, for 12.2 GB of distinct blocks, as reads of one
   block at once each fetched it; with its merging on, 1.09 times the
   distinct blocks, and Parquet's 1.00. Keeping no block, t4 fetched 144.8
   GiB for 56.6 GiB of distinct blocks (24 Oct 2026, a c6in.8xlarge), before
   reads of a discarded block shared its fetch.

3. **A plan per read.** `ReadPolicy::plan` sees the whole read: every block,
   where each is (memory, disk with the drives holding a copy, remote, or
   in flight with the requests out for it), the bytes asked, the file, the
   intent. It answers with the GETs, which must cover every remote block
   and may cover more (widened), split a run (several GETs) or span runs
   (merged, fetching the hits between, as bytes are free); a block in
   flight they cover only racing it (decision 2). And, for each block on
   disk with copies on several drives, the copy to read or a race.

4. **A race is every copy at once.** `race: n` sends n identical requests
   together, and takes the first answer: waiting to see one is slow makes
   it late. On the network the first response to arrive takes the read's
   buffers, the others dropped before their bodies. On drives no winner is
   known before its bytes land, so each copy reads into a buffer of its
   own, and the first back is the one used. A race costs n times the
   requests, or the drive reads: for small, latency-critical reads. A GET
   may also be raced late, once (decision 8).

5. **Racing on drives comes with mirroring, which is placement.**
   `PlacementPolicy` gives a fetched block its tier and its copies: one,
   striped as now, or k on k drives. Which drives, past distinct ones, is
   the allocator's; striping a single copy over the drives stays mechanism
   (chunk c on file c mod n). A mirrored block's index entry names k
   chunks, its writes go to k drives, and it leaves all of them at once.

6. **The caller says what a read is for; else the policy guesses.** A
   `ReadIntent` (`Latency`, `Throughput`, `Warm`) in `GetOptions::
   extensions`, which reach t4, is passed to both policies. Without one,
   the default policy infers: a suffix read, a file's first read, a small
   read near its end, a HEAD or a list; a Vortex-aware or MP4-aware policy
   replaces the guess.

7. **No executor assumed.** A read awaits its whole plan: widening delays
   it by the extra bytes, which the policy weighs when it widens. A race
   needs none: the read's future polls every racer and drops the losers.
   A late race waits on a timer, which the user gives, as
   `T4CachingBuilder::with_sleep`; without one, no GET is raced late.
   `warm` is a future its caller awaits. Answering a read before the rest
   of a widened fetch is in would need work to go on after the read
   returns; if it ever earns its place, t4 takes a spawn function from the
   user, never a runtime of its own.

8. **A straggler is raced late, once.** A request that never answers is
   retried, not raced: sending it again once it has been silent too long
   is liveness, as the paper's hedging is, and object_store's timeouts and
   retries do it, below t4. A GET that answers but is slow is raced:
   `PlannedGet::with_late_race(after)` sends one more request if its body
   isn't all in `after` since it went, for the bytes not in yet; the first
   to have them all is taken, the other dropped, and the late request's
   bytes land past the first's. `RaceStragglers` plans as `FetchRuns` does
   and races each normal read's GETs at a quantile (p90, p99) of the latest
   GET times it heard, from a GET's first request to its body all in. On
   24 Oct 2026's ClickBench GETs a model gave racing at p90, uncapped, 10%
   more GETs and 8 to 10% more bytes for a p99 of 495 ms, not 633, and a
   max of 1.0 s, not 3.3; the slow GETs came in bursts, when bytes in
   flight were most, which racing adds to, and the model can't say what
   that costs.

9. **The policies learn from events.** Every GET and disk read reports
   its start, first byte and end, its bytes, and its drive or host; a GET
   also reports when its blocks are cached, with how its time went: its
   first byte, its body in, the part of that spent landing the pieces
   rather than waiting for them, and the blocks cached. Policies keep what
   they want (latency percentiles, what is in flight, each drive's queue),
   as `CachePolicy` keeps its own state now.

10. **The defaults are plain.** The default read policy fetches each run of
    remote blocks in GETs of 4 MiB at most for a normal read, one whose
    intent isn't said, so that no one GET holds the read up for long; a
    read with an intent fetches each run in one GET. No race; one copy
    striped. Each mode then comes as a policy, measured.

11. **Bytes in flight are capped, and waiting GETs merge.** The user
    gives the network t4's GETs share, `T4CachingBuilder::
    with_network_rate(rate, unloaded_get_time)`: the NIC's rate, and what
    a GET of a block takes with little else in flight. t4 caps its GETs'
    bytes in flight, a racer's each counted, at what fills the NIC, the
    two multiplied (`src/cache/get_queue.rs`): 200 MB for 50 Gbit/s and
    32 ms. Past it a GET waits its turn, first come, first served, until
    another's body is in; its time then includes the wait, which it
    reports as `waited`. A GET bigger than the cap goes once nothing else
    is in flight. The cap is bytes, not GETs, as a NIC fills with bytes:
    a merged GET of 4 MiB takes four GETs' room, and a box of 100 Gbit/s
    twice the room at the same GET time.

    A GET's tail follows how much is in flight more than its size: on 26
    Oct 2026's ClickBench (Vortex, 2 MiB dictionaries, read raw through t4
    keeping no block, a c6in.8xlarge, 50 Gbit/s), t4 letting 192 GETs of
    a block be in flight, about 200 MB, cut the GET p99, its wait
    included, from 448 ms to 281 ms, the slowest from 3.4 s to 0.93 s, and
    the queries' time by 4%; 96 left the NIC short (8% slower, AWS holding
    back almost no packets), and 384 matched 192's time with a worse tail
    (1.65 s). The HTTP on 8 threads of its own didn't help (p99 598 ms).
    Those caps counted GETs; with GETs of a block, that is the cap in
    bytes.

    A late racer goes at once, beside the cap, as waiting would make it
    later still. With `with_waiting_gets_merged(up_to)`, the waiting GETs
    of a request each for a file's adjacent bytes go as one GET, of `up_to`
    bytes and the cap at most, once the first is due and there is room for
    them all; the one with the last bytes sends it, landing its own as they
    come, and the others are handed theirs once they are all in. If that
    GET fails or is given up on, they wait again. Merged up to 4 MiB under the
    192-GET cap, they cut the GETs by 28% and the queries' time by 6.5%,
    with a p99 of 322 ms: under a cap that counted GETs, a merged GET held
    more bytes than one GET's room. Raced late at the p90 as well, they
    cut the GETs by 22% and the time by 7%. Capping bytes instead, 200 MB
    (run 6), cut the p99 by 32% and the time by 3%; merged as well, with a
    GET joining the first only if there was room past the first's, which a
    backlog seldom has, it cut the GETs by 6% alone. A merge now waits for
    room for all its bytes instead, about half a millisecond at 50 Gbit/s
    for 3 MiB more. The rate and the GET time are the user's for now
    (Later).

    On an i7i.48xlarge (100 Gbit/s, so 400 MB), ClickBench's queries 20 to
    34 with the cap, merging and racing at the p90: Vortex's raw reads,
    filled into memory, had a slowest GET of 0.82 s and none over 1 s,
    against 3.3 s and 166 without the queue (24 Oct); Vortex's own merged
    reads 1.13 s and 23. Parquet's got worse, 3.9 s and 12,963: its GETs
    are mostly of 4 MiB, which hold a connection longer than the 32 ms the
    cap is sized for, so 400 MB holds about 100 of them, too few to fill the
    NIC, and they waited up to 3.9 s for room while S3 answered each in 671
    ms at most. A cap that holds a GET's room for how long it holds its
    connection is open (Later). The bench has read Vortex with its own
    merged reads since, the user's call.

## The interfaces

```rust
/// Where blocks live: today's `CachePolicy`, grown (decisions 1, 5).
pub trait PlacementPolicy: Send + Sync {
    // as now: on_memory_hit, on_disk_hit (stay or promote), on_admit,
    // pick_memory_victims, pick_disk_victims.
    /// Where a fetched block goes: its tier, and its copies on distinct
    /// drives (1, striped as now; k, mirrored).
    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit;
}

/// How a read is served, from the drives and the object store (decision 3).
pub trait ReadPolicy: Send + Sync {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan;
    fn on_io(&self, event: &IoEvent);
}

pub struct LocatedRead<'r> {
    file: FileInfo<'r>,             // path, size, version
    asked: Range<u64>,
    intent: Option<ReadIntent>,     // decision 6
    blocks: &'r [BlockSource],      // Memory, Disk(drives with a copy), Remote
}

pub struct ReadPlan {
    gets: Vec<PlannedGet>,          // cover every remote block; may widen, split, merge
    disk: Vec<(u32, DiskRead)>,     // for blocks with copies on several drives
}

pub struct PlannedGet {
    blocks: Range<u32>,
    race: NonZeroU8,                // decision 4
}

pub enum DiskRead {
    One(DriveId),                   // e.g. the least loaded
    Race,                           // every copy at once
}

pub enum ReadIntent {
    Latency,
    Throughput,
    Warm,
}
```

`BlockRead`, which `PlacementPolicy`'s hooks get, gains the intent, and
whether the read asked for the block or a widened GET brought it.

## The plan

1. **The mechanism, the defaults doing what t4 does now:** the two traits,
   `LocatedRead` built from the read's plan, the GETs carried out as
   planned, the events, a block in flight fetched once.
2. **Throughput:** split a run into GETs of 8 MiB or more while what is in
   flight can't fill the NIC; merge once it can. Measured with the replay
   of ClickBench's cold pass against S3: its time, a call's p50, p99 and
   max, and its GETs (so its cost).
3. **Latency:** races for reads whose intent is `Latency`, or guessed so;
   such blocks kept in memory. Measured on the footer reads: p50, p99, max.
4. **Cost:** `warm`, a range of a file fetched in a few large GETs.
5. **Mirroring:** k copies on k drives, the index naming each; disk races;
   the least loaded copy for throughput.

Settled since: a block is mirrored or not, chosen as it is fetched
(decision 5), not a file's region; which copy a throughput read takes is
a heuristic of the read policy over the events, each drive's queue among
them (decision 9).

## Open questions

- **Promotion** reads a block from disk, yet stays `PlacementPolicy`'s
  (`on_disk_hit`). Right there, or the read policy's?
- **What the policy knows of the machine,** the NIC's rate and what a
  request costs, given by the user or learned from the events: decided
  later, with the throughput policy. The cap on bytes in flight (decision
  11) takes both from the user for now (Later).

## Later

- **A cap that weighs a GET by how long it holds a connection** (TODO):
  decision 11's cap counts a GET's bytes against the NIC's rate times a
  GET's unloaded time, the time of a GET of a block. A GET of 4 MiB holds
  its connection for its first byte and then its bytes at S3's rate a
  connection, longer than four GETs of a block do between them, so the cap
  in bytes lets too few big GETs out to fill the NIC; Parquet's first
  passes waited for room for seconds on 26 Oct.
- **Learning the network rather than being told it** (TODO): the cap on
  bytes in flight (decision 11) is the NIC's rate times a GET's unloaded
  time, both given by the user. t4 hears every GET's bytes and times, so
  it could learn both, as TCP's BBR does: the rate the GETs delivered at
  most lately, the shortest GET times lately, the cap a little over
  their product, raised now and then to see whether the rate rises. A
  query fetches in bursts between its computing, so the rate is seen
  only in them, and the probing mustn't make the bursts it is to avoid.
  The bench's ENA counters (`bench/src/host_counters.rs`) tell how close
  a cap is: packets held back for passing the bandwidth near none, the
  queries no slower.

- **A transport that lands a fetched block in its chunk,** one copy of a
  byte, not two (`05-network.md`); AnyBlob, the paper's library, receives
  straight into its buffer manager's pages, leaving room for the HTTP
  header.
- **Choosing S3's endpoints:** the paper finds some take jumbo frames,
  which cuts the kernel's work a byte, and replaces endpoints slower than
  the rest. In us-east-1 most now do (`05-network.md`, "On a 300 Gbps
  box"): 0.13 CPU-s a GiB less than a 1,500-byte host, 0.01 to 0.05 on the
  mix a run gets. The transport's to do, not t4's.
- **Plain HTTP inside the VPC,** which the paper measures at less than
  half the CPU of HTTPS, with encryption at rest instead: the operator's
  call.
- **Measuring a small GET against a block's:** the paper's model says 31
  ms for 64 KiB and 50 ms for 1 MiB at the median; whether a latency read
  should fetch less than a block depends on it, on our setup.

## Code map

| Path | What | Built |
|---|---|---|
| `src/cache/policy/mod.rs` | `PlacementPolicy`, which was `CachePolicy`; `BlockRead`'s intent, and whether a block was asked for | yes |
| `src/cache/policy/read.rs` | `ReadPolicy`, `LocatedRead`, `ReadPlan`, `ReadIntent`, `IoEvent`; the default `FetchRuns`; whether a plan can be carried out | yes |
| `src/cache/object_cache.rs` | `LocatedRead` built from a read's blocks; each fetched block's fate (asked, widened, dropped); fetched bytes landed | yes |
| `src/cache/t4_caching.rs` | the intent from `GetOptions::extensions`; racing GETs, the first answer taken | yes |
| `src/cache/io_report.rs` | `IoReport`: an I/O told to the read policy, started to finished, failed or given up on | yes |
| `src/cache/t4_caching.rs` | a GET raced late: the rest of its body asked for once it has been out too long, on the user's timer | yes |
| `src/cache/policy/stragglers.rs` | `RaceStragglers`: GETs raced late at a quantile of the GET times heard | yes |
| `src/cache/object_cache.rs` | `start_read` and `TierWork`: the drives' work and the waits for blocks in flight awaited beside the GETs; `retry`, for blocks whose fetch cached nothing | yes |
| `src/cache/in_flight.rs` | `InFlight`: the blocks being fetched, each fetch's requests, claimed, joined by racers, ended where the block landed or abandoned | yes |
| `src/cache/get_queue.rs` | `GetQueue`: the cap on bytes in flight, from the network's rate; waiting GETs first come, first served, a file's adjacent ones merged; a late racer beside it | yes |
| `src/cache/block_index/` | an entry naming several chunks, for mirrors | with mirroring |
