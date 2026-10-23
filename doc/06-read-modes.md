# 06 — Read modes: policy over mechanism

Status: the mechanism built with default policies (2026-10-23): the two
traits, plans carried out (split, merged, widened, raced), the intent,
the events. Single-flight, and the plan's disk choices, which come with
mirroring, are not built yet; the defaults change nothing (decision 10).

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
  tails seconds long (the paper: 200–250 for the same NIC).
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
   the read's remote blocks; a block in flight fetched once, its other
   readers waiting on it; every GET and disk read reported to the policy.

3. **A plan per read.** `ReadPolicy::plan` sees the whole read: every block,
   where each is (memory, disk with the drives holding a copy, or remote),
   the bytes asked, the file, the intent. It answers with the GETs, which
   must cover every remote block and may cover more (widened), split a run
   (several GETs) or span runs (merged, fetching the hits between, as bytes
   are free); and, for each block on disk with copies on several drives, the
   copy to read or a race.

4. **A race is every copy at once.** `race: n` sends n identical requests
   together, and takes the first answer: waiting to see one is slow makes
   it late. On the network the first response to arrive takes the read's
   buffers, the others dropped before their bodies. On drives no winner is
   known before its bytes land, so each copy reads into a buffer of its
   own, and the first back is the one used. A race costs n times the
   requests, or the drive reads: for small, latency-critical reads.

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
   `warm` is a future its caller awaits. Answering a read before the rest
   of a widened fetch is in would need work to go on after the read
   returns; if it ever earns its place, t4 takes a spawn function from the
   user, never a runtime of its own.

8. **A request that never answers is retried, not raced.** Sending it
   again once it has been silent too long is liveness, as the paper's
   hedging is: object_store's timeouts and retries do it now, below t4.

9. **The policies learn from events.** Every GET and disk read reports
   its start, first byte and end, its bytes, and its drive or host;
   policies keep what they want (latency percentiles, what is in flight,
   each drive's queue), as `CachePolicy` keeps its own state now.

10. **The defaults change nothing.** The first default policies do what t4
    does today: one GET per run of remote blocks, no race, one copy striped.
    Each mode then comes as a policy, measured.

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
    fn plan(&self, read: &PlannedRead<'_>) -> ReadPlan;
    fn on_io(&self, event: &IoEvent);
}

pub struct PlannedRead<'r> {
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
   `PlannedRead` built from the read's plan, the GETs carried out as
   planned, single-flight, the events. No change measured.
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
  later, with the throughput policy.

## Later

- **A transport that lands a fetched block in its chunk,** one copy of a
  byte, not two (`05-network.md`); AnyBlob, the paper's library, receives
  straight into its buffer manager's pages, leaving room for the HTTP
  header.
- **Choosing S3's endpoints:** the paper finds some take jumbo frames
  (8,400-byte packets, where most take 1,500), which cuts the kernel's
  work a byte, and replaces endpoints slower than the rest. The
  transport's to do, not t4's.
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
| `src/cache/policy/read.rs` | `ReadPolicy`, `PlannedRead`, `ReadPlan`, `ReadIntent`, `IoEvent`; the default `FetchRuns`; whether a plan can be carried out | yes |
| `src/cache/object_cache.rs` | `PlannedRead` built from a read's blocks; each fetched block's fate (asked, widened, cached); fetched bytes landed | yes |
| `src/cache/t4_caching.rs` | the intent from `GetOptions::extensions`; racing GETs, the first answer taken | yes |
| `src/cache/io_report.rs` | `IoReport`: an I/O told to the read policy, started to finished, failed or given up on | yes |
| single-flight | a block in flight fetched once, its other readers waiting | next |
| `src/cache/block_index/` | an entry naming several chunks, for mirrors | with mirroring |
