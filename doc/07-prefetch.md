# 07 — Prefetch: blocks moved before a read asks for them

Status: design (2026-10-27), nothing built.

A prefetch moves a block closer before any read asks for it: from the
remote to the cache, or from disk to memory. When the read comes, the
block is in memory already, or on its way. As in `06-read-modes.md`, t4
splits it in two:

- the mechanism, which t4 guarantees: how a prefetch is queued, and what a
  read does when it needs a block a prefetch is moving (decisions 1 to 4).
  It knows no file format (decision 5);
- the policy, which a user can replace: what to prefetch (decision 6). The
  default one recognizes Parquet and Vortex files, keeps a history of their
  reads, and prefetches the next rows of the columns being read
  (decision 7).

Terms: a **demand** I/O is one a read waits for, a GET or a disk read; a
**prefetch** I/O is one no read waits for, yet.

## What shapes it

**DataFusion reads Parquet without reading ahead.** Each partition reads
one row group at a time: one `get_ranges` call holding every column chunk
the query needs from that row group. It decodes the row group, and only
then asks for the next (DataFusion 55.2, parquet 59.3.1). So a partition's
second row group waits for its first to land:

```text
today:          ask RG0 ──GET──▶ decode ─ ask RG1 ──GET──▶ decode ─ done
with prefetch:  ask RG0 ──GET──▶ decode ─ ask RG1 (already in) ─ decode ─ done
                   └── prefetch RG1 ──GET──▶
```

DataFusion removed opening the next file early in March 2026, as it had no
priority and fetched files that pruning then skipped
([apache/datafusion#20916](https://github.com/apache/datafusion/pull/20916)).
Prefetching the next row group is an open proposal there
([apache/datafusion#18470](https://github.com/apache/datafusion/issues/18470)).

**Vortex reads ahead itself.** Vortex 0.90 registers every split's
segments up front and keeps four splits in flight per worker thread. What
it sends late are the projection's segments, once a split's filter has
been evaluated.

**The trace says Parquet's next row group is the one to fetch.** On 23
Oct 2026's ClickBench trace (Parquet, DataFusion 55, 192 partitions, an
i7i.48xlarge reading S3 directly): 100 files, 325 row groups, 2 to 5 a
file, so each partition read 1.7 row groups. Each time a partition asked
for row group r of a file, suppose t4 had fetched row group r + 1 right
then, the same columns:

| What happened to the fetch of r + 1 | Times |
|---|---|
| it went before DataFusion asked: 36 ms sooner at the median, 134 ms at the p90 | 4,641 |
| another partition had asked for r + 1 already: nothing to do | 2,859 |
| r was the file's last row group: nothing to fetch | 3,345 |
| no partition ever read r + 1: wasted | 76 (163 MiB of the 45,927 MiB read, 0.4%) |

Had each partition's later row groups started with its first, the
queries' reads would have ended at most 19% sooner, summed over the
queries, if GETs were no slower for the extra load: a ceiling, not a
forecast. ClickBench prunes almost no row groups, so little is wasted; a
query that prunes more wastes more (decision 7 stops it). The trace has no
footers: "the same columns" there means the same number of ranges, and row
groups are numbered from q23's reads, which take every one. 41% of the
row-group reads were sent only after the same partition's previous one had
landed; 66% of those were the next row group of the same file, and 34%
another file's, which partitions take from a shared queue.

**On Vortex, guessing the late reads costs bytes.** On the Vortex trace (2
MiB dictionaries, its own merged reads), the reads sent after one of the
same file had landed would end at most 17% sooner if sent with the file's
first, most of it in q36 to q42. They depend on the filter: fetching the
columns they read, whole, costs 1.0 times the bytes they read for
unselective filters (q10 to q14, q24 to q26), 1.5 to 2 times for q21 to
q28, 2.3 to 6 for q36 to q42, and 14.5 for q20.

**Others' prefetchers.**

- AWS's [Analytics Accelerator Library for S3](https://github.com/awslabs/analytics-accelerator-s3)
  parses a Parquet file's footer and maps each GET to its column chunk. It
  keeps the columns read lately, for each schema. On the first read into
  row group R, it prefetches those columns for R. That suits Java readers,
  which read a row group a column at a time; DataFusion asks for all of a
  row group's columns at once, so the gap is R + 1.
- Linux's readahead detects sequential reads and grows its window while
  they last; AMP (FAST'07) also shrinks it when prefetched pages are
  evicted unread. Consecutive row groups' column chunks are not a fixed
  stride apart, so these can't predict them.
- DuckDB fetches a whole row group at once when the columns it needs are
  most of it and at least 90% of the row groups so far matched the filter.

## Decisions

1. **A prefetch works on both tiers.** For each block it covers:

   - remote: fetched, and placed as the placement policy says, which hears
     it as a block no read asked for (`BlockRead::asked` false), as a
     widened block is (`06-read-modes.md`, decision 3);
   - on disk: promoted to memory;
   - in memory, or being fetched or promoted: left alone.

   A prefetch returns no bytes to anyone: its blocks land in the cache
   only. No one awaits it, so t4 sends prefetches on a spawn function the
   user gives, `T4CachingBuilder::with_spawn`, as `06-read-modes.md`
   decision 7 foresaw; without one, t4 prefetches nothing.

2. **A prefetch is marked, and goes last.** Every GET and every disk read
   carries its priority: demand, or prefetch. A GET waits in `GetQueue`
   once the cap on bytes in flight is reached (`06-read-modes.md`,
   decision 11). A disk read waits in the I/O worker's queue once the ring
   is full: a channel, first in, first out, today. In both, a waiting demand
   I/O goes before every waiting prefetch, and a prefetch goes only when no
   demand I/O waits. Prefetches go in the order they came. One in flight
   stays in flight: a demand I/O never cancels it to take its room.

3. **A read that needs a queued prefetch moves it to the front.** A
   prefetch claims its blocks when it is planned, as a read claims those it
   fetches (`06-read-modes.md`, decision 2). A read that needs one of them
   finds it claimed and waits for it. If the prefetch's I/O is still
   queued, the read's wait bumps it ahead of every waiting I/O, demand
   ones too, as the read has waited behind it already. A GET of several
   blocks is bumped whole. Waiting GETs merged into one (decision 11 of
   06) take the highest priority among them.

4. **A read that needs a sent prefetch holds.** It waits for the
   prefetch's block and sends nothing of its own, as a read waits today for
   a block another read is fetching. The prefetch isn't cancelled: its bytes
   are on their way. A read may still race the block, as it may any block
   in flight (`06-read-modes.md`, decisions 4 and 8). If the prefetch
   fails, is given up on, or caches nothing, those holding on it fetch the
   block themselves, as now.

   On disk this needs a record t4 doesn't keep: while a block is promoted,
   the index still says it is on disk, and a read meanwhile reads it from
   disk a second time (`ObjectCache::promote`). A promotion in progress is
   recorded beside the fetches in flight (`InFlight`), and a read holds on
   it. This holds for every promotion, a read's own too.

5. **The mechanism knows no file format.** It moves the blocks of the byte
   ranges a policy names, tells the policy what each read asks for, and
   hands it each read's bytes. Nothing in it parses a file, or knows
   Parquet, Vortex, DataFusion or how a reader groups its reads. Formats,
   readers and their patterns live in policies, as decision 6 of 06 has it
   for a read's intent ("a Vortex-aware or MP4-aware policy replaces the
   guess").

6. **What to prefetch is a policy's.** `PrefetchPolicy`, a third policy
   beside placement and reads: one replaced, the others stay. It hears
   every read as it starts, as a `LocatedRead`: the file, the bytes asked
   and where each of its blocks is. It answers with byte ranges of that
   file to prefetch; the cache maps them to blocks and skips those decision
   1 leaves alone. It hears every read's bytes once served, to take from
   them what it wants. It keeps its own history. It is asked on every read,
   memory's too, so it keeps that cheap, as `PlacementPolicy::
   on_memory_hit` does.

7. **The default policy: the next rows of the columns being read.**
   `FormatAwarePrefetch`:

   - **It recognizes a file by its footer.** A Parquet file ends with
     `PAR1`, a Vortex file with `VTXF`, each after its footer's length. When
     a read's bytes hold a file's whole footer (DataFusion reads each
     file's tail when the table is registered: 512 KiB of a Parquet file,
     64 KiB of a Vortex file), the policy parses it into the file's layout:
     each column's pieces, by the rows they hold. A Parquet column has a
     column chunk per row group; a Vortex column, its segments per chunk of
     rows. It keeps the layouts of the files read most lately, a bounded
     number. A file it knows no layout for, it prefetches nothing for.
   - **It keeps a history of each file's reads:** which pieces of which
     columns were read, and when.
   - **It predicts the next rows.** A read of piece k of column c makes it
     prefetch piece k + 1 of c in the same file: for Parquet, the same
     column in the next row group; for Vortex, the segment at the same
     place in c's layout for the next rows. Nothing past the file's last
     rows, and nothing in another file (Later).
   - **It stops when it is wrong.** A prediction is right if a read asks
     for its piece later, wrong if none has a few seconds on: the rows
     were pruned, or a filter dropped them. Readers read a file's pieces
     out of order, so a read further on proves nothing by itself. For each
     column, by name across files, it keeps its last 64 predictions, and
     predicts only while 90% or more of them were right. While stopped it
     still records what it would have predicted, so it starts again once it
     would be right again.

   On the traces: a Parquet partition's next row group is the prediction
   that pays (What shapes it). Vortex asks for most of its next rows
   itself, so most of the policy's predictions should find the blocks
   asked for already, and cost nothing; its late, filter-dependent reads
   are what the 90% stops. Unmeasured: the plan's step 4.

   Reading each format's footer is the policy's alone, behind a cargo
   feature of its own, `parquet` and `vortex`. A file whose format's
   feature is off is a file the policy doesn't know. The policy uses the
   public traits only, as the default policies do.

## The interfaces

```rust
/// What to move closer before a read asks for it (decision 6).
pub trait PrefetchPolicy: Send + Sync {
    /// A read is starting: the bytes of its file to prefetch. Asked of
    /// every read, memory's too: keep it cheap.
    fn on_read(&self, read: &LocatedRead<'_>) -> Vec<Range<u64>>;

    /// A read's bytes, once served.
    fn on_served(&self, read: &ServedRead<'_>);
}

/// What a read returned: `asked` of the file at `path`.
pub struct ServedRead<'r> {
    path: &'r [u8],
    file_size: u64,
    asked: Range<u64>,
    bytes: &'r Bytes,
}

/// An I/O's place in `GetQueue` and in the I/O worker's queue
/// (decisions 2 and 3). The mechanism's, not a policy's.
enum IoPriority {
    /// A prefetch a read waits on now: first.
    Bumped,
    /// A read waits on it.
    Demand,
    /// No read waits on it: only while no demand I/O waits.
    Prefetch,
}
```

`LocatedRead` is today's (`src/cache/policy/read.rs`), built for every
read rather than only those with a remote block.

## The plan

1. **Priorities:** demand and prefetch I/O in `GetQueue` and in the I/O
   worker's queue, and a queued prefetch bumped. Tested under shuttle.
2. **The prefetch:** blocks moved with no output, every block claimed and
   landing as a widened one does; `with_spawn`; a promotion in progress
   held on (decision 4).
3. **`PrefetchPolicy`, `NoPrefetch`, and `FormatAwarePrefetch`** with
   Parquet's layout, then Vortex's.
4. **Measured end to end,** against no prefetch: the slowest run, the
   slowest GET and the GETs over 1 s, and the bytes prefetched that no read
   took.
   - ClickBench's Parquet: the queries the trace says gain most (q1, q2,
     q3, q12, q15, q22, q24, q28) and q36 to q42 for the waste. On the
     i7i.48xlarge's 192 partitions, and on 32, where each partition reads
     about ten row groups one after another.
   - ClickBench's Vortex, 2 MiB dictionaries, its own merged reads: the
     same queries, to see the policy cost nothing where Vortex reads ahead.

## Open questions

- **Prefetch's share of the cap.** A prefetch goes only while no demand
  I/O waits, but once sent it holds room until its body is in, and a
  demand GET that comes next waits for that room. Cap prefetches' bytes in
  flight at a part of the cap? Measured with the plan's step 4.
- **Evicting for a prefetch.** A prefetched block evicts what the
  placement policy picks, which may be a block a read takes sooner. Is the
  policy's `asked` enough, or does a prefetch evict only blocks colder than
  some mark?
- **A footer the policy has forgotten.** DataFusion keeps footers it has
  read and doesn't read them again, so a footer the policy dropped stays
  unknown. The mechanism could let a policy read a file's bytes a tier
  holds, without fetching them.
- **The format features' defaults.** The `vortex` feature pulls in
  Vortex's crates, which are large.
- **Filters pushed down.** With `pushdown_filters` on, DataFusion reads a
  row group in phases: each filter's columns, then the rest. The last phase
  depends on the filters, as Vortex's late reads do; the 90% rule is all
  that guards it.

## Later

- **Further ahead:** past the next piece, the depth grown when a read finds
  its prefetch still in flight, and shrunk when a prediction is wrong, as
  AMP does.
- **Across files:** 34% of the trace's later row-group reads went to
  another file. The columns read lately, which the AWS library keeps per
  schema, say what to fetch there; which file is next is unknown.
- **Files of no known format:** sequential reads detected from the
  history, as Linux's readahead does.

## Code map

| Path | What | Built |
|---|---|---|
| `src/cache/get_queue.rs` | demand and prefetch GETs; a queued prefetch bumped | no |
| `src/io/common.rs` | the I/O worker's queue: demand and prefetch disk reads; a queued prefetch bumped | no |
| `src/cache/in_flight.rs` | a promotion in progress, held on as a fetch is | no |
| `src/cache/object_cache.rs` | a prefetch: blocks fetched or promoted, no output; `LocatedRead` for every read | no |
| `src/cache/t4_caching.rs` | `with_spawn`; prefetches sent on it | no |
| `src/cache/policy/prefetch.rs` | `PrefetchPolicy`, `ServedRead`, `NoPrefetch` | no |
| `src/cache/policy/format_aware_prefetch/` | `FormatAwarePrefetch`: the read history, the predictions and their record; a file's layout from a Parquet footer (feature `parquet`) or a Vortex footer (feature `vortex`) | no |
