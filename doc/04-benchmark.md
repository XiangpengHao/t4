# 04 — Benchmark

Status: started (2026-10-22). Built: the trace recorder, the simulated
object store, ClickBench traced on Parquet. Not yet: the replay, the
cache states, the device ceiling ("Later").

t4 is used under a query engine: DataFusion reading Parquet or Vortex
from an object store. So it is judged by what that engine asks of it. The
benchmark learns the workload from the engine, once, then replays it
against t4 alone: in seconds, the same every run, nothing but the cache
measured.

## What DataFusion asks of t4

ClickBench, measured 2026-10-22: the single 14.8 GB `hits.parquet`, its 43
queries in order, DataFusion 55.2, 24 partitions, the file in the page
cache. `trace_queries` recorded every request at the `ObjectStore`
interface, where t4 sits.

| | pushdown off | pushdown on |
|---|---|---|
| GETs | 12,529 | 13,858 |
| bytes | 49.0 GB | 38.8 GB |
| GET size p10 / p50 / p90 / max | 25 KB / 1.2 MB / 8.6 MB / 152 MB | 20 KB / 1.1 MB / 7.6 MB / 92 MB |
| bytes in GETs of 4 MiB or more | 78% | 70% |
| GETs under 64 KiB | 12% | 15% |
| GETs in flight p50 / p90 / max | 5 / 13 / 64 | 4 / 10 / 52 |
| 1 MiB blocks touched | 13.8 GiB, the whole file | 10.3 GiB |
| touches per block | 4.2 | 4.8 |
| bytes a miss fetches in whole blocks, per byte asked | 1.27 | 1.37 |

1. **A query is 24 closed loops.** Each partition is one tokio task that
   makes one `get_ranges` call, waits for it, decodes for a while (p50
   20 ms, p90 137 ms), and makes the next. No task ever had two calls in
   flight. So t4 sees at most a call per partition, and DataFusion feels
   each call's latency, not t4's throughput.
2. **Every data read is a `get_ranges` call.** `ObjectStore`'s default,
   which t4 keeps, merges ranges less than 1 MiB apart: 1 to 5 GETs a
   call, p50 1. Its limit of 10 GETs in parallel never binds.
3. **Metadata is read once a session:** a 512 KiB suffix GET, then the
   2.4 MB footer. Each query also asks a HEAD, which t4 answers from
   memory after the first.
4. **Bytes are in large GETs, requests are mid-size:** half the GETs are
   under 1.2 MB, but GETs of 4 MiB or more carry 70–78% of the bytes.
5. **The engine is slow next to the drive.** From the page cache, a query
   takes 0.6–4.5 GB/s, and in the eight longest queries its tasks wait on
   reads 7–24% of their time.
6. **One pass misses mostly on first touch:** 24% of block touches are
   compulsory. Under LRU the miss ratio falls smoothly with size, no knee,
   and the whole file fits on disk.

What follows for t4:

- **Hit path: latency of a call, at the engine's concurrency.** A 4 MiB
  disk hit is about 0.3 ms of a 14 GB/s drive. t4 also zeroes a page
  buffer, zeroes the output and copies one into the other, on the calling
  task (`99-brainstorm.md`): costs of the same order, felt by a partition
  each time it reads. To be measured (decision 3).
- **Miss path: DataFusion alone cannot fill a fast NIC.** 24 GETs in
  flight at about 100 MB/s a stream is 2.4 GB/s, 19 Gbit/s. A 50–100
  Gbit/s NIC is full only if t4 splits a GET into parallel GETs of its
  blocks (request splits, `03-cache.md`, "Later").
- **Capacity sweeps need more data than ClickBench:** it fits on this
  machine's disk tier whole.

## Decisions

1. **The harness is its own workspace member,** `bench/` (`t4-bench`).
   DataFusion, tokio and whatever t4 is compared against stay out of t4's
   dependencies.
2. **Learn the workload from the engine; replay it without.**
   `trace_queries` runs a workload's queries in DataFusion over a
   `TracedStore`, which records every request, its `get_ranges` call and
   the task that made it. The replay re-issues each task's calls in order,
   each after its recorded think time: the gap between the task's previous
   call and this one. Its score is the replay's time against the ideal
   replay, every call at the hardware's speed. A run of DataFusion itself
   checks the replay; it isn't the benchmark.
3. **Three cache states, each measured alone:** cold (every block from the
   object store), disk-warm (every block on disk, none in memory), and
   memory-warm. Each against its ceiling:

   | State | Ceiling |
   |---|---|
   | memory-warm | the replay over an in-memory store |
   | disk-warm | the same pages read with raw io_uring and `O_DIRECT`, as many in flight |
   | cold | the simulated store, uncached, as many GETs in flight as t4 may issue |

4. **The object store is simulated:** `SimulatedStore` answers after a
   first-byte latency, at a bandwidth a stream, through one link of a
   bandwidth, over `SyntheticObjects`, whose bytes are computed from the
   path and offset. A workload over terabytes then needs no dataset, no
   drive and no page cache to serve it, and every byte t4 returns is
   checked. The link sends whole responses in turn, where TCP shares it,
   and arrivals round up to tokio's millisecond timer. The model's numbers
   are parameters until measured on GCP ("Later").
5. **Runs are isolated.** Each runs in its own cgroup, its memory capped,
   so a contender on buffered I/O gets no free page cache; the page cache
   is dropped between runs; and a disk-warm run starts only once the drive
   has settled from its writes (SLC folding).
6. **The metrics:** per call, latency by size and state (p50, p99); per
   query, replay time against the ideal; per state, bytes a second. A
   benchmark runs in under 5 minutes, once: a result that varies from run
   to run is a broken machine or a broken benchmark, not something to
   average away.

## Rejected

- **ClickBench or TPC-H run end to end as the benchmark.** The engine
  decodes for 76–93% of a task's time in the longest queries, which hides
  the cache; a run takes
  minutes; and a slowdown can't be told apart from the engine's.
- **CPU time per byte.** t4's I/O worker spins whenever I/O is in flight,
  so its CPU time measures that, not work.
- **Throughput at 256 reads in flight as the headline.** DataFusion never
  had more than 64 GETs in flight. Only the device ceiling is measured
  there.
- **divan.** It times one closure at a time: no concurrent closed loops,
  no latency distributions.
- **A real object store from this machine.** Its NIC is 2.5 Gbit/s: two
  streams fill it.

## Later

- **The replay** (decision 2) and the three states (decision 3). A
  disk-warm run rewrites the cache file each time, then reads it while the
  drive folds SLC: the cache file kept across runs ("Keeping the disk tier
  across a clean restart", `03-cache.md`) would avoid both.
- **More workloads:** ClickBench on Vortex, its 100-file partitioned
  Parquet, and TPC-H SF100, larger than memory and disk tiers here, for
  the capacity sweeps.
- **GCP:** measure first-byte latency and bandwidth a stream against GCS,
  set the model's numbers, and run the cold state against GCS itself.
- **What t4 is compared against:** to be chosen.

## Code map

| Path | What it is | Decisions |
|---|---|---|
| `bench/src/request_trace.rs` | `TracedStore` and its `RequestTrace` | 2 |
| `bench/src/simulated_store.rs` | `SimulatedStore` and its `NetworkModel` | 4 |
| `bench/src/synthetic_objects.rs` | `SyntheticObjects`: objects whose bytes are computed | 4 |
| `bench/src/bin/trace_queries.rs` | runs SQL in DataFusion over local Parquet, traced; writes the trace as CSV | 2 |
| `bench/clickbench/queries.sql` | ClickBench's 43 queries, one a line | |

```bash
cargo run --release -p t4-bench --bin trace_queries -- \
    --table hits=/path/to/hits.parquet \
    --queries bench/clickbench/queries.sql --out clickbench.csv
```

`--pushdown-filters` traces with DataFusion's late materialization;
`--query n` traces query `n` alone.
