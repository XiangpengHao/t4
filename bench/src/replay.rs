//! [`ReplayPlan`]: the calls a traced workload made of its store, grouped as
//! it made them, and [`Replayer`], which makes them again of another store
//! with nothing between them: the reads alone, at the workload's own sizes,
//! order and concurrency.
//!
//! A trace holds a setup (the table registered) and one pass or more over
//! the queries, which make the same calls; the plan takes the first pass.
//! Within a query, the calls no task made (planning: a HEAD) come first, in
//! order; then each task's calls, in its order, every task at once, as
//! DataFusion's partitions made them. A task's `get_ranges` call is replayed
//! as one: the store under test merges its ranges as it would under
//! DataFusion.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::Error;
use crate::request_trace::{RecordedRequest, RequestKind};
use crate::table_store::TableLocation;

/// One call a workload made of its store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayCall {
    GetRanges {
        object: Path,
        ranges: Vec<Range<u64>>,
    },
    Get {
        object: Path,
        range: Range<u64>,
    },
    Head {
        object: Path,
    },
    List {
        prefix: Path,
    },
}

impl ReplayCall {
    /// Makes the call of `store`, and returns the bytes it read.
    ///
    /// # Errors
    ///
    /// If the store fails, or returns other than the bytes asked for.
    async fn make(&self, store: &dyn ObjectStore) -> Result<u64, Error> {
        match self {
            Self::GetRanges { object, ranges } => {
                let fetched = store.get_ranges(object, ranges).await?;
                for (range, bytes) in ranges.iter().zip(&fetched) {
                    Self::check_len(object, range, bytes.len())?;
                }
                Ok(ranges.iter().map(|range| range.end - range.start).sum())
            }
            Self::Get { object, range } => {
                let bytes = store.get_range(object, range.clone()).await?;
                Self::check_len(object, range, bytes.len())?;
                Ok(range.end - range.start)
            }
            Self::Head { object } => {
                store.head(object).await?;
                Ok(0)
            }
            Self::List { prefix } => {
                store.list(Some(prefix)).try_collect::<Vec<_>>().await?;
                Ok(0)
            }
        }
    }

    fn check_len(object: &Path, range: &Range<u64>, got: usize) -> Result<(), Error> {
        if got as u64 == range.end - range.start {
            return Ok(());
        }
        Err(format!("{object}: asked bytes {range:?}, got {got}").into())
    }
}

/// One query's calls: those made outside any task, in order, then each
/// task's, in order.
#[derive(Debug)]
pub struct QueryPlan {
    query: usize,
    planning: Vec<ReplayCall>,
    tasks: Vec<Arc<[ReplayCall]>>,
}

impl QueryPlan {
    /// The query's number in its workload.
    pub fn query(&self) -> usize {
        self.query
    }

    /// Calls in all.
    pub fn len(&self) -> usize {
        self.planning.len() + self.tasks.iter().map(|task| task.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A traced workload's calls, ready to make again of a table at another
/// location.
#[derive(Debug)]
pub struct ReplayPlan {
    setup: Vec<ReplayCall>,
    queries: Vec<QueryPlan>,
}

/// A call as the trace recorded it: when, by which task, what.
struct TimedCall {
    issued: Duration,
    task: Option<u64>,
    call: ReplayCall,
}

impl ReplayPlan {
    /// The calls in `trace`, the objects in it put at `table`'s.
    ///
    /// # Errors
    ///
    /// If a traced object has no counterpart at `table`.
    pub fn new(trace: &[RecordedRequest], table: &TableLocation) -> Result<Self, Error> {
        let pass = trace
            .iter()
            .find_map(|request| Self::pass_and_query(request.query()))
            .map(|(pass, _)| pass);
        let mut setup = Vec::new();
        let mut queries = BTreeMap::<usize, Vec<&RecordedRequest>>::new();
        for request in trace {
            match Self::pass_and_query(request.query()) {
                None if request.query() == "setup" => setup.push(request),
                Some((p, query)) if Some(p) == pass => {
                    queries.entry(query).or_default().push(request)
                }
                _ => {}
            }
        }
        let setup = Self::timed_calls(&setup, table)?
            .into_iter()
            .map(|timed| timed.call)
            .collect();
        let queries = queries
            .into_iter()
            .map(|(query, requests)| {
                let mut planning = Vec::new();
                let mut tasks = BTreeMap::<u64, (Duration, Vec<ReplayCall>)>::new();
                for timed in Self::timed_calls(&requests, table)? {
                    match timed.task {
                        None => planning.push(timed.call),
                        Some(task) => tasks
                            .entry(task)
                            .or_insert_with(|| (timed.issued, Vec::new()))
                            .1
                            .push(timed.call),
                    }
                }
                let mut tasks: Vec<_> = tasks.into_values().collect();
                tasks.sort_by_key(|(first, _)| *first);
                Ok(QueryPlan {
                    query,
                    planning,
                    tasks: tasks.into_iter().map(|(_, calls)| calls.into()).collect(),
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self { setup, queries })
    }

    /// `p{pass}q{query}` as numbers.
    fn pass_and_query(label: &str) -> Option<(usize, usize)> {
        let (pass, query) = label.strip_prefix('p')?.split_once('q')?;
        Some((pass.parse().ok()?, query.parse().ok()?))
    }

    /// The calls `requests` record, in the order issued: a `get_ranges` call
    /// from its batch's ranges (its GETs are the store's own doing), a GET
    /// outside a batch, a HEAD, a list.
    fn timed_calls(
        requests: &[&RecordedRequest],
        table: &TableLocation,
    ) -> Result<Vec<TimedCall>, Error> {
        let mut batches = BTreeMap::<u64, TimedCall>::new();
        let mut calls = Vec::new();
        for request in requests {
            let timed = |call| TimedCall {
                issued: request.issued(),
                task: request.task(),
                call,
            };
            match (request.kind(), request.batch()) {
                (RequestKind::Range, Some(batch)) => {
                    let call = match batches.entry(batch) {
                        Entry::Occupied(call) => call.into_mut(),
                        Entry::Vacant(slot) => slot.insert(timed(ReplayCall::GetRanges {
                            object: table.counterpart(request.path())?,
                            ranges: Vec::new(),
                        })),
                    };
                    if let ReplayCall::GetRanges { ranges, .. } = &mut call.call {
                        ranges.push(request.range());
                    }
                }
                (RequestKind::Get, None) => calls.push(timed(ReplayCall::Get {
                    object: table.counterpart(request.path())?,
                    range: request.range(),
                })),
                (RequestKind::Head, _) => calls.push(timed(ReplayCall::Head {
                    object: table.counterpart(request.path())?,
                })),
                (RequestKind::List, _) => calls.push(timed(ReplayCall::List {
                    prefix: table.path()?,
                })),
                // A batch's GETs, and a range outside a batch, which no
                // trace holds.
                _ => {}
            }
        }
        calls.extend(batches.into_values());
        calls.sort_by_key(|timed| timed.issued);
        Ok(calls)
    }

    /// The calls made registering the table, in order.
    pub fn setup(&self) -> &[ReplayCall] {
        &self.setup
    }

    /// The queries, in order.
    pub fn queries(&self) -> &[QueryPlan] {
        &self.queries
    }
}

/// One call made again: the bytes it read, and how long it took.
#[derive(Clone, Copy, Debug)]
pub struct CallTime {
    bytes: u64,
    latency: Duration,
}

impl CallTime {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn latency(&self) -> Duration {
        self.latency
    }
}

/// A query made again: how long it took, and each call's time.
#[derive(Debug)]
pub struct QueryReplay {
    elapsed: Duration,
    calls: Vec<CallTime>,
}

impl QueryReplay {
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn calls(&self) -> &[CallTime] {
        &self.calls
    }

    pub fn bytes(&self) -> u64 {
        self.calls.iter().map(CallTime::bytes).sum()
    }
}

/// Makes a plan's calls of a store, each task's calls back to back.
pub struct Replayer {
    store: Arc<dyn ObjectStore>,
    copies: NonZeroUsize,
}

impl Replayer {
    /// Replays of `store`, each query's tasks run `copies` times at once.
    pub fn new(store: Arc<dyn ObjectStore>, copies: NonZeroUsize) -> Self {
        Self { store, copies }
    }

    /// Makes `calls` one after another.
    ///
    /// # Errors
    ///
    /// If a call fails.
    pub async fn sequence(&self, calls: &[ReplayCall]) -> Result<QueryReplay, Error> {
        let start = Instant::now();
        let calls = Self::in_order(self.store.as_ref(), calls).await?;
        Ok(QueryReplay {
            elapsed: start.elapsed(),
            calls,
        })
    }

    /// Makes `query`'s calls: those outside a task in order, then every
    /// task's, `copies` times over, all at once, each task on a tokio task
    /// of its own.
    ///
    /// # Errors
    ///
    /// If a call fails, or a task panics.
    pub async fn query(&self, query: &QueryPlan) -> Result<QueryReplay, Error> {
        let start = Instant::now();
        let mut calls = Self::in_order(self.store.as_ref(), &query.planning).await?;
        let tasks: Vec<_> = (0..self.copies.get())
            .flat_map(|_| &query.tasks)
            .map(|task| {
                let store = Arc::clone(&self.store);
                let task = Arc::clone(task);
                tokio::spawn(async move { Self::in_order(store.as_ref(), &task).await })
            })
            .collect();
        for task in tasks {
            calls.extend(task.await??);
        }
        Ok(QueryReplay {
            elapsed: start.elapsed(),
            calls,
        })
    }

    async fn in_order(
        store: &dyn ObjectStore,
        calls: &[ReplayCall],
    ) -> Result<Vec<CallTime>, Error> {
        let mut times = Vec::with_capacity(calls.len());
        for call in calls {
            let start = Instant::now();
            let bytes = call.make(store).await?;
            times.push(CallTime {
                bytes,
                latency: start.elapsed(),
            });
        }
        Ok(times)
    }
}

/// Latencies summed up: p50, p99 and the largest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencySummary {
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl LatencySummary {
    /// `latencies` summed up, or `None` if there are none. A percentile is
    /// the nearest rank's latency: the smallest that `p`% of the calls take
    /// at most.
    pub fn of(latencies: impl IntoIterator<Item = Duration>) -> Option<Self> {
        let mut sorted: Vec<_> = latencies.into_iter().collect();
        sorted.sort_unstable();
        let max = *sorted.last()?;
        let rank = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100) - 1];
        Some(Self {
            p50: rank(50),
            p99: rank(99),
            max,
        })
    }

    pub fn p50(&self) -> Duration {
        self.p50
    }

    pub fn p99(&self) -> Duration {
        self.p99
    }

    pub fn max(&self) -> Duration {
        self.max
    }
}

impl fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = |latency: Duration| latency.as_secs_f64() * 1e3;
        write!(
            f,
            "p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
            ms(self.p50),
            ms(self.p99),
            ms(self.max)
        )
    }
}

/// How many bytes a call read, in the groups the workload's GETs split
/// into: small ones that cost latency, large ones that cost bandwidth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CallSize {
    Under64KiB,
    Under1MiB,
    Under4MiB,
    Over4MiB,
}

impl CallSize {
    pub const ALL: [Self; 4] = [
        Self::Under64KiB,
        Self::Under1MiB,
        Self::Under4MiB,
        Self::Over4MiB,
    ];

    pub fn of(bytes: u64) -> Self {
        match bytes {
            0..0x1_0000 => Self::Under64KiB,
            0x1_0000..0x10_0000 => Self::Under1MiB,
            0x10_0000..0x40_0000 => Self::Under4MiB,
            _ => Self::Over4MiB,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Under64KiB => "< 64 KiB",
            Self::Under1MiB => "64 KiB - 1 MiB",
            Self::Under4MiB => "1 - 4 MiB",
            Self::Over4MiB => ">= 4 MiB",
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::request_trace::{RequestTrace, TracedStore};

    /// A workload traced through a [`TracedStore`] replays as the same
    /// calls: setup and planning in order, each task's calls in its order,
    /// the objects moved to the replay's table.
    #[tokio::test]
    async fn a_traced_workload_replays_as_traced() {
        let inner = Arc::new(InMemory::new());
        let object = Path::from("traced/data.parquet");
        inner.put(&object, vec![3; 8 << 20].into()).await.unwrap();
        let trace = Arc::new(RequestTrace::new());
        let traced: Arc<dyn ObjectStore> = Arc::new(TracedStore::new(inner, Arc::clone(&trace)));

        traced.head(&object).await.unwrap();
        trace.begin_query("p0q5");
        traced.head(&object).await.unwrap();
        let tasks: Vec<_> = [
            [0..10, 20..30],
            [(5 << 20)..(6 << 20), (7 << 20)..(7 << 20) + 1],
        ]
        .into_iter()
        .map(|ranges| {
            let (store, object) = (Arc::clone(&traced), object.clone());
            tokio::spawn(async move {
                store.get_ranges(&object, &ranges[..1]).await.unwrap();
                store.get_ranges(&object, &ranges).await.unwrap();
            })
        })
        .collect();
        for task in tasks {
            task.await.unwrap();
        }
        trace.begin_query("p1q5");
        traced.head(&object).await.unwrap();

        let mut csv = Vec::new();
        trace.write_csv(&mut csv).unwrap();
        let recorded = RecordedRequest::read_csv(csv.as_slice()).unwrap();
        let table = TableLocation::parse("gs://bucket/elsewhere/").unwrap();
        let plan = ReplayPlan::new(&recorded, &table).unwrap();

        let moved = Path::from("elsewhere/data.parquet");
        assert_eq!(
            plan.setup(),
            [ReplayCall::Head {
                object: moved.clone()
            }]
        );
        assert_eq!(plan.queries().len(), 1, "the first pass only");
        let query = &plan.queries()[0];
        assert_eq!((query.query(), query.len()), (5, 5));
        assert_eq!(
            query.planning,
            [ReplayCall::Head {
                object: moved.clone()
            }]
        );
        let mut tasks: Vec<Vec<ReplayCall>> =
            query.tasks.iter().map(|task| task.to_vec()).collect();
        tasks.sort_by_key(|calls| format!("{calls:?}"));
        // A call that asks for one range.
        let one = |range: Range<u64>| vec![range];
        assert_eq!(
            tasks,
            [
                vec![
                    ReplayCall::GetRanges {
                        object: moved.clone(),
                        ranges: one(0..10)
                    },
                    ReplayCall::GetRanges {
                        object: moved.clone(),
                        ranges: vec![0..10, 20..30]
                    },
                ],
                vec![
                    ReplayCall::GetRanges {
                        object: moved.clone(),
                        ranges: one((5 << 20)..(6 << 20))
                    },
                    ReplayCall::GetRanges {
                        object: moved.clone(),
                        ranges: vec![(5 << 20)..(6 << 20), (7 << 20)..(7 << 20) + 1],
                    },
                ],
            ]
        );

        let target = Arc::new(InMemory::new());
        target.put(&moved, vec![3; 8 << 20].into()).await.unwrap();
        let replayer = Replayer::new(target, NonZeroUsize::new(2).unwrap());
        let replay = replayer.query(query).await.unwrap();
        assert_eq!(
            replay.calls().len(),
            1 + 2 * 4,
            "planning once, tasks twice"
        );
        assert_eq!(replay.bytes(), 2 * (10 + 20 + (1 << 20) + (1 << 20) + 1));
    }

    #[test]
    fn latency_summary_takes_nearest_ranks() {
        let ms = |n| Duration::from_millis(n);
        let summary = LatencySummary::of((1..=200).map(ms)).unwrap();
        assert_eq!(
            (summary.p50(), summary.p99(), summary.max()),
            (ms(100), ms(198), ms(200))
        );
        let one = LatencySummary::of([ms(7)]).unwrap();
        assert_eq!((one.p50(), one.p99(), one.max()), (ms(7), ms(7), ms(7)));
        assert_eq!(LatencySummary::of([]), None);
    }

    #[test]
    fn call_sizes_split_at_their_bounds() {
        let sizes: Vec<_> = [0, (64 << 10) - 1, 64 << 10, 1 << 20, (4 << 20) - 1, 4 << 20]
            .into_iter()
            .map(CallSize::of)
            .collect();
        use CallSize::*;
        assert_eq!(
            sizes,
            [
                Under64KiB, Under64KiB, Under1MiB, Under4MiB, Under4MiB, Over4MiB
            ]
        );
    }
}
