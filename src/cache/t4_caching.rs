//! [`T4Caching`]: the cache's front end (`doc/03-cache.md`, decision 10),
//! over an [`ObjectStore`], whose files' reads the cache serves, and which
//! fetches what only the remote holds itself. [`T4CachingBuilder`] makes
//! one.
//!
//! Integrations read through its API (`doc/09-vortex-integration.md`,
//! decision 1): a file opened once ([`T4Caching::open`]), a [`CachedFile`],
//! read a range at a time. One integration is t4's own: `T4Caching` is an
//! [`ObjectStore`] too (`object_store_integration`).
//!
//! - **What a file is,** the inner store says once: its first answer for the
//!   path, a list through the cache that named it or else a HEAD, which the
//!   cache trusts (decision 11). A hit asks the store nothing. A remote read
//!   fetches with `If-Match` on that answer's e_tag, so a file changed since
//!   is caught there, and the cache forgets the path.

use std::fmt;
use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bbtree::BBTreeMap;
use bytes::Bytes;
use futures_util::future::{
    BoxFuture, Either, FutureExt, join, join_all, pending, select, select_ok,
};
use futures_util::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Extensions, GetOptions, GetRange, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt,
};

#[cfg(feature = "__fuzz")]
use super::ChunkId;
use super::cache_files::{CacheFiles, DiskFile};
use super::get_queue::{GetPlace, GetQueue, GetTurn, JoinedParts};
use super::io_report::IoReport;
use super::object_cache::{CacheableFile, FetchedBlocks, ObjectCache, PartialRead, TierWork};
#[cfg(feature = "__fuzz")]
use super::policy::Tier;
use super::policy::{
    ClockPolicy, FetchRuns, Io, PlacementPolicy, RaceStragglers, ReadIntent, ReadPolicy, TierSizes,
};
use super::{BLOCK_SIZE_U64, InsertOrAbort};
use crate::io::sync::Arc;

/// The name errors give this store.
const STORE: &str = "t4";

/// Where a GET's body goes: first the bytes of the waiting GETs joined to
/// it, then the read's own blocks.
struct BodyLanding<'c> {
    joined: JoinedParts,
    blocks: FetchedBlocks<'c>,
    /// The body's bytes landed so far.
    received: u64,
}

impl<'c> BodyLanding<'c> {
    fn new(joined: JoinedParts, blocks: FetchedBlocks<'c>) -> Self {
        Self {
            joined,
            blocks,
            received: 0,
        }
    }

    /// Lands `piece`, the body's next bytes.
    async fn land(&mut self, mut piece: Bytes) -> object_store::Result<()> {
        self.received += piece.len() as u64;
        self.joined.take_front(&mut piece);
        if piece.is_empty() {
            return Ok(());
        }
        self.blocks
            .land(&piece)
            .await
            .map_err(T4Caching::cache_error)
    }
}

/// What makes a policy, once the tiers' chunk counts are known.
type PlacementMaker = Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy>>;

/// A timer: a future done a while from now, the caller's runtime's, as t4
/// has none of its own.
type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

/// A cache of the reads of an [`ObjectStore`], in memory and on disk, and
/// an [`ObjectStore`] itself. [`Self::builder`] makes one. A clone shares
/// the cache: its tiers, its GETs and the files it knows.
#[derive(Clone)]
pub struct T4Caching {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<ObjectCache>,
    /// What plans each read's GETs, as the cache locates its blocks, and
    /// hears of its I/O: its GETs, and its reads from disk, which the
    /// cache reports to it as they go.
    reads: Arc<dyn ReadPolicy>,
    /// The GETs to the inner store in flight, and those waiting for room.
    gets: Arc<GetQueue>,
    /// What a GET's late racer waits on; none sent without it.
    sleep: Option<Sleep>,
    /// Shared with the uploads and deletes in flight, which forget their
    /// paths once done.
    known: Arc<KnownFiles>,
}

/// Makes a [`T4Caching`]: where it caches, how much, and by what policies.
///
/// ```no_run
/// # fn example(s3: std::sync::Arc<dyn object_store::ObjectStore>) -> t4::Result<()> {
/// let store = t4::T4Caching::builder(s3)
///     .with_memory_capacity(64 << 30)
///     .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
///     .with_disk_file("/mnt/nvme1/t4.cache", 3 << 40)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct T4CachingBuilder {
    inner: Arc<dyn ObjectStore>,
    memory_capacity: Option<u64>,
    disk_files: Vec<DiskFile>,
    queue_depth: u32,
    direct_io: bool,
    placement: Option<PlacementMaker>,
    reads: Option<Arc<dyn ReadPolicy>>,
    sleep: Option<Sleep>,
    network_rate: (u64, Duration),
    merged_up_to: u64,
}

impl T4CachingBuilder {
    /// The NIC's rate, in bytes a second, that t4 sizes its cap on bytes in
    /// flight for unless told (`Self::with_network_rate`): 50 Gbit/s.
    pub const DEFAULT_NETWORK_RATE: u64 = 50_000_000_000 / 8;
    /// What a GET of a block takes with little else in flight, unless told:
    /// the other half of the cap.
    pub const DEFAULT_UNLOADED_GET_TIME: Duration = Duration::from_millis(32);
    /// The most bytes waiting GETs go as, merged, unless told
    /// (`Self::with_waiting_gets_merged`): the default read policy's GET of
    /// a normal read, 4 MiB, so that merging never undoes its split.
    pub const DEFAULT_MERGED_UP_TO: u64 = FetchRuns::NORMAL_GET_MOST;

    /// Memory for cached bytes, in whole 1 MiB blocks, one at least.
    /// Required.
    pub fn with_memory_capacity(mut self, bytes: u64) -> Self {
        self.memory_capacity = Some(bytes);
        self
    }

    /// A cache file at `path`, made if missing, of `capacity` bytes: one a
    /// drive, one at least. t4 stripes its disk tier over them by 1 MiB
    /// chunk, each file holding as many chunks as the smallest has room for
    /// (`doc/04-multi-device.md`). Whatever a file held is nothing to the
    /// cache, which starts cold every time (decision 9).
    pub fn with_disk_file(mut self, path: impl Into<PathBuf>, capacity: u64) -> Self {
        self.disk_files.push(DiskFile::new(path.into(), capacity));
        self
    }

    /// The I/Os to the cache files in flight at most, all of them together;
    /// 64 unless set.
    pub fn with_queue_depth(mut self, queue_depth: u32) -> Self {
        self.queue_depth = queue_depth;
        self
    }

    /// Whether the cache files are read and written past the page cache; so
    /// unless set.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// The placement policy, where blocks live, which `build` makes for the
    /// tiers' chunk counts, once it knows them; a CLOCK for each tier
    /// ([`ClockPolicy`]) unless set.
    pub fn with_placement_policy<P: PlacementPolicy + 'static>(
        mut self,
        build: impl FnOnce(TierSizes) -> P + 'static,
    ) -> Self {
        self.placement = Some(Box::new(move |sizes| Arc::new(build(sizes))));
        self
    }

    /// The read policy, how reads are served: which GETs fetch what they
    /// only the remote holds, how many race. Unless set, [`RaceStragglers`]
    /// at the p90: it plans as `FetchRuns` does, a Parquet or Vortex file's
    /// footer raced, and races a GET late once it runs past the p90 of the
    /// GET times heard, given a timer (`Self::with_sleep`).
    pub fn with_read_policy(mut self, policy: impl ReadPolicy + 'static) -> Self {
        self.reads = Some(Arc::new(policy));
        self
    }

    /// The timer a GET's late racer waits on (`PlannedGet::with_late_race`):
    /// the caller's runtime's, `tokio::time::sleep` boxed, say, as t4 runs
    /// on none of its own. Without it no GET is raced late, by the default
    /// read policy either.
    pub fn with_sleep(
        mut self,
        sleep: impl Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        self.sleep = Some(Arc::new(sleep));
        self
    }

    /// The network t4's GETs to the inner store share: `bytes_per_second`,
    /// the NIC's rate, and `unloaded_get_time`, what a GET of a block takes
    /// when little else is in flight. t4 caps the bytes its GETs have in
    /// flight, all their racers', at what fills the NIC, the two multiplied
    /// (`doc/06-read-modes.md`, decision 11): 200 MB for 50 Gbit/s and
    /// 32 ms. GETs past it wait their turn, first come, first served, each
    /// until another's body is in; a GET bigger than the cap goes once
    /// nothing else is in flight, and a late racer goes at once, beside
    /// it. Unless set, [`Self::DEFAULT_NETWORK_RATE`] and
    /// [`Self::DEFAULT_UNLOADED_GET_TIME`], 200 MB: a NIC faster than
    /// 50 Gbit/s wants its own.
    pub fn with_network_rate(mut self, bytes_per_second: u64, unloaded_get_time: Duration) -> Self {
        self.network_rate = (bytes_per_second, unloaded_get_time);
        self
    }

    /// While GETs wait their turn (`Self::with_network_rate`), those of one
    /// request for adjacent bytes of a file go as one GET, of `up_to` bytes
    /// at most, when the first of them is due; each apart if 0. Unless set,
    /// [`Self::DEFAULT_MERGED_UP_TO`], 4 MiB.
    pub fn with_waiting_gets_merged(mut self, up_to: u64) -> Self {
        self.merged_up_to = up_to;
        self
    }

    /// The store: its cache files made and sized, its memory mapped, its
    /// policy built.
    ///
    /// # Errors
    ///
    /// If the memory capacity or a disk file wasn't given, the queue depth
    /// or the network's bytes in flight are 0, or a cache file can't be
    /// made, sized and opened, or the memory mapped.
    pub fn build(self) -> crate::Result<T4Caching> {
        let memory_chunks =
            Self::chunks(self.memory_capacity, "T4Caching needs a memory capacity")?;
        if self.disk_files.is_empty() {
            return Err(crate::Error::InvalidArgument("T4Caching needs a disk file"));
        }
        let queue_depth = NonZeroU32::new(self.queue_depth)
            .ok_or(crate::Error::InvalidArgument("queue depth must be > 0"))?;
        let disk = CacheFiles::open(&self.disk_files, queue_depth, self.direct_io)?;
        let disk_chunks = NonZeroU32::new(disk.chunks().chunk_count())
            .expect("a disk file holds a chunk at least");
        let sizes = TierSizes::new(memory_chunks, disk_chunks);
        let placement = match self.placement {
            Some(build) => build(sizes),
            None => Arc::new(ClockPolicy::new(memory_chunks, disk_chunks)),
        };
        let reads = self
            .reads
            .unwrap_or_else(|| Arc::new(RaceStragglers::default()));
        let (bytes_per_second, unloaded_get_time) = self.network_rate;
        let bytes = u128::from(bytes_per_second) * unloaded_get_time.as_nanos() / 1_000_000_000;
        let bytes_in_flight = NonZeroU64::new(u64::try_from(bytes).unwrap_or(u64::MAX)).ok_or(
            crate::Error::InvalidArgument("the network's bytes in flight must be > 0"),
        )?;
        Ok(T4Caching {
            inner: self.inner,
            cache: Arc::new(ObjectCache::new(memory_chunks, disk, placement)?),
            reads,
            gets: Arc::new(GetQueue::new(bytes_in_flight, self.merged_up_to)),
            sleep: self.sleep,
            known: Arc::new(KnownFiles::new()),
        })
    }

    /// The whole blocks `capacity` holds, one at least; `missing` if it
    /// wasn't given.
    fn chunks(capacity: Option<u64>, missing: &'static str) -> crate::Result<NonZeroU32> {
        let capacity = capacity.ok_or(crate::Error::InvalidArgument(missing))?;
        let chunks = (capacity / BLOCK_SIZE_U64).clamp(1, u64::from(u32::MAX));
        Ok(NonZeroU32::new(chunks as u32).expect("clamped to one at least"))
    }
}

impl fmt::Debug for T4CachingBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T4CachingBuilder")
            .field("inner", &self.inner)
            .field("memory_capacity", &self.memory_capacity)
            .field("disk_files", &self.disk_files)
            .field("queue_depth", &self.queue_depth)
            .field("direct_io", &self.direct_io)
            .field("network_rate", &self.network_rate)
            .field("merged_up_to", &self.merged_up_to)
            .finish_non_exhaustive()
    }
}

impl T4Caching {
    /// A builder of a cache over `inner`.
    pub fn builder(inner: Arc<dyn ObjectStore>) -> T4CachingBuilder {
        T4CachingBuilder {
            inner,
            memory_capacity: None,
            disk_files: Vec::new(),
            queue_depth: 64,
            direct_io: true,
            placement: None,
            reads: None,
            sleep: None,
            network_rate: (
                T4CachingBuilder::DEFAULT_NETWORK_RATE,
                T4CachingBuilder::DEFAULT_UNLOADED_GET_TIME,
            ),
            merged_up_to: T4CachingBuilder::DEFAULT_MERGED_UP_TO,
        }
    }

    /// The file at `location`, opened for reads: what the inner store says
    /// of it, asked once, unless a list or a read through the cache asked
    /// already (decision 11).
    ///
    /// # Errors
    ///
    /// The inner store's HEAD's: the file isn't there, say.
    pub async fn open(&self, location: &Path) -> object_store::Result<CachedFile> {
        let meta = self.meta_of(location).await?;
        Ok(CachedFile::new(self.clone(), meta))
    }

    /// The file `listed` names, opened for reads as a list of the inner
    /// store's said it is, asking nothing; unless the cache knows the path
    /// already, as it keeps to what it heard first (decision 11).
    pub fn open_listed(&self, listed: ObjectMeta) -> CachedFile {
        let listed = Arc::new(listed);
        let meta = self.known.remember(&listed.location, Arc::clone(&listed));
        CachedFile::new(self.clone(), meta)
    }

    /// The bytes `range` asks of a file of `file_size`, as the inner store
    /// takes them: a bounded range clamped to the file's end, an error if
    /// it starts past it.
    fn bytes_of(range: &GetRange, file_size: u64) -> object_store::Result<Range<u64>> {
        range
            .as_range(file_size)
            .map_err(|source| object_store::Error::Generic {
                store: STORE,
                source: Box::new(source),
            })
    }

    /// What the inner store says of the file at `location`: what it said
    /// first, unless the cache forgot it since.
    async fn meta_of(&self, location: &Path) -> object_store::Result<Arc<ObjectMeta>> {
        if let Some(meta) = self.known.get(location) {
            return Ok(meta);
        }
        let meta = Arc::new(self.inner.head(location).await?);
        Ok(self.known.remember(location, meta))
    }

    /// `range` of `file`, from memory, disk and fetches of what only the
    /// remote holds, each fetch's bytes put into the read's output and cached
    /// as the policy says as they come in; and the blocks other reads are
    /// fetching, taken from where those fetches cache them, or fetched again
    /// if they don't. `None` if the cache can't serve it: a read of the cache
    /// file failed, or a fetch found the file changed, which the cache
    /// forgets then.
    async fn read_through(
        &self,
        location: &Path,
        file: &CacheableFile<'_>,
        e_tag: &str,
        range: Range<u64>,
        extensions: &Extensions,
    ) -> object_store::Result<Option<Bytes>> {
        let intent = extensions.get::<ReadIntent>().copied();
        let reads = &*self.reads;
        let Ok((mut read, mut work)) = self.cache.start_read(file, range.clone(), intent, reads)
        else {
            return Ok(None);
        };
        loop {
            if self.plan_gets(&mut read, &mut work, file, intent).is_err() {
                return Ok(None);
            }
            let mut receivers = Vec::new();
            for fetch in read.fetches() {
                let Ok(blocks) = self.cache.receive(&mut read, fetch) else {
                    return Ok(None);
                };
                receivers.push(blocks);
            }
            // Every fetch is in flight at once, a read's remote blocks keeping
            // the NIC busy, and beside the read's work on the tiers: its disk
            // reads, and its waits for the blocks other reads fetch. The GETs
            // are polled first, so they are sent before any of it is waited
            // on.
            let fetches = join_all(
                receivers
                    .into_iter()
                    .map(|blocks| self.fetch(location, e_tag, blocks, extensions)),
            );
            let (fetched, worked) = join(fetches, work.finish(&mut read)).await;
            for fetched in fetched {
                match fetched {
                    Ok(()) => {}
                    Err(object_store::Error::Precondition { .. }) => {
                        self.known.forget(location);
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
            let Ok(retry) = worked else {
                return Ok(None);
            };
            if retry.is_empty() {
                return Ok(read.into_bytes());
            }
            // Blocks the read waited for that no fetch cached: it fetches
            // them itself, or waits again for a read that fetches them now.
            work = self.cache.retry(file, range.clone(), intent, retry, reads);
        }
    }

    /// Plans `read`'s GETs, of `file` for `intent`, run by run as `work`
    /// locates its blocks: each run's by the read policy, which the work
    /// takes, claiming the blocks its GETs widen to before it locates the
    /// next run.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted.
    fn plan_gets(
        &self,
        read: &mut PartialRead,
        work: &mut TierWork<'_>,
        file: &CacheableFile<'_>,
        intent: Option<ReadIntent>,
    ) -> crate::Result<()> {
        while let Some(run) = work.locate_next_run(read)? {
            let gets = run.planned_by(&*self.reads, file, intent);
            work.take_gets(read, &gets);
        }
        Ok(())
    }

    /// Fetches the bytes `blocks` holds from the inner store, if the file is
    /// still the version with `e_tag`, and hands each piece of the answer to
    /// `blocks` as it comes in. The GET waits its turn first, if the cap on
    /// bytes in flight is reached, and keeps its place until its body is in;
    /// it may take, before its own, the bytes of waiting GETs joined to it,
    /// or join one and take its bytes from it. As many identical GETs as
    /// the plan says race for them, sent at once: the first answer takes
    /// the bytes, and the others are dropped, their requests with them. If
    /// the plan races it late too, and the cache has a timer, one more
    /// request goes once the GET has been out that long, for the bytes not
    /// in yet: the first to have them all is taken, and the other dropped.
    /// The read policy hears the GET finished once its body is in, and
    /// landed once its blocks are cached.
    async fn fetch(
        &self,
        location: &Path,
        e_tag: &str,
        mut blocks: FetchedBlocks<'_>,
        extensions: &Extensions,
    ) -> object_store::Result<()> {
        let waiting = Instant::now();
        let Some((place, joined)) = self.turn(&mut blocks).await? else {
            // Its bytes came with another GET's.
            return blocks.finish().await.map_err(Self::cache_error);
        };
        let own = blocks.range();
        let bytes = joined.start().unwrap_or(own.start)..own.end;
        let mut body = BodyLanding::new(joined, blocks);
        let options = GetOptions {
            range: Some(GetRange::Bounded(bytes.clone())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let path = location.as_ref().as_bytes();
        let sent = Instant::now();
        let waited = sent - waiting;
        let late = body.blocks.late_race().zip(self.sleep.clone());
        let racers = body.blocks.race();
        // The time the body's pieces took to land, apart from the time
        // spent waiting for them; and how many bytes have come, for a late
        // racer to ask for the rest.
        let mut landing = Duration::ZERO;
        let landed = AtomicU64::new(0);
        let first = async {
            let (answer, mut report) = self
                .first_answer(location, &options, path, &bytes, racers)
                .await?;
            let received = async {
                let mut answer = answer.into_stream();
                while let Some(piece) = answer.try_next().await? {
                    let started = Instant::now();
                    let len = piece.len() as u64;
                    body.land(piece).await?;
                    landing += started.elapsed();
                    landed.fetch_add(len, Ordering::Relaxed);
                }
                Ok::<_, object_store::Error>(())
            };
            match received.await {
                Ok(()) => {
                    report.finished(bytes.end - bytes.start);
                    Ok(report)
                }
                Err(error) => {
                    report.failed();
                    Err(error)
                }
            }
        };
        let report = match late {
            None => first.await?,
            Some((after, sleep)) => {
                let rest = async {
                    sleep(after).await;
                    let from = bytes.start + landed.load(Ordering::Relaxed);
                    let racer = racers.get();
                    self.late_answer(location, &options, path, from..bytes.end, racer, sent)
                        .await
                };
                // The first request, or the late racer, with the rest.
                let won = match select(Box::pin(first), Box::pin(rest)).await {
                    Either::Left((done, _rest)) => Either::Left(done?),
                    Either::Right((Ok(rest), _first)) => Either::Right(rest),
                    Either::Right((Err(_), first)) => Either::Left(first.await?),
                };
                match won {
                    Either::Left(report) => report,
                    // The first request given up on: the bytes not in yet
                    // land from the late racer's.
                    Either::Right((from, rest, report)) => {
                        let skip = usize::try_from(bytes.start + body.received - from)
                            .expect("a GET's bytes fit in memory");
                        let started = Instant::now();
                        body.land(rest.slice(skip..)).await?;
                        landing += started.elapsed();
                        report
                    }
                }
            }
        };
        // Its body in, the GET gives its place back.
        drop(place);
        body.blocks.finish().await.map_err(Self::cache_error)?;
        report.landed(landing, waited);
        Ok(())
    }

    /// The turn of the GET of `blocks`, once it waited for it, if the cap on
    /// bytes in flight is reached: its place, and the waiting GETs joined to
    /// it. `None` if it joined another GET, whose bytes `blocks` took; if
    /// that GET failed or was given up on, it waits again.
    async fn turn(
        &self,
        blocks: &mut FetchedBlocks<'_>,
    ) -> object_store::Result<Option<(GetPlace, JoinedParts)>> {
        loop {
            match self
                .gets
                .turn(blocks.file(), blocks.range(), blocks.race())
                .await
            {
                GetTurn::Send { place, joined } => return Ok(Some((place, joined))),
                GetTurn::Joined(get) => {
                    let Some(pieces) = get.bytes().await else {
                        continue;
                    };
                    for piece in pieces {
                        blocks.land(&piece).await.map_err(Self::cache_error)?;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// The first answer of `racers` identical GETs, sent at once, the
    /// others dropped before their bodies; and its report.
    async fn first_answer<'a>(
        &'a self,
        location: &Path,
        options: &GetOptions,
        path: &'a [u8],
        bytes: &Range<u64>,
        racers: NonZeroU8,
    ) -> object_store::Result<(GetResult, IoReport<'a>)> {
        let racers = (0..racers.get()).map(|racer| {
            let get = Io::Get {
                path,
                bytes: bytes.clone(),
                racer,
            };
            let mut report = IoReport::start(&*self.reads, get);
            let get = self.inner.get_opts(location, options.clone());
            async move {
                match get.await {
                    Ok(answer) => {
                        report.answered();
                        Ok((answer, report))
                    }
                    Err(error) => {
                        report.failed();
                        Err(error)
                    }
                }
            }
            .boxed()
        });
        let (first, _losers) = select_ok(racers).await?;
        Ok(first)
    }

    /// A late racer's GET of `rest`, the bytes of a GET not in yet, whole:
    /// where they start, and its report, timed from `sent`, when the GET's
    /// first request went. Never done if no bytes are left.
    async fn late_answer<'a>(
        &'a self,
        location: &Path,
        options: &GetOptions,
        path: &'a [u8],
        rest: Range<u64>,
        racer: u8,
        sent: Instant,
    ) -> object_store::Result<(u64, Bytes, IoReport<'a>)> {
        if rest.is_empty() {
            return pending().await;
        }
        let get = Io::Get {
            path,
            bytes: rest.clone(),
            racer,
        };
        let mut report = IoReport::start_at(&*self.reads, get, sent);
        let options = GetOptions {
            range: Some(GetRange::Bounded(rest.clone())),
            ..options.clone()
        };
        let answer = match self.inner.get_opts(location, options).await {
            Ok(answer) => answer,
            Err(error) => {
                report.failed();
                return Err(error);
            }
        };
        report.answered();
        match answer.bytes().await {
            Ok(body) => {
                report.finished(rest.end - rest.start);
                Ok((rest.start, body, report))
            }
            Err(error) => {
                report.failed();
                Err(error)
            }
        }
    }

    /// A failure of the cache's, as the store's error.
    fn cache_error(error: crate::io::error::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: STORE,
            source: Box::new(error),
        }
    }

    /// Whether the cache's index and tiers agree, as they must once nothing
    /// is in flight; if so, the chunks that hold a block, each with its tier.
    /// For the fuzz target (`fuzz/`), not part of the API.
    #[cfg(feature = "__fuzz")]
    #[doc(hidden)]
    pub fn check_at_rest(&self) -> Result<Vec<(Tier, ChunkId)>, String> {
        self.cache.check_at_rest()
    }
}

impl fmt::Display for T4Caching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t4 cache over {}", self.inner)
    }
}

impl fmt::Debug for T4Caching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T4Caching")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// What the inner store said of each file the cache has read, by path.
struct KnownFiles {
    metas: BBTreeMap<Box<[u8]>, Arc<ObjectMeta>>,
}

impl KnownFiles {
    fn new() -> Self {
        Self {
            metas: BBTreeMap::new(),
        }
    }

    fn get(&self, location: &Path) -> Option<Arc<ObjectMeta>> {
        self.metas
            .read(Self::key(location), |_, meta| Arc::clone(meta))
    }

    /// Keeps `meta` for `location`, unless another read kept an answer
    /// first; returns the one kept.
    fn remember(&self, location: &Path, meta: Arc<ObjectMeta>) -> Arc<ObjectMeta> {
        self.metas
            .get_or_insert(Self::key(location).into(), meta, |_, kept| Arc::clone(kept))
            .or_abort()
    }

    fn forget(&self, location: &Path) {
        self.metas.remove(Self::key(location));
    }

    /// `listed`, a list of the inner store's, each file it names kept as the
    /// store's answer for that path, unless one was kept first: a read of a
    /// listed file asks for no HEAD.
    fn remembering(
        self: &Arc<Self>,
        listed: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let known = Arc::clone(self);
        listed
            .inspect(move |listed| {
                if let Ok(meta) = listed {
                    known.remember(&meta.location, Arc::new(meta.clone()));
                }
            })
            .boxed()
    }

    fn key(location: &Path) -> &[u8] {
        location.as_ref().as_bytes()
    }
}

impl fmt::Debug for KnownFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KnownFiles").finish_non_exhaustive()
    }
}

mod cached_file;
mod object_store_integration;

pub use cached_file::CachedFile;

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
