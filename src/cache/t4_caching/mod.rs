//! [`T4Caching`]: the cache's front end (`doc/03-cache.md`, decision 10),
//! over an [`ObjectStore`], whose files' reads the cache serves, and which
//! fetches what only the remote holds itself. [`T4CachingBuilder`] makes
//! one.
//!
//! Integrations read through its API (`doc/09-vortex-integration.md`,
//! decision 1): a file opened once ([`T4Caching::open`]), a [`FileHandle`],
//! read a range at a time. One integration is t4's own: `T4Caching` is an
//! [`ObjectStore`] too (`object_store_integration`).
//!
//! - **What a file is,** the inner store says once: its first answer for the
//!   path, a list through the cache that named it or else a HEAD, which the
//!   cache trusts (decision 11). A hit asks the store nothing. A remote read
//!   fetches with `If-Match` on that answer's e_tag, so a file changed since
//!   is caught there, and the cache forgets the path.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Range;
use std::path::PathBuf;
use std::time::Duration;

use bbtree::BBTreeMap;
use bytes::Bytes;
use futures_util::future::{BoxFuture, join, join_all};
use futures_util::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{Extensions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt};

#[cfg(feature = "__fuzz")]
use super::ChunkId;
use super::cache_stores::{
    CacheStore, InProcessCacheStore, PlacementMaker, SharedCacheStore, TierSettings,
};
use super::network::{Network, Sleep};
#[cfg(feature = "__fuzz")]
use super::policy::Tier;
use super::policy::{
    FetchRuns, PlacementPolicy, RaceStragglers, ReadIntent, ReadPolicy, TierSizes,
};
use super::storage::{DiskFile, FileAllocation};
use super::{BLOCK_SIZE_U64, InsertOrAbort};
use crate::io::sync::Arc;

/// A cache of the reads of an [`ObjectStore`], in memory and on disk, and
/// an [`ObjectStore`] itself. [`Self::builder`] makes one. A clone shares
/// the cache: its tiers, its GETs and the files it knows.
#[derive(Clone)]
pub struct T4Caching {
    inner: Arc<dyn ObjectStore>,
    /// The cache store: this process's own, or its owner's, until that
    /// owner goes (`doc/11-shared-mode.md`).
    cache_store: Arc<dyn CacheStore>,
    /// The network half: the GETs of what only the remote holds, and the
    /// read policy that plans them.
    network: Network,
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
    growing_disk_files: bool,
    placement: Option<PlacementMaker>,
    reads: Option<Arc<dyn ReadPolicy>>,
    sleep: Option<Sleep>,
    network_rate: (u64, Duration),
    merged_up_to: u64,
    sharing: bool,
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
    /// How long a cache built for sharing tries to reach the owner of its
    /// files (`Self::with_multiprocess_sharing`) before it fails with `Error::Busy`: an
    /// owner starting or leaving refuses connections a while. An owner of
    /// another protocol version says so, and the cache fails at once.
    pub const OWNER_WAIT: Duration = Duration::from_secs(10);

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

    /// The cache files grow as blocks are written, rather than each taking
    /// its whole capacity on its drive as the cache is built; and a block
    /// is cached on disk only while every drive keeps 64 MiB and two blocks
    /// free, as Kismet's rule is. For a user's machine, whose drive the
    /// cache shares (`doc/10-spiral-integration.md`, decision 9). Unless
    /// set, each file takes its capacity at build, and no write finds its
    /// drive full.
    pub fn with_growing_disk_files(mut self) -> Self {
        self.growing_disk_files = true;
        self
    }

    /// The placement policy, where blocks live, which `build` makes for the
    /// tiers' chunk counts, once it knows them; a CLOCK for each tier
    /// ([`ClockPolicy`](super::policy::ClockPolicy)) unless set. A cache
    /// built for sharing makes it once it owns the cache files, which may
    /// be later, on another thread: as a client whose owner went
    /// (`Self::with_multiprocess_sharing`).
    pub fn with_placement_policy<P: PlacementPolicy + 'static>(
        mut self,
        build: impl FnOnce(TierSizes) -> P + Send + 'static,
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

    /// Shares the cache with the other processes on the machine that build
    /// one for sharing on the same cache files (`doc/11-shared-mode.md`).
    /// The first to build owns the cache and serves it to the others, its
    /// clients, from a thread of its own; another, rather than fail with
    /// `Error::Busy`, becomes a client. A client's tier settings and
    /// placement policy count only once it owns the cache; its read policy
    /// and its credentials, the inner store's, stay its own. Off unless
    /// set: a second cache on owned files is `Busy`.
    pub fn with_multiprocess_sharing(mut self) -> Self {
        self.sharing = true;
        self
    }

    /// The store: its cache files made and sized, its memory mapped, its
    /// policy built; or, built for sharing on files another cache owns, a
    /// client of that cache's.
    ///
    /// # Errors
    ///
    /// If the memory capacity or a disk file wasn't given, the queue depth
    /// or the network's bytes in flight are 0, or a cache file can't be
    /// made, sized and opened, or the memory mapped. `Error::Busy` if
    /// another cache owns a cache file, and either this one isn't built for
    /// sharing or that one's owner didn't admit it within
    /// [`Self::OWNER_WAIT`], or speaks another version of the protocol
    /// between them.
    pub fn build(mut self) -> crate::Result<T4Caching> {
        let memory_chunks =
            Self::chunks(self.memory_capacity, "T4Caching needs a memory capacity")?;
        if self.disk_files.is_empty() {
            return Err(crate::Error::InvalidArgument("T4Caching needs a disk file"));
        }
        let queue_depth = NonZeroU32::new(self.queue_depth)
            .ok_or(crate::Error::InvalidArgument("queue depth must be > 0"))?;
        let (bytes_per_second, unloaded_get_time) = self.network_rate;
        let bytes = u128::from(bytes_per_second) * unloaded_get_time.as_nanos() / 1_000_000_000;
        let bytes_in_flight = NonZeroU64::new(u64::try_from(bytes).unwrap_or(u64::MAX)).ok_or(
            crate::Error::InvalidArgument("the network's bytes in flight must be > 0"),
        )?;
        let reads = self
            .reads
            .take()
            .unwrap_or_else(|| Arc::new(RaceStragglers::default()));
        let allocation = if self.growing_disk_files {
            FileAllocation::growing()
        } else {
            FileAllocation::Whole
        };
        let mut tiers = TierSettings::new(
            memory_chunks,
            self.disk_files,
            queue_depth,
            self.direct_io,
            allocation,
            self.placement,
        );
        let cache_store: Arc<dyn CacheStore> = if self.sharing {
            let reads = Arc::clone(&reads);
            Arc::new(SharedCacheStore::open(tiers, reads, Self::OWNER_WAIT)?)
        } else {
            Arc::new(InProcessCacheStore::open(&mut tiers)?)
        };
        let network = Network::new(
            Arc::clone(&self.inner),
            reads,
            bytes_in_flight,
            self.merged_up_to,
            self.sleep,
        );
        Ok(T4Caching {
            inner: self.inner,
            cache_store,
            network,
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
            .field("growing_disk_files", &self.growing_disk_files)
            .field("network_rate", &self.network_rate)
            .field("merged_up_to", &self.merged_up_to)
            .field("sharing", &self.sharing)
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
            growing_disk_files: false,
            placement: None,
            reads: None,
            sleep: None,
            network_rate: (
                T4CachingBuilder::DEFAULT_NETWORK_RATE,
                T4CachingBuilder::DEFAULT_UNLOADED_GET_TIME,
            ),
            merged_up_to: T4CachingBuilder::DEFAULT_MERGED_UP_TO,
            sharing: false,
        }
    }

    /// The file at `location`, opened for reads: what the inner store says
    /// of it, asked once, unless a list or a read through the cache asked
    /// already (decision 11).
    ///
    /// # Errors
    ///
    /// The inner store's HEAD's: the file isn't there, say.
    pub async fn open(&self, location: &Path) -> object_store::Result<FileHandle> {
        let meta = self.meta_of(location).await?;
        Ok(FileHandle::new(self.clone(), meta))
    }

    /// The file `listed` names, opened for reads as a list of the inner
    /// store's said it is, asking nothing; unless the cache knows the path
    /// already, as it keeps to what it heard first (decision 11).
    pub fn open_listed(&self, listed: ObjectMeta) -> FileHandle {
        let listed = Arc::new(listed);
        let meta = self.known.remember(&listed.location, Arc::clone(&listed));
        FileHandle::new(self.clone(), meta)
    }

    /// The size of the version of the file at `location` the cache holds,
    /// if it holds one: read through the cache since it was built, or kept
    /// on disk across a restart (`doc/10-spiral-integration.md`, decision
    /// 7). Asks the inner store nothing, so a caller that names each version
    /// apart, as Spiral does, can open a file it cached before with
    /// [`Self::open_listed`] and no HEAD. A path whose file changed since
    /// the cache last read it answers the old version's size.
    pub async fn known_size(&self, location: &Path) -> Option<u64> {
        let path = location.as_ref().as_bytes();
        self.cache_store.known_size(path).await
    }

    /// The bytes `range` asks of a file of `file_size`, as the inner store
    /// takes them: a bounded range clamped to the file's end, an error if
    /// it starts past it.
    fn bytes_of(range: &GetRange, file_size: u64) -> object_store::Result<Range<u64>> {
        range
            .as_range(file_size)
            .map_err(|source| object_store::Error::Generic {
                store: Network::STORE,
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

    /// `range` of the version of the file at `location` with `e_tag` and
    /// `size`, within it, from memory, disk and fetches of what only the
    /// remote holds, each fetch's bytes put into the read's output and cached
    /// as the policy says as they come in; and the blocks other reads are
    /// fetching, taken from where those fetches cache them, or fetched again
    /// if they don't. The cache store is this process's own, or its owner's,
    /// whose storage half does the same for it (`doc/11-shared-mode.md`).
    /// `None` if the cache can't serve it: it can't hold the file, a read of
    /// the cache file failed, a fetch found the file changed, which the cache
    /// forgets then, or the owner failed the read or went.
    async fn read_through(
        &self,
        location: &Path,
        e_tag: &str,
        size: u64,
        range: Range<u64>,
        extensions: &Extensions,
    ) -> object_store::Result<Option<Bytes>> {
        let intent = extensions.get::<ReadIntent>().copied();
        let path = location.as_ref().as_bytes();
        let started = self
            .cache_store
            .start_read(path, e_tag, size, range, intent, self.network.read_policy())
            .await;
        let Ok(Some(mut read)) = started else {
            return Ok(None);
        };
        loop {
            if self.network.plan_gets(&mut *read, intent).await.is_err() {
                return Ok(None);
            }
            let Ok(receivers) = read.fetches() else {
                return Ok(None);
            };
            // Every fetch is in flight at once, a read's remote blocks keeping
            // the NIC busy, and beside the read's work on the tiers: its disk
            // reads, and its waits for the blocks other reads fetch. The GETs
            // are polled first, so they are sent before any of it is waited
            // on.
            let fetches = join_all(
                receivers
                    .into_iter()
                    .map(|blocks| self.network.fetch(location, e_tag, blocks, extensions)),
            );
            let (fetched, worked) = join(fetches, read.finish()).await;
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
            match worked {
                // Blocks the read waited for that no fetch cached: it
                // fetches them itself, or waits again for a read that
                // fetches them now.
                Ok(true) => {}
                Ok(false) => return Ok(read.into_bytes()),
                Err(_) => return Ok(None),
            }
        }
    }

    /// Whether the cache's index and tiers agree, as they must once nothing
    /// is in flight; if so, the chunks that hold a block, each with its tier.
    /// For the fuzz target (`fuzz/`), not part of the API.
    #[cfg(feature = "__fuzz")]
    #[doc(hidden)]
    pub fn check_at_rest(&self) -> Result<Vec<(Tier, ChunkId)>, String> {
        match self.cache_store.owned() {
            Some(cache) => cache.check_at_rest(),
            None => Err("a client holds no cache: its owner does".to_owned()),
        }
    }

    /// The cache store read through now: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn cache_store(&self) -> Arc<dyn CacheStore> {
        Arc::clone(&self.cache_store)
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

mod file_handle;
mod object_store_integration;

pub use file_handle::FileHandle;

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
