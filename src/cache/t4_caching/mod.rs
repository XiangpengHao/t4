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

/// A cache of an [`ObjectStore`]'s reads, in memory and on disk, and an [`ObjectStore`] itself.
/// A clone shares the cache.
#[derive(Clone)]
pub struct T4Caching {
    inner: Arc<dyn ObjectStore>,
    cache_store: Arc<dyn CacheStore>,
    network: Network,
    known: Arc<KnownFiles>,
}

/// Makes a [`T4Caching`].
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
    disk_allocation: FileAllocation,
    placement: Option<PlacementMaker>,
    reads: Option<Arc<dyn ReadPolicy>>,
    sleep: Option<Sleep>,
    network_rate: (u64, Duration),
    merged_up_to: u64,
    sharing: bool,
}

impl T4CachingBuilder {
    /// The NIC's rate t4 assumes unless told, in bytes a second: 50 Gbit/s.
    pub const DEFAULT_NETWORK_RATE: u64 = 50_000_000_000 / 8;
    pub const DEFAULT_UNLOADED_GET_TIME: Duration = Duration::from_millis(32);
    /// The default read policy's normal GET, so that merging never undoes its split.
    pub const DEFAULT_MERGED_UP_TO: u64 = FetchRuns::NORMAL_GET_MOST;
    /// How long a cache built for sharing tries to reach its owner before `Error::Busy`: an
    /// owner starting or leaving refuses connections a while.
    pub const OWNER_WAIT: Duration = Duration::from_secs(10);

    /// Memory for cached bytes, in whole 1 MiB blocks, one at least. Required.
    pub fn with_memory_capacity(mut self, bytes: u64) -> Self {
        self.memory_capacity = Some(bytes);
        self
    }

    /// A cache file of `capacity` bytes, one a drive, one at least. Each file holds as many
    /// chunks as the smallest has room for.
    pub fn with_disk_file(mut self, path: impl Into<PathBuf>, capacity: u64) -> Self {
        self.disk_files.push(DiskFile::new(path.into(), capacity));
        self
    }

    /// The most I/Os in flight to all cache files together; 64 unless set.
    pub fn with_queue_depth(mut self, queue_depth: u32) -> Self {
        self.queue_depth = queue_depth;
        self
    }

    /// Whether the cache files bypass the page cache; true unless set.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// For a drive the cache shares: files grow as blocks are written, and a block goes to disk
    /// only while every drive keeps 64 MiB and two blocks free. Eager (whole capacity) unless set.
    pub fn with_on_demand_disk_allocation(mut self) -> Self {
        self.disk_allocation = FileAllocation::on_demand();
        self
    }

    /// A CLOCK per tier unless set. A cache built for sharing may build it later, on another
    /// thread, when it takes over from an owner that went.
    pub fn with_placement_policy<P: PlacementPolicy + 'static>(
        mut self,
        build: impl FnOnce(TierSizes) -> P + Send + 'static,
    ) -> Self {
        self.placement = Some(Box::new(move |sizes| Arc::new(build(sizes))));
        self
    }

    /// [`RaceStragglers`] at the p90 unless set.
    pub fn with_read_policy(mut self, policy: impl ReadPolicy + 'static) -> Self {
        self.reads = Some(Arc::new(policy));
        self
    }

    /// The timer a GET's late racer waits on, `tokio::time::sleep` boxed, say: t4 has no
    /// runtime of its own. Without it no GET is raced late.
    pub fn with_sleep(
        mut self,
        sleep: impl Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        self.sleep = Some(Arc::new(sleep));
        self
    }

    /// Caps the bytes t4's GETs have in flight at `bytes_per_second * unloaded_get_time`, what
    /// fills the NIC; GETs past it wait. 50 Gbit/s and 32 ms, 200 MB, unless set.
    pub fn with_network_rate(mut self, bytes_per_second: u64, unloaded_get_time: Duration) -> Self {
        self.network_rate = (bytes_per_second, unloaded_get_time);
        self
    }

    /// Waiting GETs of one request for adjacent bytes go as one GET of `up_to` bytes at most;
    /// each apart if 0.
    pub fn with_waiting_gets_merged(mut self, up_to: u64) -> Self {
        self.merged_up_to = up_to;
        self
    }

    /// Shares the cache with other processes built for sharing on the same files: the first owns
    /// it, the rest are its clients, whose tier settings count only once one takes over.
    pub fn with_multiprocess_sharing(mut self) -> Self {
        self.sharing = true;
        self
    }

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
        let mut tiers = TierSettings::new(
            memory_chunks,
            self.disk_files,
            queue_depth,
            self.direct_io,
            self.disk_allocation,
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
            .field("disk_allocation", &self.disk_allocation)
            .field("network_rate", &self.network_rate)
            .field("merged_up_to", &self.merged_up_to)
            .field("sharing", &self.sharing)
            .finish_non_exhaustive()
    }
}

impl T4Caching {
    pub fn builder(inner: Arc<dyn ObjectStore>) -> T4CachingBuilder {
        T4CachingBuilder {
            inner,
            memory_capacity: None,
            disk_files: Vec::new(),
            queue_depth: 64,
            direct_io: true,
            disk_allocation: FileAllocation::Eager,
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

    /// Opens the file at `location`, with a HEAD only if no list or read through the cache saw it.
    pub async fn open(&self, location: &Path) -> object_store::Result<FileHandle> {
        let meta = self.meta_of(location).await?;
        Ok(FileHandle::new(self.clone(), meta))
    }

    /// Opens the file `listed` names with no HEAD; a path the cache knows keeps what it heard
    /// first.
    pub fn open_listed(&self, listed: ObjectMeta) -> FileHandle {
        let listed = Arc::new(listed);
        let meta = self.known.remember(&listed.location, Arc::clone(&listed));
        FileHandle::new(self.clone(), meta)
    }

    /// The size of the version of `location` the cache holds, asking the inner store nothing: a
    /// file changed since the cache read it answers the old version's size.
    pub async fn known_size(&self, location: &Path) -> Option<u64> {
        let path = location.as_ref().as_bytes();
        self.cache_store.known_size(path).await
    }

    /// Keeps the disk tier for the next cache on these files, as dropping does, for a process
    /// that exits without dropping its cache. Reads go on; nothing more is written to disk.
    pub fn shut_down(&self) -> crate::Result<()> {
        self.cache_store.shut_down()
    }

    fn bytes_of(range: &GetRange, file_size: u64) -> object_store::Result<Range<u64>> {
        range
            .as_range(file_size)
            .map_err(|source| object_store::Error::Generic {
                store: Network::STORE,
                source: Box::new(source),
            })
    }

    async fn meta_of(&self, location: &Path) -> object_store::Result<Arc<ObjectMeta>> {
        if let Some(meta) = self.known.get(location) {
            return Ok(meta);
        }
        let meta = Arc::new(self.inner.head(location).await?);
        Ok(self.known.remember(location, meta))
    }

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
            // The GETs are polled first, so they are sent before the tier work is waited on.
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
                Ok(true) => {}
                Ok(false) => return Ok(read.into_bytes()),
                Err(_) => return Ok(None),
            }
        }
    }

    /// For the fuzz target, not part of the API.
    #[cfg(feature = "__fuzz")]
    #[doc(hidden)]
    pub fn check_at_rest(&self) -> Result<Vec<(Tier, ChunkId)>, String> {
        match self.cache_store.owned() {
            Some(cache) => cache.check_at_rest(),
            None => Err("a client holds no cache: its owner does".to_owned()),
        }
    }

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

    fn remember(&self, location: &Path, meta: Arc<ObjectMeta>) -> Arc<ObjectMeta> {
        self.metas
            .get_or_insert(Self::key(location).into(), meta, |_, kept| Arc::clone(kept))
            .or_abort()
    }

    fn forget(&self, location: &Path) {
        self.metas.remove(Self::key(location));
    }

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
