//! [`InProcessCacheStore`]: the cache store of a process that holds the
//! cache files' lock and reads its cache alone, its storage half run in
//! this process; and [`TierSettings`], what its tiers are made of.

use std::num::{NonZeroU8, NonZeroU32};
use std::ops::Range;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::{CacheStore, CacheStoreFetch, CacheStoreRead};
use crate::cache::FileVersionId;
use crate::cache::policy::{
    ClockPolicy, PlacementPolicy, ReadIntent, ReadPlan, ReadPolicy, TierSizes,
};
use crate::cache::storage::{
    CacheFiles, CacheableFile, DiskFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead,
    TierWork,
};
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

/// What makes a placement policy, once the tiers' chunk counts are known.
pub(in crate::cache) type PlacementMaker =
    Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy> + Send>;

/// The placement policy the cache places blocks by, once it is made.
enum PlacementSource {
    /// A CLOCK for each tier, the default.
    Clock,
    /// The caller's, made once the tiers' chunk counts are known.
    Given(PlacementMaker),
    /// The caller's, made already for a cache that failed to open: no
    /// other policy takes its place.
    Spent,
}

/// What a cache's tiers are made of, as its builder said: the memory's
/// chunks, the cache files and how they are read, and the placement policy.
pub(in crate::cache) struct TierSettings {
    memory_chunks: NonZeroU32,
    disk_files: Vec<DiskFile>,
    queue_depth: NonZeroU32,
    direct_io: bool,
    placement: PlacementSource,
}

impl TierSettings {
    pub(in crate::cache) fn new(
        memory_chunks: NonZeroU32,
        disk_files: Vec<DiskFile>,
        queue_depth: NonZeroU32,
        direct_io: bool,
        placement: Option<PlacementMaker>,
    ) -> Self {
        Self {
            memory_chunks,
            disk_files,
            queue_depth,
            direct_io,
            placement: placement.map_or(PlacementSource::Clock, PlacementSource::Given),
        }
    }

    /// The first cache file's path, which names the cache.
    pub(super) fn first_file(&self) -> &Path {
        self.disk_files[0].path()
    }

    /// The placement policy for the memory's chunks and `disk_chunks`: a
    /// CLOCK's, or the caller's, made once.
    ///
    /// # Errors
    ///
    /// If the caller's was spent on a cache that failed to open.
    fn placement(&mut self, disk_chunks: NonZeroU32) -> Result<Arc<dyn PlacementPolicy>> {
        let sizes = TierSizes::new(self.memory_chunks, disk_chunks);
        match std::mem::replace(&mut self.placement, PlacementSource::Spent) {
            PlacementSource::Clock => {
                self.placement = PlacementSource::Clock;
                Ok(Arc::new(ClockPolicy::new(self.memory_chunks, disk_chunks)))
            }
            PlacementSource::Given(make) => Ok(make(sizes)),
            PlacementSource::Spent => Err(Error::InvalidArgument(
                "the placement policy was spent on a cache that failed to open",
            )),
        }
    }
}

/// The cache store of a process that holds the cache files' lock: its
/// storage half, read in this process.
pub(in crate::cache) struct InProcessCacheStore {
    cache: Arc<ObjectCache>,
}

impl InProcessCacheStore {
    /// The cache of `tiers`, their files' lock taken and their memory
    /// mapped.
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] if another cache holds a cache file's lock. If a
    /// cache file can't be made, sized and opened, or the memory mapped; or
    /// if the caller's placement policy was spent on a cache that failed so.
    pub(in crate::cache) fn open(tiers: &mut TierSettings) -> Result<Self> {
        let disk = CacheFiles::open(&tiers.disk_files, tiers.queue_depth, tiers.direct_io)?;
        let disk_chunks = NonZeroU32::new(disk.chunks().chunk_count())
            .expect("a disk file holds a chunk at least");
        let placement = tiers.placement(disk_chunks)?;
        let cache = ObjectCache::new(tiers.memory_chunks, disk, placement)?;
        Ok(Self {
            cache: Arc::new(cache),
        })
    }

    /// The storage half.
    pub(super) fn cache(&self) -> &Arc<ObjectCache> {
        &self.cache
    }
}

impl CacheStore for InProcessCacheStore {
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>> {
        Box::pin(async move {
            let Ok(file) = self.cache.resolve(path, Some(e_tag), size) else {
                return Ok(None);
            };
            let (read, work) = self.cache.start_read(&file, range.clone(), intent, reads)?;
            let read: Box<dyn CacheStoreRead<'s> + 's> = Box::new(InProcessRead {
                cache: &self.cache,
                reads,
                file,
                range,
                intent,
                read,
                work: Some(work),
            });
            Ok(Some(read))
        })
    }

    #[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
    fn owned(&self) -> Option<&ObjectCache> {
        Some(&self.cache)
    }
}

/// A read through this process's own cache store: its output, and its work
/// on the tiers, made again for the blocks left to fetch once finished.
pub(in crate::cache) struct InProcessRead<'s> {
    cache: &'s ObjectCache,
    reads: &'s dyn ReadPolicy,
    file: CacheableFile<'s>,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    read: PartialRead,
    /// Taken while it finishes.
    work: Option<TierWork<'s>>,
}

impl<'s> CacheStoreRead<'s> for InProcessRead<'s> {
    fn file(&self) -> CacheableFile<'s> {
        self.file
    }

    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        let work = self
            .work
            .as_mut()
            .expect("a read's work lives till it is finished");
        work.locate_next_run(&mut self.read)
    }

    fn take_gets<'a>(&'a mut self, gets: &'a ReadPlan) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let work = self
                .work
                .as_mut()
                .expect("a read's work lives till it is finished");
            work.take_gets(&mut self.read, gets);
            Ok(())
        })
    }

    fn fetches(&mut self) -> Result<Vec<Box<dyn CacheStoreFetch<'s> + 's>>> {
        let cache = self.cache;
        let fetches = self.read.fetches();
        fetches
            .into_iter()
            .map(|fetch| {
                let blocks: Box<dyn CacheStoreFetch<'s> + 's> =
                    Box::new(cache.receive(&mut self.read, fetch)?);
                Ok(blocks)
            })
            .collect()
    }

    fn finish(&mut self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let work = self
                .work
                .take()
                .expect("a read's work lives till it is finished");
            let left = work.finish(&mut self.read).await?;
            if left.is_empty() {
                return Ok(false);
            }
            // Blocks the read waited for that no fetch cached: it fetches
            // them itself, or waits again for a read that fetches them now.
            let (file, range) = (&self.file, self.range.clone());
            let work = self.cache.retry(file, range, self.intent, left, self.reads);
            self.work = Some(work);
            Ok(true)
        })
    }

    fn into_bytes(self: Box<Self>) -> Option<Bytes> {
        self.read.into_bytes()
    }
}

impl<'s> CacheStoreFetch<'s> for FetchedBlocks<'s> {
    fn range(&self) -> Range<u64> {
        FetchedBlocks::range(self)
    }

    fn file(&self) -> FileVersionId {
        FetchedBlocks::file(self)
    }

    fn race(&self) -> NonZeroU8 {
        FetchedBlocks::race(self)
    }

    fn late_race(&self) -> Option<Duration> {
        FetchedBlocks::late_race(self)
    }

    fn land<'a>(&'a mut self, piece: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        Box::pin(FetchedBlocks::land(self, piece))
    }

    fn finish(self: Box<Self>) -> BoxFuture<'s, Result<()>> {
        Box::pin(FetchedBlocks::finish(*self))
    }
}
