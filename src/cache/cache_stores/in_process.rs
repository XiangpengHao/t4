use std::num::{NonZeroU8, NonZeroU32};
use std::ops::Range;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::{
    CacheStore, CacheStoreFetch, FileVersionKey, ReadRound, RoundEnd, RoundFetches, ServedOutput,
    VersionIdSource,
};
use crate::cache::policy::{ClockPolicy, PlacementPolicy, ReadIntent, ReadPolicy, TierSizes};
use crate::cache::storage::{
    CacheFiles, CacheableFile, DiskFile, FetchedBlocks, ObjectCache, PartialRead, TierWork,
};
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

pub(in crate::cache) type PlacementMaker =
    Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy> + Send>;

enum PlacementSource {
    Clock,
    Given(PlacementMaker),
    /// Made already for a cache that failed to open: no other policy takes its place.
    Spent,
}

pub(in crate::cache) struct TierSettings {
    clock: crate::io::time::Clock,
    memory_chunks: NonZeroU32,
    disk_files: Vec<DiskFile>,
    queue_depth: NonZeroU32,
    direct_io: bool,
    placement: PlacementSource,
}

impl TierSettings {
    pub(in crate::cache) fn set_clock(&mut self, clock: crate::io::time::Clock) {
        self.clock = clock;
    }

    pub(in crate::cache) fn new(
        memory_chunks: NonZeroU32,
        disk_files: Vec<DiskFile>,
        queue_depth: NonZeroU32,
        direct_io: bool,
        placement: Option<PlacementMaker>,
    ) -> Self {
        Self {
            clock: Default::default(),
            memory_chunks,
            disk_files,
            queue_depth,
            direct_io,
            placement: placement.map_or(PlacementSource::Clock, PlacementSource::Given),
        }
    }

    pub(super) fn first_file(&self) -> &Path {
        self.disk_files[0].path()
    }

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

pub(in crate::cache) struct InProcessCacheStore {
    cache: Arc<ObjectCache>,
}

impl InProcessCacheStore {
    pub(in crate::cache) fn open(tiers: &mut TierSettings) -> Result<Self> {
        let disk = CacheFiles::open(&tiers.disk_files, tiers.queue_depth, tiers.direct_io)?;
        let disk_chunks = NonZeroU32::new(disk.chunks().chunk_count())
            .expect("a disk file holds a chunk at least");
        let placement = tiers.placement(disk_chunks)?;
        let cache =
            ObjectCache::new(tiers.memory_chunks, disk, placement)?.with_clock(tiers.clock.clone());
        Ok(Self {
            cache: Arc::new(cache),
        })
    }

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
    ) -> BoxFuture<'s, Result<Option<Box<dyn ReadRound<'s> + 's>>>> {
        Box::pin(async move {
            let Ok(file) = self.cache.resolve(path, Some(e_tag), size) else {
                return Ok(None);
            };
            let (read, work) = self.cache.start_read(&file, range.clone(), intent, reads)?;
            let round: Box<dyn ReadRound<'s> + 's> = Box::new(InProcessRound {
                cache: &self.cache,
                reads,
                file,
                range,
                intent,
                read,
                work,
            });
            Ok(Some(round))
        })
    }

    fn known_size<'s>(&'s self, path: &'s [u8]) -> BoxFuture<'s, Option<u64>> {
        Box::pin(std::future::ready(self.cache.known_size(path)))
    }

    fn shut_down(&self) -> Result<()> {
        Ok(self.cache.shut_down()?)
    }

    #[cfg(any(test, feature = "__fuzz"))]
    fn owned(&self) -> Option<&ObjectCache> {
        Some(&self.cache)
    }
}

struct InProcessRound<'s> {
    cache: &'s ObjectCache,
    reads: &'s dyn ReadPolicy,
    file: CacheableFile<'s>,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    read: PartialRead,
    work: TierWork<'s>,
}

impl<'s> InProcessRound<'s> {
    fn take_planned_gets(self) -> Result<RoundFetches<'s>> {
        let Self {
            cache,
            reads,
            file,
            range,
            intent,
            mut read,
            mut work,
        } = self;
        if let Some(held) = work.locate(&mut read)? {
            let gets = held.run().planned_by(reads, &file, intent);
            work.take_gets(&mut read, held, &gets);
        }
        let fetches = (read.fetches().into_iter())
            .map(|fetch| {
                let blocks: Box<dyn CacheStoreFetch<'s> + 's> =
                    Box::new(cache.receive(&mut read, fetch)?);
                Ok(blocks)
            })
            .collect::<Result<_>>()?;
        let ended = Box::pin(async move {
            let left = work.finish(&mut read).await?;
            if left.is_empty() {
                return Ok(RoundEnd::Served(Box::new(read)));
            }
            let work = cache.retry(&file, range.clone(), intent, left, reads);
            let next = Self {
                cache,
                reads,
                file,
                range,
                intent,
                read,
                work,
            };
            Ok(RoundEnd::Retry(Box::new(next)))
        });
        Ok(RoundFetches { fetches, ended })
    }
}

impl<'s> ReadRound<'s> for InProcessRound<'s> {
    fn plan_gets(self: Box<Self>) -> BoxFuture<'s, Result<RoundFetches<'s>>> {
        Box::pin(std::future::ready(self.take_planned_gets()))
    }
}

impl ServedOutput for PartialRead {
    fn into_bytes(self: Box<Self>) -> Option<Bytes> {
        PartialRead::into_bytes(*self)
    }
}

impl<'s> CacheStoreFetch<'s> for FetchedBlocks<'s> {
    fn range(&self) -> Range<u64> {
        FetchedBlocks::range(self)
    }

    fn file(&self) -> FileVersionKey {
        FileVersionKey::new(VersionIdSource::OwnCache, FetchedBlocks::file(self))
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
