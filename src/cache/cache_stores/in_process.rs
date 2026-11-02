//! [`InProcessCacheStore`]: the cache store of the process that holds the
//! cache files' lock, its storage half run in this process; served to its
//! clients too, if it was built for sharing and could listen.

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::{CacheStore, CacheStoreFetch, CacheStoreRead};
use crate::cache::FileVersionId;
use crate::cache::policy::{ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::sharing::OwnerListener;
use crate::cache::storage::{
    CacheableFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead, TierWork,
};
use crate::io::error::Result;
use crate::io::sync::Arc;

/// This process's own cache store: the storage half, and the listener that
/// serves it to clients, as long as it lives.
pub(in crate::cache) struct InProcessCacheStore {
    /// Goes first as the store drops, and with it its sessions' handles on
    /// the cache.
    _listener: Option<OwnerListener>,
    cache: Arc<ObjectCache>,
}

impl InProcessCacheStore {
    /// The store of `cache`, served to clients by `listener`, if any.
    pub(super) fn new(cache: Arc<ObjectCache>, listener: Option<OwnerListener>) -> Self {
        Self {
            _listener: listener,
            cache,
        }
    }
}

impl CacheStore for InProcessCacheStore {
    fn owner_gone(&self) -> bool {
        false
    }

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
