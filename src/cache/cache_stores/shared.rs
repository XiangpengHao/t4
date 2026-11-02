//! [`SharedCacheStore`]: the cache store of a process another one owns the
//! cache for, its storage half run in that owner, reached over the
//! client's connection (`sharing::client`).

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::{CacheStore, CacheStoreFetch, CacheStoreRead};
use crate::cache::FileVersionId;
use crate::cache::policy::{ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::sharing::{ClientFetch, ClientRead, OwnerConnection};
use crate::cache::storage::{CacheableFile, LocatedRun};
use crate::io::error::Result;

/// The cache store of the process that owns the cache, read through the
/// connection to it, until it goes.
pub(in crate::cache) struct SharedCacheStore {
    owner: OwnerConnection,
}

impl SharedCacheStore {
    /// The store `owner` serves.
    pub(super) fn new(owner: OwnerConnection) -> Self {
        Self { owner }
    }
}

impl CacheStore for SharedCacheStore {
    fn owner_gone(&self) -> bool {
        self.owner.owner_gone()
    }

    /// The owner hears its reads from disk itself, and tells the client's
    /// read policy of them, which the connection holds: `_reads` goes
    /// unused.
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        _reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>> {
        Box::pin(async move {
            let read = ClientRead::start(&self.owner, path, e_tag, size, range, intent).await?;
            Ok(read.map(|read| Box::new(read) as Box<dyn CacheStoreRead<'s> + 's>))
        })
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    fn owner_connection(&self) -> Option<&OwnerConnection> {
        Some(&self.owner)
    }
}

impl<'s> CacheStoreRead<'s> for ClientRead<'s> {
    fn file(&self) -> CacheableFile<'s> {
        ClientRead::file(self)
    }

    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        Ok(ClientRead::locate_next_run(self))
    }

    fn take_gets<'a>(&'a mut self, gets: &'a ReadPlan) -> BoxFuture<'a, Result<()>> {
        Box::pin(ClientRead::take_gets(self, gets))
    }

    fn fetches(&mut self) -> Result<Vec<Box<dyn CacheStoreFetch<'s> + 's>>> {
        let fetches = ClientRead::fetches(self).into_iter();
        Ok(fetches
            .map(|fetch| Box::new(fetch) as Box<dyn CacheStoreFetch<'s> + 's>)
            .collect())
    }

    fn finish(&mut self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(ClientRead::finish(self))
    }

    fn into_bytes(self: Box<Self>) -> Option<Bytes> {
        ClientRead::into_bytes(*self)
    }
}

impl<'s> CacheStoreFetch<'s> for ClientFetch<'s> {
    fn range(&self) -> Range<u64> {
        ClientFetch::range(self)
    }

    fn file(&self) -> FileVersionId {
        ClientFetch::file(self)
    }

    fn race(&self) -> NonZeroU8 {
        ClientFetch::race(self)
    }

    fn late_race(&self) -> Option<Duration> {
        ClientFetch::late_race(self)
    }

    fn land<'a>(&'a mut self, piece: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ClientFetch::land(self, piece);
            Ok(())
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'s, Result<()>> {
        Box::pin(ClientFetch::finish(*self))
    }
}
