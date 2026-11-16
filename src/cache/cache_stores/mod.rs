use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::FileVersionId;
use super::policy::{ReadIntent, ReadPlan, ReadPolicy};
#[cfg(any(test, feature = "__fuzz"))]
use super::storage::ObjectCache;
use super::storage::{CacheableFile, LocatedRun};
use crate::io::error::Result;

mod in_process;
mod shared;

#[cfg(feature = "__fuzz")]
pub use shared::FuzzedFrame;
#[cfg(all(feature = "__fuzz", feature = "shuttle", t4_simulated))]
pub use shared::{FuzzedHello, FuzzedPath, FuzzedRead, FuzzedSession, FuzzedSpan, FuzzedStep};

pub(super) use in_process::{InProcessCacheStore, PlacementMaker, TierSettings};
#[cfg(any(test, all(feature = "shuttle", t4_simulated)))]
use shared::OwnerConnection;
pub(super) use shared::SharedCacheStore;

pub(super) trait CacheStore: Send + Sync {
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>>;

    fn known_size<'s>(&'s self, path: &'s [u8]) -> BoxFuture<'s, Option<u64>>;

    fn shut_down(&self) -> Result<()>;

    #[cfg(any(test, feature = "__fuzz"))]
    fn owned(&self) -> Option<&ObjectCache> {
        None
    }

    #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
    fn live_sessions(&self) -> usize {
        0
    }

    #[cfg(any(test, all(feature = "shuttle", t4_simulated)))]
    fn owner_connection(&self) -> Option<std::sync::Arc<OwnerConnection>> {
        None
    }
}

pub(super) trait CacheStoreRead<'s>: Send {
    fn file(&self) -> CacheableFile<'s>;

    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>>;

    fn take_gets<'a>(&'a mut self, gets: &'a ReadPlan) -> BoxFuture<'a, Result<()>>;

    fn fetches(&mut self) -> Result<Vec<Box<dyn CacheStoreFetch<'s> + 's>>>;

    fn finish(&mut self) -> BoxFuture<'_, Result<bool>>;

    /// `None` if a read from disk is still writing the bytes, as after one failed.
    fn into_bytes(self: Box<Self>) -> Option<Bytes>;
}

pub(super) trait CacheStoreFetch<'s>: Send {
    fn range(&self) -> Range<u64>;

    fn file(&self) -> FileVersionKey;

    fn race(&self) -> NonZeroU8;

    fn late_race(&self) -> Option<Duration>;

    fn land<'a>(&'a mut self, piece: &'a [u8]) -> BoxFuture<'a, Result<()>>;

    fn finish(self: Box<Self>) -> BoxFuture<'s, Result<()>>;
}

/// An id names a version only in the cache that gave it, and GETs under a gone owner may
/// still wait to go beside the next cache's (`doc/11-shared-mode.md`, decision 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct FileVersionKey {
    source: VersionIdSource,
    id: FileVersionId,
}

impl FileVersionKey {
    pub(super) fn new(source: VersionIdSource, id: FileVersionId) -> Self {
        Self { source, id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum VersionIdSource {
    /// A process owns a cache once at most, and for good.
    OwnCache,
    Owner {
        connection: u64,
    },
}

#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(in crate::cache) use shared::TestTransport;
