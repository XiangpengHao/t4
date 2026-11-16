use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::FileVersionId;
use super::policy::{ReadIntent, ReadPolicy};
#[cfg(any(test, feature = "__fuzz"))]
use super::storage::ObjectCache;
use crate::io::error::Result;

mod in_process;
mod shared;

#[cfg(feature = "__fuzz")]
pub use shared::FuzzedFrame;
#[cfg(all(feature = "__fuzz", feature = "shuttle", t4_simulated))]
pub use shared::{FuzzedHello, FuzzedPath, FuzzedRead, FuzzedSession, FuzzedSpan, FuzzedStep};

use in_process::InProcessRound;
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
    ) -> BoxFuture<'s, Result<Option<Box<dyn ReadRound<'s> + 's>>>>;

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

/// A pass over the blocks a read still lacks: hits taken, the rest fetched.
pub(super) trait ReadRound<'s>: Send {
    /// Plans the round's GETs with the read's policy and claims the blocks they fetch.
    fn plan_gets(self: Box<Self>) -> BoxFuture<'s, Result<RoundFetches<'s>>>;
}

pub(super) struct RoundFetches<'s> {
    pub(super) fetches: Vec<Box<dyn CacheStoreFetch<'s> + 's>>,
    /// Done once the fetches and the round's reads from the tiers are.
    pub(super) ended: BoxFuture<'s, Result<BoxedRoundEnd<'s>>>,
}

pub(super) enum RoundEnd<Round, Output> {
    /// Blocks it waited for ended uncached: the read fetches them itself.
    Retry(Round),
    Served(Output),
}

pub(super) type BoxedRoundEnd<'s> =
    RoundEnd<Box<dyn ReadRound<'s> + 's>, Box<dyn ServedOutput + 's>>;

/// Taken once the round's fetches are done: till then they may be landing in it.
pub(super) trait ServedOutput: Send {
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
