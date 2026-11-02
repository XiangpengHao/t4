//! The cache store a process reads through, picked as the cache is built
//! (`doc/11-shared-mode.md`, decision 2): [`InProcessCacheStore`], the
//! process's own, which it reads alone; or [`SharedCacheStore`], one cache
//! read by several processes. Either is a [`CacheStore`], which the front
//! end and the network half read through as a `dyn`.
//!
//! - `in_process`: a process's own cache store, and what its tiers are made
//!   of.
//! - `shared`: shared mode, all of it: the owner serving its clients, a
//!   client reading through its owner, and the hand-off once it goes.

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::FileVersionId;
use super::policy::{ReadIntent, ReadPlan, ReadPolicy};
#[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
use super::storage::ObjectCache;
use super::storage::{CacheableFile, LocatedRun};
use crate::io::error::Result;

mod in_process;
mod shared;

pub(super) use in_process::{InProcessCacheStore, PlacementMaker, TierSettings};
#[cfg(all(test, not(feature = "shuttle")))]
use shared::OwnerConnection;
pub(super) use shared::SharedCacheStore;

/// A cache store a process reads through: the storage half, run in this
/// process or in its owner's.
pub(super) trait CacheStore: Send + Sync {
    /// Starts a read of `range` of the version of the file at `path` with
    /// `e_tag` and `size`, within it, for `intent`, its reads from disk
    /// heard by `reads`: the bytes the cache holds served or on their way,
    /// and the rest located, run by run, for the caller to plan the GETs of
    /// ([`CacheStoreRead::locate_next_run`]). `None` if the cache can't
    /// hold the file.
    ///
    /// # Errors
    ///
    /// If the read's output can't be made, or the owner failed the read or
    /// is gone.
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>>;

    /// The storage half, if this process runs it.
    #[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
    fn owned(&self) -> Option<&ObjectCache> {
        None
    }

    /// The connection to the owner, if another process runs the storage
    /// half: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    fn owner_connection(&self) -> Option<std::sync::Arc<OwnerConnection>> {
        None
    }
}

/// A read through a [`CacheStore`], its steps in order: each run located,
/// and its GETs taken; the fetches made; the read finished, again for the
/// blocks left; its bytes.
pub(super) trait CacheStoreRead<'s>: Send {
    /// The read's file, the version of it the cache named.
    fn file(&self) -> CacheableFile<'s>;

    /// The read's next run of adjacent blocks the tiers don't all hold,
    /// located, for the caller's read policy to plan the GETs of
    /// ([`Self::take_gets`]); `None` once every run is planned.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted.
    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>>;

    /// Takes `gets`, planned for the run located last; or the default's, if
    /// the cache can't carry them out.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    fn take_gets<'a>(&'a mut self, gets: &'a ReadPlan) -> BoxFuture<'a, Result<()>>;

    /// The fetches of the GETs taken, for the caller to make, once every
    /// run is planned.
    ///
    /// # Errors
    ///
    /// If the read's output can't be lent to a fetch.
    fn fetches(&mut self) -> Result<Vec<Box<dyn CacheStoreFetch<'s> + 's>>>;

    /// Waits for the read's work on the tiers, its fetches made beside it:
    /// whether blocks are left to fetch, those it waited for that no fetch
    /// cached, located again ([`Self::locate_next_run`]).
    ///
    /// # Errors
    ///
    /// If a read from the cache file failed, or the owner failed the read or
    /// is gone.
    fn finish(&mut self) -> BoxFuture<'_, Result<bool>>;

    /// The read's bytes, once finished with no block left to fetch: `None`
    /// if a read from disk is still writing them, as after one failed.
    fn into_bytes(self: Box<Self>) -> Option<Bytes>;
}

/// One of a read's fetches: its bytes landed in the cache as they come in,
/// in this process or through its owner.
pub(super) trait CacheStoreFetch<'s>: Send {
    /// The file's bytes the fetch holds.
    fn range(&self) -> Range<u64>;

    /// The version of the file they are of.
    fn file(&self) -> FileVersionKey;

    /// How many identical requests race for them.
    fn race(&self) -> NonZeroU8;

    /// When one more request races them, if they aren't all in.
    fn late_race(&self) -> Option<Duration>;

    /// Takes `piece`, the fetch's next bytes.
    ///
    /// # Errors
    ///
    /// If the fetch brings more bytes than it holds.
    fn land<'a>(&'a mut self, piece: &'a [u8]) -> BoxFuture<'a, Result<()>>;

    /// Caches the blocks where they landed, once every byte is in.
    ///
    /// # Errors
    ///
    /// If the fetch brought fewer bytes than it holds: nothing is cached.
    fn finish(self: Box<Self>) -> BoxFuture<'s, Result<()>>;
}

/// A file version as a process's fetches tell it apart: its id, and the
/// cache that gave the id. An id names one version in the cache that gave
/// it only: a client whose owner went reads through the next owner, or its
/// own cache, which give its ids to other files, while its GETs under the
/// last owner may still wait to go (`doc/11-shared-mode.md`, decision 7).
/// Those must never go as one with GETs of another file.
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

/// The cache a file version's id came from, as this process tells it apart
/// from the others it reads through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum VersionIdSource {
    /// This process's own: it owns a cache once at most, and for good.
    OwnCache,
    /// The owner this process reached on its connection numbered so: each
    /// connection a number of its own, one to each owner in turn.
    Owner { connection: u64 },
}
