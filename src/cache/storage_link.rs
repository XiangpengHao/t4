//! [`StorageLink`]: how a cache's fetch half reaches its storage half
//! (`doc/11-shared-mode.md`, decision 2): in the process that owns the
//! cache, directly; in a client, through its owner, over a socket. A read
//! through it is a [`LinkedRead`], each of its fetches a [`LinkedFetch`]:
//! the fetch half reads either way with the same code.

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;

use super::FileVersionId;
use super::object_cache::{
    CacheableFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead, TierWork,
};
use super::policy::{ReadIntent, ReadPlan, ReadPolicy};
use super::sharing::{ClientFetch, ClientRead, OwnerConnection, OwnerListener};
use crate::io::error::Result;
use crate::io::sync::Arc;

mod current_link;

pub(super) use current_link::{CurrentLink, LinkMaker, PlacementMaker};

pub(super) enum StorageLink {
    /// This process holds the cache files' lock: it runs the storage half,
    /// and serves it to clients if it was built for sharing and could
    /// listen, as long as the listener lives. The listener goes first as it drops, and with it its
    /// sessions' handles on the cache.
    Owner {
        _listener: Option<OwnerListener>,
        cache: Arc<ObjectCache>,
    },
    /// Another process owns the cache, and this one reads through it.
    Client(OwnerConnection),
}

impl StorageLink {
    /// Whether this is a client's link whose owner has gone: its calls
    /// fail, and a new link is due (decision 7).
    pub(super) fn owner_gone(&self) -> bool {
        match self {
            Self::Owner { .. } => false,
            Self::Client(owner) => owner.owner_gone(),
        }
    }

    /// The storage half, if this process runs it.
    #[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
    pub(super) fn owned(&self) -> Option<&ObjectCache> {
        match self {
            Self::Owner { cache, .. } => Some(cache),
            Self::Client(_) => None,
        }
    }

    /// Starts a read of `range` of the version of the file at `path` with
    /// `e_tag` and `size`, within it, for `intent`, its reads from disk
    /// heard by `reads`: the bytes the cache holds served or on their way,
    /// and the rest located, run by run, for the caller to plan the GETs of
    /// ([`LinkedRead::locate_next_run`]). `None` if the cache can't hold
    /// the file.
    ///
    /// # Errors
    ///
    /// If the read's output can't be made, or the owner failed the read or
    /// is gone.
    pub(super) async fn start_read<'l>(
        &'l self,
        path: &'l [u8],
        e_tag: &str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'l dyn ReadPolicy,
    ) -> Result<Option<LinkedRead<'l>>> {
        match self {
            Self::Owner { cache, .. } => {
                let Ok(file) = cache.resolve(path, Some(e_tag), size) else {
                    return Ok(None);
                };
                let (read, work) = cache.start_read(&file, range.clone(), intent, reads)?;
                Ok(Some(LinkedRead::Owner(OwnerRead {
                    cache,
                    reads,
                    file,
                    range,
                    intent,
                    read,
                    work: Some(work),
                })))
            }
            Self::Client(owner) => {
                let read = ClientRead::start(owner, path, e_tag, size, range, intent).await?;
                Ok(read.map(LinkedRead::Client))
            }
        }
    }
}

/// A read through a [`StorageLink`]: the owner's own, its work on the
/// tiers in this process, or a client's, through its owner.
// One lives in each read's future: boxing the owner's would cost every
// read of the owner's an allocation, to save a client's read some bytes.
#[allow(clippy::large_enum_variant)]
pub(super) enum LinkedRead<'l> {
    Owner(OwnerRead<'l>),
    Client(ClientRead<'l>),
}

impl<'l> LinkedRead<'l> {
    /// The read's file, the version of it the cache named.
    pub(super) fn file(&self) -> CacheableFile<'l> {
        match self {
            Self::Owner(read) => read.file,
            Self::Client(read) => read.file(),
        }
    }

    /// The read's next run of adjacent blocks the tiers don't all hold,
    /// located, for the caller's read policy to plan the GETs of
    /// ([`Self::take_gets`]); `None` once every run is planned.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted.
    pub(super) fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        match self {
            Self::Owner(read) => read.locate_next_run(),
            Self::Client(read) => Ok(read.locate_next_run()),
        }
    }

    /// Takes `gets`, planned for the run located last; or the default's, if
    /// the cache can't carry them out.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    pub(super) async fn take_gets(&mut self, gets: &ReadPlan) -> Result<()> {
        match self {
            Self::Owner(read) => {
                read.take_gets(gets);
                Ok(())
            }
            Self::Client(read) => read.take_gets(gets).await,
        }
    }

    /// The fetches of the GETs taken, for the caller to make, once every
    /// run is planned.
    ///
    /// # Errors
    ///
    /// If the read's output can't be lent to a fetch.
    pub(super) fn fetches(&mut self) -> Result<Vec<LinkedFetch<'l>>> {
        match self {
            Self::Owner(read) => read.fetches(),
            Self::Client(read) => Ok(read
                .fetches()
                .into_iter()
                .map(LinkedFetch::Client)
                .collect()),
        }
    }

    /// Waits for the read's work on the tiers, its fetches made beside it:
    /// whether blocks are left to fetch, those it waited for that no fetch
    /// cached, located again ([`Self::locate_next_run`]).
    ///
    /// # Errors
    ///
    /// If a read from the cache file failed, or the owner failed the read or
    /// is gone.
    pub(super) async fn finish(&mut self) -> Result<bool> {
        match self {
            Self::Owner(read) => read.finish().await,
            Self::Client(read) => read.finish().await,
        }
    }

    /// The read's bytes, once finished with no block left to fetch: `None`
    /// if a read from disk is still writing them, as after one failed.
    pub(super) fn into_bytes(self) -> Option<Bytes> {
        match self {
            Self::Owner(read) => read.read.into_bytes(),
            Self::Client(read) => read.into_bytes(),
        }
    }
}

/// The owner's own read: its output, and its work on the tiers, made again
/// for the blocks left to fetch once finished.
pub(super) struct OwnerRead<'l> {
    cache: &'l ObjectCache,
    reads: &'l dyn ReadPolicy,
    file: CacheableFile<'l>,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    read: PartialRead,
    /// Taken while it finishes.
    work: Option<TierWork<'l>>,
}

impl<'l> OwnerRead<'l> {
    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        self.work
            .as_mut()
            .expect("a read's work lives till it is finished")
            .locate_next_run(&mut self.read)
    }

    fn take_gets(&mut self, gets: &ReadPlan) {
        self.work
            .as_mut()
            .expect("a read's work lives till it is finished")
            .take_gets(&mut self.read, gets);
    }

    fn fetches(&mut self) -> Result<Vec<LinkedFetch<'l>>> {
        let cache = self.cache;
        let fetches = self.read.fetches();
        fetches
            .into_iter()
            .map(|fetch| Ok(LinkedFetch::Owner(cache.receive(&mut self.read, fetch)?)))
            .collect()
    }

    async fn finish(&mut self) -> Result<bool> {
        let work = self
            .work
            .take()
            .expect("a read's work lives till it is finished");
        let left = work.finish(&mut self.read).await?;
        if left.is_empty() {
            return Ok(false);
        }
        // Blocks the read waited for that no fetch cached: it fetches them
        // itself, or waits again for a read that fetches them now.
        let (file, range) = (&self.file, self.range.clone());
        let work = self.cache.retry(file, range, self.intent, left, self.reads);
        self.work = Some(work);
        Ok(true)
    }
}

/// A fetch of a [`LinkedRead`]'s: its bytes landed in the owner's cache as
/// they come in, directly or through the owner.
pub(super) enum LinkedFetch<'l> {
    Owner(FetchedBlocks<'l>),
    Client(ClientFetch<'l>),
}

impl LinkedFetch<'_> {
    /// The file's bytes the fetch holds.
    pub(super) fn range(&self) -> Range<u64> {
        match self {
            Self::Owner(blocks) => blocks.range(),
            Self::Client(fetch) => fetch.range(),
        }
    }

    /// The version of the file they are of.
    pub(super) fn file(&self) -> FileVersionId {
        match self {
            Self::Owner(blocks) => blocks.file(),
            Self::Client(fetch) => fetch.file(),
        }
    }

    /// How many identical requests race for them.
    pub(super) fn race(&self) -> NonZeroU8 {
        match self {
            Self::Owner(blocks) => blocks.race(),
            Self::Client(fetch) => fetch.race(),
        }
    }

    /// When one more request races them, if they aren't all in.
    pub(super) fn late_race(&self) -> Option<Duration> {
        match self {
            Self::Owner(blocks) => blocks.late_race(),
            Self::Client(fetch) => fetch.late_race(),
        }
    }

    /// Takes `piece`, the fetch's next bytes.
    ///
    /// # Errors
    ///
    /// If the fetch brings more bytes than it holds.
    pub(super) async fn land(&mut self, piece: &[u8]) -> Result<()> {
        match self {
            Self::Owner(blocks) => blocks.land(piece).await,
            Self::Client(fetch) => {
                fetch.land(piece);
                Ok(())
            }
        }
    }

    /// Caches the blocks where they landed, once every byte is in.
    ///
    /// # Errors
    ///
    /// If the fetch brought fewer bytes than it holds: nothing is cached.
    pub(super) async fn finish(self) -> Result<()> {
        match self {
            Self::Owner(blocks) => blocks.finish().await,
            Self::Client(fetch) => fetch.finish().await,
        }
    }
}
