//! [`CurrentLink`]: the link a cache reads through now, and [`LinkMaker`],
//! what makes it: the cache files' lock taken, or the owner's connection
//! made (`doc/11-shared-mode.md`, decisions 1 and 7). A client whose owner
//! went makes a new link at a read: owning the cache if it takes the lock,
//! else a client of whoever took it.

use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError, RwLock, TryLockError};
use std::time::{Duration, Instant};

use super::StorageLink;
use crate::cache::cache_files::{CacheFiles, DiskFile};
use crate::cache::object_cache::ObjectCache;
use crate::cache::policy::{ClockPolicy, PlacementPolicy, ReadPolicy, TierSizes};
use crate::cache::sharing::{OwnerConnection, OwnerListener};
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

/// What makes a placement policy, once the tiers' chunk counts are known.
pub(in crate::cache) type PlacementMaker =
    Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy> + Send>;

/// The link a cache reads through now, which all its clones share; and,
/// once a client's owner is gone, what makes the next.
pub(in crate::cache) struct CurrentLink {
    link: RwLock<Arc<StorageLink>>,
    /// Taken by one read at a time, the one that makes a new link; the
    /// others go on with the link they have.
    maker: Mutex<LinkMaker>,
}

impl CurrentLink {
    pub(in crate::cache) fn new(link: StorageLink, maker: LinkMaker) -> Self {
        Self {
            link: RwLock::new(Arc::new(link)),
            maker: Mutex::new(maker),
        }
    }

    /// The link to read through: the current one; or, if it is a client's
    /// whose owner is gone, a new one, if a try of it is due and no other
    /// read is trying (`LinkMaker::relink`). A read that finds no new link
    /// goes on with the dead one, whose calls fail at once: it reads from
    /// the remote.
    pub(in crate::cache) fn get(&self) -> Arc<StorageLink> {
        let link = self.current();
        if !link.owner_gone() {
            return link;
        }
        let mut maker = match self.maker.try_lock() {
            Ok(maker) => maker,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return link,
        };
        // Another read may have made one since.
        let current = self.current();
        if !Arc::ptr_eq(&current, &link) {
            return current;
        }
        let Some(relinked) = maker.relink() else {
            return link;
        };
        let relinked = Arc::new(relinked);
        *self.link.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&relinked);
        relinked
    }

    /// The current link, as it is.
    pub(in crate::cache) fn current(&self) -> Arc<StorageLink> {
        let link = self.link.read().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(&link)
    }
}

/// The placement policy the cache places blocks by, once it owns them.
enum PlacementSource {
    /// A CLOCK for each tier, the default.
    Clock,
    /// The caller's, made once the tiers' chunk counts are known.
    Given(PlacementMaker),
    /// The caller's, made already for a cache that failed to build: no
    /// other policy takes its place.
    Spent,
}

/// What makes a cache's link, as its builder said: its tiers, how its
/// files are read, its placement policy, whether it shares them, and the
/// read policy that hears its owner's reads from disk for it as a client.
pub(in crate::cache) struct LinkMaker {
    memory_chunks: NonZeroU32,
    disk_files: Vec<DiskFile>,
    queue_depth: NonZeroU32,
    direct_io: bool,
    placement: PlacementSource,
    sharing: bool,
    reads: Arc<dyn ReadPolicy>,
    /// When the next try of a new link is due.
    next_try: Instant,
}

impl LinkMaker {
    /// How long a client waits between tries of the lock and its owner.
    const RETRY: Duration = Duration::from_millis(10);

    pub(in crate::cache) fn new(
        memory_chunks: NonZeroU32,
        disk_files: Vec<DiskFile>,
        queue_depth: NonZeroU32,
        direct_io: bool,
        placement: Option<PlacementMaker>,
        sharing: bool,
        reads: Arc<dyn ReadPolicy>,
    ) -> Self {
        Self {
            memory_chunks,
            disk_files,
            queue_depth,
            direct_io,
            placement: placement.map_or(PlacementSource::Clock, PlacementSource::Given),
            sharing,
            reads,
            next_try: Instant::now(),
        }
    }

    /// The link: the cache files' lock taken; else, built for sharing, a
    /// connection to their owner. An owner starting or leaving refuses
    /// connections a while: the lock is tried again, and the owner again,
    /// until `wait` is up.
    ///
    /// # Errors
    ///
    /// If a cache file can't be made, sized and opened, or the memory
    /// mapped. `Error::Busy` if another cache owns a cache file, and either
    /// this one isn't built for sharing or that one's owner didn't admit it
    /// within `wait`.
    pub(in crate::cache) fn link_within(&mut self, wait: Duration) -> Result<StorageLink> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(link) = self.try_once()? {
                return Ok(link);
            }
            if Instant::now() >= deadline {
                return Err(Error::Busy);
            }
            std::thread::sleep(Self::RETRY);
        }
    }

    /// A new link, for a client whose owner went (decision 7): one try of
    /// the lock, then one of its owner, if a try is due; `None` if neither
    /// came, or a try isn't due. A read waits for no owner.
    fn relink(&mut self) -> Option<StorageLink> {
        let now = Instant::now();
        if now < self.next_try {
            return None;
        }
        self.next_try = now + Self::RETRY;
        self.try_once().ok().flatten()
    }

    /// The link, if the lock or the owner came at the first try.
    ///
    /// # Errors
    ///
    /// As [`Self::link_within`]'s, but for an owner that didn't answer.
    fn try_once(&mut self) -> Result<Option<StorageLink>> {
        match CacheFiles::open(&self.disk_files, self.queue_depth, self.direct_io) {
            Ok(disk) => return self.own(disk).map(Some),
            Err(Error::Busy) if self.sharing => {}
            Err(error) => return Err(error),
        }
        let first_file = self.disk_files[0].path();
        let owner = OwnerConnection::connect(first_file, Arc::clone(&self.reads))?;
        Ok(owner.map(StorageLink::Client))
    }

    /// The cache of the memory's chunks and the chunks of `disk`, its
    /// files' lock taken: this process's own, served to clients if built
    /// for sharing.
    ///
    /// # Errors
    ///
    /// If the memory can't be mapped, or the listener started; or if the
    /// caller's placement policy was spent on a cache that failed so.
    fn own(&mut self, disk: CacheFiles) -> Result<StorageLink> {
        let disk_chunks = NonZeroU32::new(disk.chunks().chunk_count())
            .expect("a disk file holds a chunk at least");
        let sizes = TierSizes::new(self.memory_chunks, disk_chunks);
        let placement = match std::mem::replace(&mut self.placement, PlacementSource::Spent) {
            PlacementSource::Clock => {
                self.placement = PlacementSource::Clock;
                Arc::new(ClockPolicy::new(self.memory_chunks, disk_chunks))
            }
            PlacementSource::Given(build) => build(sizes),
            PlacementSource::Spent => {
                return Err(Error::InvalidArgument(
                    "the placement policy was spent on a cache that failed to build",
                ));
            }
        };
        let first_file = disk.first_file();
        let cache = Arc::new(ObjectCache::new(self.memory_chunks, disk, placement)?);
        let listener = match first_file {
            Some(first_file) if self.sharing => {
                Some(OwnerListener::listen(Arc::clone(&cache), first_file)?)
            }
            _ => None,
        };
        Ok(StorageLink::Owner {
            _listener: listener,
            cache,
        })
    }
}
