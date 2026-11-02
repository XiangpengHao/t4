//! [`CurrentCacheStore`]: the cache store a process reads through now, and
//! [`CacheStoreMaker`], what makes it: the cache files' lock taken, or the
//! owner's connection made (`doc/11-shared-mode.md`, decisions 1 and 7). A
//! client whose owner went makes a new store at a read: its own if it takes
//! the lock, else that of whoever took it.

use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError, RwLock, TryLockError};
use std::time::{Duration, Instant};

use super::{CacheStore, InProcessCacheStore, SharedCacheStore};
use crate::cache::policy::{ClockPolicy, PlacementPolicy, ReadPolicy, TierSizes};
use crate::cache::sharing::{OwnerConnection, OwnerListener};
use crate::cache::storage::ObjectCache;
use crate::cache::storage::{CacheFiles, DiskFile};
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

/// What makes a placement policy, once the tiers' chunk counts are known.
pub(in crate::cache) type PlacementMaker =
    Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy> + Send>;

/// The cache store a cache reads through now, which all its clones share;
/// and, once a client's owner is gone, what makes the next.
pub(in crate::cache) struct CurrentCacheStore {
    store: RwLock<Arc<dyn CacheStore>>,
    /// Taken by one read at a time, the one that makes a new store; the
    /// others go on with the store they have.
    maker: Mutex<CacheStoreMaker>,
}

impl CurrentCacheStore {
    pub(in crate::cache) fn new(store: Arc<dyn CacheStore>, maker: CacheStoreMaker) -> Self {
        Self {
            store: RwLock::new(store),
            maker: Mutex::new(maker),
        }
    }

    /// The store to read through: the current one; or, if its owner is
    /// gone, a new one, if a try of it is due and no other read is trying
    /// (`CacheStoreMaker::remake`). A read that finds no new store goes on
    /// with the dead one, whose calls fail at once: it reads from the
    /// remote.
    pub(in crate::cache) fn get(&self) -> Arc<dyn CacheStore> {
        let store = self.current();
        if !store.owner_gone() {
            return store;
        }
        let mut maker = match self.maker.try_lock() {
            Ok(maker) => maker,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return store,
        };
        // Another read may have made one since.
        let current = self.current();
        if !Arc::ptr_eq(&current, &store) {
            return current;
        }
        let Some(remade) = maker.remake() else {
            return store;
        };
        *self.store.write().unwrap_or_else(PoisonError::into_inner) = Arc::clone(&remade);
        remade
    }

    /// The current store, as it is.
    pub(in crate::cache) fn current(&self) -> Arc<dyn CacheStore> {
        let store = self.store.read().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(&store)
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

/// What makes a cache's store, as its builder said: its tiers, how its
/// files are read, its placement policy, whether it shares them, and the
/// read policy that hears its owner's reads from disk for it as a client.
pub(in crate::cache) struct CacheStoreMaker {
    memory_chunks: NonZeroU32,
    disk_files: Vec<DiskFile>,
    queue_depth: NonZeroU32,
    direct_io: bool,
    placement: PlacementSource,
    sharing: bool,
    reads: Arc<dyn ReadPolicy>,
    /// When the next try of a new store is due.
    next_try: Instant,
}

impl CacheStoreMaker {
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

    /// The store: its own, the cache files' lock taken; else, built for
    /// sharing, its owner's, connected to. An owner starting or leaving refuses
    /// connections a while: the lock is tried again, and the owner again,
    /// until `wait` is up.
    ///
    /// # Errors
    ///
    /// If a cache file can't be made, sized and opened, or the memory
    /// mapped. `Error::Busy` if another cache owns a cache file, and either
    /// this one isn't built for sharing or that one's owner didn't admit it
    /// within `wait`.
    pub(in crate::cache) fn make_within(&mut self, wait: Duration) -> Result<Arc<dyn CacheStore>> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(store) = self.try_once()? {
                return Ok(store);
            }
            if Instant::now() >= deadline {
                return Err(Error::Busy);
            }
            std::thread::sleep(Self::RETRY);
        }
    }

    /// A new store, for a client whose owner went (decision 7): one try of
    /// the lock, then one of its owner, if a try is due; `None` if neither
    /// came, or a try isn't due. A read waits for no owner.
    fn remake(&mut self) -> Option<Arc<dyn CacheStore>> {
        let now = Instant::now();
        if now < self.next_try {
            return None;
        }
        self.next_try = now + Self::RETRY;
        self.try_once().ok().flatten()
    }

    /// The store, if the lock or the owner came at the first try.
    ///
    /// # Errors
    ///
    /// As [`Self::make_within`]'s, but for an owner that didn't answer.
    fn try_once(&mut self) -> Result<Option<Arc<dyn CacheStore>>> {
        match CacheFiles::open(&self.disk_files, self.queue_depth, self.direct_io) {
            Ok(disk) => return self.own(disk).map(Some),
            Err(Error::Busy) if self.sharing => {}
            Err(error) => return Err(error),
        }
        let first_file = self.disk_files[0].path();
        let owner = OwnerConnection::connect(first_file, Arc::clone(&self.reads))?;
        Ok(owner.map(|owner| Arc::new(SharedCacheStore::new(owner)) as Arc<dyn CacheStore>))
    }

    /// The cache of the memory's chunks and the chunks of `disk`, its
    /// files' lock taken: this process's own, served to clients if built
    /// for sharing and it can listen.
    ///
    /// # Errors
    ///
    /// If the memory can't be mapped; or if the caller's placement policy
    /// was spent on a cache that failed so.
    fn own(&mut self, disk: CacheFiles) -> Result<Arc<dyn CacheStore>> {
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
            // An owner that can't listen owns its cache unshared, its
            // address held by another process: one of another user, as any
            // may hold an abstract name on Linux, or an owner of a deleted
            // cache file whose inode this one's took (decision 6).
            Some(first_file) if self.sharing => {
                OwnerListener::listen(Arc::clone(&cache), first_file).ok()
            }
            _ => None,
        };
        Ok(Arc::new(InProcessCacheStore::new(cache, listener)))
    }
}
