//! [`SharedCacheStore`]: shared mode, one cache read by several processes
//! (`doc/11-shared-mode.md`). The process that holds the cache files' lock
//! owns the cache: it reads through its own in-process store, and serves
//! the others, its clients, over a Unix socket. A client reads through its
//! owner; once that owner goes, a client's read tries the lock, then the
//! next owner (decision 7). All of it is the store's own: a read through it
//! is a read, whichever this process is.
//!
//! - `owner`: an owner serving its clients.
//! - `client`: a client's connection to its owner, and its reads through it.
//! - `protocol`: what the two speak: the frames, the socket they cross, the
//!   owner's address, the handshake, and the client's region.

use std::ops::Range;
use std::sync::{Mutex, OnceLock, PoisonError, RwLock, TryLockError};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;

use super::{CacheStore, CacheStoreRead, InProcessCacheStore, TierSettings};
use crate::cache::policy::{ReadIntent, ReadPolicy};
#[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
use crate::cache::storage::ObjectCache;
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

mod client;
mod owner;
mod protocol;

use client::ClientRead;
pub(super) use client::OwnerConnection;
use owner::OwnerListener;

/// Shared mode's cache store: this process's own once it holds the cache
/// files' lock, served to its clients; until then its owner's, through a
/// connection to it, or to the next owner once that one goes.
pub(in crate::cache) struct SharedCacheStore {
    /// Set once this process takes the lock: it owns the cache from then
    /// on, for good, as an owner never goes back to being a client.
    owned: OnceLock<OwnedCache>,
    /// Until then, the connection to the owner: replaced by one to the next
    /// once it goes, and dropped once this process owns the cache. Reads
    /// in flight hold their own.
    client: RwLock<Option<std::sync::Arc<OwnerConnection>>>,
    /// Taken by one read at a time, the one that tries the lock and the
    /// next owner; the others go on with the connection they have.
    hand_off: Mutex<HandOff>,
}

/// The cache this process owns: its in-process store, and the listener
/// that serves it to clients, if it could listen.
struct OwnedCache {
    /// Goes first as it drops, and with it its sessions' handles on the
    /// cache.
    _listener: Option<OwnerListener>,
    store: InProcessCacheStore,
}

/// What a client tries once its owner goes: the lock, its tiers made of
/// `tiers`; then the next owner, which tells `reads` of the reads from disk
/// it makes for this process.
struct HandOff {
    tiers: TierSettings,
    reads: Arc<dyn ReadPolicy>,
    /// When the next try is due.
    next_try: Instant,
    /// The connections tried so far: the next one's number, which tells
    /// the version ids its owner gives apart from the last owner's.
    connections: u64,
}

/// What serves a read: this process's own store, or its owner, through
/// the connection to it.
enum Server<'s> {
    Owned(&'s InProcessCacheStore),
    Owner(std::sync::Arc<OwnerConnection>),
}

impl SharedCacheStore {
    /// How long a client waits between tries of the lock and its owner.
    const RETRY: Duration = Duration::from_millis(10);

    /// Shared mode's store of the cache `tiers` are made of: this
    /// process's own if it takes their files' lock; else that of the
    /// process that holds it, which admits this one, and tells `reads` of
    /// the reads from disk it makes for it. An owner starting or leaving
    /// refuses connections a while: the lock is tried again, and the owner
    /// again, until `wait` is up.
    ///
    /// # Errors
    ///
    /// If a cache file can't be made, sized and opened, or the memory
    /// mapped. [`Error::Busy`] if the owner didn't admit this process
    /// within `wait`, or speaks another version of the protocol.
    pub(in crate::cache) fn open(
        tiers: TierSettings,
        reads: Arc<dyn ReadPolicy>,
        wait: Duration,
    ) -> Result<Self> {
        let store = Self {
            owned: OnceLock::new(),
            client: RwLock::new(None),
            hand_off: Mutex::new(HandOff {
                tiers,
                reads,
                next_try: Instant::now(),
                connections: 0,
            }),
        };
        let deadline = Instant::now() + wait;
        let mut hand_off = store.hand_off();
        while !store.try_once(&mut hand_off)? {
            if Instant::now() >= deadline {
                return Err(Error::Busy);
            }
            std::thread::sleep(Self::RETRY);
        }
        drop(hand_off);
        Ok(store)
    }

    /// What serves a read now: this process's own store, once it owns the
    /// cache; else its owner; or, that owner gone, whatever the hand-off
    /// came to (`Self::hand_off_from`).
    fn server(&self) -> Server<'_> {
        match self.current() {
            Server::Owner(owner) if owner.owner_gone() => self.hand_off_from(owner),
            server => server,
        }
    }

    /// What serves a read now, as it is.
    fn current(&self) -> Server<'_> {
        if let Some(owned) = self.owned.get() {
            return Server::Owned(&owned.store);
        }
        let client = self.client.read().unwrap_or_else(PoisonError::into_inner);
        // The cache is owned before the connection drops: a read that finds
        // none finds the cache owned.
        if let Some(owned) = self.owned.get() {
            return Server::Owned(&owned.store);
        }
        let owner = client
            .as_ref()
            .expect("a connection until this process owns the cache");
        Server::Owner(std::sync::Arc::clone(owner))
    }

    /// What serves a read once `gone`, the owner it read through, went: a
    /// try of the lock, then of the next owner, if one is due and no other
    /// read is trying. A read waits for no owner: one that didn't come
    /// leaves the dead connection, whose calls fail at once, and the read
    /// goes to the remote.
    fn hand_off_from(&self, gone: std::sync::Arc<OwnerConnection>) -> Server<'_> {
        let mut hand_off = match self.hand_off.try_lock() {
            Ok(hand_off) => hand_off,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return Server::Owner(gone),
        };
        // Another read may have handed off since.
        match self.current() {
            Server::Owner(owner) if std::sync::Arc::ptr_eq(&owner, &gone) => {}
            server => return server,
        }
        let now = Instant::now();
        if now < hand_off.next_try {
            return Server::Owner(gone);
        }
        hand_off.next_try = now + Self::RETRY;
        if let Err(_) | Ok(false) = self.try_once(&mut hand_off) {
            return Server::Owner(gone);
        }
        self.current()
    }

    /// One try of the lock, then of its owner: whether either came.
    ///
    /// # Errors
    ///
    /// As [`Self::open`]'s, but for an owner that didn't answer.
    fn try_once(&self, hand_off: &mut HandOff) -> Result<bool> {
        match InProcessCacheStore::open(&mut hand_off.tiers) {
            Ok(store) => {
                self.own(store);
                return Ok(true);
            }
            Err(Error::Busy) => {}
            Err(error) => return Err(error),
        }
        hand_off.connections += 1;
        let first_file = hand_off.tiers.first_file();
        let reads = Arc::clone(&hand_off.reads);
        let Some(owner) = OwnerConnection::connect(first_file, reads, hand_off.connections)? else {
            return Ok(false);
        };
        *self.client.write().unwrap_or_else(PoisonError::into_inner) =
            Some(std::sync::Arc::new(owner));
        Ok(true)
    }

    /// Owns the cache through `store`: served to clients if this process
    /// can listen, and the connection to the last owner, if any, dropped.
    fn own(&self, store: InProcessCacheStore) {
        let cache = store.cache();
        // An owner that can't listen owns its cache unshared, its address
        // held by another process: one of another user, as any may hold an
        // abstract name on Linux, or an owner of a deleted cache file whose
        // inode this one's took (decision 6).
        let listener = cache
            .first_file()
            .and_then(|first_file| OwnerListener::listen(Arc::clone(cache), first_file).ok());
        let owned = OwnedCache {
            _listener: listener,
            store,
        };
        if self.owned.set(owned).is_err() {
            unreachable!("a process takes the lock once, and owns the cache for good");
        }
        *self.client.write().unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn hand_off(&self) -> std::sync::MutexGuard<'_, HandOff> {
        self.hand_off.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl CacheStore for SharedCacheStore {
    /// A read through this process's own store, once it owns the cache;
    /// else through its owner, which hears its reads from disk itself, and
    /// tells the read policy the connection holds of them: `reads` goes
    /// unused then.
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>> {
        match self.server() {
            Server::Owned(store) => store.start_read(path, e_tag, size, range, intent, reads),
            Server::Owner(owner) => Box::pin(async move {
                let read = ClientRead::start(owner, path, e_tag, size, range, intent).await?;
                Ok(read.map(|read| Box::new(read) as Box<dyn CacheStoreRead<'s> + 's>))
            }),
        }
    }

    /// This process's own store's answer, once it owns the cache; else its
    /// owner's.
    fn known_size<'s>(&'s self, path: &'s [u8]) -> BoxFuture<'s, Option<u64>> {
        match self.server() {
            Server::Owned(store) => store.known_size(path),
            Server::Owner(owner) => Box::pin(async move { owner.known_size(path).await.ok()? }),
        }
    }

    #[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
    fn owned(&self) -> Option<&ObjectCache> {
        self.owned.get().map(|owned| &**owned.store.cache())
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    fn owner_connection(&self) -> Option<std::sync::Arc<OwnerConnection>> {
        let client = self.client.read().unwrap_or_else(PoisonError::into_inner);
        client.as_ref().map(std::sync::Arc::clone)
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
