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

#[cfg(feature = "__fuzz")]
pub use protocol::FuzzedFrame;

use client::ClientRead;
pub(super) use client::OwnerConnection;
use owner::OwnerListener;

pub(in crate::cache) struct SharedCacheStore {
    owned: OnceLock<OwnedCache>,
    client: RwLock<Option<std::sync::Arc<OwnerConnection>>>,
    hand_off: Mutex<HandOff>,
}

struct OwnedCache {
    /// Drops first, and with it its sessions' handles on the cache.
    _listener: Option<OwnerListener>,
    store: InProcessCacheStore,
}

struct HandOff {
    tiers: TierSettings,
    reads: Arc<dyn ReadPolicy>,
    next_try: Instant,
    connections: u64,
    shut_down: bool,
}

enum Server<'s> {
    Owned(&'s InProcessCacheStore),
    Owner(std::sync::Arc<OwnerConnection>),
}

impl SharedCacheStore {
    const RETRY: Duration = Duration::from_millis(10);

    /// An owner starting or leaving refuses connections a while: both are tried until `wait`
    /// is up.
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
                shut_down: false,
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

    fn server(&self) -> Server<'_> {
        match self.current() {
            Server::Owner(owner) if owner.owner_gone() => self.hand_off_from(owner),
            server => server,
        }
    }

    fn current(&self) -> Server<'_> {
        if let Some(owned) = self.owned.get() {
            return Server::Owned(&owned.store);
        }
        let client = self.client.read().unwrap_or_else(PoisonError::into_inner);
        // The cache is owned before the connection drops: a read that finds none finds it owned.
        if let Some(owned) = self.owned.get() {
            return Server::Owned(&owned.store);
        }
        let owner = client
            .as_ref()
            .expect("a connection until this process owns the cache");
        Server::Owner(std::sync::Arc::clone(owner))
    }

    /// A read waits for no owner: the dead connection's calls fail at once, sending it remote.
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

    fn try_once(&self, hand_off: &mut HandOff) -> Result<bool> {
        if !hand_off.shut_down {
            match InProcessCacheStore::open(&mut hand_off.tiers) {
                Ok(store) => {
                    self.own(store);
                    return Ok(true);
                }
                Err(Error::Busy) => {}
                Err(error) => return Err(error),
            }
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

    fn own(&self, store: InProcessCacheStore) {
        let cache = store.cache();
        // Can't listen: another process holds the address, so own the cache unshared
        // (`doc/11-shared-mode.md`, decision 6).
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
    /// Through an owner, `reads` goes unused: the owner tells the connection's read policy.
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

    /// Set under the hand-off's lock, which a process takes the cache under: a hand-off is done
    /// before it or never comes.
    fn shut_down(&self) -> Result<()> {
        self.hand_off().shut_down = true;
        match self.owned.get() {
            Some(owned) => owned.store.shut_down(),
            None => Ok(()),
        }
    }

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
