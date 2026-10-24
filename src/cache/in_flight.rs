//! [`InFlight`]: the blocks being fetched, so that a read that needs one
//! waits for its fetch rather than sending a request of its own
//! (`doc/06-read-modes.md`, decision 2), unless it races: a racing read may
//! add requests for a block while fewer are in flight for it than its GET
//! races.
//!
//! A read claims each remote block it will fetch and cache ([`FetchClaim`]),
//! before its GETs go out; a block the placement policy discards is never
//! claimed, as no other read could take it from the cache. A read that finds
//! a block claimed waits on its [`BlockFetch`], beside its own GETs, or joins
//! it with requests of its own. The fetch ends when one of its requests
//! caches the block, which the readers waiting then take from where it went;
//! or when every request has ended without, failed, given up on, or the
//! block finding no room, and the readers waiting fetch it themselves.

use std::collections::HashMap;
use std::future::poll_fn;
use std::num::NonZeroU8;
use std::task::{Poll, Waker};

use super::BlockKey;
use super::block_index::Location;
use super::policy::Admit;
use crate::io::sync::{Arc, Mutex, MutexGuard};

/// The blocks being fetched, each with its fetch.
#[derive(Debug, Default)]
pub(super) struct InFlight {
    fetches: Mutex<HashMap<u64, Arc<BlockFetch>>>,
}

impl InFlight {
    /// The fetch of `key`'s block, if one is in flight.
    pub(super) fn get(&self, key: BlockKey) -> Option<Arc<BlockFetch>> {
        Self::live(&self.fetches(), key).cloned()
    }

    /// Claims the fetch of `key`'s block, which goes where `admit` says: the
    /// claim, one request in flight for the block, if none was fetching it;
    /// else the fetch in flight.
    pub(super) fn claim(
        self: &Arc<Self>,
        key: BlockKey,
        admit: Admit,
    ) -> Result<FetchClaim, Arc<BlockFetch>> {
        let mut fetches = self.fetches();
        if let Some(fetch) = Self::live(&fetches, key) {
            return Err(Arc::clone(fetch));
        }
        let fetch = Arc::new(BlockFetch {
            key,
            admit,
            state: Mutex::new(FetchState {
                requests: 1,
                ended: None,
                waiters: Vec::new(),
            }),
        });
        fetches.insert(key.packed(), Arc::clone(&fetch));
        Ok(FetchClaim {
            in_flight: Arc::clone(self),
            fetch,
            requests: 1,
        })
    }

    /// Whether no block is being fetched, as once every read is done.
    #[cfg(any(test, feature = "__fuzz"))]
    pub(super) fn is_empty(&self) -> bool {
        self.fetches().is_empty()
    }

    /// `key`'s fetch in `fetches`, unless it has ended: an ended fetch stays
    /// until its thread removes it, and a read that found it would wait for
    /// nothing, and come back to it, until that thread runs.
    fn live(fetches: &HashMap<u64, Arc<BlockFetch>>, key: BlockKey) -> Option<&Arc<BlockFetch>> {
        fetches
            .get(&key.packed())
            .filter(|fetch| fetch.state().ended.is_none())
    }

    /// Forgets `fetch`, which has ended, if it is still the one in flight
    /// for its block.
    fn remove(&self, fetch: &Arc<BlockFetch>) {
        let mut fetches = self.fetches();
        if fetches
            .get(&fetch.key.packed())
            .is_some_and(|in_flight| Arc::ptr_eq(in_flight, fetch))
        {
            fetches.remove(&fetch.key.packed());
        }
    }

    fn fetches(&self) -> MutexGuard<'_, HashMap<u64, Arc<BlockFetch>>> {
        // Each change is one insert or remove: a panic leaves the map whole.
        self.fetches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One block's fetch: where the block goes, the requests in flight for it,
/// how the fetch ended once it has, and the reads waiting for that.
#[derive(Debug)]
pub(super) struct BlockFetch {
    key: BlockKey,
    admit: Admit,
    state: Mutex<FetchState>,
}

#[derive(Debug)]
struct FetchState {
    /// Requests in flight for the block, over every GET that fetches it, a
    /// GET's racers each counted.
    requests: u32,
    ended: Option<FetchEnd>,
    waiters: Vec<Waker>,
}

/// How a block's fetch ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FetchEnd {
    /// A request cached the block, here.
    Cached(Location),
    /// Every request ended without caching it: a reader waiting fetches it
    /// itself.
    Abandoned,
}

impl BlockFetch {
    /// Where the block goes once fetched, as the placement policy told the
    /// read that claimed it: where a racing read's requests put it too.
    pub(super) fn admit(&self) -> Admit {
        self.admit
    }

    /// The requests in flight for the block.
    pub(super) fn requests(&self) -> u32 {
        self.state().requests
    }

    /// Joins the fetch with `requests` of a racing read's own.
    pub(super) fn join(self: &Arc<Self>, in_flight: &Arc<InFlight>, requests: u32) -> FetchClaim {
        self.state().requests += requests;
        FetchClaim {
            in_flight: Arc::clone(in_flight),
            fetch: Arc::clone(self),
            requests,
        }
    }

    /// How the fetch ended, once it has.
    pub(super) async fn ended(&self) -> FetchEnd {
        poll_fn(|cx| {
            let mut state = self.state();
            if let Some(ended) = state.ended {
                return Poll::Ready(ended);
            }
            if !state
                .waiters
                .iter()
                .any(|waiter| waiter.will_wake(cx.waker()))
            {
                state.waiters.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }

    /// `requests` of the fetch's ended, the block `cached` there or not.
    /// The first to cache it ends the fetch; so does the last to end, if
    /// none did.
    fn end_requests(
        self: &Arc<Self>,
        in_flight: &InFlight,
        requests: u32,
        cached: Option<Location>,
    ) {
        let waiters = {
            let mut state = self.state();
            state.requests -= requests;
            let ended = match cached {
                Some(location) => FetchEnd::Cached(location),
                None if state.requests == 0 => FetchEnd::Abandoned,
                None => return,
            };
            if state.ended.is_some() {
                return;
            }
            state.ended = Some(ended);
            std::mem::take(&mut state.waiters)
        };
        in_flight.remove(self);
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn state(&self) -> MutexGuard<'_, FetchState> {
        // Every change is a count or a field set whole: a panic leaves the
        // state whole.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The requests a read holds in a block's fetch: the claim's, or a racer's.
/// They end when the read's own fetch of the block does ([`Self::ended`]);
/// dropped first, as when that fetch fails or its read is given up on, they
/// end with the block cached nowhere.
#[derive(Debug)]
pub(super) struct FetchClaim {
    in_flight: Arc<InFlight>,
    fetch: Arc<BlockFetch>,
    requests: u32,
}

impl FetchClaim {
    #[cfg(all(test, not(feature = "shuttle")))]
    fn fetch(&self) -> &Arc<BlockFetch> {
        &self.fetch
    }

    /// The claim's requests are the racers of the GET that fetches the
    /// block.
    pub(super) fn fetched_by(&mut self, race: NonZeroU8) {
        let race = u32::from(race.get());
        let mut state = self.fetch.state();
        state.requests = state.requests - self.requests + race;
        self.requests = race;
    }

    /// The read's fetch of the block ended, the block cached at `location`,
    /// by this fetch or another, or nowhere.
    pub(super) fn ended(mut self, location: Option<Location>) {
        self.end(location);
    }

    fn end(&mut self, location: Option<Location>) {
        let requests = std::mem::take(&mut self.requests);
        if requests > 0 {
            self.fetch.end_requests(&self.in_flight, requests, location);
        }
    }
}

impl Drop for FetchClaim {
    fn drop(&mut self) {
        self.end(None);
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use futures_util::FutureExt;

    use super::*;
    use crate::cache::{ChunkId, FileVersionId};

    fn key(index: u32) -> BlockKey {
        BlockKey::new(FileVersionId::new(1), index)
    }

    fn memory(chunk: u32) -> Location {
        Location::Memory(ChunkId::new(chunk))
    }

    #[test]
    fn a_block_is_claimed_once_until_its_fetch_ends() {
        let in_flight = Arc::new(InFlight::default());
        let claim = in_flight.claim(key(0), Admit::Memory).unwrap();
        let fetch = in_flight.claim(key(0), Admit::Disk).unwrap_err();
        assert!(Arc::ptr_eq(&fetch, claim.fetch()));
        assert_eq!(fetch.admit(), Admit::Memory, "the claim's");
        assert!(
            in_flight.claim(key(1), Admit::Memory).is_ok(),
            "another block"
        );

        claim.ended(Some(memory(3)));
        assert_eq!(
            fetch.ended().now_or_never(),
            Some(FetchEnd::Cached(memory(3)))
        );
        assert!(in_flight.get(key(0)).is_none());
        assert!(
            in_flight.claim(key(0), Admit::Memory).is_ok(),
            "fetched anew"
        );
    }

    #[test]
    fn a_waiter_waits_until_the_fetch_ends() {
        let in_flight = Arc::new(InFlight::default());
        let claim = in_flight.claim(key(0), Admit::Memory).unwrap();
        let fetch = in_flight.get(key(0)).unwrap();
        let mut ended = Box::pin(fetch.ended());
        assert_eq!((&mut ended).now_or_never(), None);
        claim.ended(Some(memory(1)));
        assert_eq!(ended.now_or_never(), Some(FetchEnd::Cached(memory(1))));
    }

    /// A claim dropped, as when its read is given up on, abandons the fetch
    /// once no racer is left: its waiters fetch the block themselves.
    #[test]
    fn the_last_request_to_end_without_the_block_abandons_the_fetch() {
        let in_flight = Arc::new(InFlight::default());
        let mut claim = in_flight.claim(key(0), Admit::Memory).unwrap();
        claim.fetched_by(NonZeroU8::new(2).unwrap());
        let fetch = in_flight.get(key(0)).unwrap();
        let racer = fetch.join(&in_flight, 1);
        assert_eq!(fetch.requests(), 3);

        drop(claim);
        assert_eq!(fetch.requests(), 1);
        assert_eq!(fetch.ended().now_or_never(), None, "a racer is in flight");
        racer.ended(None);
        assert_eq!(fetch.ended().now_or_never(), Some(FetchEnd::Abandoned));
        assert!(in_flight.get(key(0)).is_none());
    }

    #[test]
    fn the_first_request_to_cache_the_block_ends_the_fetch() {
        let in_flight = Arc::new(InFlight::default());
        let claim = in_flight.claim(key(0), Admit::Memory).unwrap();
        let fetch = in_flight.get(key(0)).unwrap();
        let racer = fetch.join(&in_flight, 1);
        racer.ended(Some(memory(2)));
        assert_eq!(
            fetch.ended().now_or_never(),
            Some(FetchEnd::Cached(memory(2)))
        );
        claim.ended(Some(memory(2)));
        assert_eq!(fetch.requests(), 0);
    }
}
