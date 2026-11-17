use std::collections::HashMap;
use std::future::poll_fn;
use std::num::NonZeroU8;
use std::task::{Poll, Waker};

use bytes::Bytes;

use super::index::Location;
use crate::cache::BlockKey;
use crate::cache::policy::Admit;
use crate::io::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Default)]
pub(in crate::cache) struct InFlight {
    fetches: Mutex<HashMap<u64, Arc<BlockFetch>>>,
}

impl InFlight {
    #[cfg(test)]
    pub(in crate::cache) fn waiting_reads(&self) -> usize {
        self.fetches()
            .values()
            .map(|fetch| fetch.state().waiters.len())
            .sum()
    }

    pub(in crate::cache) fn get(&self, key: BlockKey) -> Option<Arc<BlockFetch>> {
        Self::live(&self.fetches(), key).cloned()
    }

    pub(in crate::cache) fn claim(
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
                landed: None,
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

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn is_empty(&self) -> bool {
        self.fetches().is_empty()
    }

    /// An ended fetch stays until its thread removes it; a read must not wait on it. A landed one
    /// stays live until its write is done, so that no read fetches its block again meanwhile.
    fn live(fetches: &HashMap<u64, Arc<BlockFetch>>, key: BlockKey) -> Option<&Arc<BlockFetch>> {
        fetches
            .get(&key.packed())
            .filter(|fetch| fetch.state().ended.is_none())
    }

    /// A new fetch may have replaced this ended one.
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

#[derive(Debug)]
pub(in crate::cache) struct BlockFetch {
    key: BlockKey,
    admit: Admit,
    state: Mutex<FetchState>,
}

#[derive(Debug)]
struct FetchState {
    requests: u32,
    ended: Option<FetchEnd>,
    /// A block bound for disk, all in, waiting for its write.
    landed: Option<Bytes>,
    waiters: Vec<Waker>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache) enum FetchEnd {
    Cached(Location),
    Passed(Bytes),
    Abandoned,
}

impl BlockFetch {
    pub(in crate::cache) fn admit(&self) -> Admit {
        self.admit
    }

    pub(in crate::cache) fn requests(&self) -> u32 {
        self.state().requests
    }

    pub(in crate::cache) fn join(
        self: &Arc<Self>,
        in_flight: &Arc<InFlight>,
        requests: u32,
    ) -> FetchClaim {
        self.state().requests += requests;
        FetchClaim {
            in_flight: Arc::clone(in_flight),
            fetch: Arc::clone(self),
            requests,
        }
    }

    pub(in crate::cache) fn landed(&self) -> Option<Bytes> {
        self.state().landed.clone()
    }

    /// A landed block ends a wait at once: its bytes are in, though not yet on disk.
    pub(in crate::cache) async fn ended(&self) -> FetchEnd {
        poll_fn(|cx| {
            let mut state = self.state();
            if let Some(ended) = &state.ended {
                return Poll::Ready(ended.clone());
            }
            if let Some(landed) = &state.landed {
                return Poll::Ready(FetchEnd::Passed(landed.clone()));
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

    fn end_requests(self: &Arc<Self>, in_flight: &InFlight, requests: u32, had: Option<FetchEnd>) {
        let waiters = {
            let mut state = self.state();
            state.requests -= requests;
            let ended = match had {
                Some(had) => had,
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
        // Each change sets a count or a field whole: a panic leaves the state whole.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub(in crate::cache) struct FetchClaim {
    in_flight: Arc<InFlight>,
    fetch: Arc<BlockFetch>,
    requests: u32,
}

impl FetchClaim {
    #[cfg(all(test, feature = "shuttle"))]
    fn fetch(&self) -> &Arc<BlockFetch> {
        &self.fetch
    }

    pub(in crate::cache) fn fetched_by(&mut self, race: NonZeroU8) {
        let race = u32::from(race.get());
        let mut state = self.fetch.state();
        state.requests = state.requests - self.requests + race;
        self.requests = race;
    }

    pub(in crate::cache) fn ended(mut self, location: Option<Location>) {
        self.end(location.map(FetchEnd::Cached));
    }

    pub(in crate::cache) fn passed(mut self, bytes: Bytes) {
        self.end(Some(FetchEnd::Passed(bytes)));
    }

    /// Serves the block's waiting reads, and the reads that find it, from `bytes` until it ends.
    pub(in crate::cache) fn landed(&self, bytes: Bytes) {
        let waiters = {
            let mut state = self.fetch.state();
            if state.ended.is_some() {
                return;
            }
            state.landed = Some(bytes);
            std::mem::take(&mut state.waiters)
        };
        waiters.into_iter().for_each(Waker::wake);
    }

    fn end(&mut self, had: Option<FetchEnd>) {
        let requests = std::mem::take(&mut self.requests);
        if requests > 0 {
            self.fetch.end_requests(&self.in_flight, requests, had);
        }
    }
}

impl Drop for FetchClaim {
    fn drop(&mut self) {
        self.end(None);
    }
}

#[cfg(all(test, feature = "shuttle"))]
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
        crate::io::test_schedule::TestSchedule::once(|| {
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
        });
    }

    #[test]
    fn a_waiter_waits_until_the_fetch_ends() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let in_flight = Arc::new(InFlight::default());
            let claim = in_flight.claim(key(0), Admit::Memory).unwrap();
            let fetch = in_flight.get(key(0)).unwrap();
            let mut ended = Box::pin(fetch.ended());
            assert_eq!((&mut ended).now_or_never(), None);
            claim.ended(Some(memory(1)));
            assert_eq!(ended.now_or_never(), Some(FetchEnd::Cached(memory(1))));
        });
    }

    #[test]
    fn a_discarded_blocks_fetch_passes_its_bytes_to_its_waiters() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let in_flight = Arc::new(InFlight::default());
            let claim = in_flight.claim(key(0), Admit::Discard).unwrap();
            let fetch = in_flight.get(key(0)).unwrap();
            let mut ended = Box::pin(fetch.ended());
            assert_eq!((&mut ended).now_or_never(), None);
            claim.passed(Bytes::from_static(b"the block"));
            let passed = FetchEnd::Passed(Bytes::from_static(b"the block"));
            assert_eq!(ended.now_or_never(), Some(passed.clone()));
            assert_eq!(fetch.ended().now_or_never(), Some(passed), "every waiter");
            assert!(in_flight.get(key(0)).is_none());
        });
    }

    #[test]
    fn the_last_request_to_end_without_the_block_abandons_the_fetch() {
        crate::io::test_schedule::TestSchedule::once(|| {
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
        });
    }

    #[test]
    fn the_first_request_to_cache_the_block_ends_the_fetch() {
        crate::io::test_schedule::TestSchedule::once(|| {
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
        });
    }
}
