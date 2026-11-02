//! The cache's network half (`doc/11-shared-mode.md`, decision 2, the
//! fetch half): everything that talks to the remote store for the cache.
//! Each process runs its own, with its own credentials, whoever owns the
//! cache.
//!
//! - [`Network`]: the remote store, the read policy, the GET queue and the
//!   timer; a read's GETs planned, run by run, as the cache locates them.
//! - `get`: one GET, its turn in the queue, its racers, and its body landed
//!   in the cache as it comes in.
//! - `get_queue`: the cap on bytes in flight, and the waiting GETs merged or
//!   joined.

use std::num::NonZeroU64;
use std::time::Duration;

use futures_util::future::BoxFuture;
use object_store::ObjectStore;

use super::cache_stores::CacheStoreRead;
use super::policy::{ReadIntent, ReadPolicy};
use crate::io::sync::Arc;

mod get;
mod get_queue;

use get_queue::GetQueue;

/// A timer: a future done a while from now, the caller's runtime's, as t4
/// has none of its own.
pub(super) type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

/// The cache's network half: the remote store it fetches from, the read
/// policy that plans its GETs and hears of its I/O, the GETs in flight and
/// those waiting for room, and a timer for late racers. A clone shares them.
#[derive(Clone)]
pub(super) struct Network {
    /// The store the cache fetches what only the remote holds from.
    inner: Arc<dyn ObjectStore>,
    /// What plans each read's GETs, as the cache locates its blocks, and
    /// hears of its I/O: its GETs, and its reads from disk, which the
    /// cache reports to it as they go.
    reads: Arc<dyn ReadPolicy>,
    /// The GETs to the inner store in flight, and those waiting for room.
    gets: Arc<GetQueue>,
    /// What a GET's late racer waits on; none sent without it.
    sleep: Option<Sleep>,
}

impl Network {
    /// The name errors give this store.
    pub(super) const STORE: &str = "t4";

    /// The network over `inner`, its GETs planned by `reads`, at most
    /// `bytes_in_flight` of them out at once, the waiting ones merged up to
    /// `merged_up_to` bytes; `sleep`, if any, the timer late racers wait on.
    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        reads: Arc<dyn ReadPolicy>,
        bytes_in_flight: NonZeroU64,
        merged_up_to: u64,
        sleep: Option<Sleep>,
    ) -> Self {
        Self {
            inner,
            reads,
            gets: Arc::new(GetQueue::new(bytes_in_flight, merged_up_to)),
            sleep,
        }
    }

    /// The read policy, which the cache's reads hear their I/O through.
    pub(super) fn read_policy(&self) -> &dyn ReadPolicy {
        &*self.reads
    }

    /// Plans `read`'s GETs, for `intent`, run by run as the cache locates
    /// its blocks: each run's by the read policy, which the cache takes,
    /// claiming the blocks its GETs widen to before it locates the next
    /// run.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted, or the owner failed
    /// the read or went.
    pub(super) async fn plan_gets(
        &self,
        read: &mut dyn CacheStoreRead<'_>,
        intent: Option<ReadIntent>,
    ) -> crate::Result<()> {
        while let Some(run) = read.locate_next_run()? {
            let gets = run.planned_by(&*self.reads, &read.file(), intent);
            read.take_gets(&gets).await?;
        }
        Ok(())
    }

    /// A failure of the cache's, as the store's error.
    pub(super) fn cache_error(error: crate::io::error::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: Self::STORE,
            source: Box::new(error),
        }
    }
}
