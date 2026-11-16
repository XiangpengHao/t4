//! Each process fetches for itself, with its own credentials, whoever owns the cache.

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

/// The caller's runtime's timer: t4 has no runtime of its own.
pub(super) type Sleep = Arc<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone)]
pub(super) struct Network {
    clock: crate::io::time::Clock,
    inner: Arc<dyn ObjectStore>,
    reads: Arc<dyn ReadPolicy>,
    gets: Arc<GetQueue>,
    sleep: Option<Sleep>,
}

impl Network {
    pub(super) const STORE: &str = "t4";

    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        reads: Arc<dyn ReadPolicy>,
        bytes_in_flight: NonZeroU64,
        merged_up_to: u64,
        sleep: Option<Sleep>,
        clock: crate::io::time::Clock,
    ) -> Self {
        Self {
            clock,
            inner,
            reads,
            gets: Arc::new(GetQueue::new(bytes_in_flight, merged_up_to)),
            sleep,
        }
    }

    pub(super) fn read_policy(&self) -> &dyn ReadPolicy {
        &*self.reads
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn gets_waiting(&self) -> usize {
        self.gets.waiting()
    }

    /// Takes each run's GETs before locating the next: they claim the blocks they widen to.
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

    pub(super) fn cache_error(error: crate::io::error::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: Self::STORE,
            source: Box::new(error),
        }
    }
}
