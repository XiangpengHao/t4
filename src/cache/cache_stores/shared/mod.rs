//! [`SharedCacheStore`]: the cache store of a process another one owns the
//! cache for (`doc/11-shared-mode.md`, decisions 5 to 7): its storage half
//! runs in that owner, reached over a socket; this process plans and makes
//! its GETs.
//!
//! - `connection`: the connection to the owner, its calls and their answers.
//! - `read`: a read through the owner.
//! - `fetch`: one of a read's fetches, its bytes handed to the owner.

use std::ops::Range;
use std::path::Path;

use futures_util::future::BoxFuture;

use super::{CacheStore, CacheStoreRead};
use crate::cache::policy::{ReadIntent, ReadPolicy};
use crate::io::error::Result;
use crate::io::sync::Arc;

mod connection;
mod fetch;
mod read;

pub(super) use connection::OwnerConnection;
use read::ClientRead;

/// The cache store of the process that owns the cache, read through the
/// connection to it, until it goes.
pub(super) struct SharedCacheStore {
    owner: OwnerConnection,
}

impl SharedCacheStore {
    /// The store of the owner of the cache whose first cache file is at
    /// `first_file`, connected to and admitted, the reads from disk the
    /// owner makes for this process's reads told to `reads`; `None` if no
    /// owner answered, or one closed the connection first.
    ///
    /// # Errors
    ///
    /// As [`OwnerConnection::connect`]'s.
    pub(super) fn connect(first_file: &Path, reads: Arc<dyn ReadPolicy>) -> Result<Option<Self>> {
        let owner = OwnerConnection::connect(first_file, reads)?;
        Ok(owner.map(|owner| Self { owner }))
    }
}

impl CacheStore for SharedCacheStore {
    fn owner_gone(&self) -> bool {
        self.owner.owner_gone()
    }

    /// The owner hears its reads from disk itself, and the connection tells
    /// the read policy it holds of them: `_reads` goes unused.
    fn start_read<'s>(
        &'s self,
        path: &'s [u8],
        e_tag: &'s str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        _reads: &'s dyn ReadPolicy,
    ) -> BoxFuture<'s, Result<Option<Box<dyn CacheStoreRead<'s> + 's>>>> {
        Box::pin(async move {
            let read = ClientRead::start(&self.owner, path, e_tag, size, range, intent).await?;
            Ok(read.map(|read| Box::new(read) as Box<dyn CacheStoreRead<'s> + 's>))
        })
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    fn owner_connection(&self) -> Option<&OwnerConnection> {
        Some(&self.owner)
    }
}
