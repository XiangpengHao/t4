//! [`StorageLink`]: how a cache's fetch half reaches its storage half
//! (`doc/11-shared-mode.md`, decision 2): in the process that owns the
//! cache, directly; in a client, through its owner, over a socket.

use super::object_cache::ObjectCache;
use super::sharing::{OwnerConnection, OwnerListener};
use crate::io::sync::Arc;

pub(super) enum StorageLink {
    /// This process holds the cache files' lock: it runs the storage half,
    /// and serves it to clients if it was built for sharing, as long as the
    /// listener lives. The listener goes first as it drops, and with it its
    /// sessions' handles on the cache.
    Owner {
        _listener: Option<OwnerListener>,
        cache: Arc<ObjectCache>,
    },
    /// Another process owns the cache, and this one reads through it; until
    /// the socket serves reads, a client's go to the remote.
    Client(
        #[allow(dead_code)] // Serves the client's reads: the next commit.
        OwnerConnection,
    ),
}

impl StorageLink {
    /// The storage half, if this process runs it.
    pub(super) fn owned(&self) -> Option<&ObjectCache> {
        match self {
            Self::Owner { cache, .. } => Some(cache),
            Self::Client(_) => None,
        }
    }
}
