use std::ops::Range;

use bytes::Bytes;
use futures_util::stream::TryStreamExt;
use object_store::{Extensions, GetOptions, GetRange, ObjectMeta, ObjectStore};

use super::T4Caching;
use crate::cache::network::Network;
use crate::cache::policy::ReadIntent;
use crate::cache::read_output::ReadOutput;
use crate::io::sync::Arc;

/// A remote file opened through a [`T4Caching`]. Its reads are of the version opened: a read of a
/// file changed since fails rather than mix two versions.
#[derive(Clone, Debug)]
pub struct FileHandle {
    store: T4Caching,
    meta: Arc<ObjectMeta>,
}

impl FileHandle {
    pub(super) fn new(store: T4Caching, meta: Arc<ObjectMeta>) -> Self {
        Self { store, meta }
    }

    pub fn meta(&self) -> &ObjectMeta {
        &self.meta
    }

    /// `range` of the file, clamped to its end. The bytes' address agrees with their file offset
    /// mod 4 KiB.
    pub async fn read(
        &self,
        range: Range<u64>,
        intent: Option<ReadIntent>,
    ) -> object_store::Result<Bytes> {
        let range = T4Caching::bytes_of(&GetRange::Bounded(range), self.meta.size)?;
        let mut extensions = Extensions::new();
        if let Some(intent) = intent {
            extensions.insert(intent);
        }
        let read = self.read_cached(range.clone(), &extensions).await?;
        match read {
            Some(bytes) => Ok(bytes),
            None => self.read_from_inner(range).await,
        }
    }

    pub(super) async fn read_cached(
        &self,
        range: Range<u64>,
        extensions: &Extensions,
    ) -> object_store::Result<Option<Bytes>> {
        let Some(e_tag) = self.meta.e_tag.as_deref() else {
            return Ok(None);
        };
        let (location, size) = (&self.meta.location, self.meta.size);
        self.store
            .read_through(location, e_tag, size, range, extensions)
            .await
    }

    /// Copied into an output so the address agrees with the file offset, as a cache read's does.
    async fn read_from_inner(&self, range: Range<u64>) -> object_store::Result<Bytes> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range.clone())),
            if_match: self.meta.e_tag.clone(),
            ..GetOptions::default()
        };
        let answer = self
            .store
            .inner
            .get_opts(&self.meta.location, options)
            .await?;
        let mut output = ReadOutput::new(range.clone()).map_err(Network::cache_error)?;
        let mut body = answer.into_stream();
        let mut at = range.start;
        while let Some(piece) = body.try_next().await? {
            if at + piece.len() as u64 > range.end {
                return Err(Self::body_not_asked(&range));
            }
            output.copy_in(at, &piece);
            at += piece.len() as u64;
        }
        if at != range.end {
            return Err(Self::body_not_asked(&range));
        }
        Ok(output
            .into_bytes()
            .expect("an output that lent no buffer is its caller's once filled"))
    }

    fn body_not_asked(range: &Range<u64>) -> object_store::Error {
        object_store::Error::Generic {
            store: Network::STORE,
            source: format!("the inner store's body for {range:?} held other bytes").into(),
        }
    }
}
