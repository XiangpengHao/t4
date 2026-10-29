//! [`CachedFile`]: a file opened for reads through t4
//! (`doc/09-vortex-integration.md`, decision 1), what an integration reads
//! by: t4's `ObjectStore`, or a reader of Vortex's.

use std::ops::Range;

use bytes::Bytes;
use futures_util::stream::TryStreamExt;
use object_store::{Extensions, GetOptions, GetRange, ObjectMeta, ObjectStore};

use super::{STORE, T4Caching};
use crate::cache::policy::ReadIntent;
use crate::cache::read_output::ReadOutput;
use crate::io::sync::Arc;

/// A file opened for reads through a [`T4Caching`] ([`T4Caching::open`],
/// [`T4Caching::open_listed`]): what the inner store said of it, asked
/// once. Its reads are of that version: a read of a file changed since
/// fails rather than mix two versions. A clone shares the cache.
#[derive(Clone, Debug)]
pub struct CachedFile {
    store: T4Caching,
    meta: Arc<ObjectMeta>,
}

impl CachedFile {
    pub(super) fn new(store: T4Caching, meta: Arc<ObjectMeta>) -> Self {
        Self { store, meta }
    }

    /// What the inner store said of the file: its size, its e_tag.
    pub fn meta(&self) -> &ObjectMeta {
        &self.meta
    }

    /// `range` of the file, as a bounded GET takes it: clamped to the
    /// file's end. The cache serves it, from memory, disk and fetches, as
    /// the read policy plans for `intent`. What the cache can't serve, a
    /// file with no e_tag or a read of a cache file that failed, comes from
    /// the inner store, still of the version opened (`If-Match`). Either
    /// way, the answer's address agrees with its file offset mod 4 KiB, as
    /// the cache's read output does (`doc/03-cache.md`, decision 3).
    ///
    /// # Errors
    ///
    /// The range starts past the file's end or ends before it starts; or
    /// the inner store's: the file is gone, or changed since it was opened
    /// (`object_store::Error::Precondition`).
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

    /// `range` of the file, within it, from the cache: the read policy
    /// told the `ReadIntent` `extensions` hold, and the GETs to the inner
    /// store carrying them. `None` if the cache can't serve it: the file
    /// has no e_tag, the cache can't hold it, a read of a cache file failed,
    /// a fetch found the file changed, which the cache forgets then, or the
    /// owner of a client's cache failed the read or went.
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

    /// `range` of the file, within it, from the inner store, of the version
    /// opened if it has an e_tag: its body copied into an output, as a
    /// fetch's lands, so that its address agrees with its file offset.
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
        let mut output = ReadOutput::new(range.clone()).map_err(T4Caching::cache_error)?;
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

    /// The inner store's answer to a GET of `range` held other bytes than
    /// it asked.
    fn body_not_asked(range: &Range<u64>) -> object_store::Error {
        object_store::Error::Generic {
            store: STORE,
            source: format!("the inner store's body for {range:?} held other bytes").into(),
        }
    }
}
