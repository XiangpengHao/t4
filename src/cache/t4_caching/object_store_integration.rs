//! `T4Caching` as an [`ObjectStore`]: an integration of t4's API
//! (`doc/09-vortex-integration.md`, decision 2). A read opens its file
//! ([`T4Caching::open`]) and reads its range through the cache.
//!
//! - **Writes,** puts, copies, renames and deletes, go to the inner store,
//!   and the cache forgets the paths they change: the next read asks again.
//!   It forgets them once the write is done, has failed, or was given up
//!   on, its future dropped: a write given up on may have landed anyway.
//! - **Reads the cache can't serve** go to the inner store as they are:
//!   conditional ones but `If-Match` on the file's e_tag, reads of a named
//!   version, of a file with no e_tag, those a read of the cache file
//!   fails, and those whose fetch finds the file changed.

use std::ops::Range;
use std::sync::PoisonError;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::ready;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    UploadPart,
};

use super::{KnownFiles, T4Caching};
use crate::io::sync::{Arc, Mutex, MutexGuard};

impl T4Caching {
    /// Whether the cache may serve a read with `options`: no condition on
    /// the file but `If-Match`, which it checks, and no version named.
    fn serves(options: &GetOptions) -> bool {
        options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none()
    }

    /// A get's answer: `bytes`, the file's `range`.
    fn answer(meta: &ObjectMeta, range: Range<u64>, bytes: Bytes) -> GetResult {
        GetResult {
            payload: GetResultPayload::Stream(stream::once(ready(Ok(bytes))).boxed()),
            meta: meta.clone(),
            range,
            attributes: Attributes::default(),
        }
    }
}

#[async_trait]
impl ObjectStore for T4Caching {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _forget = ForgetOnDrop::new(&self.known, [location]);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(ForgettingUpload {
            upload,
            known: Arc::clone(&self.known),
            location: location.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !Self::serves(&options) {
            return self.inner.get_opts(location, options).await;
        }
        let file = self.open(location).await?;
        let meta = file.meta();
        if options.head {
            return Ok(Self::answer(meta, 0..0, Bytes::new()));
        }
        let (Some(_), true) = (
            meta.e_tag.as_deref(),
            options.if_match.is_none() || options.if_match == meta.e_tag,
        ) else {
            return self.inner.get_opts(location, options).await;
        };
        let range = match &options.range {
            Some(range) => Self::bytes_of(range, meta.size)?,
            None => 0..meta.size,
        };
        let read = file.read_cached(range.clone(), &options.extensions).await?;
        match read {
            Some(bytes) => Ok(Self::answer(meta, range, bytes)),
            None => self.inner.get_opts(location, options).await,
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        // Each location the inner store takes, forgotten as its delete is
        // done, and when this stream drops, done or given up on.
        let forget = Arc::new(ForgetOnDrop::new(&self.known, []));
        let taken = Arc::clone(&forget);
        let locations = locations
            .inspect(move |location| {
                if let Ok(location) = location {
                    taken.add(location);
                }
            })
            .boxed();
        self.inner
            .delete_stream(locations)
            .map(move |deleted| {
                if let Ok(location) = &deleted {
                    forget.known.forget(location);
                }
                deleted
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.known.remembering(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.known
            .remembering(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let listed = self.inner.list_with_delimiter(prefix).await?;
        for meta in &listed.objects {
            self.known.remember(&meta.location, Arc::new(meta.clone()));
        }
        Ok(listed)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let _forget = ForgetOnDrop::new(&self.known, [to]);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let _forget = ForgetOnDrop::new(&self.known, [from, to]);
        self.inner.rename_opts(from, to, options).await
    }
}

/// The paths a write through the cache changes, which it forgets when this
/// drops: once the write is done, has failed, or was given up on, as a
/// write given up on may have landed all the same.
struct ForgetOnDrop {
    known: Arc<KnownFiles>,
    paths: Mutex<Vec<Path>>,
}

impl ForgetOnDrop {
    fn new<'p>(known: &Arc<KnownFiles>, paths: impl IntoIterator<Item = &'p Path>) -> Self {
        Self {
            known: Arc::clone(known),
            paths: Mutex::new(paths.into_iter().cloned().collect()),
        }
    }

    /// One more path the write changes.
    fn add(&self, path: &Path) {
        self.paths().push(path.clone());
    }

    fn paths(&self) -> MutexGuard<'_, Vec<Path>> {
        // A push can't leave the list half made: a panic leaves it whole.
        self.paths.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for ForgetOnDrop {
    fn drop(&mut self) {
        for path in self.paths().iter() {
            self.known.forget(path);
        }
    }
}

/// An upload through the cache, which forgets its path once done.
#[derive(Debug)]
struct ForgettingUpload {
    upload: Box<dyn MultipartUpload>,
    known: Arc<KnownFiles>,
    location: Path,
}

#[async_trait]
impl MultipartUpload for ForgettingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let _forget = ForgetOnDrop::new(&self.known, [&self.location]);
        self.upload.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.upload.abort().await
    }
}
