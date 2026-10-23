//! [`T4Caching`]: the cache's front end (`doc/03-cache.md`, decision 10),
//! an [`ObjectStore`] over another, whose reads the cache serves, and which
//! fetches what the cache misses itself. [`T4CachingBuilder`] makes one.
//!
//! - **What a file is,** the inner store says once: its first answer for the
//!   path, a HEAD, which the cache trusts (decision 11). A hit asks the store
//!   nothing. A miss fetches with `If-Match` on that answer's e_tag, so a file
//!   changed since is caught there: the cache forgets the path, and the read
//!   goes to the inner store.
//! - **Writes,** puts, copies, renames and deletes, go to the inner store,
//!   and the cache forgets the paths they change: the next read asks again.
//!   It forgets them once the write is done, has failed, or was given up
//!   on, its future dropped: a write given up on may have landed anyway.
//! - **Reads the cache can't serve** go to the inner store as they are:
//!   conditional ones but `If-Match` on the file's e_tag, reads of a named
//!   version, of a file with no e_tag, and those a read of the cache file
//!   fails.

use std::fmt;
use std::num::NonZeroU32;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::PoisonError;

use async_trait::async_trait;
use bbtree::BBTreeMap;
use bytes::Bytes;
use futures_util::future::{join_all, ready};
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};

#[cfg(feature = "__fuzz")]
use super::ChunkId;
use super::cache_files::{CacheFiles, DiskFile};
use super::object_cache::{CacheableFile, FetchedBlocks, ObjectCache};
#[cfg(feature = "__fuzz")]
use super::policy::Tier;
use super::policy::{CachePolicy, ClockPolicy, TierSizes};
use super::{BLOCK_SIZE_U64, InsertOrAbort};
use crate::io::sync::{Arc, Mutex, MutexGuard};

/// The name errors give this store.
const STORE: &str = "t4";

/// What makes a policy, once the tiers' chunk counts are known.
type PolicyMaker = Box<dyn FnOnce(TierSizes) -> Arc<dyn CachePolicy>>;

/// An [`ObjectStore`] that caches the reads of another, in memory and on
/// disk. [`Self::builder`] makes one.
pub struct T4Caching {
    inner: Arc<dyn ObjectStore>,
    cache: ObjectCache,
    /// Shared with the uploads and deletes in flight, which forget their
    /// paths once done.
    known: Arc<KnownFiles>,
}

/// Makes a [`T4Caching`]: where it caches, how much, and by what policy.
///
/// ```no_run
/// # fn example(s3: std::sync::Arc<dyn object_store::ObjectStore>) -> t4::Result<()> {
/// let store = t4::T4Caching::builder(s3)
///     .with_memory_capacity(64 << 30)
///     .with_disk_file("/mnt/nvme0/t4.cache", 3 << 40)
///     .with_disk_file("/mnt/nvme1/t4.cache", 3 << 40)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct T4CachingBuilder {
    inner: Arc<dyn ObjectStore>,
    memory_capacity: Option<u64>,
    disk_files: Vec<DiskFile>,
    queue_depth: u32,
    direct_io: bool,
    policy: Option<PolicyMaker>,
}

impl T4CachingBuilder {
    /// Memory for cached bytes, in whole 1 MiB blocks, one at least.
    /// Required.
    pub fn with_memory_capacity(mut self, bytes: u64) -> Self {
        self.memory_capacity = Some(bytes);
        self
    }

    /// A cache file at `path`, made if missing, of `capacity` bytes: one a
    /// drive, one at least. t4 stripes its disk tier over them by 1 MiB
    /// chunk, each file holding as many chunks as the smallest has room for
    /// (`doc/04-multi-device.md`). Whatever a file held is nothing to the
    /// cache, which starts cold every time (decision 9).
    pub fn with_disk_file(mut self, path: impl Into<PathBuf>, capacity: u64) -> Self {
        self.disk_files.push(DiskFile::new(path.into(), capacity));
        self
    }

    /// The I/Os to the cache files in flight at most, all of them together;
    /// 64 unless set.
    pub fn with_queue_depth(mut self, queue_depth: u32) -> Self {
        self.queue_depth = queue_depth;
        self
    }

    /// Whether the cache files are read and written past the page cache; so
    /// unless set.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// The policy, which `build` makes for the tiers' chunk counts, once it
    /// knows them; a CLOCK for each tier ([`ClockPolicy`]) unless set.
    pub fn with_policy<P: CachePolicy + 'static>(
        mut self,
        build: impl FnOnce(TierSizes) -> P + 'static,
    ) -> Self {
        self.policy = Some(Box::new(move |sizes| Arc::new(build(sizes))));
        self
    }

    /// The store: its cache files made and sized, its memory mapped, its
    /// policy built.
    ///
    /// # Errors
    ///
    /// If the memory capacity or a disk file wasn't given, the queue depth
    /// is 0, or a cache file can't be made, sized and opened, or the memory
    /// mapped.
    pub fn build(self) -> crate::Result<T4Caching> {
        let memory_chunks =
            Self::chunks(self.memory_capacity, "T4Caching needs a memory capacity")?;
        if self.disk_files.is_empty() {
            return Err(crate::Error::InvalidArgument("T4Caching needs a disk file"));
        }
        let queue_depth = NonZeroU32::new(self.queue_depth)
            .ok_or(crate::Error::InvalidArgument("queue depth must be > 0"))?;
        let disk = CacheFiles::open(&self.disk_files, queue_depth, self.direct_io)?;
        let disk_chunks = NonZeroU32::new(disk.chunks().chunk_count())
            .expect("a disk file holds a chunk at least");
        let sizes = TierSizes::new(memory_chunks, disk_chunks);
        let policy = match self.policy {
            Some(build) => build(sizes),
            None => Arc::new(ClockPolicy::new(memory_chunks, disk_chunks)),
        };
        Ok(T4Caching {
            inner: self.inner,
            cache: ObjectCache::new(memory_chunks, disk, policy)?,
            known: Arc::new(KnownFiles::new()),
        })
    }

    /// The whole blocks `capacity` holds, one at least; `missing` if it
    /// wasn't given.
    fn chunks(capacity: Option<u64>, missing: &'static str) -> crate::Result<NonZeroU32> {
        let capacity = capacity.ok_or(crate::Error::InvalidArgument(missing))?;
        let chunks = (capacity / BLOCK_SIZE_U64).clamp(1, u64::from(u32::MAX));
        Ok(NonZeroU32::new(chunks as u32).expect("clamped to one at least"))
    }
}

impl fmt::Debug for T4CachingBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T4CachingBuilder")
            .field("inner", &self.inner)
            .field("memory_capacity", &self.memory_capacity)
            .field("disk_files", &self.disk_files)
            .field("queue_depth", &self.queue_depth)
            .field("direct_io", &self.direct_io)
            .finish_non_exhaustive()
    }
}

impl T4Caching {
    /// A builder of a cache over `inner`.
    pub fn builder(inner: Arc<dyn ObjectStore>) -> T4CachingBuilder {
        T4CachingBuilder {
            inner,
            memory_capacity: None,
            disk_files: Vec::new(),
            queue_depth: 64,
            direct_io: true,
            policy: None,
        }
    }

    /// What the inner store says of the file at `location`: what it said
    /// first, unless the cache forgot it since.
    async fn meta_of(&self, location: &Path) -> object_store::Result<Arc<ObjectMeta>> {
        if let Some(meta) = self.known.get(location) {
            return Ok(meta);
        }
        let meta = Arc::new(self.inner.head(location).await?);
        Ok(self.known.remember(location, meta))
    }

    /// `range` of `file`, from the cache and fetches of what it misses, each
    /// fetch's bytes put into the read's output and cached as the policy
    /// says as they come in. `None` if the cache can't serve it: a read of
    /// the cache file failed, or a fetch found the file changed, which the
    /// cache forgets then.
    async fn read_through(
        &self,
        location: &Path,
        file: &CacheableFile<'_>,
        e_tag: &str,
        range: Range<u64>,
        extensions: &Extensions,
    ) -> object_store::Result<Option<Bytes>> {
        let Ok(mut read) = self.cache.read(file, range).await else {
            return Ok(None);
        };
        let mut receivers = Vec::new();
        for fetch in read.fetches().to_vec() {
            let Ok(blocks) = self.cache.receive(&mut read, fetch) else {
                return Ok(None);
            };
            receivers.push(blocks);
        }
        // Every fetch is in flight at once: a read's misses keep the NIC busy.
        let fetched = receivers
            .into_iter()
            .map(|blocks| self.fetch(location, e_tag, blocks, extensions));
        for fetched in join_all(fetched).await {
            match fetched {
                Ok(()) => {}
                Err(object_store::Error::Precondition { .. }) => {
                    self.known.forget(location);
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(read.into_bytes())
    }

    /// Fetches the bytes `blocks` holds from the inner store, if the file is
    /// still the version with `e_tag`, and hands each piece of the answer to
    /// `blocks` as it comes in.
    async fn fetch(
        &self,
        location: &Path,
        e_tag: &str,
        mut blocks: FetchedBlocks<'_>,
        extensions: &Extensions,
    ) -> object_store::Result<()> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(blocks.range())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let cache_error = |error: crate::io::error::Error| object_store::Error::Generic {
            store: STORE,
            source: Box::new(error),
        };
        let mut body = self.inner.get_opts(location, options).await?.into_stream();
        while let Some(piece) = body.try_next().await? {
            blocks.land(&piece).await.map_err(cache_error)?;
        }
        blocks.finish().await.map_err(cache_error)
    }

    /// Whether the cache may serve a read with `options`: no condition on
    /// the file but `If-Match`, which it checks, and no version named.
    fn serves(options: &GetOptions) -> bool {
        options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none()
    }

    /// Whether the cache's index and tiers agree, as they must once nothing
    /// is in flight; if so, the chunks that hold a block, each with its tier.
    /// For the fuzz target (`fuzz/`), not part of the API.
    #[cfg(feature = "__fuzz")]
    #[doc(hidden)]
    pub fn check_at_rest(&self) -> Result<Vec<(Tier, ChunkId)>, String> {
        self.cache.check_at_rest()
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
        let meta = self.meta_of(location).await?;
        if options.head {
            return Ok(Self::answer(&meta, 0..0, Bytes::new()));
        }
        let (Some(e_tag), true) = (
            meta.e_tag.as_deref(),
            options.if_match.is_none() || options.if_match == meta.e_tag,
        ) else {
            return self.inner.get_opts(location, options).await;
        };
        let range = match &options.range {
            Some(range) => {
                range
                    .as_range(meta.size)
                    .map_err(|source| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(source),
                    })?
            }
            None => 0..meta.size,
        };
        let path = location.as_ref().as_bytes();
        let Ok(file) = self.cache.resolve(path, Some(e_tag), meta.size) else {
            return self.inner.get_opts(location, options).await;
        };
        let read = self
            .read_through(location, &file, e_tag, range.clone(), &options.extensions)
            .await?;
        match read {
            Some(bytes) => Ok(Self::answer(&meta, range, bytes)),
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
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
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

impl fmt::Display for T4Caching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t4 cache over {}", self.inner)
    }
}

impl fmt::Debug for T4Caching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T4Caching")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// What the inner store said of each file the cache has read, by path.
struct KnownFiles {
    metas: BBTreeMap<Box<[u8]>, Arc<ObjectMeta>>,
}

impl KnownFiles {
    fn new() -> Self {
        Self {
            metas: BBTreeMap::new(),
        }
    }

    fn get(&self, location: &Path) -> Option<Arc<ObjectMeta>> {
        self.metas
            .read(Self::key(location), |_, meta| Arc::clone(meta))
    }

    /// Keeps `meta` for `location`, unless another read kept an answer
    /// first; returns the one kept.
    fn remember(&self, location: &Path, meta: Arc<ObjectMeta>) -> Arc<ObjectMeta> {
        self.metas
            .get_or_insert(Self::key(location).into(), meta, |_, kept| Arc::clone(kept))
            .or_abort()
    }

    fn forget(&self, location: &Path) {
        self.metas.remove(Self::key(location));
    }

    fn key(location: &Path) -> &[u8] {
        location.as_ref().as_bytes()
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

impl fmt::Debug for KnownFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KnownFiles").finish_non_exhaustive()
    }
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

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
