//! [`T4Caching`]: the cache's front end (`doc/03-cache.md`, decision 10),
//! an [`ObjectStore`] over another, whose reads the cache serves, and which
//! fetches what only the remote holds itself. [`T4CachingBuilder`] makes
//! one.
//!
//! - **What a file is,** the inner store says once: its first answer for the
//!   path, a list through the cache that named it or else a HEAD, which the
//!   cache trusts (decision 11). A hit asks the store nothing. A remote read fetches with `If-Match` on that answer's e_tag, so a file
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
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bbtree::BBTreeMap;
use bytes::Bytes;
use futures_util::future::{FutureExt, join, join_all, ready, select_ok};
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
use super::io_report::IoReport;
use super::object_cache::{CacheableFile, FetchedBlocks, ObjectCache};
#[cfg(feature = "__fuzz")]
use super::policy::Tier;
use super::policy::{
    ClockPolicy, FetchRuns, Io, PlacementPolicy, ReadIntent, ReadPolicy, TierSizes,
};
use super::{BLOCK_SIZE_U64, InsertOrAbort};
use crate::io::sync::{Arc, Mutex, MutexGuard};

/// The name errors give this store.
const STORE: &str = "t4";

/// What makes a policy, once the tiers' chunk counts are known.
type PlacementMaker = Box<dyn FnOnce(TierSizes) -> Arc<dyn PlacementPolicy>>;

/// An [`ObjectStore`] that caches the reads of another, in memory and on
/// disk. [`Self::builder`] makes one.
pub struct T4Caching {
    inner: Arc<dyn ObjectStore>,
    cache: ObjectCache,
    /// Shared with the uploads and deletes in flight, which forget their
    /// paths once done.
    known: Arc<KnownFiles>,
}

/// Makes a [`T4Caching`]: where it caches, how much, and by what policies.
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
    placement: Option<PlacementMaker>,
    reads: Option<Arc<dyn ReadPolicy>>,
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

    /// The placement policy, where blocks live, which `build` makes for the
    /// tiers' chunk counts, once it knows them; a CLOCK for each tier
    /// ([`ClockPolicy`]) unless set.
    pub fn with_placement_policy<P: PlacementPolicy + 'static>(
        mut self,
        build: impl FnOnce(TierSizes) -> P + 'static,
    ) -> Self {
        self.placement = Some(Box::new(move |sizes| Arc::new(build(sizes))));
        self
    }

    /// The read policy, how reads are served: which GETs fetch what they
    /// only the remote holds, how many race; each run of remote blocks fetched once
    /// ([`FetchRuns`]) unless set.
    pub fn with_read_policy(mut self, policy: impl ReadPolicy + 'static) -> Self {
        self.reads = Some(Arc::new(policy));
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
        let placement = match self.placement {
            Some(build) => build(sizes),
            None => Arc::new(ClockPolicy::new(memory_chunks, disk_chunks)),
        };
        let reads = self.reads.unwrap_or_else(|| Arc::new(FetchRuns));
        Ok(T4Caching {
            inner: self.inner,
            cache: ObjectCache::new(memory_chunks, disk, placement, reads)?,
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
            placement: None,
            reads: None,
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

    /// `range` of `file`, from memory, disk and fetches of what only the
    /// remote holds, each fetch's bytes put into the read's output and cached
    /// as the policy says as they come in; and the blocks other reads are
    /// fetching, taken from where those fetches cache them, or fetched again
    /// if they don't. `None` if the cache can't serve it: a read of the cache
    /// file failed, or a fetch found the file changed, which the cache
    /// forgets then.
    async fn read_through(
        &self,
        location: &Path,
        file: &CacheableFile<'_>,
        e_tag: &str,
        range: Range<u64>,
        extensions: &Extensions,
    ) -> object_store::Result<Option<Bytes>> {
        let intent = extensions.get::<ReadIntent>().copied();
        let Ok((mut read, mut work)) = self.cache.start_read(file, range.clone(), intent) else {
            return Ok(None);
        };
        loop {
            let mut receivers = Vec::new();
            for fetch in read.fetches() {
                let Ok(blocks) = self.cache.receive(&mut read, fetch) else {
                    return Ok(None);
                };
                receivers.push(blocks);
            }
            // Every fetch is in flight at once, a read's remote blocks keeping
            // the NIC busy, and beside the read's work on the tiers: its disk
            // reads, and its waits for the blocks other reads fetch. The GETs
            // are polled first, so they are sent before any of it is waited
            // on.
            let fetches = join_all(
                receivers
                    .into_iter()
                    .map(|blocks| self.fetch(location, e_tag, blocks, extensions)),
            );
            let (fetched, worked) = join(fetches, work.finish(&mut read)).await;
            for fetched in fetched {
                match fetched {
                    Ok(()) => {}
                    Err(object_store::Error::Precondition { .. }) => {
                        self.known.forget(location);
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
            let Ok(retry) = worked else {
                return Ok(None);
            };
            if retry.is_empty() {
                return Ok(read.into_bytes());
            }
            // Blocks the read waited for that no fetch cached: it fetches
            // them itself, or waits again for a read that fetches them now.
            let Ok(again) = self
                .cache
                .retry(&mut read, file, range.clone(), intent, &retry)
            else {
                return Ok(None);
            };
            work = again;
        }
    }

    /// Fetches the bytes `blocks` holds from the inner store, if the file is
    /// still the version with `e_tag`, and hands each piece of the answer to
    /// `blocks` as it comes in. As many identical GETs as the plan says race
    /// for them, sent at once: the first answer takes the bytes, and the
    /// others are dropped, their requests with them. The read policy hears
    /// the GET finished once its body is in, and landed once its blocks are
    /// cached.
    async fn fetch(
        &self,
        location: &Path,
        e_tag: &str,
        mut blocks: FetchedBlocks<'_>,
        extensions: &Extensions,
    ) -> object_store::Result<()> {
        let bytes = blocks.range();
        let options = GetOptions {
            range: Some(GetRange::Bounded(bytes.clone())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let path = location.as_ref().as_bytes();
        let racers = (0..blocks.race().get()).map(|racer| {
            let get = Io::Get {
                path,
                bytes: bytes.clone(),
                racer,
            };
            let mut report = IoReport::start(self.cache.read_policy(), get);
            let get = self.inner.get_opts(location, options.clone());
            async move {
                match get.await {
                    Ok(answer) => {
                        report.answered();
                        Ok((answer, report))
                    }
                    Err(error) => {
                        report.failed();
                        Err(error)
                    }
                }
            }
            .boxed()
        });
        let ((answer, mut report), _losers) = select_ok(racers).await?;
        // The time the body's pieces took to land, apart from the time
        // spent waiting for them.
        let mut landing = Duration::ZERO;
        let received = async {
            let mut body = answer.into_stream();
            while let Some(piece) = body.try_next().await? {
                let started = Instant::now();
                blocks.land(&piece).await.map_err(Self::cache_error)?;
                landing += started.elapsed();
            }
            Ok::<_, object_store::Error>(())
        };
        match received.await {
            Ok(()) => report.finished(bytes.end - bytes.start),
            Err(error) => {
                report.failed();
                return Err(error);
            }
        }
        blocks.finish().await.map_err(Self::cache_error)?;
        report.landed(landing);
        Ok(())
    }

    /// A failure of the cache's, as the store's error.
    fn cache_error(error: crate::io::error::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: STORE,
            source: Box::new(error),
        }
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

    /// `listed`, a list of the inner store's, each file it names kept as the
    /// store's answer for that path, unless one was kept first: a read of a
    /// listed file asks for no HEAD.
    fn remembering(
        self: &Arc<Self>,
        listed: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let known = Arc::clone(self);
        listed
            .inspect(move |listed| {
                if let Ok(meta) = listed {
                    known.remember(&meta.location, Arc::new(meta.clone()));
                }
            })
            .boxed()
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
