use std::future::poll_fn;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use futures_util::{FutureExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::{ListResult, ObjectStoreExt};
use pollster::block_on;
use tempfile::TempDir;

use super::*;
use crate::cache::policy::{
    Admit, BlockKey, BlockRead, ChunkId, DiskHit, IoEvent, IoPhase, MemoryVictim, PlannedGet,
    PlannedRead, ReadPlan, Tier,
};

const MIB: u64 = BLOCK_SIZE_U64;

/// A request the cache made of the store under it.
#[derive(Clone, Debug, PartialEq)]
enum Asked {
    Head,
    Get(Option<GetRange>),
}

/// An in-memory store that records every get the cache makes of it, and
/// can acknowledge its writes late.
#[derive(Debug, Default)]
struct CountingStore {
    store: InMemory,
    asked: Mutex<Vec<Asked>>,
    acks: Acks,
}

impl CountingStore {
    /// What the cache asked since the last call.
    fn take_asked(&self) -> Vec<Asked> {
        std::mem::take(&mut self.asked.lock().unwrap())
    }

    fn ack_writes_late(&self, late: bool) {
        self.acks.0.store(late, Ordering::Relaxed);
    }
}

/// Whether a store acknowledges a write a poll after doing it, as a store
/// across a network does: a caller that gives up then has written anyway.
/// Shared by the store, its uploads and its deletes.
#[derive(Clone, Debug, Default)]
struct Acks(Arc<AtomicBool>);

impl Acks {
    /// The acknowledgement of a write that is done: a poll later, if late.
    async fn arrive(&self) {
        let mut on_its_way = self.0.load(Ordering::Relaxed);
        poll_fn(|cx| {
            if std::mem::take(&mut on_its_way) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }
}

/// An upload whose completion its store acknowledges as it does writes.
#[derive(Debug)]
struct AckedUpload {
    upload: Box<dyn MultipartUpload>,
    acks: Acks,
}

#[async_trait]
impl MultipartUpload for AckedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let completed = self.upload.complete().await;
        self.acks.arrive().await;
        completed
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.upload.abort().await
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "counting {}", self.store)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let put = self.store.put_opts(location, payload, opts).await;
        self.acks.arrive().await;
        put
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let upload = self.store.put_multipart_opts(location, opts).await?;
        Ok(Box::new(AckedUpload {
            upload,
            acks: self.acks.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let asked = if options.head {
            Asked::Head
        } else {
            Asked::Get(options.range.clone())
        };
        self.asked.lock().unwrap().push(asked);
        self.store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let acks = self.acks.clone();
        self.store
            .delete_stream(locations)
            .then(move |deleted| {
                let acks = acks.clone();
                async move {
                    acks.arrive().await;
                    deleted
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let copied = self.store.copy_opts(from, to, options).await;
        self.acks.arrive().await;
        copied
    }
}

/// A builder of a cache of 4 MiB of memory and 8 on disk over `inner`, in
/// `dir`.
fn builder(inner: &Arc<CountingStore>, dir: &TempDir) -> T4CachingBuilder {
    T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
}

/// A cache over a [`CountingStore`], in a directory of its own that lives
/// as long as the cache.
fn t4_caching() -> (TempDir, T4Caching, Arc<CountingStore>) {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let store = builder(&inner, &dir).build().unwrap();
    (dir, store, inner)
}

/// A file's bytes: each a function of its offset and `seed`.
fn file_bytes(size: u64, seed: u8) -> Bytes {
    (0..size)
        .map(|offset| (offset % 251) as u8 ^ seed)
        .collect::<Vec<_>>()
        .into()
}

fn put(store: &dyn ObjectStore, path: &Path, bytes: &Bytes) {
    block_on(store.put(path, bytes.clone().into())).unwrap();
}

fn get_range(store: &T4Caching, path: &Path, range: Range<u64>) -> Bytes {
    block_on(store.get_range(path, range)).unwrap()
}

fn bounded(range: Range<u64>) -> Asked {
    Asked::Get(Some(GetRange::Bounded(range)))
}

#[test]
fn a_builder_needs_a_memory_capacity_and_a_disk_file() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let store = || T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>);
    let path = dir.path().join("cache");
    let incomplete = [
        store().with_disk_file(&path, MIB),
        store().with_memory_capacity(MIB),
        builder(&inner, &dir).with_queue_depth(0),
    ];
    for builder in incomplete {
        let error = builder.build().unwrap_err();
        assert!(matches!(error, crate::Error::InvalidArgument(_)), "{error}");
    }
    // Less than a block is a block.
    builder(&inner, &dir)
        .with_memory_capacity(1)
        .build()
        .unwrap();
}

/// Files of unequal size each hold as many chunks as the smallest, and
/// take only those; the tier is all of them, and serves reads.
#[test]
fn the_disk_tier_stripes_over_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let sizes = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&sizes);
    let (small, large) = (dir.path().join("small"), dir.path().join("large"));
    let store = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(MIB)
        .with_disk_file(&small, 3 * MIB)
        .with_disk_file(&large, 5 * MIB)
        .with_placement_policy(move |tiers: TierSizes| {
            *seen.lock().unwrap() = Some(tiers);
            ClockPolicy::new(tiers.memory_chunks(), tiers.disk_chunks())
        })
        .build()
        .unwrap();
    let tiers = sizes.lock().unwrap().expect("build made the policy");
    assert_eq!(tiers.disk_chunks().get(), 6);
    for file in [&small, &large] {
        assert_eq!(std::fs::metadata(file).unwrap().len(), 3 * MIB);
    }

    // More blocks than memory holds: most go to disk, and read back.
    let path = Path::from("data");
    let bytes = file_bytes(6 * MIB, 3);
    put(inner.as_ref(), &path, &bytes);
    for _ in 0..2 {
        for block in 0..6 {
            let range = block * MIB..(block + 1) * MIB;
            let read = get_range(&store, &path, range.clone());
            assert_eq!(read, bytes.slice(range.start as usize..range.end as usize));
        }
    }
}

#[test]
fn a_custom_policy_is_built_for_the_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let sizes = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&sizes);
    let store = builder(&inner, &dir)
        .with_placement_policy(move |tiers: TierSizes| {
            *seen.lock().unwrap() = Some(tiers);
            ClockPolicy::new(tiers.memory_chunks(), tiers.disk_chunks())
        })
        .build()
        .unwrap();
    let tiers = sizes.lock().unwrap().expect("build made the policy");
    assert_eq!(
        (tiers.memory_chunks().get(), tiers.disk_chunks().get()),
        (4, 8)
    );

    let path = Path::from("a");
    let data = file_bytes(MIB, 0);
    put(inner.as_ref(), &path, &data);
    assert_eq!(get_range(&store, &path, 0..MIB), data);
}

#[test]
fn reads_return_the_stores_bytes() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("data/a.parquet");
    let size = 3 * MIB + MIB / 2;
    let data = file_bytes(size, 0);
    put(inner.as_ref(), &path, &data);

    let whole = block_on(async { store.get(&path).await?.bytes().await }).unwrap();
    assert_eq!(whole, data);
    for range in [0..1, 100..MIB + 100, 3 * MIB..size, MIB - 1..MIB + 1] {
        let bytes = get_range(&store, &path, range.clone());
        assert_eq!(bytes, data.slice(range.start as usize..range.end as usize));
    }
    for (asked, expected) in [
        (GetRange::Offset(size - 10), size - 10..size),
        (GetRange::Suffix(10), size - 10..size),
    ] {
        let options = GetOptions {
            range: Some(asked),
            ..GetOptions::default()
        };
        let result = block_on(store.get_opts(&path, options)).unwrap();
        assert_eq!(result.range, expected);
        let bytes = block_on(result.bytes()).unwrap();
        assert_eq!(
            bytes,
            data.slice(expected.start as usize..expected.end as usize)
        );
    }
    let meta = block_on(store.head(&path)).unwrap();
    assert_eq!(meta.size, size);
    assert_eq!(meta, block_on(inner.head(&path)).unwrap());
}

#[test]
fn a_hit_asks_the_store_nothing() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 0);
    put(inner.as_ref(), &path, &data);

    get_range(&store, &path, 10..MIB + 10);
    assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..2 * MIB)]);
    let bytes = get_range(&store, &path, 5..2 * MIB - 5);
    assert_eq!(bytes, data.slice(5..2 * MIB as usize - 5));
    assert_eq!(inner.take_asked(), []);
}

#[test]
fn a_read_fetches_only_the_blocks_the_cache_lacks() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let size = 3 * MIB + MIB / 2;
    let data = file_bytes(size, 0);
    put(inner.as_ref(), &path, &data);

    get_range(&store, &path, MIB + 1..MIB + 2);
    inner.take_asked();
    let bytes = get_range(&store, &path, 0..size);
    assert_eq!(bytes, data);
    let mut asked = inner.take_asked();
    asked.sort_by_key(|asked| format!("{asked:?}"));
    assert_eq!(asked, [bounded(0..MIB), bounded(2 * MIB..size)]);
}

#[test]
fn ranges_read_through_the_cache() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let data = file_bytes(3 * MIB, 0);
    put(inner.as_ref(), &path, &data);

    let ranges = [0..10, 2 * MIB..2 * MIB + 10, MIB + 5..MIB + 6];
    let read = block_on(store.get_ranges(&path, &ranges)).unwrap();
    for (range, bytes) in ranges.iter().zip(read) {
        assert_eq!(bytes, data.slice(range.start as usize..range.end as usize));
    }
    inner.take_asked();
    block_on(store.get_ranges(&path, &ranges)).unwrap();
    assert_eq!(inner.take_asked(), []);
}

#[test]
fn a_write_through_the_cache_is_read_back() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    put(inner.as_ref(), &path, &file_bytes(MIB, 0));
    get_range(&store, &path, 0..MIB);

    let replaced = file_bytes(MIB + 7, 1);
    put(&store, &path, &replaced);
    assert_eq!(get_range(&store, &path, 0..MIB + 7), replaced);
}

/// The cache trusts the store's first answer: a hit asks nothing, so a file
/// changed behind it reads as it was, until a remote read catches the change.
#[test]
fn a_file_changed_behind_the_cache_is_caught_by_a_remote_read() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let old = file_bytes(2 * MIB, 0);
    put(inner.as_ref(), &path, &old);
    get_range(&store, &path, 0..10);

    let new = file_bytes(2 * MIB, 1);
    put(inner.as_ref(), &path, &new);
    assert_eq!(get_range(&store, &path, 0..10), old.slice(0..10), "a hit");
    // Block 1 is remote: its fetch, on the old e_tag, finds the new file.
    let bytes = get_range(&store, &path, MIB..MIB + 10);
    assert_eq!(bytes, new.slice(MIB as usize..MIB as usize + 10));
    // The cache forgot the path: the next read asks again, and caches anew.
    assert_eq!(get_range(&store, &path, 0..10), new.slice(0..10));
    inner.take_asked();
    assert_eq!(get_range(&store, &path, 0..10), new.slice(0..10));
    assert_eq!(inner.take_asked(), []);
}

#[test]
fn a_deleted_file_is_not_found() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    put(inner.as_ref(), &path, &file_bytes(MIB, 0));
    get_range(&store, &path, 0..10);

    block_on(store.delete(&path)).unwrap();
    let error = block_on(store.get_range(&path, 0..10)).unwrap_err();
    assert!(
        matches!(error, object_store::Error::NotFound { .. }),
        "{error}"
    );
    let missing = block_on(store.get(&Path::from("missing"))).unwrap_err();
    assert!(
        matches!(missing, object_store::Error::NotFound { .. }),
        "{missing}"
    );
}

/// A write through the cache to `to`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum WriteThrough {
    Put,
    Multipart,
    /// Of `from`, onto `to`.
    Copy,
    /// Of `from`, onto `to`.
    Rename,
    Delete,
}

impl WriteThrough {
    const ALL: [Self; 5] = [
        Self::Put,
        Self::Multipart,
        Self::Copy,
        Self::Rename,
        Self::Delete,
    ];

    /// Writes `to` through `store`: `bytes` for a put, `from` for a copy or
    /// a rename.
    async fn run(
        self,
        store: &T4Caching,
        from: &Path,
        to: &Path,
        bytes: &Bytes,
    ) -> object_store::Result<()> {
        match self {
            Self::Put => store.put(to, bytes.clone().into()).await.map(drop),
            Self::Multipart => {
                let mut upload = store.put_multipart(to).await?;
                upload.put_part(bytes.clone().into()).await?;
                upload.complete().await.map(drop)
            }
            Self::Copy => store.copy(from, to).await,
            Self::Rename => store.rename(from, to).await,
            Self::Delete => store.delete(to).await,
        }
    }
}

/// A write given up on once the store has done it, its acknowledgement
/// still on the way, as a caller dropping its future does: the cache
/// forgets the path all the same, so a read gets what the store has, not
/// the old file's cached blocks.
#[test]
fn a_write_given_up_on_after_it_lands_is_read_back() {
    let stale: Vec<_> = WriteThrough::ALL
        .into_iter()
        .filter(|&write| {
            let (_dir, store, inner) = t4_caching();
            let (from, to) = (Path::from("from"), Path::from("to"));
            put(inner.as_ref(), &from, &file_bytes(MIB, 1));
            put(inner.as_ref(), &to, &file_bytes(MIB, 2));
            get_range(&store, &to, 0..MIB);

            inner.ack_writes_late(true);
            let bytes = file_bytes(MIB, 3);
            let written = write.run(&store, &from, &to, &bytes);
            assert!(written.now_or_never().is_none(), "{write:?} given up on");
            inner.ack_writes_late(false);

            let in_store = block_on(async { inner.get(&to).await?.bytes().await });
            let read = block_on(async { store.get(&to).await?.bytes().await });
            // Both the same bytes, or both no file.
            in_store.ok() != read.ok()
        })
        .collect();
    assert_eq!(stale, [], "these writes left the old file to be read");
}

#[test]
fn conditional_reads_go_to_the_store() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let data = file_bytes(MIB, 0);
    put(inner.as_ref(), &path, &data);
    let e_tag = block_on(store.head(&path)).unwrap().e_tag;
    get_range(&store, &path, 0..10);
    inner.take_asked();

    let unchanged = GetOptions {
        if_none_match: e_tag.clone(),
        ..GetOptions::default()
    };
    let error = block_on(store.get_opts(&path, unchanged)).unwrap_err();
    assert!(
        matches!(error, object_store::Error::NotModified { .. }),
        "{error}"
    );
    assert_eq!(inner.take_asked(), [Asked::Get(None)]);

    // `If-Match` on the e_tag the cache holds: a hit.
    let matching = GetOptions {
        if_match: e_tag,
        range: Some(GetRange::Bounded(0..10)),
        ..GetOptions::default()
    };
    let bytes = block_on(async { store.get_opts(&path, matching).await?.bytes().await }).unwrap();
    assert_eq!(bytes, data.slice(0..10));
    assert_eq!(inner.take_asked(), []);
}

#[test]
fn listing_passes_through() {
    let (_dir, store, inner) = t4_caching();
    for name in ["x/1", "x/2", "y/3"] {
        put(inner.as_ref(), &Path::from(name), &file_bytes(10, 0));
    }
    let listed: Vec<_> = block_on(store.list(Some(&Path::from("x"))).try_collect()).unwrap();
    let mut names: Vec<_> = listed
        .iter()
        .map(|meta| meta.location.to_string())
        .collect();
    names.sort();
    assert_eq!(names, ["x/1", "x/2"]);
}

/// What a read policy heard of an I/O, its timings dropped.
#[derive(Clone, Debug, PartialEq)]
enum Heard {
    Get {
        bytes: Range<u64>,
        racer: u8,
        phase: &'static str,
    },
    DiskRead {
        drive: u32,
        bytes: u32,
        phase: &'static str,
    },
}

impl Heard {
    fn of(event: &IoEvent<'_>) -> Self {
        let phase = match event.phase() {
            IoPhase::Started => "started",
            IoPhase::Answered { .. } => "answered",
            IoPhase::Finished { .. } => "finished",
            IoPhase::Failed { .. } => "failed",
            IoPhase::Cancelled { .. } => "cancelled",
        };
        match event.io() {
            Io::Get { bytes, racer, .. } => Self::Get {
                bytes: bytes.clone(),
                racer: *racer,
                phase,
            },
            Io::DiskRead { drive, bytes } => Self::DiskRead {
                drive: *drive,
                bytes: *bytes,
                phase,
            },
        }
    }
}

/// A read policy that plans each read with a script, and keeps the intent
/// of each read it planned and every event it heard. Its clones share them.
#[derive(Clone)]
struct ScriptedReads {
    script: Arc<dyn Fn(&PlannedRead<'_>) -> ReadPlan + Send + Sync>,
    intents: Arc<Mutex<Vec<Option<ReadIntent>>>>,
    heard: Arc<Mutex<Vec<Heard>>>,
}

impl ScriptedReads {
    fn new(script: impl Fn(&PlannedRead<'_>) -> ReadPlan + Send + Sync + 'static) -> Self {
        Self {
            script: Arc::new(script),
            intents: Arc::default(),
            heard: Arc::default(),
        }
    }

    fn heard(&self) -> Vec<Heard> {
        self.heard.lock().unwrap().clone()
    }
}

impl ReadPolicy for ScriptedReads {
    fn plan(&self, read: &PlannedRead<'_>) -> ReadPlan {
        self.intents.lock().unwrap().push(read.intent());
        (self.script)(read)
    }

    fn on_io(&self, event: &IoEvent<'_>) {
        self.heard.lock().unwrap().push(Heard::of(event));
    }
}

/// Each block a placement policy was asked where to put: its number, the
/// read's intent, and whether the read asked for it.
type RemoteReads = Arc<Mutex<Vec<(u32, Option<ReadIntent>, bool)>>>;

/// A CLOCK that puts every block it is asked about where `admit` says, and
/// keeps what it was asked.
struct RecordingPlacement {
    clock: ClockPolicy,
    admit: Admit,
    remote_reads: RemoteReads,
}

impl PlacementPolicy for RecordingPlacement {
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) {
        self.clock.on_memory_hit(chunk, read);
    }

    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit {
        self.clock.on_disk_hit(chunk, read)
    }

    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit {
        self.remote_reads
            .lock()
            .unwrap()
            .push((read.block().index(), read.intent(), read.asked()));
        self.admit
    }

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey) {
        self.clock.on_admit(tier, chunk, block);
    }

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim> {
        self.clock.pick_memory_victims(count)
    }

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId> {
        self.clock.pick_disk_victims(count)
    }
}

/// A cache over a [`CountingStore`] holding a 3 MiB file at `a`, its reads
/// planned by `reads`, its blocks put where `admit` says; what the store was
/// asked so far forgotten.
fn planned_by(
    reads: &ScriptedReads,
    admit: Admit,
) -> (TempDir, T4Caching, Arc<CountingStore>, RemoteReads, Bytes) {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let remote_reads = RemoteReads::default();
    let recorded = Arc::clone(&remote_reads);
    let store = builder(&inner, &dir)
        .with_placement_policy(move |tiers: TierSizes| RecordingPlacement {
            clock: ClockPolicy::new(tiers.memory_chunks(), tiers.disk_chunks()),
            admit,
            remote_reads: recorded,
        })
        .with_read_policy(reads.clone())
        .build()
        .unwrap();
    let data = file_bytes(3 * MIB, 7);
    put(inner.as_ref(), &Path::from("a"), &data);
    inner.take_asked();
    (dir, store, inner, remote_reads, data)
}

#[test]
fn a_read_policy_splits_a_run_into_gets() {
    let reads = ScriptedReads::new(|read| {
        let blocks = read.remote_runs().flatten();
        ReadPlan::new(
            blocks
                .map(|block| PlannedGet::once(block..block + 1))
                .collect(),
        )
    });
    let (_dir, store, inner, _, data) = planned_by(&reads, Admit::Memory);
    let path = Path::from("a");
    assert_eq!(get_range(&store, &path, 0..3 * MIB), data);
    assert_eq!(
        inner.take_asked(),
        [
            Asked::Head,
            bounded(0..MIB),
            bounded(MIB..2 * MIB),
            bounded(2 * MIB..3 * MIB)
        ]
    );
    assert_eq!(get_range(&store, &path, 0..3 * MIB), data);
    assert_eq!(inner.take_asked(), [], "every block cached");
}

/// One GET over a read's blocks, the hit between its remote blocks too: the hit's
/// bytes fetched are dropped, its block left where it is.
#[test]
fn a_get_merged_over_a_hit_drops_its_bytes() {
    let reads = ScriptedReads::new(|read| ReadPlan::new(vec![PlannedGet::once(read.blocks())]));
    let (_dir, store, inner, remote_reads, data) = planned_by(&reads, Admit::Memory);
    let path = Path::from("a");
    get_range(&store, &path, MIB..2 * MIB);
    inner.take_asked();
    remote_reads.lock().unwrap().clear();

    assert_eq!(
        get_range(&store, &path, 10..3 * MIB - 10),
        data.slice(10..3 * MIB as usize - 10)
    );
    assert_eq!(inner.take_asked(), [bounded(0..3 * MIB)]);
    assert_eq!(
        *remote_reads.lock().unwrap(),
        [(0, None, true), (2, None, true)],
        "the hit asked about nothing"
    );
    assert_eq!(get_range(&store, &path, 0..3 * MIB), data);
    assert_eq!(inner.take_asked(), []);
    store.cache.check_at_rest().unwrap();
}

/// A GET widened past the read caches the blocks it brings, where the
/// placement policy says, told they weren't asked for.
#[test]
fn a_widened_get_caches_past_the_read() {
    let reads = ScriptedReads::new(|read| {
        ReadPlan::new(vec![PlannedGet::once(
            read.blocks().start..read.file_blocks(),
        )])
    });
    let (_dir, store, inner, remote_reads, data) = planned_by(&reads, Admit::Memory);
    let path = Path::from("a");
    assert_eq!(
        get_range(&store, &path, 0..MIB),
        data.slice(0..MIB as usize)
    );
    assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..3 * MIB)]);
    assert_eq!(
        *remote_reads.lock().unwrap(),
        [(0, None, true), (1, None, false), (2, None, false)]
    );
    assert_eq!(
        get_range(&store, &path, MIB..3 * MIB),
        data.slice(MIB as usize..)
    );
    assert_eq!(inner.take_asked(), [], "the widened blocks were cached");
}

/// Three requests race for a run: the first answer takes its bytes, and the
/// others are given up on.
#[test]
fn racing_gets_take_the_first_answer() {
    let three = std::num::NonZeroU8::new(3).unwrap();
    let reads = ScriptedReads::new(move |read| {
        ReadPlan::new(
            read.remote_runs()
                .map(|run| PlannedGet::new(run, three))
                .collect(),
        )
    });
    let (_dir, store, _inner, _, data) = planned_by(&reads, Admit::Memory);
    assert_eq!(
        get_range(&store, &Path::from("a"), 0..MIB),
        data.slice(0..MIB as usize)
    );
    let get = |racer, phase| Heard::Get {
        bytes: 0..MIB,
        racer,
        phase,
    };
    let heard = reads.heard();
    for racer in 0..3 {
        assert!(heard.contains(&get(racer, "started")), "{heard:?}");
    }
    let ended: Vec<_> = heard
        .iter()
        .filter(|heard| {
            matches!(
                heard,
                Heard::Get {
                    phase: "finished" | "cancelled",
                    ..
                }
            )
        })
        .collect();
    assert_eq!(ended.len(), 3, "each racer ended once: {heard:?}");
    assert_eq!(
        ended
            .iter()
            .filter(|heard| matches!(
                heard,
                Heard::Get {
                    phase: "finished",
                    ..
                }
            ))
            .count(),
        1,
        "one answer taken: {heard:?}"
    );
}

/// A plan that leaves a remote block out can't be carried out: the read fetches its
/// runs, as the default does.
#[test]
fn a_plan_the_cache_cant_carry_out_fetches_the_runs() {
    let reads = ScriptedReads::new(|_| ReadPlan::default());
    let (_dir, store, inner, _, data) = planned_by(&reads, Admit::Memory);
    assert_eq!(
        get_range(&store, &Path::from("a"), 0..2 * MIB),
        data.slice(0..2 * MIB as usize)
    );
    assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..2 * MIB)]);
}

#[test]
fn the_callers_intent_reaches_both_policies() {
    let reads = ScriptedReads::new(|read| FetchRuns.plan(read));
    let (_dir, store, _inner, remote_reads, data) = planned_by(&reads, Admit::Memory);
    let mut options = GetOptions {
        range: Some(GetRange::Bounded(0..MIB)),
        ..GetOptions::default()
    };
    options.extensions.insert(ReadIntent::Latency);
    let bytes = block_on(async {
        store
            .get_opts(&Path::from("a"), options)
            .await?
            .bytes()
            .await
    });
    assert_eq!(bytes.unwrap(), data.slice(0..MIB as usize));
    assert_eq!(*reads.intents.lock().unwrap(), [Some(ReadIntent::Latency)]);
    assert_eq!(
        *remote_reads.lock().unwrap(),
        [(0, Some(ReadIntent::Latency), true)]
    );
}

/// The read policy hears each read of the disk tier: which drive, how many
/// bytes, started and finished.
#[test]
fn the_read_policy_hears_reads_from_disk() {
    let reads = ScriptedReads::new(|read| FetchRuns.plan(read));
    let (_dir, store, _inner, _, data) = planned_by(&reads, Admit::Disk);
    let path = Path::from("a");
    get_range(&store, &path, 0..MIB);
    reads.heard.lock().unwrap().clear();

    assert_eq!(
        get_range(&store, &path, 0..MIB),
        data.slice(0..MIB as usize)
    );
    let read = |phase| Heard::DiskRead {
        drive: 0,
        bytes: MIB as u32,
        phase,
    };
    assert_eq!(reads.heard(), [read("started"), read("finished")]);
}
