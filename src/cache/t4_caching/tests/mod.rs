use crate::io::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::io::sync::{Mutex, mpsc, thread};
use std::future::poll_fn;
use std::task::Poll;

use async_trait::async_trait;
use futures_util::future::pending;
use futures_util::{FutureExt, TryStreamExt, stream};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
#[cfg(not(feature = "shuttle"))]
use pollster::block_on;
#[cfg(feature = "shuttle")]
use shuttle::future::block_on;
use tempfile::TempDir;

use super::*;
use crate::cache::policy::{
    Admit, BlockKey, BlockRead, ChunkId, ClockPolicy, DiskHit, FetchRuns, Io, IoEvent, IoPhase,
    LocatedRead, MemoryVictim, PlannedGet, ReadPlan, Tier,
};

const MIB: u64 = BLOCK_SIZE_U64;

#[derive(Clone, Debug, PartialEq)]
enum Asked {
    Head,
    Get(Option<GetRange>),
}

#[derive(Debug, Default)]
struct CountingStore {
    store: InMemory,
    asked: Mutex<Vec<Asked>>,
    acks: Acks,
    /// Answers gets a poll late, so a read planned meanwhile finds its blocks in flight.
    answers: Acks,
    stall: Mutex<Option<usize>>,
    stalled: Arc<AtomicBool>,
    gets_out: Arc<GetsOut>,
    gate: Mutex<Option<mpsc::Receiver<()>>>,
}

#[derive(Debug, Default)]
struct GetsOut {
    now: AtomicUsize,
    most: AtomicUsize,
}

impl GetsOut {
    fn sent(self: &Arc<Self>) -> GetOut {
        let now = self.now.fetch_add(1, Ordering::Relaxed) + 1;
        self.most.fetch_max(now, Ordering::Relaxed);
        GetOut(Arc::clone(self))
    }
}

struct GetOut(Arc<GetsOut>);

impl Drop for GetOut {
    fn drop(&mut self) {
        self.0.now.fetch_sub(1, Ordering::Relaxed);
    }
}

impl CountingStore {
    fn most_gets_out(&self) -> usize {
        self.gets_out.most.load(Ordering::Relaxed)
    }

    fn gets_out_now(&self) -> usize {
        self.gets_out.now.load(Ordering::Relaxed)
    }

    fn held_out(answer: GetResult, out: GetOut) -> GetResult {
        let (meta, range, attributes) = (
            answer.meta.clone(),
            answer.range.clone(),
            answer.attributes.clone(),
        );
        let body = answer.into_stream().map(move |piece| {
            let _held = &out;
            piece
        });
        GetResult {
            payload: GetResultPayload::Stream(body.boxed()),
            meta,
            range,
            attributes,
        }
    }

    fn take_asked(&self) -> Vec<Asked> {
        std::mem::take(&mut self.asked.lock().unwrap())
    }

    fn ack_writes_late(&self, late: bool) {
        self.acks.0.store(late, Ordering::Relaxed);
    }

    fn answer_gets_late(&self, late: bool) {
        self.answers.0.store(late, Ordering::Relaxed);
    }

    /// Blocks the next non-HEAD get's thread until the sender sends or drops.
    fn gate_next_get(&self) -> mpsc::Sender<()> {
        let (open, gate) = mpsc::channel();
        *self.gate.lock().unwrap() = Some(gate);
        open
    }

    fn stall_next_body(&self, after: usize) {
        *self.stall.lock().unwrap() = Some(after);
    }

    async fn stalled(&self, answer: GetResult, after: usize) -> object_store::Result<GetResult> {
        let (meta, range, attributes) = (
            answer.meta.clone(),
            answer.range.clone(),
            answer.attributes.clone(),
        );
        let mut sent = Some(answer.bytes().await?.slice(..after));
        let stalled = Arc::clone(&self.stalled);
        let body = stream::poll_fn(move |_| match sent.take() {
            Some(piece) => Poll::Ready(Some(Ok(piece))),
            None => {
                stalled.store(true, Ordering::Relaxed);
                Poll::Pending
            }
        });
        Ok(GetResult {
            payload: GetResultPayload::Stream(body.boxed()),
            meta,
            range,
            attributes,
        })
    }
}

/// Acks a write a poll after doing it, so a caller that gives up then has written anyway.
#[derive(Clone, Debug, Default)]
struct Acks(Arc<AtomicBool>);

impl Acks {
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
        let head = options.head;
        self.asked.lock().unwrap().push(asked);
        let out = self.gets_out.sent();
        if !head {
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
        }
        self.answers.arrive().await;
        let answer = self.store.get_opts(location, options).await?;
        if head {
            return Ok(answer);
        }
        let stall = self.stall.lock().unwrap().take();
        let answer = match stall {
            Some(after) => self.stalled(answer, after).await?,
            None => answer,
        };
        Ok(Self::held_out(answer, out))
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

fn builder(inner: &Arc<CountingStore>, dir: impl AsRef<std::path::Path>) -> T4CachingBuilder {
    T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.as_ref().join("cache"), 8 * MIB)
}

#[cfg(feature = "shuttle")]
fn snapshot_at(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("cache.snapshot")
}

/// No deadline: shuttle fails a schedule that stalls here.
fn wait_until(mut done: impl FnMut() -> bool, _what: &str) {
    while !done() {
        thread::yield_now();
    }
}

fn t4_caching() -> (TempDir, T4Caching, Arc<CountingStore>) {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let store = builder(&inner, &dir).build().unwrap();
    (dir, store, inner)
}

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

#[cfg(feature = "shuttle")]
#[test]
fn a_builder_needs_a_memory_capacity_and_a_disk_file() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let store = || T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>);
        let path = dir.path().join("cache");
        let incomplete = [
            store().with_disk_file(&path, MIB),
            store().with_memory_capacity(MIB),
            builder(&inner, &dir).with_queue_depth(0),
            builder(&inner, &dir).with_network_rate(0, Duration::from_secs(1)),
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
    });
}

/// By any path that reaches the file, a symlink too.
#[cfg(feature = "shuttle")]
#[test]
fn a_cache_file_has_one_owner() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let first = builder(&inner, &dir).build().unwrap();

        let again = builder(&inner, &dir).build().unwrap_err();
        assert!(matches!(again, crate::Error::Busy), "{again}");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(dir.path().join("cache"), &link).unwrap();
        let through_link = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
            .with_memory_capacity(MIB)
            .with_disk_file(dir.path().join("other"), MIB)
            .with_disk_file(&link, MIB)
            .build()
            .unwrap_err();
        assert!(matches!(through_link, crate::Error::Busy), "{through_link}");

        drop(first);
        builder(&inner, &dir).build().unwrap();
    });
}

/// Files of unequal size each hold as many chunks as the smallest.
#[cfg(feature = "shuttle")]
#[test]
fn the_disk_tier_stripes_over_its_files() {
    crate::io::test_schedule::TestSchedule::check(|| {
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

        // More blocks than memory holds, so most go to disk.
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_custom_policy_is_built_for_the_tiers() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn reads_return_the_stores_bytes() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_hit_asks_the_store_nothing() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &data);

        get_range(&store, &path, 10..MIB + 10);
        assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..2 * MIB)]);
        let bytes = get_range(&store, &path, 5..2 * MIB - 5);
        assert_eq!(bytes, data.slice(5..2 * MIB as usize - 5));
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_read_fetches_only_the_blocks_the_cache_lacks() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn ranges_read_through_the_cache() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_write_through_the_cache_is_read_back() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        put(inner.as_ref(), &path, &file_bytes(MIB, 0));
        get_range(&store, &path, 0..MIB);

        let replaced = file_bytes(MIB + 7, 1);
        put(&store, &path, &replaced);
        assert_eq!(get_range(&store, &path, 0..MIB + 7), replaced);
    });
}

/// A hit asks the store nothing, so the old file reads on until a remote read catches it.
#[cfg(feature = "shuttle")]
#[test]
fn a_file_changed_behind_the_cache_is_caught_by_a_remote_read() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
        assert_eq!(get_range(&store, &path, 0..10), new.slice(0..10));
        inner.take_asked();
        assert_eq!(get_range(&store, &path, 0..10), new.slice(0..10));
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn overlapping_reads_of_a_block_send_one_get() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &data);
        block_on(store.head(&path)).unwrap();
        inner.take_asked();

        inner.answer_gets_late(true);
        let (first, second) = block_on(join(
            store.get_range(&path, 0..10),
            store.get_range(&path, 5..MIB),
        ));
        assert_eq!(first.unwrap(), data.slice(0..10));
        assert_eq!(second.unwrap(), data.slice(5..MIB as usize));
        assert_eq!(inner.take_asked(), [bounded(0..MIB)]);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn overlapping_reads_of_a_discarded_block_send_one_get() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
        let (_dir, store, inner, _, data) = planned_by(&reads, Admit::Discard);
        let path = Path::from("a");
        block_on(store.head(&path)).unwrap();
        inner.take_asked();

        inner.answer_gets_late(true);
        let (first, second) = block_on(join(
            store.get_range(&path, 0..10),
            store.get_range(&path, 5..MIB),
        ));
        assert_eq!(first.unwrap(), data.slice(0..10));
        assert_eq!(second.unwrap(), data.slice(5..MIB as usize));
        assert_eq!(inner.take_asked(), [bounded(0..MIB)]);
        assert_eq!(
            get_range(&store, &path, 0..MIB),
            data.slice(0..MIB as usize)
        );
        assert_eq!(inner.take_asked(), [bounded(0..MIB)], "nothing cached");
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_deleted_file_is_not_found() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum WriteThrough {
    Put,
    Multipart,
    Copy,
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

#[cfg(feature = "shuttle")]
#[test]
fn a_write_given_up_on_after_it_lands_is_read_back() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
                in_store.ok() != read.ok()
            })
            .collect();
        assert_eq!(stale, [], "these writes left the old file to be read");
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn conditional_reads_go_to_the_store() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
        let bytes =
            block_on(async { store.get_opts(&path, matching).await?.bytes().await }).unwrap();
        assert_eq!(bytes, data.slice(0..10));
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_listed_file_is_read_without_a_head() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let data = file_bytes(2 * MIB, 0);
        for name in ["x/1", "x/2", "y/3"] {
            put(inner.as_ref(), &Path::from(name), &data);
        }
        block_on(store.list(Some(&Path::from("x"))).try_collect::<Vec<_>>()).unwrap();
        block_on(store.list_with_delimiter(Some(&Path::from("y")))).unwrap();
        for name in ["x/1", "x/2", "y/3"] {
            let bytes = get_range(&store, &Path::from(name), 10..20);
            assert_eq!(bytes, data.slice(10..20));
            assert_eq!(inner.take_asked(), [bounded(0..MIB)], "{name}");
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn listing_passes_through() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

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
            IoPhase::Landed { .. } => "landed",
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

#[derive(Clone)]
struct ScriptedReads {
    script: Arc<dyn Fn(&LocatedRead<'_>) -> ReadPlan + Send + Sync>,
    intents: Arc<Mutex<Vec<Option<ReadIntent>>>>,
    heard: Arc<Mutex<Vec<Heard>>>,
}

impl ScriptedReads {
    fn new(script: impl Fn(&LocatedRead<'_>) -> ReadPlan + Send + Sync + 'static) -> Self {
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
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        self.intents.lock().unwrap().push(read.intent());
        (self.script)(read)
    }

    fn on_io(&self, event: &IoEvent<'_>) {
        self.heard.lock().unwrap().push(Heard::of(event));
    }
}

/// Per block placed: its index, the read's intent, and whether the read asked for it.
type RemoteReads = Arc<Mutex<Vec<(u32, Option<ReadIntent>, bool)>>>;

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

#[cfg(feature = "shuttle")]
#[test]
fn a_read_policy_splits_a_run_into_gets() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_get_merged_over_a_hit_drops_its_bytes() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
        store
            .cache_store()
            .owned()
            .unwrap()
            .check_at_rest()
            .unwrap();
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_widened_get_caches_past_the_read() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn racing_gets_take_the_first_answer() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
    });
}

type TestSleep = Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>;

fn raced_late_by(
    inner: &Arc<CountingStore>,
    reads: &ScriptedReads,
    sleep: Option<TestSleep>,
) -> (TempDir, T4Caching, Bytes) {
    let dir = tempfile::tempdir().unwrap();
    let builder = builder(inner, &dir).with_read_policy(reads.clone());
    let builder = match sleep {
        Some(sleep) => builder.with_sleep(sleep),
        None => builder,
    };
    let store = builder.build().unwrap();
    let data = file_bytes(3 * MIB, 7);
    put(inner.as_ref(), &Path::from("a"), &data);
    inner.take_asked();
    (dir, store, data)
}

fn late_racing() -> ScriptedReads {
    ScriptedReads::new(|read| {
        ReadPlan::new(
            read.remote_runs()
                .map(|run| PlannedGet::once(run).with_late_race(Duration::from_millis(1)))
                .collect(),
        )
    })
}

#[cfg(feature = "shuttle")]
#[test]
fn a_straggling_get_is_raced_for_the_rest_of_its_body() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = late_racing();
        let inner = Arc::new(CountingStore::default());
        // The timer is done once the store's stalled body is waited on.
        let stalled = Arc::clone(&inner.stalled);
        let sleep = move |_| {
            let stalled = Arc::clone(&stalled);
            poll_fn(move |cx| {
                if stalled.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .boxed()
        };
        let (_dir, store, data) = raced_late_by(&inner, &reads, Some(Box::new(sleep)));
        inner.stall_next_body((MIB / 4) as usize);
        assert_eq!(
            get_range(&store, &Path::from("a"), 0..MIB),
            data.slice(0..MIB as usize)
        );
        assert_eq!(
            inner.take_asked(),
            [Asked::Head, bounded(0..MIB), bounded(MIB / 4..MIB)]
        );
        let heard = reads.heard();
        let get = |bytes, racer, phase| Heard::Get {
            bytes,
            racer,
            phase,
        };
        assert!(heard.contains(&get(0..MIB, 0, "cancelled")), "{heard:?}");
        for phase in ["started", "answered", "finished", "landed"] {
            assert!(heard.contains(&get(MIB / 4..MIB, 1, phase)), "{heard:?}");
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_late_race_waits_for_its_timer() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let never = |_| pending().boxed();
        let timers: [Option<TestSleep>; 2] = [None, Some(Box::new(never))];
        for sleep in timers {
            let reads = late_racing();
            let inner = Arc::new(CountingStore::default());
            let (_dir, store, data) = raced_late_by(&inner, &reads, sleep);
            assert_eq!(
                get_range(&store, &Path::from("a"), 0..MIB),
                data.slice(0..MIB as usize)
            );
            assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..MIB)]);
            assert!(
                reads
                    .heard()
                    .iter()
                    .all(|heard| !matches!(heard, Heard::Get { racer: 1, .. })),
                "no late racer"
            );
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_get_is_heard_landed_after_it_finished() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
        let (_dir, store, _inner, _, data) = planned_by(&reads, Admit::Disk);
        assert_eq!(
            get_range(&store, &Path::from("a"), 0..MIB),
            data.slice(0..MIB as usize)
        );
        let phases: Vec<_> = reads
            .heard()
            .into_iter()
            .filter_map(|heard| match heard {
                Heard::Get { phase, .. } => Some(phase),
                Heard::DiskRead { .. } => None,
            })
            .collect();
        assert_eq!(phases, ["started", "answered", "finished", "landed"]);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_plan_the_cache_cant_carry_out_fetches_the_runs() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|_| ReadPlan::default());
        let (_dir, store, inner, _, data) = planned_by(&reads, Admit::Memory);
        assert_eq!(
            get_range(&store, &Path::from("a"), 0..2 * MIB),
            data.slice(0..2 * MIB as usize)
        );
        assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..2 * MIB)]);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_callers_intent_reaches_both_policies() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_read_policy_hears_reads_from_disk() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_reads_gets_go_out_before_its_disk_reads_are_waited_on() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
        let (_dir, store, _inner, _, data) = planned_by(&reads, Admit::Disk);
        let path = Path::from("a");
        get_range(&store, &path, 0..MIB);
        reads.heard.lock().unwrap().clear();

        assert_eq!(
            get_range(&store, &path, 0..2 * MIB),
            data.slice(0..2 * MIB as usize)
        );
        let heard = reads.heard();
        let at = |wanted: &Heard| heard.iter().position(|heard| heard == wanted).unwrap();
        let get_started = at(&Heard::Get {
            bytes: MIB..2 * MIB,
            racer: 0,
            phase: "started",
        });
        let disk_read_finished = at(&Heard::DiskRead {
            drive: 0,
            bytes: MIB as u32,
            phase: "finished",
        });
        assert!(get_started < disk_read_finished, "{heard:?}");
    });
}

/// `most` a second over GETs of a second caps the bytes in flight at `most`.
fn capped(inner: &Arc<CountingStore>, most: u64, merged_up_to: u64) -> (TempDir, T4Caching, Bytes) {
    let dir = tempfile::tempdir().unwrap();
    let store = builder(inner, &dir)
        .with_network_rate(most, Duration::from_secs(1))
        .with_waiting_gets_merged(merged_up_to)
        .build()
        .unwrap();
    let path = Path::from("a");
    let data = file_bytes(6 * MIB, 5);
    put(inner.as_ref(), &path, &data);
    block_on(store.head(&path)).unwrap();
    inner.take_asked();
    (dir, store, data)
}

fn read_at_once(store: &T4Caching, data: &Bytes, ranges: &[Range<u64>]) {
    let path = Path::from("a");
    let reads = block_on(join_all(
        ranges
            .iter()
            .map(|range| store.get_range(&path, range.clone())),
    ));
    for (read, range) in reads.into_iter().zip(ranges) {
        assert_eq!(
            read.unwrap(),
            data.slice(range.start as usize..range.end as usize)
        );
    }
}

fn block_start(block: u64) -> Range<u64> {
    block * MIB..block * MIB + 10
}

#[cfg(feature = "shuttle")]
#[test]
fn gets_past_the_cap_wait_for_a_place() {
    crate::io::test_schedule::TestSchedule::check(|| {
        for (most, out) in [(MIB, 1), (8 * MIB, 3)] {
            let inner = Arc::new(CountingStore::default());
            let (_dir, store, data) = capped(&inner, most, 0);
            inner.answer_gets_late(true);
            read_at_once(
                &store,
                &data,
                &[block_start(0), block_start(2), block_start(4)],
            );
            assert_eq!(
                inner.take_asked(),
                [
                    bounded(0..MIB),
                    bounded(2 * MIB..3 * MIB),
                    bounded(4 * MIB..5 * MIB)
                ]
            );
            assert_eq!(inner.most_gets_out(), out, "at most {most}");
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn waiting_gets_of_adjacent_blocks_go_as_one() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let inner = Arc::new(CountingStore::default());
        let (_dir, store, data) = capped(&inner, 3 * MIB, 4 * MIB);
        inner.answer_gets_late(true);
        let ranges = [
            3 * MIB..6 * MIB,
            block_start(1),
            block_start(0),
            block_start(2),
        ];
        read_at_once(&store, &data, &ranges);
        assert_eq!(
            inner.take_asked(),
            [bounded(3 * MIB..6 * MIB), bounded(0..3 * MIB)]
        );
        read_at_once(
            &store,
            &data,
            &[block_start(0), block_start(1), block_start(2)],
        );
        assert_eq!(inner.take_asked(), [], "each block cached");
    });
}

/// A straggler's race mustn't wait behind the straggler.
#[cfg(feature = "shuttle")]
#[test]
fn a_late_race_goes_beside_the_cap() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = late_racing();
        let inner = Arc::new(CountingStore::default());
        let stalled = Arc::clone(&inner.stalled);
        let sleep = move |_| {
            let stalled = Arc::clone(&stalled);
            poll_fn(move |cx| {
                if stalled.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .boxed()
        };
        let dir = tempfile::tempdir().unwrap();
        let store = builder(&inner, &dir)
            .with_read_policy(reads)
            .with_sleep(sleep)
            .with_network_rate(MIB, Duration::from_secs(1))
            .build()
            .unwrap();
        let data = file_bytes(3 * MIB, 7);
        put(inner.as_ref(), &Path::from("a"), &data);
        inner.take_asked();
        inner.stall_next_body((MIB / 4) as usize);
        assert_eq!(
            get_range(&store, &Path::from("a"), 0..MIB),
            data.slice(0..MIB as usize)
        );
        assert_eq!(
            inner.take_asked(),
            [Asked::Head, bounded(0..MIB), bounded(MIB / 4..MIB)]
        );
    });
}

fn agrees_with_its_offset(bytes: &Bytes, offset: u64) -> bool {
    bytes.as_ptr() as u64 % crate::PAGE_SIZE_U64 == offset % crate::PAGE_SIZE_U64
}

#[cfg(feature = "shuttle")]
#[test]
fn an_opened_file_reads_through_the_cache() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &data);

        let file = block_on(store.open(&path)).unwrap();
        assert_eq!(file.meta().size, 2 * MIB);
        assert_eq!(inner.take_asked(), [Asked::Head]);
        let bytes = block_on(file.read(10..MIB + 10, None)).unwrap();
        assert_eq!(bytes, data.slice(10..MIB as usize + 10));
        assert_eq!(inner.take_asked(), [bounded(0..2 * MIB)]);
        let bytes = block_on(file.read(5..2 * MIB, None)).unwrap();
        assert_eq!(bytes, data.slice(5..));
        assert_eq!(get_range(&store, &path, 5..15), data.slice(5..15));
        assert_eq!(inner.take_asked(), Vec::<Asked>::new());
        let bytes = block_on(file.read(MIB..10 * MIB, None)).unwrap();
        assert_eq!(bytes, data.slice(MIB as usize..));
        assert!(block_on(file.read(3 * MIB..3 * MIB + 1, None)).is_err());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_listed_file_opens_without_a_head() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &data);
        let listed = block_on(inner.head(&path)).unwrap();
        inner.take_asked();

        let file = store.open_listed(listed);
        assert_eq!(
            block_on(file.read(10..20, None)).unwrap(),
            data.slice(10..20)
        );
        assert_eq!(inner.take_asked(), [bounded(0..MIB)]);
        assert_eq!(
            get_range(&store, &path, MIB..MIB + 1),
            data.slice(MIB as usize..MIB as usize + 1)
        );
        assert_eq!(inner.take_asked(), [bounded(MIB..2 * MIB)]);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn an_opened_files_bytes_agree_with_their_offsets() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let size = 3 * MIB + 7;
        let data = file_bytes(size, 0);
        put(inner.as_ref(), &path, &data);

        let file = block_on(store.open(&path)).unwrap();
        for _ in 0..2 {
            for range in [0..1, 100..MIB + 100, 4095..4097, 3 * MIB + 3..size] {
                let bytes = block_on(file.read(range.clone(), None)).unwrap();
                assert_eq!(bytes, data.slice(range.start as usize..range.end as usize));
                assert!(agrees_with_its_offset(&bytes, range.start), "{range:?}");
            }
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_file_with_no_e_tag_is_read_from_the_store_aligned() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &data);
        let listed = ObjectMeta {
            e_tag: None,
            ..block_on(inner.head(&path)).unwrap()
        };
        inner.take_asked();

        let file = store.open_listed(listed);
        for _ in 0..2 {
            let bytes = block_on(file.read(4097..MIB + 3, None)).unwrap();
            assert_eq!(bytes, data.slice(4097..MIB as usize + 3));
            assert!(agrees_with_its_offset(&bytes, 4097));
            assert_eq!(inner.take_asked(), [bounded(4097..MIB + 3)]);
        }
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn an_opened_file_changed_since_fails_to_read() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (_dir, store, inner) = t4_caching();
        let path = Path::from("a");
        let old = file_bytes(2 * MIB, 0);
        put(inner.as_ref(), &path, &old);
        let file = block_on(store.open(&path)).unwrap();
        assert_eq!(block_on(file.read(0..10, None)).unwrap(), old.slice(0..10));

        put(inner.as_ref(), &path, &file_bytes(2 * MIB, 1));
        // Block 1 is remote: its fetch, on the old e_tag, finds the new file.
        let error = block_on(file.read(MIB..MIB + 10, None)).unwrap_err();
        assert!(
            matches!(error, object_store::Error::Precondition { .. }),
            "{error}"
        );
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn an_opened_files_intent_reaches_both_policies() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
        let (_dir, store, _inner, remote_reads, data) = planned_by(&reads, Admit::Memory);
        let file = block_on(store.open(&Path::from("a"))).unwrap();
        let bytes = block_on(file.read(0..MIB, Some(ReadIntent::Latency))).unwrap();
        assert_eq!(bytes, data.slice(0..MIB as usize));
        assert_eq!(*reads.intents.lock().unwrap(), [Some(ReadIntent::Latency)]);
        assert_eq!(
            *remote_reads.lock().unwrap(),
            [(0, Some(ReadIntent::Latency), true)]
        );
    });
}

#[cfg(not(feature = "shuttle"))]
mod processes;
#[cfg(feature = "shuttle")]
mod restarts;
#[cfg(feature = "shuttle")]
mod sharing;

#[cfg(feature = "shuttle")]
mod timing;
