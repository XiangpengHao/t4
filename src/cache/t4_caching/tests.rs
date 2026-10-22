use std::sync::Mutex;

use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::{ListResult, ObjectStoreExt};
use pollster::block_on;
use tempfile::TempDir;

use super::*;

const MIB: u64 = BLOCK_SIZE_U64;

/// A request the cache made of the store under it.
#[derive(Clone, Debug, PartialEq)]
enum Asked {
    Head,
    Get(Option<GetRange>),
}

/// An in-memory store that records every get the cache makes of it.
#[derive(Debug, Default)]
struct CountingStore {
    store: InMemory,
    asked: Mutex<Vec<Asked>>,
}

impl CountingStore {
    /// What the cache asked since the last call.
    fn take_asked(&self) -> Vec<Asked> {
        std::mem::take(&mut self.asked.lock().unwrap())
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
        self.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(location, opts).await
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
        self.store.delete_stream(locations)
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
        self.store.copy_opts(from, to, options).await
    }
}

/// A builder of a cache of 4 MiB of memory and 8 on disk over `inner`, in
/// `dir`.
fn builder(inner: &Arc<CountingStore>, dir: &TempDir) -> T4CachingBuilder {
    T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_path(dir.path().join("cache"))
        .with_disk_capacity(8 * MIB)
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
fn a_builder_needs_both_capacities_and_a_disk_path() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let store = || T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>);
    let path = dir.path().join("cache");
    let incomplete = [
        store().with_disk_path(&path).with_disk_capacity(MIB),
        store().with_memory_capacity(MIB).with_disk_capacity(MIB),
        store().with_memory_capacity(MIB).with_disk_path(&path),
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

#[test]
fn a_custom_policy_is_built_for_the_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(CountingStore::default());
    let sizes = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&sizes);
    let store = builder(&inner, &dir)
        .with_policy(move |tiers: TierSizes| {
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
/// changed behind it reads as it was, until a miss catches the change.
#[test]
fn a_file_changed_behind_the_cache_is_caught_by_a_miss() {
    let (_dir, store, inner) = t4_caching();
    let path = Path::from("a");
    let old = file_bytes(2 * MIB, 0);
    put(inner.as_ref(), &path, &old);
    get_range(&store, &path, 0..10);

    let new = file_bytes(2 * MIB, 1);
    put(inner.as_ref(), &path, &new);
    assert_eq!(get_range(&store, &path, 0..10), old.slice(0..10), "a hit");
    // Block 1 is missing: its fetch, on the old e_tag, finds the new file.
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
