use super::*;
use crate::cache::policy::FixedPlacementPolicy;

fn on_disk(inner: &Arc<CountingStore>, dir: &TempDir) -> T4CachingBuilder {
    builder(inner, dir).with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
}

fn read_whole(cache: &T4Caching, meta: &ObjectMeta) -> Bytes {
    let bytes = block_on(cache.open_listed(meta.clone()).read(0..meta.size, None)).unwrap();
    cache.cache_store().owned().unwrap().wait_for_disk_writes();
    bytes
}

#[test]
fn a_cache_built_again_serves_the_disk_tier_its_files_held() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let path = Path::from("a");
        let data = file_bytes(3 * MIB + 5, 21);
        put(inner.as_ref(), &path, &data);
        let meta = block_on(inner.head(&path)).unwrap();
        let first = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&first, &meta), data);
        drop(first);
        assert!(log_at(&dir).exists());
        inner.take_asked();

        let again = on_disk(&inner, &dir).build().unwrap();
        assert!(log_at(&dir).exists(), "rebased before writes");
        assert_eq!(block_on(again.known_size(&path)), Some(meta.size));
        assert_eq!(read_whole(&again, &meta), data);
        assert_eq!(inner.take_asked(), [], "served from disk");
        again
            .cache_store()
            .owned()
            .unwrap()
            .check_at_rest()
            .unwrap();

        drop(again);
        let third = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&third, &meta), data);
        assert_eq!(inner.take_asked(), [], "kept again");
    });
}

#[test]
fn memorys_blocks_go_with_the_cache() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let path = Path::from("a");
        let data = file_bytes(MIB, 22);
        put(inner.as_ref(), &path, &data);
        let meta = block_on(inner.head(&path)).unwrap();
        let first = builder(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&first, &meta), data);
        drop(first);
        inner.take_asked();

        let again = builder(&inner, &dir).build().unwrap();
        assert_eq!(block_on(again.known_size(&path)), None);
        assert_eq!(read_whole(&again, &meta), data);
        assert_eq!(inner.take_asked(), [bounded(0..MIB)]);
    });
}

/// Its chunks are other chunks, so the log can't be replayed.
#[test]
fn a_cache_of_another_capacity_starts_cold() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let path = Path::from("a");
        let data = file_bytes(2 * MIB, 23);
        put(inner.as_ref(), &path, &data);
        let meta = block_on(inner.head(&path)).unwrap();
        let first = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&first, &meta), data);
        drop(first);
        inner.take_asked();

        let larger = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
            .with_memory_capacity(4 * MIB)
            .with_disk_file(dir.path().join("cache"), 16 * MIB)
            .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
            .build()
            .unwrap();
        assert!(log_at(&dir).exists());
        assert_eq!(block_on(larger.known_size(&path)), None);
        assert_eq!(read_whole(&larger, &meta), data);
        assert_eq!(inner.take_asked(), [bounded(0..2 * MIB)]);
    });
}

/// Until a read names the new version, the size the cache knows is the old one's.
#[test]
fn a_file_written_over_between_caches_is_fetched_again() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let path = Path::from("a");
        let old = file_bytes(2 * MIB, 24);
        put(inner.as_ref(), &path, &old);
        let first = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(get_range(&first, &path, 0..2 * MIB), old);
        drop(first);
        let new = file_bytes(3 * MIB, 25);
        put(inner.as_ref(), &path, &new);
        inner.take_asked();

        let again = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(block_on(again.known_size(&path)), Some(2 * MIB));
        assert_eq!(get_range(&again, &path, 0..3 * MIB), new);
        assert_eq!(inner.take_asked(), [Asked::Head, bounded(0..3 * MIB)]);
        assert_eq!(block_on(again.known_size(&path)), Some(3 * MIB));
    });
}

#[test]
fn a_cache_shut_down_keeps_its_disk_tier_and_writes_no_more() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(CountingStore::default());
        let (a, b) = (Path::from("a"), Path::from("b"));
        let (a_data, b_data) = (file_bytes(2 * MIB, 26), file_bytes(MIB, 27));
        put(inner.as_ref(), &a, &a_data);
        put(inner.as_ref(), &b, &b_data);
        let (a_meta, b_meta) = (
            block_on(inner.head(&a)).unwrap(),
            block_on(inner.head(&b)).unwrap(),
        );
        let cache = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&cache, &a_meta), a_data);
        cache.shut_down().unwrap();
        assert!(log_at(&dir).exists(), "kept at once");
        inner.take_asked();

        assert_eq!(read_whole(&cache, &a_meta), a_data);
        assert_eq!(inner.take_asked(), [], "served from disk still");
        for _ in 0..2 {
            assert_eq!(read_whole(&cache, &b_meta), b_data);
            assert_eq!(
                inner.take_asked(),
                [bounded(0..MIB)],
                "written to disk no more"
            );
        }
        drop(cache);

        let again = on_disk(&inner, &dir).build().unwrap();
        assert_eq!(read_whole(&again, &a_meta), a_data);
        assert_eq!(read_whole(&again, &b_meta), b_data);
        assert_eq!(
            inner.take_asked(),
            [bounded(0..MIB)],
            "a kept, b never cached"
        );
    });
}
