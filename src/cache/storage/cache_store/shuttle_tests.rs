//! Shuttle runs one thread at a time: it finds bad interleavings, not too-weak memory orderings.

use std::num::NonZeroU32;

use crate::io::test_schedule::TestSchedule;
use futures_util::FutureExt;
use shuttle::future::block_on;

use super::test_policy::TestPolicy;
use super::{CacheableFile, ObjectCache};
use crate::cache::policy::{Admit, DiskHit, FetchRuns, PlacementPolicy};
use crate::cache::storage::CacheFiles;
use crate::cache::storage::index::Location;
use crate::cache::{BlockKey, ChunkId};
use crate::io::io_worker::IoWorker;
use crate::io::simulated::SimDisk;
use crate::io::sync::{Arc, thread};

/// Enough to tell files apart, little to copy.
const FILE_SIZE: u64 = 64;

fn explore(test: impl Fn() + Send + Sync + 'static) {
    TestSchedule::iterations(500, test);
}

fn cache(memory_chunks: u32, disk_chunks: u32) -> (Arc<ObjectCache>, Arc<TestPolicy>) {
    let files = if disk_chunks.is_multiple_of(2) { 2 } else { 1 };
    let disks = (0..files).map(|_| SimDisk::new()).collect();
    let io = IoWorker::simulated(NonZeroU32::new(4).unwrap(), disks).unwrap();
    let files = NonZeroU32::new(files).unwrap();
    let disk = CacheFiles::on(io, files, disk_chunks / files).unwrap();
    let policy = Arc::new(TestPolicy::new());
    let memory = NonZeroU32::new(memory_chunks).unwrap();
    let cache = ObjectCache::new(
        memory,
        disk,
        Arc::clone(&policy) as Arc<dyn PlacementPolicy>,
    );
    (Arc::new(cache.unwrap()), policy)
}

fn resolve(cache: &ObjectCache, path: &'static [u8]) -> CacheableFile<'static> {
    cache.resolve(path, Some("v1"), FILE_SIZE).unwrap()
}

fn contents(path: &[u8]) -> Vec<u8> {
    vec![path[0]; FILE_SIZE as usize]
}

fn read(cache: &ObjectCache, file: &CacheableFile<'static>, fill: bool) {
    read_fetching(cache, file, fill);
}

fn read_fetching(cache: &ObjectCache, file: &CacheableFile<'static>, fill: bool) -> bool {
    let mut read = block_on(cache.read(file, 0..FILE_SIZE, None, &FetchRuns::default())).unwrap();
    if read.fetches().is_empty() {
        assert_eq!(
            read.into_bytes().unwrap(),
            contents(file.path),
            "a hit reads its own block's bytes"
        );
        return false;
    }
    assert_eq!(read.fetches().len(), 1);
    assert_eq!(
        read.fetches()[0],
        0..FILE_SIZE,
        "a remote read fetches the file"
    );
    if fill {
        block_on(cache.take_fetched(&mut read, 0..FILE_SIZE, &contents(file.path))).unwrap();
    }
    cache.tiers.pending.wait_for_idle();
    true
}

fn location(cache: &ObjectCache, file: &CacheableFile<'_>) -> Option<Location> {
    cache
        .tiers
        .blocks
        .get(BlockKey::new(file.version().id(), 0))
}

fn memory_chunk(cache: &ObjectCache, file: &CacheableFile<'_>) -> Option<ChunkId> {
    match location(cache, file) {
        Some(Location::Memory(chunk)) => Some(chunk),
        _ => None,
    }
}

fn disk_chunk(cache: &ObjectCache, file: &CacheableFile<'_>) -> Option<ChunkId> {
    match location(cache, file) {
        Some(Location::Disk(chunk)) => Some(chunk),
        _ => None,
    }
}

fn spawn<T: Send + 'static>(
    cache: &Arc<ObjectCache>,
    job: impl FnOnce(&ObjectCache) -> T + Send + 'static,
) -> thread::JoinHandle<T> {
    let cache = Arc::clone(cache);
    thread::spawn(move || job(&cache))
}

fn join_all<T>(handles: impl IntoIterator<Item = thread::JoinHandle<T>>) -> Vec<T> {
    handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect()
}

#[test]
fn concurrent_reads_of_one_remote_block_fetch_it_once() {
    explore(|| {
        let (cache, _policy) = cache(2, 1);
        let a = resolve(&cache, b"a");
        let fetched =
            join_all((0..3).map(|_| spawn(&cache, move |cache| read_fetching(cache, &a, true))));
        assert_eq!(fetched.into_iter().filter(|&fetched| fetched).count(), 1);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn concurrent_reads_of_one_discarded_block_take_its_bytes_from_its_fetch() {
    explore(|| {
        let (cache, policy) = cache(2, 1);
        policy.set_admit(Admit::Discard);
        let a = resolve(&cache, b"a");
        let fetched =
            join_all((0..3).map(|_| spawn(&cache, move |cache| read_fetching(cache, &a, true))));
        assert!(fetched.contains(&true), "one fetched it");
        assert_eq!(location(&cache, &a), None, "nothing cached");
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_read_given_up_on_hands_its_block_to_the_read_waiting() {
    explore(|| {
        let (cache, _policy) = cache(2, 1);
        let a = resolve(&cache, b"a");
        join_all([
            spawn(&cache, move |cache| read(cache, &a, false)),
            spawn(&cache, move |cache| read(cache, &a, true)),
        ]);
        assert!(
            memory_chunk(&cache, &a).is_some(),
            "the read that fills caches the block"
        );
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn racing_fills_of_one_block_take_one_chunk() {
    explore(|| {
        let (cache, _policy) = cache(3, 1);
        let a = resolve(&cache, b"a");
        join_all((0..3).map(|_| spawn(&cache, move |cache| read(cache, &a, true))));
        cache.check_at_rest().unwrap();
        assert_eq!(cache.tiers.memory.free_count(), 2);
        read(&cache, &a, false);
        assert!(memory_chunk(&cache, &a).is_some());
    });
}

/// The race the chunk table's owner check is for.
#[test]
fn a_read_racing_eviction_and_reuse_reads_its_bytes_or_reads_remote() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read(cache, &a, false);
            read(cache, &a, false);
        });
        let evictor = spawn(&cache, move |cache| {
            assert!(cache.tiers.evict_from_memory(chunk));
            read(cache, &b, true);
        });
        join_all([reader, evictor]);
        assert_eq!(location(&cache, &a), None);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn eviction_racing_a_refill_keeps_entries_and_chunks_in_step() {
    explore(|| {
        let (cache, _policy) = cache(2, 1);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        join_all([
            spawn(&cache, move |cache| {
                assert!(cache.tiers.evict_from_memory(chunk))
            }),
            spawn(&cache, move |cache| read(cache, &a, true)),
            spawn(&cache, move |cache| read(cache, &a, false)),
        ]);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn of_two_evictions_of_one_chunk_one_evicts() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let evictors: Vec<_> = (0..2)
            .map(|_| spawn(&cache, move |cache| cache.tiers.evict_from_memory(chunk)))
            .collect();
        let reader = spawn(&cache, move |cache| read(cache, &a, false));
        let evicted = join_all(evictors).into_iter().filter(|&e| e).count();
        reader.join().unwrap();
        assert_eq!(evicted, 1);
        assert_eq!(
            cache.tiers.memory.free_count(),
            1,
            "freed once, by the last to leave"
        );
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_read_racing_a_move_to_disk_reads_its_bytes() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let reader = spawn(&cache, move |cache| {
            assert!(!read_fetching(cache, &a, false));
            assert!(!read_fetching(cache, &a, false));
        });
        let mover = spawn(&cache, move |cache| {
            assert!(
                cache
                    .tiers
                    .pending
                    .enqueue(chunk, BlockKey::new(a.version().id(), 0))
            );
            cache.wait_for_disk_writes();
        });
        join_all([reader, mover]);
        assert!(disk_chunk(&cache, &a).is_some());
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_move_racing_an_eviction_from_memory_keeps_the_tiers_in_step() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let mover = spawn(&cache, move |cache| {
            assert!(
                cache
                    .tiers
                    .pending
                    .enqueue(chunk, BlockKey::new(a.version().id(), 0))
            );
            cache.wait_for_disk_writes();
        });
        let evictor = spawn(&cache, move |cache| {
            cache.tiers.evict_from_memory(chunk);
        });
        let reader = spawn(&cache, move |cache| read(cache, &a, false));
        join_all([mover, evictor, reader]);
        assert!(matches!(
            location(&cache, &a),
            None | Some(Location::Disk(_))
        ));
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn concurrent_requests_to_move_one_block_leave_one_disk_copy() {
    explore(|| {
        let (cache, _policy) = cache(1, 2);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        join_all((0..2).map(|_| {
            spawn(&cache, move |cache| {
                assert!(
                    cache
                        .tiers
                        .pending
                        .enqueue(chunk, BlockKey::new(a.version().id(), 0))
                );
                cache.wait_for_disk_writes();
            })
        }));
        assert!(disk_chunk(&cache, &a).is_some());
        cache.check_at_rest().unwrap();
        assert_eq!(
            cache.tiers.disk.chunks().free_count(),
            1,
            "one copy on disk"
        );
    });
}

#[test]
fn concurrent_requests_to_move_distinct_blocks_use_distinct_disk_chunks() {
    explore(|| {
        let (cache, _policy) = cache(2, 2);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        read(&cache, &a, true);
        read(&cache, &b, true);
        let chunk_a = memory_chunk(&cache, &a).unwrap();
        let chunk_b = memory_chunk(&cache, &b).unwrap();
        join_all(
            [(a, chunk_a), (b, chunk_b)]
                .into_iter()
                .map(|(file, chunk)| {
                    spawn(&cache, move |cache| {
                        assert!(
                            cache
                                .tiers
                                .pending
                                .enqueue(chunk, BlockKey::new(file.version().id(), 0))
                        );
                        cache.wait_for_disk_writes();
                    })
                }),
        );
        assert_ne!(
            disk_chunk(&cache, &a).unwrap(),
            disk_chunk(&cache, &b).unwrap()
        );
        assert!(!read_fetching(&cache, &a, false));
        assert!(!read_fetching(&cache, &b, false));
        cache.check_at_rest().unwrap();
        assert_eq!(cache.tiers.disk.chunks().free_count(), 0);
    });
}

/// The race the owner check for disk chunks is for.
#[test]
fn a_read_racing_eviction_from_disk_and_reuse_reads_its_bytes_or_reads_remote() {
    explore(|| {
        let (cache, policy) = cache(1, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        policy.set_admit(Admit::Disk);
        read(&cache, &a, true);
        let on_disk = disk_chunk(&cache, &a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read(cache, &a, false);
            read(cache, &a, false);
        });
        let evictor = spawn(&cache, move |cache| {
            assert!(cache.tiers.evict_from_disk(on_disk));
            // Straight to disk: full while the reader still holds the chunk.
            read(cache, &b, true);
        });
        join_all([reader, evictor]);
        assert_eq!(location(&cache, &a), None);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_dropped_batch_keeps_its_chunks_until_its_io_completes() {
    explore(|| {
        let (disk, pause) = SimDisk::with_paused_writes();
        let io = IoWorker::simulated(NonZeroU32::new(4).unwrap(), vec![disk]).unwrap();
        let disk = CacheFiles::on(io, NonZeroU32::MIN, 1).unwrap();
        let policy = Arc::new(TestPolicy::new());
        let cache = ObjectCache::new(NonZeroU32::new(2).unwrap(), disk, policy.clone()).unwrap();
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        read(&cache, &a, true);
        read(&cache, &b, true);
        let chunk_a = memory_chunk(&cache, &a).unwrap();
        let chunk_b = memory_chunk(&cache, &b).unwrap();
        let key_a = BlockKey::new(a.version().id(), 0);
        let key_b = BlockKey::new(b.version().id(), 0);
        assert_eq!(policy.pick_memory_victims(2).len(), 2);

        assert!(
            cache
                .tiers
                .write_disk_batch(&[(chunk_a, key_a)])
                .now_or_never()
                .is_none()
        );
        pause.wait_until_started();
        assert!(cache.tiers.evict_from_memory(chunk_a));
        assert_eq!(
            cache.tiers.memory.free_count(),
            0,
            "the I/O still holds a's source"
        );
        assert!(cache.tiers.memory.allocate().is_none());
        assert_eq!(
            cache.tiers.disk.chunks().free_count(),
            0,
            "the I/O still holds its destination"
        );
        assert!(cache.tiers.allocate_disk().is_none());

        assert!(cache.tiers.pending.enqueue(chunk_b, key_b));
        cache.wait_for_disk_writes();
        assert_eq!(memory_chunk(&cache, &b), Some(chunk_b));
        assert_eq!(policy.queued(crate::cache::policy::Tier::Memory), [chunk_b]);
        pause.resume();
        let mut waits = 0;
        while cache.tiers.disk.chunks().free_count() != 1 || cache.tiers.memory.free_count() != 1 {
            waits += 1;
            assert!(waits < 1000, "the completed I/O's chunks were not released");
            thread::yield_now();
        }
        assert_eq!(policy.pick_memory_victims(1).len(), 1);
        assert!(cache.tiers.pending.enqueue(chunk_b, key_b));
        cache.wait_for_disk_writes();
        assert!(!read_fetching(&cache, &b, false));
        assert!(disk_chunk(&cache, &b).is_some());
        assert_eq!(location(&cache, &a), None);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn giving_up_a_read_does_not_cancel_its_queued_write() {
    explore(|| {
        let (disk, pause) = SimDisk::with_paused_writes();
        let io = IoWorker::simulated(NonZeroU32::new(4).unwrap(), vec![disk]).unwrap();
        let disk = CacheFiles::on(io, NonZeroU32::MIN, 1).unwrap();
        let policy = Arc::new(TestPolicy::new());
        policy.set_admit(Admit::Disk);
        let cache = ObjectCache::new(NonZeroU32::MIN, disk, policy).unwrap();
        let a = resolve(&cache, b"a");
        let mut read = block_on(cache.read(&a, 0..FILE_SIZE, None, &FetchRuns::default())).unwrap();
        assert!(
            cache
                .take_fetched(&mut read, 0..FILE_SIZE, &contents(a.path))
                .now_or_never()
                .unwrap()
                .is_ok()
        );
        pause.wait_until_started();
        let chunk = memory_chunk(&cache, &a).unwrap();
        drop(read);
        assert!(cache.tiers.evict_from_memory(chunk));
        assert!(cache.tiers.memory.allocate().is_none());
        assert!(cache.tiers.allocate_disk().is_none());
        pause.resume();
        cache.wait_for_disk_writes();
        assert_eq!(cache.tiers.memory.free_count(), 1);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);
        assert_eq!(location(&cache, &a), None);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_dropped_eviction_gives_its_victim_back() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let (a, b, c) = (
            resolve(&cache, b"a"),
            resolve(&cache, b"b"),
            resolve(&cache, b"c"),
        );
        read(&cache, &a, true);
        // Giving up the fill must not give up the victim's queued write.
        let mut remote =
            block_on(cache.read(&b, 0..FILE_SIZE, None, &FetchRuns::default())).unwrap();
        let _ = cache
            .take_fetched(&mut remote, 0..FILE_SIZE, &contents(b.path))
            .now_or_never();
        let mut waits = 0;
        while cache.check_at_rest().is_err() {
            waits += 1;
            assert!(waits < 1000, "the dropped write's chunks never went free");
            thread::yield_now();
        }
        read(&cache, &c, true);
        if memory_chunk(&cache, &c).is_none() {
            read(&cache, &c, true);
        }
        assert!(
            memory_chunk(&cache, &c).is_some(),
            "memory made room for it"
        );
    });
}

#[test]
fn insertions_racing_for_a_full_memory_keep_the_tiers_in_step() {
    explore(|| {
        let (cache, _policy) = cache(1, 2);
        let (a, b, c) = (
            resolve(&cache, b"a"),
            resolve(&cache, b"b"),
            resolve(&cache, b"c"),
        );
        read(&cache, &c, true);
        join_all([
            spawn(&cache, move |cache| read(cache, &a, true)),
            spawn(&cache, move |cache| read(cache, &b, true)),
            spawn(&cache, move |cache| read(cache, &c, false)),
        ]);
        cache.check_at_rest().unwrap();
        for file in [a, b, c] {
            read(&cache, &file, false);
        }
    });
}

#[test]
fn a_promotion_racing_an_eviction_from_disk_keeps_the_tiers_in_step() {
    explore(|| {
        let (cache, policy) = cache(1, 1);
        let a = resolve(&cache, b"a");
        policy.set_admit(Admit::Disk);
        read(&cache, &a, true);
        let on_disk = disk_chunk(&cache, &a).unwrap();
        policy.set_disk_hit(DiskHit::Promote);
        join_all([
            spawn(&cache, move |cache| read(cache, &a, false)),
            spawn(&cache, move |cache| {
                cache.tiers.evict_from_disk(on_disk);
            }),
        ]);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn two_promotions_of_one_block_leave_one_copy() {
    explore(|| {
        let (cache, policy) = cache(2, 1);
        let a = resolve(&cache, b"a");
        policy.set_admit(Admit::Disk);
        read(&cache, &a, true);
        policy.set_disk_hit(DiskHit::Promote);
        join_all((0..2).map(|_| spawn(&cache, move |cache| read(cache, &a, false))));
        cache.check_at_rest().unwrap();
        assert!(memory_chunk(&cache, &a).is_some());
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);
    });
}

#[test]
fn racing_resolutions_of_one_version_share_one_id() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let ids =
            join_all((0..3).map(|_| spawn(&cache, |cache| resolve(cache, b"a").version().id())));
        assert!(ids.iter().all(|&id| id == ids[0]), "{ids:?}");
        assert_eq!(resolve(&cache, b"a").version().id(), ids[0]);
    });
}

#[test]
fn racing_resolutions_of_two_versions_never_share_an_id() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let resolved = join_all(["v1", "v2", "v1"].into_iter().map(|e_tag| {
            spawn(&cache, move |cache| {
                let file = cache.resolve(b"a", Some(e_tag), FILE_SIZE).unwrap();
                (e_tag, file.version().id())
            })
        }));
        for (e_tag, id) in &resolved {
            for (other_e_tag, other_id) in &resolved {
                assert!(e_tag == other_e_tag || id != other_id, "{resolved:?}");
            }
        }
    });
}

#[test]
fn disk_admissions_wait_readably_in_memory_until_sixteen_are_ready() {
    TestSchedule::check(|| {
        let (disk, release_writes) = SimDisk::with_paused_writes();
        let io = IoWorker::simulated(NonZeroU32::new(16).unwrap(), vec![disk]).unwrap();
        let disk = CacheFiles::on(io, NonZeroU32::MIN, 32).unwrap();
        let policy = Arc::new(TestPolicy::new());
        policy.set_admit(Admit::Disk);
        let cache = ObjectCache::new(NonZeroU32::new(16).unwrap(), disk, policy).unwrap();
        for index in 0..16 {
            let path = format!("{index}");
            let file = cache
                .resolve(path.as_bytes(), Some("v1"), FILE_SIZE)
                .unwrap();
            let mut read =
                block_on(cache.read(&file, 0..FILE_SIZE, None, &FetchRuns::default())).unwrap();
            let bytes = vec![index as u8; FILE_SIZE as usize];
            assert!(
                cache
                    .take_fetched(&mut read, 0..FILE_SIZE, &bytes)
                    .now_or_never()
                    .unwrap()
                    .is_ok()
            );
            assert_eq!(read.into_bytes().unwrap(), bytes);
            let key = BlockKey::new(file.version().id(), 0);
            let Some(Location::Memory(chunk)) = cache.tiers.blocks.get(key) else {
                panic!("the write is still paused");
            };
            assert_eq!(
                &cache.tiers.memory.read(chunk, key).unwrap().bytes()[..bytes.len()],
                bytes
            );
            if index < 15 {
                assert_eq!(
                    cache.tiers.disk.chunks().free_count(),
                    32,
                    "no partial batch goes"
                );
            }
        }
        release_writes.wait_until_started();
        release_writes.resume();
        cache.wait_for_disk_writes();
        assert_eq!(cache.tiers.disk.chunks().free_count(), 16);
        assert_eq!(cache.tiers.memory.free_count(), 16);
        cache.check_at_rest().unwrap();
    });
}
