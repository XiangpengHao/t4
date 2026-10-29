//! The cache under shuttle: reads, fills, evictions, moves to disk and
//! promotions racing on a block or two, on a simulated disk whose I/O
//! completes in any order, and resolutions racing on one path.
//!
//! Shuttle switches threads only at its own primitives: here the chunk
//! tables' state words and owners, the free bitmaps' locks, the path table's
//! id counter, the test policy's locks, and the I/O worker's channel and
//! completions. A bbtree operation runs as one step between them. And it
//! runs threads one at a time, so it finds the interleavings that break the
//! cache, not a memory ordering too weak: those rest on the `Acquire` and
//! `Release` pairs in `chunk_table.rs`.

use std::num::NonZeroU32;

use futures_util::FutureExt;
use shuttle::future::block_on;
use shuttle::scheduler::PctScheduler;

use super::test_policy::TestPolicy;
use super::{CacheableFile, MoveToDisk, ObjectCache};
use crate::cache::block_index::Location;
use crate::cache::cache_files::CacheFiles;
use crate::cache::policy::{Admit, DiskHit, FetchRuns, PlacementPolicy};
use crate::cache::{BlockKey, ChunkId};
use crate::io::io_worker::IoWorker;
use crate::io::simulated::SimDisk;
use crate::io::sync::{Arc, thread};

/// Every file is one block of this many bytes: enough to tell files apart,
/// little to copy.
const FILE_SIZE: u64 = 64;

/// Runs `test` under the schedules PCT explores.
fn explore(test: impl Fn() + Send + Sync + 'static) {
    let mut config = shuttle::Config::new();
    // The default 60 KiB stack is too small for bbtree's descent in a debug
    // build.
    config.stack_size = 256 * 1024;
    shuttle::Runner::new(PctScheduler::new(3, 1000), config).run(test);
}

/// A cache of `memory_chunks` chunks of memory and `disk_chunks` on
/// simulated disks, run by a [`TestPolicy`]: two disks, chunks striped over
/// them, if `disk_chunks` is even; else one.
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

/// The file at `path`, its one version.
fn resolve(cache: &ObjectCache, path: &'static [u8]) -> CacheableFile<'static> {
    cache.resolve(path, Some("v1"), FILE_SIZE).unwrap()
}

/// The bytes of the file at `path`: its first byte throughout.
fn contents(path: &[u8]) -> Vec<u8> {
    vec![path[0]; FILE_SIZE as usize]
}

/// Reads the file whole, its GETs planned by the default read policy, and
/// checks the read got the file's own bytes from a tier, or nothing and a
/// fetch of the file; then, if `fill`, caches what it fetched.
fn read(cache: &ObjectCache, file: &CacheableFile<'static>, fill: bool) {
    read_fetching(cache, file, fill);
}

/// [`read`], and whether the read fetched the file.
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
    true
}

/// Where the index says `file`'s block is.
fn location(cache: &ObjectCache, file: &CacheableFile<'_>) -> Option<Location> {
    cache.blocks.get(BlockKey::new(file.version().id(), 0))
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

/// Reads of one remote block at once fetch it once: one claims and fills
/// it, the others wait for that fetch and take the block from memory, or
/// hit it there.
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

/// Of concurrent reads of one block the placement policy discards, those
/// that find it in flight take its bytes from the fetch; none is cached.
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

/// A read given up on before it fills its fetch hands the block to the read
/// waiting for it, which fetches it itself.
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
        assert_eq!(cache.memory.free_count(), 2);
        read(&cache, &a, false);
        assert!(memory_chunk(&cache, &a).is_some());
    });
}

/// The race the chunk table's owner check is for: a read that found the
/// entry before eviction, its chunk then reused for another file.
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
            assert!(cache.evict_from_memory(chunk));
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
            spawn(&cache, move |cache| assert!(cache.evict_from_memory(chunk))),
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
            .map(|_| spawn(&cache, move |cache| cache.evict_from_memory(chunk)))
            .collect();
        let reader = spawn(&cache, move |cache| read(cache, &a, false));
        let evicted = join_all(evictors).into_iter().filter(|&e| e).count();
        reader.join().unwrap();
        assert_eq!(evicted, 1);
        assert_eq!(
            cache.memory.free_count(),
            1,
            "freed once, by the last to leave"
        );
        cache.check_at_rest().unwrap();
    });
}

/// Reads go on in memory while the block is written to disk, then from
/// disk, and those that found the memory entry finish there.
#[test]
fn a_read_racing_a_move_to_disk_reads_its_bytes() {
    explore(|| {
        let (cache, _policy) = cache(1, 1);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read(cache, &a, false);
            read(cache, &a, false);
        });
        let mover = spawn(&cache, move |cache| {
            block_on(cache.move_to_disk(chunk)).unwrap()
        });
        reader.join().unwrap();
        assert_eq!(mover.join().unwrap(), MoveToDisk::Moved);
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
            block_on(cache.move_to_disk(chunk)).unwrap()
        });
        let evictor = spawn(&cache, move |cache| cache.evict_from_memory(chunk));
        let reader = spawn(&cache, move |cache| read(cache, &a, false));
        let moved = mover.join().unwrap();
        let evicted = evictor.join().unwrap();
        reader.join().unwrap();
        // The block ends on disk only if the move won.
        assert_eq!(disk_chunk(&cache, &a).is_some(), moved == MoveToDisk::Moved);
        assert!(moved == MoveToDisk::Moved || evicted);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn of_two_moves_of_one_chunk_one_moves() {
    explore(|| {
        let (cache, _policy) = cache(1, 2);
        let a = resolve(&cache, b"a");
        read(&cache, &a, true);
        let chunk = memory_chunk(&cache, &a).unwrap();
        let outcomes = join_all((0..2).map(|_| {
            spawn(&cache, move |cache| {
                block_on(cache.move_to_disk(chunk)).unwrap()
            })
        }));
        let moved = outcomes
            .iter()
            .filter(|&&outcome| outcome == MoveToDisk::Moved)
            .count();
        assert_eq!(moved, 1, "{outcomes:?}");
        cache.check_at_rest().unwrap();
        assert_eq!(cache.disk.chunks().free_count(), 1, "one copy on disk");
    });
}

/// The owner check for disk chunks: a read that found the disk entry, the
/// chunk then evicted and written with another file's block.
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
            assert!(cache.evict_from_disk(on_disk));
            // Straight to disk: full while the reader still holds the chunk.
            read(cache, &b, true);
        });
        join_all([reader, evictor]);
        assert_eq!(location(&cache, &a), None);
        cache.check_at_rest().unwrap();
    });
}

/// A move dropped while its write is in flight: the chunks it held go free
/// once the write lands, and not before. Were the disk chunk free sooner,
/// another block's move could take it, and the dropped write land over that
/// block's bytes.
#[test]
fn a_dropped_move_frees_its_chunks_after_its_write() {
    explore(|| {
        let (cache, _policy) = cache(2, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        read(&cache, &a, true);
        read(&cache, &b, true);
        let (chunk_a, chunk_b) = (
            memory_chunk(&cache, &a).unwrap(),
            memory_chunk(&cache, &b).unwrap(),
        );
        // One poll submits the write; dropping the future then is a caller
        // giving up on it, unless that poll finished the move.
        let moved_a = cache.move_to_disk(chunk_a).now_or_never();
        // The disk is full while `a`'s write may still land.
        let moved_b = block_on(cache.move_to_disk(chunk_b)).unwrap();
        read(&cache, &b, false);
        read(&cache, &a, false);
        let mut waits = 0;
        while cache.check_at_rest().is_err() {
            waits += 1;
            assert!(waits < 1000, "the dropped write's chunks never went free");
            thread::yield_now();
        }
        match moved_a {
            Some(outcome) => assert_eq!(outcome.unwrap(), MoveToDisk::Moved),
            None => assert_eq!(location(&cache, &a), Some(Location::Memory(chunk_a))),
        }
        assert!(matches!(moved_b, MoveToDisk::Moved | MoveToDisk::DiskFull));
        read(&cache, &b, false);
    });
}

/// A fill dropped while the move of its victim to disk is being written:
/// the policy, which forgot the victim when it picked it, gets it back, so
/// memory can evict it for the next block. Were it lost, it would hold its
/// chunk for good, and memory would take no other block.
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
        // `b`'s fill picks `a` and writes it to disk. One poll submits the
        // write; dropping the fill then is a caller giving up on it, unless
        // that poll finished it.
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
        assert!(
            memory_chunk(&cache, &c).is_some(),
            "memory made room for it"
        );
    });
}

/// Two insertions into a full memory, each evicting the other's block to
/// disk, a read of one meanwhile.
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

/// A read promoting a block from disk while the disk evicts it: the read
/// takes the block's bytes either way.
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
                cache.evict_from_disk(on_disk);
            }),
        ]);
        cache.check_at_rest().unwrap();
    });
}

/// Two reads promoting one block: one copy wins memory, the other goes back.
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
        assert_eq!(cache.disk.chunks().free_count(), 1);
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
