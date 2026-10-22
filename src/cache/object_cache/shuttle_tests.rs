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
use crate::cache::cache_file::CacheFile;
use crate::cache::policy::{Admit, CachePolicy, DiskHit};
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

/// A cache of `memory_chunks` chunks of memory and `disk_chunks` on a
/// simulated disk, run by a [`TestPolicy`].
fn cache(memory_chunks: u32, disk_chunks: u32) -> (Arc<ObjectCache>, Arc<TestPolicy>) {
    let io = IoWorker::simulated(NonZeroU32::new(4).unwrap(), SimDisk::new()).unwrap();
    let disk = CacheFile::on(io, NonZeroU32::new(disk_chunks).unwrap()).unwrap();
    let policy = Arc::new(TestPolicy::new());
    let memory = NonZeroU32::new(memory_chunks).unwrap();
    let cache = ObjectCache::new(memory, disk, Arc::clone(&policy) as Arc<dyn CachePolicy>);
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

/// Reads the file whole, and checks the read got the file's own bytes from
/// a tier, or nothing and a fetch of the file; then, if `fill`, caches what
/// it fetched.
fn read(cache: &ObjectCache, file: &CacheableFile<'static>, fill: bool) {
    let mut out = Vec::new();
    let read = block_on(cache.read(file, 0..FILE_SIZE, &mut out)).unwrap();
    if read.fetches().is_empty() {
        assert_eq!(
            out,
            contents(file.path),
            "a hit reads its own block's bytes"
        );
        return;
    }
    assert_eq!(read.fetches().len(), 1);
    assert_eq!(read.fetches()[0], 0..FILE_SIZE, "a miss fetches the file");
    if fill {
        block_on(cache.cache_fetched(&read, 0, &contents(file.path))).unwrap();
    }
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

/// Whether the index and both tiers agree: each entry names a chunk that
/// holds its block, and each chunk in use, in either tier, has an entry
/// naming it. True once every thread and I/O is done.
fn in_step(cache: &ObjectCache, files: &[CacheableFile<'_>]) -> bool {
    let (mut in_memory, mut on_disk) = (0, 0);
    for file in files {
        let block = BlockKey::new(file.version().id(), 0);
        let holds_its_block = match location(cache, file) {
            Some(Location::Memory(chunk)) => {
                in_memory += 1;
                cache.memory.read(chunk, block).is_some()
            }
            Some(Location::Disk(chunk)) => {
                on_disk += 1;
                cache.disk.chunks().read(chunk, block).is_some()
            }
            None => true,
        };
        if !holds_its_block {
            return false;
        }
    }
    let memory = &cache.memory;
    let disk = cache.disk.chunks();
    memory.chunk_count() - memory.free_count() == in_memory
        && disk.chunk_count() - disk.free_count() == on_disk
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
fn racing_fills_of_one_block_take_one_chunk() {
    explore(|| {
        let (cache, _policy) = cache(3, 1);
        let a = resolve(&cache, b"a");
        join_all((0..3).map(|_| spawn(&cache, move |cache| read(cache, &a, true))));
        assert!(in_step(&cache, &[a]));
        assert_eq!(cache.memory.free_count(), 2);
        read(&cache, &a, false);
        assert!(memory_chunk(&cache, &a).is_some());
    });
}

/// The race the chunk table's owner check is for: a read that found the
/// entry before eviction, its chunk then reused for another file.
#[test]
fn a_read_racing_eviction_and_reuse_reads_its_bytes_or_misses() {
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
        assert!(in_step(&cache, &[a, b]));
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
        assert!(in_step(&cache, &[a]));
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
        assert!(in_step(&cache, &[a]));
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
        assert!(in_step(&cache, &[a]));
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
        assert!(in_step(&cache, &[a]));
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
        assert!(in_step(&cache, &[a]));
        assert_eq!(cache.disk.chunks().free_count(), 1, "one copy on disk");
    });
}

/// The owner check for disk chunks: a read that found the disk entry, the
/// chunk then evicted and written with another file's block.
#[test]
fn a_read_racing_eviction_from_disk_and_reuse_reads_its_bytes_or_misses() {
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
        assert!(in_step(&cache, &[a, b]));
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
        while !in_step(&cache, &[a, b]) {
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
        assert!(in_step(&cache, &[a, b, c]));
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
        assert!(in_step(&cache, &[a]));
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
        assert!(in_step(&cache, &[a]));
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
