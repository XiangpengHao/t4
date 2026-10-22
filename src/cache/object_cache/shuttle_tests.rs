//! The cache under shuttle: reads, fills, moves to disk and evictions racing
//! on one block, on a simulated disk whose I/O completes in any order, and
//! resolutions racing on one path.
//!
//! Shuttle switches threads only at its own primitives: here the chunk
//! tables' state words and owners, the free bitmaps' locks, the path table's
//! id counter, and the I/O worker's channel and completions. A bbtree
//! operation runs as one step between them. And it runs threads one at a
//! time, so it finds the interleavings that break the cache, not a memory
//! ordering too weak: those rest on the `Acquire` and `Release` pairs in
//! `chunk_table.rs`.

use std::num::NonZeroU32;

use futures_util::FutureExt;
use shuttle::future::block_on;
use shuttle::scheduler::PctScheduler;

use super::{MoveToDisk, ObjectCache};
use crate::cache::block_index::{FileVersion, Location};
use crate::cache::cache_file::CacheFile;
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
/// simulated disk.
fn cache(memory_chunks: u32, disk_chunks: u32) -> Arc<ObjectCache> {
    let io = IoWorker::simulated(NonZeroU32::new(4).unwrap(), SimDisk::new()).unwrap();
    let disk = CacheFile::on(io, NonZeroU32::new(disk_chunks).unwrap()).unwrap();
    Arc::new(ObjectCache::new(NonZeroU32::new(memory_chunks).unwrap(), disk).unwrap())
}

/// The file at `path`, its one version.
fn resolve(cache: &ObjectCache, path: &[u8]) -> FileVersion {
    cache.resolve(path, Some("v1"), FILE_SIZE).unwrap()
}

/// The bytes of the file at `path`: its first byte throughout.
fn contents(path: &[u8]) -> Vec<u8> {
    vec![path[0]; FILE_SIZE as usize]
}

fn fill(cache: &ObjectCache, version: FileVersion, path: &[u8]) {
    cache.cache_fetched(version, 0, &contents(path)).unwrap();
}

/// Reads the file at `path` whole, and checks the read got the file's own
/// bytes from a tier, or nothing and a fetch of the file.
fn read_checked(cache: &ObjectCache, version: FileVersion, path: &[u8]) {
    let mut out = Vec::new();
    let read = block_on(cache.read(version, 0..FILE_SIZE, &mut out)).unwrap();
    if read.fetches().is_empty() {
        assert_eq!(out, contents(path), "a hit reads its own block's bytes");
    } else {
        assert_eq!(read.fetches().len(), 1);
        assert_eq!(read.fetches()[0], 0..FILE_SIZE, "a miss fetches the file");
    }
}

/// Where the index says `version`'s block is.
fn location(cache: &ObjectCache, version: FileVersion) -> Option<Location> {
    cache.blocks.get(BlockKey::new(version.id(), 0))
}

fn memory_chunk(cache: &ObjectCache, version: FileVersion) -> Option<ChunkId> {
    match location(cache, version) {
        Some(Location::Memory(chunk)) => Some(chunk),
        _ => None,
    }
}

fn disk_chunk(cache: &ObjectCache, version: FileVersion) -> Option<ChunkId> {
    match location(cache, version) {
        Some(Location::Disk(chunk)) => Some(chunk),
        _ => None,
    }
}

/// Whether the index and both tiers agree: each entry names a chunk that
/// holds its block, and each chunk in use, in either tier, has an entry
/// naming it. True once every thread and I/O is done.
fn in_step(cache: &ObjectCache, versions: &[FileVersion]) -> bool {
    let (mut in_memory, mut on_disk) = (0, 0);
    for &version in versions {
        let block = BlockKey::new(version.id(), 0);
        let holds_its_block = match location(cache, version) {
            Some(Location::Memory(chunk)) => {
                in_memory += 1;
                cache.memory.read(chunk, block).is_some()
            }
            Some(Location::Disk(chunk)) => {
                on_disk += 1;
                cache.disk.chunks().read(chunk, block).is_some()
            }
            Some(Location::Fetching) | None => true,
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

#[test]
fn racing_fills_of_one_block_take_one_chunk() {
    explore(|| {
        let cache = cache(3, 1);
        let version = resolve(&cache, b"a");
        let fillers: Vec<_> = (0..3)
            .map(|_| spawn(&cache, move |cache| fill(cache, version, b"a")))
            .collect();
        for filler in fillers {
            filler.join().unwrap();
        }
        assert!(in_step(&cache, &[version]));
        assert_eq!(cache.memory.free_count(), 2);
        let mut out = Vec::new();
        let read = block_on(cache.read(version, 0..FILE_SIZE, &mut out)).unwrap();
        assert!(read.fetches().is_empty());
        assert_eq!(out, contents(b"a"));
    });
}

/// The race the chunk table's owner check is for: a read that found the
/// entry before eviction, its chunk then reused for another file.
#[test]
fn a_read_racing_eviction_and_reuse_reads_its_bytes_or_misses() {
    explore(|| {
        let cache = cache(1, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read_checked(cache, a, b"a");
            read_checked(cache, a, b"a");
        });
        let evictor = spawn(&cache, move |cache| {
            assert!(cache.evict_from_memory(chunk));
            // The pool is full while the reader still holds the chunk.
            let _ = cache.cache_fetched(b, 0, &contents(b"b"));
        });
        reader.join().unwrap();
        evictor.join().unwrap();
        assert_eq!(location(&cache, a), None);
        assert!(in_step(&cache, &[a, b]));
    });
}

#[test]
fn eviction_racing_a_refill_keeps_entries_and_chunks_in_step() {
    explore(|| {
        let cache = cache(2, 1);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let handles = [
            spawn(&cache, move |cache| assert!(cache.evict_from_memory(chunk))),
            spawn(&cache, move |cache| fill(cache, a, b"a")),
            spawn(&cache, move |cache| read_checked(cache, a, b"a")),
        ];
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(in_step(&cache, &[a]));
    });
}

#[test]
fn of_two_evictions_of_one_chunk_one_evicts() {
    explore(|| {
        let cache = cache(1, 1);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let evictors: Vec<_> = (0..2)
            .map(|_| spawn(&cache, move |cache| cache.evict_from_memory(chunk)))
            .collect();
        let reader = spawn(&cache, move |cache| read_checked(cache, a, b"a"));
        let evicted = evictors
            .into_iter()
            .map(|evictor| evictor.join().unwrap())
            .filter(|&evicted| evicted)
            .count();
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
        let cache = cache(1, 1);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read_checked(cache, a, b"a");
            read_checked(cache, a, b"a");
        });
        let mover = spawn(&cache, move |cache| {
            block_on(cache.move_to_disk(chunk)).unwrap()
        });
        reader.join().unwrap();
        assert_eq!(mover.join().unwrap(), MoveToDisk::Moved);
        assert!(disk_chunk(&cache, a).is_some());
        assert!(in_step(&cache, &[a]));
    });
}

#[test]
fn a_move_racing_an_eviction_from_memory_keeps_the_tiers_in_step() {
    explore(|| {
        let cache = cache(1, 1);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let mover = spawn(&cache, move |cache| {
            block_on(cache.move_to_disk(chunk)).unwrap()
        });
        let evictor = spawn(&cache, move |cache| cache.evict_from_memory(chunk));
        let reader = spawn(&cache, move |cache| read_checked(cache, a, b"a"));
        let moved = mover.join().unwrap();
        let evicted = evictor.join().unwrap();
        reader.join().unwrap();
        // The block ends on disk only if the move won.
        assert_eq!(disk_chunk(&cache, a).is_some(), moved == MoveToDisk::Moved);
        assert!(moved == MoveToDisk::Moved || evicted);
        assert!(in_step(&cache, &[a]));
    });
}

#[test]
fn of_two_moves_of_one_chunk_one_moves() {
    explore(|| {
        let cache = cache(1, 2);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let movers: Vec<_> = (0..2)
            .map(|_| {
                spawn(&cache, move |cache| {
                    block_on(cache.move_to_disk(chunk)).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<_> = movers
            .into_iter()
            .map(|mover| mover.join().unwrap())
            .collect();
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
        let cache = cache(1, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        fill(&cache, a, b"a");
        let in_memory = memory_chunk(&cache, a).unwrap();
        assert_eq!(
            block_on(cache.move_to_disk(in_memory)).unwrap(),
            MoveToDisk::Moved
        );
        let on_disk = disk_chunk(&cache, a).unwrap();
        let reader = spawn(&cache, move |cache| {
            read_checked(cache, a, b"a");
            read_checked(cache, a, b"a");
        });
        let evictor = spawn(&cache, move |cache| {
            assert!(cache.evict_from_disk(on_disk));
            // Into memory, then onto disk: the disk is full while the reader
            // still holds the chunk.
            let _ = cache.cache_fetched(b, 0, &contents(b"b"));
            if let Some(chunk) = memory_chunk(cache, b) {
                block_on(cache.move_to_disk(chunk)).unwrap();
            }
        });
        reader.join().unwrap();
        evictor.join().unwrap();
        assert_eq!(location(&cache, a), None);
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
        let cache = cache(2, 1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        fill(&cache, a, b"a");
        fill(&cache, b, b"b");
        let (chunk_a, chunk_b) = (
            memory_chunk(&cache, a).unwrap(),
            memory_chunk(&cache, b).unwrap(),
        );
        // One poll submits the write; dropping the future then is a caller
        // giving up on it, unless that poll finished the move.
        let moved_a = cache.move_to_disk(chunk_a).now_or_never();
        // The disk is full while `a`'s write may still land.
        let moved_b = block_on(cache.move_to_disk(chunk_b)).unwrap();
        read_checked(&cache, b, b"b");
        read_checked(&cache, a, b"a");
        let mut waits = 0;
        while !in_step(&cache, &[a, b]) {
            waits += 1;
            assert!(waits < 1000, "the dropped write's chunks never went free");
            thread::yield_now();
        }
        match moved_a {
            Some(outcome) => assert_eq!(outcome.unwrap(), MoveToDisk::Moved),
            None => assert_eq!(location(&cache, a), Some(Location::Memory(chunk_a))),
        }
        assert!(matches!(moved_b, MoveToDisk::Moved | MoveToDisk::DiskFull));
        read_checked(&cache, b, b"b");
    });
}

#[test]
fn racing_resolutions_of_one_version_share_one_id() {
    explore(|| {
        let cache = cache(1, 1);
        let resolvers: Vec<_> = (0..3)
            .map(|_| spawn(&cache, |cache| resolve(cache, b"a").id()))
            .collect();
        let ids: Vec<_> = resolvers
            .into_iter()
            .map(|resolver| resolver.join().unwrap())
            .collect();
        assert!(ids.iter().all(|&id| id == ids[0]), "{ids:?}");
        assert_eq!(resolve(&cache, b"a").id(), ids[0]);
    });
}

#[test]
fn racing_resolutions_of_two_versions_never_share_an_id() {
    explore(|| {
        let cache = cache(1, 1);
        let resolvers: Vec<_> = ["v1", "v2", "v1"]
            .into_iter()
            .map(|e_tag| {
                spawn(&cache, move |cache| {
                    let id = cache.resolve(b"a", Some(e_tag), FILE_SIZE).unwrap().id();
                    (e_tag, id)
                })
            })
            .collect();
        let resolved: Vec<_> = resolvers
            .into_iter()
            .map(|resolver| resolver.join().unwrap())
            .collect();
        for (e_tag, id) in &resolved {
            for (other_e_tag, other_id) in &resolved {
                assert!(e_tag == other_e_tag || id != other_id, "{resolved:?}");
            }
        }
    });
}
