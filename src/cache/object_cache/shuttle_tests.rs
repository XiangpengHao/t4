//! The cache under shuttle: reads, fills and evictions racing on one block,
//! and resolutions racing on one path.
//!
//! Shuttle switches threads only at its own primitives: here the chunk
//! table's state words and owners, the free bitmap's lock, and the path
//! table's id counter. A bbtree operation runs as one step between them. And
//! it runs threads one at a time, so it finds the interleavings that break
//! the cache, not a memory ordering too weak: those rest on the `Acquire`
//! and `Release` pairs in `chunk_table.rs`.

use std::num::NonZeroU32;

use shuttle::scheduler::PctScheduler;

use super::ObjectCache;
use crate::cache::block_index::{FileVersion, Location};
use crate::cache::{BlockKey, ChunkId};
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

fn cache(memory_chunks: u32) -> Arc<ObjectCache> {
    Arc::new(ObjectCache::new(NonZeroU32::new(memory_chunks).unwrap()).unwrap())
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
/// bytes from memory, or nothing and a fetch of the file.
fn read_checked(cache: &ObjectCache, version: FileVersion, path: &[u8]) {
    let mut out = Vec::new();
    let read = cache.read(version, 0..FILE_SIZE, &mut out);
    if read.fetches().is_empty() {
        assert_eq!(out, contents(path), "a hit reads its own block's bytes");
    } else {
        assert_eq!(read.fetches().len(), 1);
        assert_eq!(read.fetches()[0], 0..FILE_SIZE, "a miss fetches the file");
    }
}

/// The memory chunk the index names for `version`'s block.
fn memory_chunk(cache: &ObjectCache, version: FileVersion) -> Option<ChunkId> {
    match cache.blocks.get(BlockKey::new(version.id(), 0)) {
        Some(Location::Memory(chunk)) => Some(chunk),
        _ => None,
    }
}

/// Checks the index and the memory chunks agree, once every thread is done:
/// each entry names a chunk that holds its block, and each chunk in use has
/// an entry naming it.
fn assert_in_step(cache: &ObjectCache, versions: &[FileVersion]) {
    let mut named = 0;
    for &version in versions {
        if let Some(chunk) = memory_chunk(cache, version) {
            let block = BlockKey::new(version.id(), 0);
            assert!(
                cache.memory.read(chunk, block).is_some(),
                "an entry names a chunk without its block"
            );
            named += 1;
        }
    }
    let in_use = cache.memory.chunk_count() - cache.memory.free_count();
    assert_eq!(in_use, named, "a chunk in use that no entry names");
}

#[test]
fn racing_fills_of_one_block_take_one_chunk() {
    explore(|| {
        let cache = cache(3);
        let version = resolve(&cache, b"a");
        let fillers: Vec<_> = (0..3)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || fill(&cache, version, b"a"))
            })
            .collect();
        for filler in fillers {
            filler.join().unwrap();
        }
        assert_in_step(&cache, &[version]);
        assert_eq!(cache.memory.free_count(), 2);
        let mut out = Vec::new();
        assert!(
            cache
                .read(version, 0..FILE_SIZE, &mut out)
                .fetches()
                .is_empty()
        );
        assert_eq!(out, contents(b"a"));
    });
}

/// The race the chunk table's owner check is for: a read that found the
/// entry before eviction, its chunk then reused for another file.
#[test]
fn a_read_racing_eviction_and_reuse_reads_its_bytes_or_misses() {
    explore(|| {
        let cache = cache(1);
        let (a, b) = (resolve(&cache, b"a"), resolve(&cache, b"b"));
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let reader = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                read_checked(&cache, a, b"a");
                read_checked(&cache, a, b"a");
            })
        };
        let evictor = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                assert!(cache.evict_from_memory(chunk));
                // The pool is full while the reader still holds the chunk.
                let _ = cache.cache_fetched(b, 0, &contents(b"b"));
            })
        };
        reader.join().unwrap();
        evictor.join().unwrap();
        assert_eq!(memory_chunk(&cache, a), None);
        assert_in_step(&cache, &[a, b]);
    });
}

#[test]
fn eviction_racing_a_refill_keeps_entries_and_chunks_in_step() {
    explore(|| {
        let cache = cache(2);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let evictor = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || assert!(cache.evict_from_memory(chunk)))
        };
        let filler = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || fill(&cache, a, b"a"))
        };
        let reader = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || read_checked(&cache, a, b"a"))
        };
        for handle in [evictor, filler, reader] {
            handle.join().unwrap();
        }
        assert_in_step(&cache, &[a]);
    });
}

#[test]
fn of_two_evictions_of_one_chunk_one_evicts() {
    explore(|| {
        let cache = cache(1);
        let a = resolve(&cache, b"a");
        fill(&cache, a, b"a");
        let chunk = memory_chunk(&cache, a).unwrap();
        let evictors: Vec<_> = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || cache.evict_from_memory(chunk))
            })
            .collect();
        let reader = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || read_checked(&cache, a, b"a"))
        };
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
        assert_in_step(&cache, &[a]);
    });
}

#[test]
fn racing_resolutions_of_one_version_share_one_id() {
    explore(|| {
        let cache = cache(1);
        let resolvers: Vec<_> = (0..3)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || resolve(&cache, b"a").id())
            })
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
        let cache = cache(1);
        let resolvers: Vec<_> = ["v1", "v2", "v1"]
            .into_iter()
            .map(|e_tag| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
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
