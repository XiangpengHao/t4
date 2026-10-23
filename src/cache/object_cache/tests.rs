use std::sync::Arc;

use pollster::block_on;
use tempfile::TempDir;

use super::test_policy::{Heard, TestPolicy};
use super::*;
use crate::cache::cache_files::DiskFile;

const MIB: u64 = BLOCK_SIZE_U64;

/// A cache of `memory_chunks` chunks of memory and `disk_chunks` on disk,
/// run by a [`TestPolicy`], in a directory of its own that lives as long as
/// the cache.
fn cache(memory_chunks: u32, disk_chunks: u32) -> (TempDir, ObjectCache, Arc<TestPolicy>) {
    let dir = tempfile::tempdir().unwrap();
    let file = DiskFile::new(dir.path().join("cache"), u64::from(disk_chunks) * MIB);
    let disk = CacheFiles::open(&[file], NonZeroU32::new(8).unwrap(), true).unwrap();
    let policy = Arc::new(TestPolicy::new());
    let memory = NonZeroU32::new(memory_chunks).unwrap();
    let cache =
        ObjectCache::new(memory, disk, Arc::clone(&policy) as Arc<dyn CachePolicy>).unwrap();
    (dir, cache, policy)
}

/// A file's bytes: each byte a function of its offset.
fn file_bytes(size: u64) -> Vec<u8> {
    (0..size).map(|offset| (offset % 251) as u8).collect()
}

/// Reads `range`, then fetches what the read names from `data` and caches
/// it, as the front end will; returns what the caller got, and the fetches
/// the read named.
fn read_through(
    cache: &ObjectCache,
    file: &CacheableFile<'_>,
    data: &[u8],
    range: Range<u64>,
) -> (Vec<u8>, Vec<Range<u64>>) {
    let mut read = block_on(cache.read(file, range)).unwrap();
    let fetches = read.fetches().to_vec();
    for fetch in &fetches {
        let fetched = &data[fetch.start as usize..fetch.end as usize];
        block_on(cache.take_fetched(&mut read, fetch.clone(), fetched)).unwrap();
    }
    (read.into_bytes().unwrap().to_vec(), fetches)
}

fn location(cache: &ObjectCache, file: &CacheableFile<'_>, index: u32) -> Option<Location> {
    cache.blocks.get(BlockKey::new(file.version().id(), index))
}

fn memory_chunk(cache: &ObjectCache, file: &CacheableFile<'_>, index: u32) -> ChunkId {
    let Some(Location::Memory(chunk)) = location(cache, file, index) else {
        panic!("block {index} is in memory");
    };
    chunk
}

fn disk_chunk(cache: &ObjectCache, file: &CacheableFile<'_>, index: u32) -> ChunkId {
    let Some(Location::Disk(chunk)) = location(cache, file, index) else {
        panic!("block {index} is on disk");
    };
    chunk
}

fn key(file: &CacheableFile<'_>, index: u32) -> BlockKey {
    BlockKey::new(file.version().id(), index)
}

/// The default policy, end to end: a stream of blocks through both full
/// tiers, the disk striped over three files, every read right, each tier
/// full, the newest blocks in memory.
#[test]
fn the_default_policy_streams_blocks_through_both_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let files: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|name| DiskFile::new(dir.path().join(name), MIB))
        .collect();
    let disk = CacheFiles::open(&files, NonZeroU32::new(8).unwrap(), true).unwrap();
    let (memory, disk_chunks) = (NonZeroU32::new(2).unwrap(), NonZeroU32::new(3).unwrap());
    let policy = Arc::new(crate::cache::policy::ClockPolicy::new(memory, disk_chunks));
    let cache = ObjectCache::new(memory, disk, policy).unwrap();
    let size = 10 * MIB;
    let data = file_bytes(size);
    let file = cache.resolve(b"a", Some("v1"), size).unwrap();
    for index in 0..10 {
        let start = index * MIB;
        let (out, _) = read_through(&cache, &file, &data, start..start + MIB);
        assert_eq!(out, data[start as usize..(start + MIB) as usize]);
    }
    assert_eq!(cache.memory.free_count(), 0);
    assert_eq!(cache.disk.chunks().free_count(), 0);
    memory_chunk(&cache, &file, 9);
    let (out, _) = read_through(&cache, &file, &data, 0..size);
    assert_eq!(out, data, "every read right, wherever its blocks were");
    cache.check_at_rest().unwrap();
}

#[test]
fn a_miss_fetches_whole_blocks_and_then_hits() {
    let (_dir, cache, policy) = cache(4, 1);
    let size = 2 * MIB + MIB / 2;
    let data = file_bytes(size);
    let file = cache.resolve(b"a.parquet", Some("v1"), size).unwrap();

    let range = 100..2 * MIB + 10;
    let (out, fetches) = read_through(&cache, &file, &data, range.clone());
    assert_eq!(fetches, [0..size], "all three blocks in one GET");
    assert_eq!(out, data[100..2 * MIB as usize + 10]);
    assert_eq!(cache.memory.free_count(), 1);

    let (out, fetches) = read_through(&cache, &file, &data, range);
    assert!(fetches.is_empty());
    assert_eq!(out, data[100..2 * MIB as usize + 10]);

    // Past the end, the read is clamped to the file.
    let (out, fetches) = read_through(&cache, &file, &data, 2 * MIB..u64::MAX);
    assert!(fetches.is_empty());
    assert_eq!(out, data[2 * MIB as usize..]);

    let misses = policy
        .heard()
        .iter()
        .filter(|heard| matches!(heard, Heard::Miss(_)))
        .count();
    assert_eq!(misses, 3, "one a block, on the first read");
}

#[test]
fn a_read_fetches_only_the_blocks_memory_lacks() {
    let (_dir, cache, _policy) = cache(4, 1);
    let data = file_bytes(4 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
    read_through(&cache, &file, &data, MIB..2 * MIB);
    read_through(&cache, &file, &data, 3 * MIB..4 * MIB);

    let mut read = block_on(cache.read(&file, 10..4 * MIB - 10)).unwrap();
    assert_eq!(read.bytes(), 10..4 * MIB - 10);
    assert_eq!(read.fetches(), [0..MIB, 2 * MIB..3 * MIB]);
    for fetch in read.fetches().to_vec() {
        let fetched = &data[fetch.start as usize..fetch.end as usize];
        block_on(cache.take_fetched(&mut read, fetch, fetched)).unwrap();
    }
    // The cached blocks came from memory, the missing ones from the fetches.
    assert_eq!(read.into_bytes().unwrap(), data[10..4 * MIB as usize - 10]);
}

#[test]
fn the_policy_hears_each_block_where_it_is() {
    let (_dir, cache, policy) = cache(4, 4);
    let data = file_bytes(3 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
    read_through(&cache, &file, &data, 0..2 * MIB);
    let in_memory = memory_chunk(&cache, &file, 0);
    assert_eq!(
        block_on(cache.move_to_disk(memory_chunk(&cache, &file, 1))).unwrap(),
        MoveToDisk::Moved
    );
    let on_disk = disk_chunk(&cache, &file, 1);

    let before = policy.heard().len();
    read_through(&cache, &file, &data, 0..3 * MIB);
    let heard = &policy.heard()[before..];
    assert_eq!(heard[0], Heard::MemoryHit(in_memory));
    assert_eq!(heard[1], Heard::DiskHit(on_disk));
    assert_eq!(heard[2], Heard::Miss(key(&file, 2)));
    assert!(matches!(heard[3], Heard::Admit(Tier::Memory, _, block) if block == key(&file, 2)));
}

#[test]
fn racing_fetches_of_one_block_take_one_chunk() {
    const THREADS: usize = 8;
    let (_dir, cache, _policy) = cache(THREADS as u32, 1);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    let barrier = std::sync::Barrier::new(THREADS);
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                let mut read = block_on(cache.read(&file, 0..MIB)).unwrap();
                barrier.wait();
                for fetch in read.fetches().to_vec() {
                    block_on(cache.take_fetched(&mut read, fetch, &data)).unwrap();
                }
            });
        }
    });
    assert_eq!(cache.memory.free_count(), THREADS as u32 - 1);
    cache.check_at_rest().unwrap();
    let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
    assert!(fetches.is_empty());
    assert_eq!(out, data);
}

#[test]
fn a_stale_entry_reads_as_a_miss() {
    let (_dir, cache, _policy) = cache(1, 1);
    let data = file_bytes(MIB);
    let old = cache.resolve(b"old", Some("v1"), MIB).unwrap();
    read_through(&cache, &old, &data, 0..MIB);
    let chunk = memory_chunk(&cache, &old, 0);

    // Evicted: closed, but its entry not yet dropped.
    assert!(cache.memory.pin_for_eviction(chunk).unwrap().unpublish());
    let (_, fetches) = read_through(&cache, &old, &data, 0..MIB);
    assert_eq!(fetches, [0..MIB]);

    // Dropped, and the chunk reused for another file's block while a read
    // still holds the old entry.
    assert!(
        cache
            .blocks
            .remove_if(key(&old, 0), Location::Memory(chunk))
    );
    let other = cache.resolve(b"other", Some("v1"), MIB).unwrap();
    read_through(&cache, &other, &data, 0..MIB);
    assert_eq!(location(&cache, &other, 0), Some(Location::Memory(chunk)));
    assert!(matches!(
        cache.hold(key(&old, 0), Some(Location::Memory(chunk))),
        HeldBlock::Missing
    ));
}

#[test]
fn an_evicted_block_misses_and_its_chunk_goes_free() {
    let (_dir, cache, _policy) = cache(1, 1);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    let chunk = memory_chunk(&cache, &file, 0);

    // A read in flight keeps the chunk, not the block.
    let reader = cache.memory.read(chunk, key(&file, 0));
    assert!(cache.evict_from_memory(chunk));
    assert!(!cache.evict_from_memory(chunk), "one eviction of a chunk");
    assert_eq!(location(&cache, &file, 0), None);
    assert_eq!(cache.memory.free_count(), 0, "the read is still in it");
    drop(reader);
    assert_eq!(cache.memory.free_count(), 1);

    let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
    assert_eq!(fetches, [0..MIB]);
    assert_eq!(out, data);
}

#[test]
fn blocks_moved_to_disk_read_back_from_disk() {
    let (_dir, cache, _policy) = cache(4, 4);
    let size = 2 * MIB + 5000;
    let data = file_bytes(size);
    let file = cache.resolve(b"a", Some("v1"), size).unwrap();
    read_through(&cache, &file, &data, 0..size);
    for index in 0..3 {
        let chunk = memory_chunk(&cache, &file, index);
        assert_eq!(
            block_on(cache.move_to_disk(chunk)).unwrap(),
            MoveToDisk::Moved
        );
        disk_chunk(&cache, &file, index);
    }
    assert_eq!(cache.memory.free_count(), 4);
    assert_eq!(cache.disk.chunks().free_count(), 1);

    // Whole, within a page, across pages and blocks, and the short last
    // block: every read from disk, none fetched.
    for range in [0..size, 5000..5001, 4095..MIB + 4097, 2 * MIB + 10..size] {
        let (out, fetches) = read_through(&cache, &file, &data, range.clone());
        assert!(fetches.is_empty(), "{range:?}");
        assert_eq!(out, data[range.start as usize..range.end as usize]);
    }
    cache.check_at_rest().unwrap();
}

#[test]
fn a_full_memory_moves_its_victims_to_disk() {
    let (_dir, cache, policy) = cache(1, 2);
    let data = file_bytes(2 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    read_through(&cache, &file, &data, MIB..2 * MIB);
    // Block 0 made room for block 1, moving to disk.
    let on_disk = disk_chunk(&cache, &file, 0);
    memory_chunk(&cache, &file, 1);
    assert!(
        policy
            .heard()
            .contains(&Heard::Admit(Tier::Disk, on_disk, key(&file, 0)))
    );
    let (out, fetches) = read_through(&cache, &file, &data, 0..2 * MIB);
    assert!(fetches.is_empty());
    assert_eq!(out, data);
}

#[test]
fn a_dropped_victim_leaves_the_cache() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_victim_action(VictimAction::Drop);
    let data = file_bytes(2 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    read_through(&cache, &file, &data, MIB..2 * MIB);
    assert_eq!(location(&cache, &file, 0), None);
    memory_chunk(&cache, &file, 1);
    assert_eq!(cache.disk.chunks().free_count(), 1, "nothing written");
}

#[test]
fn a_full_disk_drops_its_victims_to_take_memorys() {
    let (_dir, cache, _policy) = cache(1, 1);
    let data = file_bytes(3 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
    for index in 0..3_u32 {
        let start = u64::from(index) * MIB;
        read_through(&cache, &file, &data, start..start + MIB);
    }
    // Block 1 pushed block 0 to disk; block 2 pushed block 1 there, which
    // dropped block 0.
    assert_eq!(location(&cache, &file, 0), None);
    disk_chunk(&cache, &file, 1);
    memory_chunk(&cache, &file, 2);
    let (out, fetches) = read_through(&cache, &file, &data, MIB..3 * MIB);
    assert!(fetches.is_empty());
    assert_eq!(out, data[MIB as usize..]);
    cache.check_at_rest().unwrap();
}

#[test]
fn a_block_admitted_to_disk_goes_straight_there() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_admit(Admit::Disk);
    let size = MIB / 2 + 7;
    let data = file_bytes(size);
    let file = cache.resolve(b"a", Some("v1"), size).unwrap();
    read_through(&cache, &file, &data, 0..size);
    disk_chunk(&cache, &file, 0);
    assert_eq!(cache.memory.free_count(), 1, "memory untouched");
    let (out, fetches) = read_through(&cache, &file, &data, 3..size - 3);
    assert!(fetches.is_empty());
    assert_eq!(out, data[3..size as usize - 3]);
}

#[test]
fn a_discarded_block_is_fetched_every_time() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_admit(Admit::Discard);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    for _ in 0..2 {
        let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, data);
    }
    assert_eq!(location(&cache, &file, 0), None);
    assert_eq!(cache.memory.free_count(), 1);
    assert_eq!(cache.disk.chunks().free_count(), 1);
}

#[test]
fn a_promoted_block_moves_to_memory() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_admit(Admit::Disk);
    let size = MIB - 100;
    let data = file_bytes(size);
    let file = cache.resolve(b"a", Some("v1"), size).unwrap();
    read_through(&cache, &file, &data, 0..size);
    let on_disk = disk_chunk(&cache, &file, 0);

    policy.set_disk_hit(DiskHit::Promote);
    let (out, fetches) = read_through(&cache, &file, &data, 5000..6000);
    assert!(fetches.is_empty());
    assert_eq!(out, data[5000..6000]);
    let in_memory = memory_chunk(&cache, &file, 0);
    assert_eq!(
        cache.disk.chunks().free_count(),
        1,
        "the disk chunk went free"
    );
    assert!(
        policy
            .heard()
            .contains(&Heard::Admit(Tier::Memory, in_memory, key(&file, 0)))
    );
    assert!(policy.heard().contains(&Heard::DiskHit(on_disk)));
    let (out, fetches) = read_through(&cache, &file, &data, 0..size);
    assert!(fetches.is_empty());
    assert_eq!(out, data);
    cache.check_at_rest().unwrap();
}

#[test]
fn a_promotion_with_no_room_reads_from_disk() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_admit(Admit::Disk);
    let data = file_bytes(2 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    policy.set_admit(Admit::Memory);
    read_through(&cache, &file, &data, MIB..2 * MIB);
    // A read in flight keeps memory's one chunk, which eviction can't free.
    let in_memory = memory_chunk(&cache, &file, 1);
    let held = cache.memory.read(in_memory, key(&file, 1)).unwrap();

    policy.set_victim_action(VictimAction::Drop);
    policy.set_disk_hit(DiskHit::Promote);
    let (out, fetches) = read_through(&cache, &file, &data, 10..MIB - 10);
    assert!(fetches.is_empty());
    assert_eq!(out, data[10..MIB as usize - 10]);
    disk_chunk(&cache, &file, 0);
    drop(held);
    cache.check_at_rest().unwrap();
}

#[test]
fn a_victim_kept_for_want_of_disk_goes_back_to_the_policy() {
    let (_dir, cache, policy) = cache(1, 1);
    let data = file_bytes(3 * MIB);
    let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
    policy.set_admit(Admit::Disk);
    read_through(&cache, &file, &data, 0..MIB);
    // A read in flight keeps the disk's one chunk, which eviction can't free.
    let on_disk = disk_chunk(&cache, &file, 0);
    let held = cache.disk.chunks().read(on_disk, key(&file, 0)).unwrap();
    policy.set_admit(Admit::Memory);
    read_through(&cache, &file, &data, MIB..2 * MIB);
    let in_memory = memory_chunk(&cache, &file, 1);

    // Block 2 finds memory full; its victim, block 1, finds the disk full.
    let (out, fetches) = read_through(&cache, &file, &data, 2 * MIB..3 * MIB);
    assert_eq!(fetches, [2 * MIB..3 * MIB]);
    assert_eq!(out, data[2 * MIB as usize..]);
    assert_eq!(location(&cache, &file, 2), None, "no room for it");
    assert_eq!(memory_chunk(&cache, &file, 1), in_memory);
    assert_eq!(policy.queued(Tier::Memory), [in_memory]);
    drop(held);
    cache.check_at_rest().unwrap();
}

#[test]
fn a_free_chunk_has_no_block_to_move_or_evict() {
    let (_dir, cache, _policy) = cache(1, 1);
    let free = ChunkId::new(0);
    assert_eq!(
        block_on(cache.move_to_disk(free)).unwrap(),
        MoveToDisk::NoBlock
    );
    assert!(!cache.evict_from_memory(free));
    assert!(!cache.evict_from_disk(free));
    assert_eq!(cache.disk.chunks().free_count(), 1);
}

#[test]
fn a_block_evicted_from_disk_misses_and_its_chunk_goes_free() {
    let (_dir, cache, policy) = cache(1, 1);
    policy.set_admit(Admit::Disk);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    let on_disk = disk_chunk(&cache, &file, 0);

    assert!(cache.evict_from_disk(on_disk));
    assert!(!cache.evict_from_disk(on_disk), "one eviction of a chunk");
    assert_eq!(location(&cache, &file, 0), None);
    assert_eq!(cache.disk.chunks().free_count(), 1);
    let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
    assert_eq!(fetches, [0..MIB]);
    assert_eq!(out, data);
}

#[test]
fn a_new_version_misses_the_old_ones_blocks() {
    let (_dir, cache, _policy) = cache(4, 1);
    let data = file_bytes(MIB);
    let v1 = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    read_through(&cache, &v1, &data, 0..MIB);
    let v2 = cache.resolve(b"a", Some("v2"), MIB).unwrap();
    let (_, fetches) = read_through(&cache, &v2, &data, 0..MIB);
    assert_eq!(fetches, [0..MIB]);
}

#[test]
fn the_check_at_rest_finds_held_free_and_unnamed_chunks() {
    let (_dir, cache, _policy) = cache(2, 1);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    read_through(&cache, &file, &data, 0..MIB);
    let chunk = memory_chunk(&cache, &file, 0);
    assert_eq!(cache.check_at_rest(), Ok(vec![(Tier::Memory, chunk)]));

    let reader = cache.memory.read(chunk, key(&file, 0)).unwrap();
    let held = cache.check_at_rest().unwrap_err();
    assert!(held.contains("is held"), "{held}");
    // Evicted, and its entry not yet dropped.
    assert!(reader.unpublish());
    drop(reader);
    let named = cache.check_at_rest().unwrap_err();
    assert!(named.contains("which holds None"), "{named}");
    assert!(
        cache
            .blocks
            .remove_if(key(&file, 0), Location::Memory(chunk))
    );
    assert_eq!(cache.check_at_rest(), Ok(vec![]));

    // Published, with no entry naming it.
    drop(cache.memory.allocate().unwrap().publish(key(&file, 0)));
    let unnamed = cache.check_at_rest().unwrap_err();
    assert!(unnamed.contains("which no entry names"), "{unnamed}");
}

/// However the answer is cut, each byte lands where it goes: the read's
/// part of it in the output, each block whole in the cache.
#[test]
fn fetched_bytes_land_in_pieces_of_any_size() {
    let (_dir, cache, _policy) = cache(4, 1);
    let size = 2 * MIB + MIB / 2;
    let data = file_bytes(size);
    let file = cache.resolve(b"a", Some("v1"), size).unwrap();
    let mut read = block_on(cache.read(&file, 100..size - 100)).unwrap();
    assert_eq!(read.fetches(), [0..size]);
    let mut blocks = cache.receive(&mut read, 0..size).unwrap();
    let mut at: usize = 0;
    for len in [1, 4095, 100_000, MIB as usize, usize::MAX] {
        let end = at.saturating_add(len).min(data.len());
        block_on(blocks.land(&data[at..end])).unwrap();
        at = end;
    }
    block_on(blocks.finish()).unwrap();
    assert_eq!(read.into_bytes().unwrap(), data[100..size as usize - 100]);
    let (out, fetches) = read_through(&cache, &file, &data, 0..size);
    assert!(fetches.is_empty(), "every block cached");
    assert_eq!(out, data);
}

#[test]
fn a_fetch_bringing_more_than_it_holds_is_refused() {
    let (_dir, cache, _policy) = cache(1, 1);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
    let mut read = block_on(cache.read(&file, 0..MIB)).unwrap();
    let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
    assert!(block_on(blocks.land(&file_bytes(MIB + 1))).is_err());
}

/// Neither a fetch that ends short nor one given up on halfway caches its
/// block, and both give back the chunk it took.
#[test]
fn a_fetch_cut_short_caches_nothing() {
    let (_dir, cache, _policy) = cache(1, 1);
    let data = file_bytes(MIB);
    let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();

    let mut read = block_on(cache.read(&file, 0..MIB)).unwrap();
    let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
    block_on(blocks.land(&data[..MIB as usize / 2])).unwrap();
    assert!(block_on(blocks.finish()).is_err());
    assert!(location(&cache, &file, 0).is_none());

    let mut read = block_on(cache.read(&file, 0..MIB)).unwrap();
    let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
    block_on(blocks.land(&data[..MIB as usize / 2])).unwrap();
    drop(blocks);
    assert!(location(&cache, &file, 0).is_none());
    assert_eq!(cache.memory.free_count(), 1);
    cache.check_at_rest().unwrap();
}
