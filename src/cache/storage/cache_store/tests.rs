use crate::io::sync::{Arc, Mutex};

use std::path::Path;

use shuttle::future::block_on;

use super::locate::HeldBlock;
use super::test_policy::{Heard, TestPolicy};
use super::*;
use crate::cache::policy::{
    Admit, BlockSource, DiskHit, FetchRuns, IoEvent, LocatedRead, PlannedGet, ReadPlan, Tier,
    VictimAction,
};
use crate::cache::storage::DiskFile;
use crate::cache::storage::index::Location;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::file_system::FileSystem;
use std::num::NonZeroU8;

const MIB: u64 = BLOCK_SIZE_U64;

fn cache(memory_chunks: u32, disk_chunks: u32) -> (ObjectCache, Arc<TestPolicy>) {
    let file = DiskFile::new("cache".into(), u64::from(disk_chunks) * MIB);
    let disk = CacheFiles::open(
        &FileSystem::default(),
        &[file],
        NonZeroU32::new(8).unwrap(),
        true,
    )
    .unwrap();
    let policy = Arc::new(TestPolicy::new());
    let memory = NonZeroU32::new(memory_chunks).unwrap();
    let cache = ObjectCache::new(
        memory,
        disk,
        Arc::clone(&policy) as Arc<dyn PlacementPolicy>,
    )
    .unwrap();
    (cache, policy)
}

fn file_bytes(size: u64) -> Vec<u8> {
    (0..size).map(|offset| (offset % 251) as u8).collect()
}

fn read_through(
    cache: &ObjectCache,
    file: &CacheableFile<'_>,
    data: &[u8],
    range: Range<u64>,
) -> (Vec<u8>, Vec<Range<u64>>) {
    let mut read = block_on(cache.read(file, range, None, &FetchRuns::default())).unwrap();
    let fetches = read.fetches().to_vec();
    for fetch in &fetches {
        let fetched = &data[fetch.start as usize..fetch.end as usize];
        block_on(cache.take_fetched(&mut read, fetch.clone(), fetched)).unwrap();
    }
    cache.tiers.pending.wait_for_idle();
    (read.into_bytes().unwrap().to_vec(), fetches)
}

fn location(cache: &ObjectCache, file: &CacheableFile<'_>, index: u32) -> Option<Location> {
    cache
        .tiers
        .blocks
        .get(BlockKey::new(file.version().id(), index))
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

#[test]
fn the_default_policy_streams_blocks_through_both_tiers() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let files: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|name| DiskFile::new(name.into(), MIB))
            .collect();
        let queue_depth = NonZeroU32::new(8).unwrap();
        let disk = CacheFiles::open(&FileSystem::default(), &files, queue_depth, true).unwrap();
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
            if cache.tiers.blocks.get(key(&file, index as u32)).is_none() {
                read_through(&cache, &file, &data, start..start + MIB);
            }
        }
        assert_eq!(cache.tiers.memory.free_count(), 0);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 0);
        memory_chunk(&cache, &file, 9);
        let (out, _) = read_through(&cache, &file, &data, 0..size);
        assert_eq!(out, data, "every read right, wherever its blocks were");
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_remote_read_fetches_whole_blocks_and_then_hits() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(4, 1);
        let size = 2 * MIB + MIB / 2;
        let data = file_bytes(size);
        let file = cache.resolve(b"a.parquet", Some("v1"), size).unwrap();

        let range = 100..2 * MIB + 10;
        let (out, fetches) = read_through(&cache, &file, &data, range.clone());
        assert_eq!(fetches, [0..size], "all three blocks in one GET");
        assert_eq!(out, data[100..2 * MIB as usize + 10]);
        assert_eq!(cache.tiers.memory.free_count(), 1);

        let (out, fetches) = read_through(&cache, &file, &data, range);
        assert!(fetches.is_empty());
        assert_eq!(out, data[100..2 * MIB as usize + 10]);

        // Past the end, the read is clamped to the file.
        let (out, fetches) = read_through(&cache, &file, &data, 2 * MIB..u64::MAX);
        assert!(fetches.is_empty());
        assert_eq!(out, data[2 * MIB as usize..]);

        let remote_reads = policy
            .heard()
            .iter()
            .filter(|heard| matches!(heard, Heard::Remote(_)))
            .count();
        assert_eq!(remote_reads, 3, "one a block, on the first read");
    });
}

#[test]
fn a_read_fetches_only_the_blocks_memory_lacks() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 1);
        let data = file_bytes(4 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        read_through(&cache, &file, &data, MIB..2 * MIB);
        read_through(&cache, &file, &data, 3 * MIB..4 * MIB);

        let mut read =
            block_on(cache.read(&file, 10..4 * MIB - 10, None, &FetchRuns::default())).unwrap();
        assert_eq!(read.bytes(), 10..4 * MIB - 10);
        assert_eq!(read.fetches(), [0..MIB, 2 * MIB..3 * MIB]);
        for fetch in read.fetches().to_vec() {
            let fetched = &data[fetch.start as usize..fetch.end as usize];
            block_on(cache.take_fetched(&mut read, fetch, fetched)).unwrap();
        }
        assert_eq!(read.into_bytes().unwrap(), data[10..4 * MIB as usize - 10]);
    });
}

#[test]
fn the_policy_hears_each_block_where_it_is() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(4, 4);
        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        read_through(&cache, &file, &data, 0..2 * MIB);
        let in_memory = memory_chunk(&cache, &file, 0);
        assert!(
            cache
                .tiers
                .pending
                .enqueue(memory_chunk(&cache, &file, 1), key(&file, 1))
        );
        cache.wait_for_disk_writes();
        let on_disk = disk_chunk(&cache, &file, 1);

        let before = policy.heard().len();
        read_through(&cache, &file, &data, 0..3 * MIB);
        let heard = &policy.heard()[before..];
        assert_eq!(heard[0], Heard::MemoryHit(in_memory));
        assert_eq!(heard[1], Heard::DiskHit(on_disk));
        assert_eq!(heard[2], Heard::Remote(key(&file, 2)));
        assert!(matches!(heard[3], Heard::Admit(Tier::Memory, _, block) if block == key(&file, 2)));
    });
}

#[test]
fn concurrent_reads_of_one_block_fetch_it_once() {
    crate::io::test_schedule::TestSchedule::check(|| {
        const THREADS: usize = 8;
        let (cache, _policy) = cache(THREADS as u32, 1);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let fetched: Vec<_> = crate::io::sync::thread::scope(|scope| {
            let readers: Vec<_> = (0..THREADS)
                .map(|_| scope.spawn(|| read_through(&cache, &file, &data, 0..MIB)))
                .collect();
            readers
                .into_iter()
                .map(|reader| reader.join().unwrap())
                .collect()
        });
        for (out, _) in &fetched {
            assert_eq!(out, &data);
        }
        let fetches: Vec<_> = fetched.iter().flat_map(|(_, fetches)| fetches).collect();
        assert_eq!(fetches, [&(0..MIB)], "one fetch");
        assert_eq!(cache.tiers.memory.free_count(), THREADS as u32 - 1);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_stale_entry_reads_as_remote() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let data = file_bytes(MIB);
        let old = cache.resolve(b"old", Some("v1"), MIB).unwrap();
        read_through(&cache, &old, &data, 0..MIB);
        let chunk = memory_chunk(&cache, &old, 0);

        // Evicted: closed, but its entry not yet dropped.
        assert!(
            cache
                .tiers
                .memory
                .pin_for_eviction(chunk)
                .unwrap()
                .unpublish()
        );
        let (_, fetches) = read_through(&cache, &old, &data, 0..MIB);
        assert_eq!(fetches, [0..MIB]);

        // The chunk reused for another file's block, a read still holding the old entry.
        assert!(
            cache
                .tiers
                .blocks
                .remove_if(key(&old, 0), Location::Memory(chunk))
        );
        let other = cache.resolve(b"other", Some("v1"), MIB).unwrap();
        read_through(&cache, &other, &data, 0..MIB);
        assert_eq!(location(&cache, &other, 0), Some(Location::Memory(chunk)));
        assert!(matches!(
            cache.hold(key(&old, 0), Some(Location::Memory(chunk))),
            HeldBlock::Remote
        ));
    });
}

#[test]
fn an_evicted_block_reads_as_remote_and_its_chunk_goes_free() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        let chunk = memory_chunk(&cache, &file, 0);

        // A read in flight keeps the chunk, not the block.
        let reader = cache.tiers.memory.read(chunk, key(&file, 0));
        assert!(cache.tiers.evict_from_memory(chunk));
        assert!(
            !cache.tiers.evict_from_memory(chunk),
            "one eviction of a chunk"
        );
        assert_eq!(location(&cache, &file, 0), None);
        assert_eq!(
            cache.tiers.memory.free_count(),
            0,
            "the read is still in it"
        );
        drop(reader);
        assert_eq!(cache.tiers.memory.free_count(), 1);

        let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, data);
    });
}

#[test]
fn blocks_moved_to_disk_read_back_from_disk() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 4);
        let size = 2 * MIB + 5000;
        let data = file_bytes(size);
        let file = cache.resolve(b"a", Some("v1"), size).unwrap();
        read_through(&cache, &file, &data, 0..size);
        for index in 0..3 {
            let chunk = memory_chunk(&cache, &file, index);
            assert!(cache.tiers.pending.enqueue(chunk, key(&file, index)));
            cache.wait_for_disk_writes();
            disk_chunk(&cache, &file, index);
        }
        assert_eq!(cache.tiers.memory.free_count(), 4);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);

        for range in [0..size, 5000..5001, 4095..MIB + 4097, 2 * MIB + 10..size] {
            let (out, fetches) = read_through(&cache, &file, &data, range.clone());
            assert!(fetches.is_empty(), "{range:?}");
            assert_eq!(out, data[range.start as usize..range.end as usize]);
        }
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_full_memory_moves_its_victims_to_disk() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 2);
        let data = file_bytes(2 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        read_through(&cache, &file, &data, MIB..2 * MIB);
        let on_disk = disk_chunk(&cache, &file, 0);
        if location(&cache, &file, 1).is_none() {
            read_through(&cache, &file, &data, MIB..2 * MIB);
        }
        memory_chunk(&cache, &file, 1);
        assert!(
            policy
                .heard()
                .contains(&Heard::Admit(Tier::Disk, on_disk, key(&file, 0)))
        );
        let (out, fetches) = read_through(&cache, &file, &data, 0..2 * MIB);
        assert!(fetches.is_empty());
        assert_eq!(out, data);
    });
}

#[test]
fn a_dropped_victim_leaves_the_cache() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_victim_action(VictimAction::Drop);
        let data = file_bytes(2 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        read_through(&cache, &file, &data, MIB..2 * MIB);
        assert_eq!(location(&cache, &file, 0), None);
        memory_chunk(&cache, &file, 1);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1, "nothing written");
    });
}

#[test]
fn a_full_disk_drops_its_victims_to_take_memorys() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        for index in 0..3_u32 {
            let start = u64::from(index) * MIB;
            read_through(&cache, &file, &data, start..start + MIB);
            if location(&cache, &file, index).is_none() {
                read_through(&cache, &file, &data, start..start + MIB);
            }
        }
        assert_eq!(location(&cache, &file, 0), None);
        disk_chunk(&cache, &file, 1);
        memory_chunk(&cache, &file, 2);
        let (out, fetches) = read_through(&cache, &file, &data, MIB..3 * MIB);
        assert!(fetches.is_empty());
        assert_eq!(out, data[MIB as usize..]);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_block_admitted_to_disk_goes_straight_there() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_admit(Admit::Disk);
        let size = MIB / 2 + 7;
        let data = file_bytes(size);
        let file = cache.resolve(b"a", Some("v1"), size).unwrap();
        read_through(&cache, &file, &data, 0..size);
        disk_chunk(&cache, &file, 0);
        assert_eq!(cache.tiers.memory.free_count(), 1, "memory untouched");
        let (out, fetches) = read_through(&cache, &file, &data, 3..size - 3);
        assert!(fetches.is_empty());
        assert_eq!(out, data[3..size as usize - 3]);
    });
}

#[test]
fn every_block_of_a_fetch_reaches_disk_through_one_memory_chunk() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 4);
        policy.set_admit(Admit::Disk);
        let size = 3 * MIB;
        let data = file_bytes(size);
        let file = cache.resolve(b"a", Some("v1"), size).unwrap();
        let (_, fetches) = read_through(&cache, &file, &data, 0..size);
        assert_eq!(fetches, [0..size]);
        for index in 0..3 {
            disk_chunk(&cache, &file, index);
        }
        assert_eq!(cache.tiers.memory.free_count(), 1);
        let (out, fetches) = read_through(&cache, &file, &data, 0..size);
        assert!(fetches.is_empty());
        assert_eq!(out, data);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_block_for_disk_waits_for_no_writer_that_makes_no_room() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_admit(Admit::Disk);
        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        // A read in flight keeps the disk's one chunk, which eviction can't free.
        let on_disk = disk_chunk(&cache, &file, 0);
        let held = cache
            .tiers
            .disk
            .chunks()
            .read(on_disk, key(&file, 0))
            .unwrap();

        let (out, _) = read_through(&cache, &file, &data, MIB..3 * MIB);
        assert_eq!(out, data[MIB as usize..]);
        memory_chunk(&cache, &file, 1);
        assert_eq!(location(&cache, &file, 2), None, "no room for it");
        drop(held);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_discarded_block_is_fetched_every_time() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_admit(Admit::Discard);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        for _ in 0..2 {
            let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
            assert_eq!(fetches, [0..MIB]);
            assert_eq!(out, data);
        }
        assert_eq!(location(&cache, &file, 0), None);
        assert_eq!(cache.tiers.memory.free_count(), 1);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);
    });
}

#[test]
fn a_promoted_block_moves_to_memory() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
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
            cache.tiers.disk.chunks().free_count(),
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
    });
}

#[test]
fn a_promotion_with_no_room_reads_from_disk() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_admit(Admit::Disk);
        let data = file_bytes(2 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        policy.set_admit(Admit::Memory);
        read_through(&cache, &file, &data, MIB..2 * MIB);
        // A read in flight keeps memory's one chunk, which eviction can't free.
        let in_memory = memory_chunk(&cache, &file, 1);
        let held = cache.tiers.memory.read(in_memory, key(&file, 1)).unwrap();

        policy.set_victim_action(VictimAction::Drop);
        policy.set_disk_hit(DiskHit::Promote);
        let (out, fetches) = read_through(&cache, &file, &data, 10..MIB - 10);
        assert!(fetches.is_empty());
        assert_eq!(out, data[10..MIB as usize - 10]);
        disk_chunk(&cache, &file, 0);
        drop(held);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_victim_kept_for_want_of_disk_goes_back_to_the_policy() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        policy.set_admit(Admit::Disk);
        read_through(&cache, &file, &data, 0..MIB);
        // A read in flight keeps the disk's one chunk, which eviction can't free.
        let on_disk = disk_chunk(&cache, &file, 0);
        let held = cache
            .tiers
            .disk
            .chunks()
            .read(on_disk, key(&file, 0))
            .unwrap();
        policy.set_admit(Admit::Memory);
        read_through(&cache, &file, &data, MIB..2 * MIB);
        let in_memory = memory_chunk(&cache, &file, 1);

        let (out, fetches) = read_through(&cache, &file, &data, 2 * MIB..3 * MIB);
        assert_eq!(fetches, [2 * MIB..3 * MIB]);
        assert_eq!(out, data[2 * MIB as usize..]);
        assert_eq!(location(&cache, &file, 2), None, "no room for it");
        assert_eq!(memory_chunk(&cache, &file, 1), in_memory);
        assert_eq!(policy.queued(Tier::Memory), [in_memory]);
        drop(held);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_free_chunk_has_no_block_to_move_or_evict() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let free = ChunkId::new(0);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        assert!(cache.tiers.pending.enqueue(free, key(&file, 0)));
        cache.wait_for_disk_writes();
        assert!(!cache.tiers.evict_from_memory(free));
        assert!(!cache.tiers.evict_from_disk(free));
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);
    });
}

#[test]
fn a_block_evicted_from_disk_reads_as_remote_and_its_chunk_goes_free() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, policy) = cache(1, 1);
        policy.set_admit(Admit::Disk);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        let on_disk = disk_chunk(&cache, &file, 0);

        assert!(cache.tiers.evict_from_disk(on_disk));
        assert!(
            !cache.tiers.evict_from_disk(on_disk),
            "one eviction of a chunk"
        );
        assert_eq!(location(&cache, &file, 0), None);
        assert_eq!(cache.tiers.disk.chunks().free_count(), 1);
        let (out, fetches) = read_through(&cache, &file, &data, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, data);
    });
}

#[test]
fn a_new_version_reads_none_of_the_old_ones_blocks() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 1);
        let data = file_bytes(MIB);
        let v1 = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        read_through(&cache, &v1, &data, 0..MIB);
        let v2 = cache.resolve(b"a", Some("v2"), MIB).unwrap();
        let (_, fetches) = read_through(&cache, &v2, &data, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
    });
}

#[test]
fn the_check_at_rest_finds_held_free_and_unnamed_chunks() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(2, 1);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        read_through(&cache, &file, &data, 0..MIB);
        let chunk = memory_chunk(&cache, &file, 0);
        assert_eq!(cache.check_at_rest(), Ok(vec![(Tier::Memory, chunk)]));

        let reader = cache.tiers.memory.read(chunk, key(&file, 0)).unwrap();
        let held = cache.check_at_rest().unwrap_err();
        assert!(held.contains("is held"), "{held}");
        // Evicted, and its entry not yet dropped.
        assert!(reader.unpublish());
        drop(reader);
        let named = cache.check_at_rest().unwrap_err();
        assert!(named.contains("which holds None"), "{named}");
        assert!(
            cache
                .tiers
                .blocks
                .remove_if(key(&file, 0), Location::Memory(chunk))
        );
        assert_eq!(cache.check_at_rest(), Ok(vec![]));

        drop(
            cache
                .tiers
                .memory
                .allocate()
                .unwrap()
                .publish(key(&file, 0)),
        );
        let unnamed = cache.check_at_rest().unwrap_err();
        assert!(unnamed.contains("which no entry names"), "{unnamed}");
    });
}

#[test]
fn fetched_bytes_land_in_pieces_of_any_size() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 1);
        let size = 2 * MIB + MIB / 2;
        let data = file_bytes(size);
        let file = cache.resolve(b"a", Some("v1"), size).unwrap();
        let mut read =
            block_on(cache.read(&file, 100..size - 100, None, &FetchRuns::default())).unwrap();
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
    });
}

#[test]
fn a_fetch_bringing_more_than_it_holds_is_refused() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let mut read = block_on(cache.read(&file, 0..MIB, None, &FetchRuns::default())).unwrap();
        let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
        assert!(block_on(blocks.land(&file_bytes(MIB + 1))).is_err());
    });
}

#[test]
fn a_fetch_cut_short_caches_nothing() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(1, 1);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();

        let mut read = block_on(cache.read(&file, 0..MIB, None, &FetchRuns::default())).unwrap();
        let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
        block_on(blocks.land(&data[..MIB as usize / 2])).unwrap();
        assert!(block_on(blocks.finish()).is_err());
        assert!(location(&cache, &file, 0).is_none());

        let mut read = block_on(cache.read(&file, 0..MIB, None, &FetchRuns::default())).unwrap();
        let mut blocks = cache.receive(&mut read, 0..MIB).unwrap();
        block_on(blocks.land(&data[..MIB as usize / 2])).unwrap();
        drop(blocks);
        assert!(location(&cache, &file, 0).is_none());
        assert_eq!(cache.tiers.memory.free_count(), 1);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_read_waits_for_a_block_another_is_fetching() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(2, 1);
        let reads = FetchRuns::default();
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let mut fetching = block_on(cache.read(&file, 0..MIB, None, &reads)).unwrap();
        assert_eq!(fetching.fetches(), [0..MIB]);
        let (mut waiting, work) = cache
            .start_planned_read(&file, 10..20, None, &reads)
            .unwrap();
        assert_eq!(waiting.fetches(), [], "no second fetch");

        block_on(cache.take_fetched(&mut fetching, 0..MIB, &data)).unwrap();
        let left = block_on(work.finish(&mut waiting)).unwrap();
        assert!(left.is_empty(), "{left:?}");
        assert_eq!(waiting.into_bytes().unwrap(), data[10..20]);
        assert_eq!(fetching.into_bytes().unwrap(), data);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_read_waiting_for_a_fetch_given_up_on_fetches_the_block_itself() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(2, 1);
        let reads = FetchRuns::default();
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let given_up = block_on(cache.read(&file, 0..MIB, None, &reads)).unwrap();
        let (mut waiting, work) = cache
            .start_planned_read(&file, 0..MIB, None, &reads)
            .unwrap();
        drop(given_up);

        let retry = block_on(work.finish(&mut waiting)).unwrap();
        assert_eq!(retry.len(), 1, "the fetch ended without the block");
        let work = cache
            .retry_planned(&mut waiting, &file, 0..MIB, None, retry, &reads)
            .unwrap();
        assert_eq!(waiting.fetches(), [0..MIB]);
        block_on(cache.take_fetched(&mut waiting, 0..MIB, &data)).unwrap();
        let left = block_on(work.finish(&mut waiting)).unwrap();
        assert!(left.is_empty(), "{left:?}");
        assert_eq!(waiting.into_bytes().unwrap(), data);
        cache.check_at_rest().unwrap();
    });
}

/// A plan asked of the policy: the blocks it saw and each one's source.
type HeardPlan = (Range<u32>, Vec<Option<BlockSource>>);

#[derive(Default)]
struct HeardPlans(Mutex<Vec<HeardPlan>>);

impl ReadPolicy for HeardPlans {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let sources = read.blocks().map(|block| read.source(block)).collect();
        self.0.lock().unwrap().push((read.blocks(), sources));
        FetchRuns::default().plan(read)
    }

    fn on_io(&self, _event: &IoEvent<'_>) {}
}

#[test]
fn a_retry_of_blocks_apart_is_planned_once() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 1);
        let reads = FetchRuns::default();
        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        read_through(&cache, &file, &data, MIB..2 * MIB);
        let given_up = block_on(cache.read(&file, 0..3 * MIB, None, &reads)).unwrap();
        let (mut waiting, work) = cache
            .start_planned_read(&file, 0..3 * MIB, None, &reads)
            .unwrap();
        drop(given_up);

        let retry = block_on(work.finish(&mut waiting)).unwrap();
        assert_eq!(retry.len(), 2, "blocks 0 and 2");
        let heard = HeardPlans::default();
        let work = cache
            .retry_planned(&mut waiting, &file, 0..3 * MIB, None, retry, &heard)
            .unwrap();
        let (memory, remote) = (Some(BlockSource::Memory), Some(BlockSource::Remote));
        assert_eq!(
            *heard.0.lock().unwrap(),
            [(0..3, vec![remote, memory, remote])]
        );
        assert_eq!(waiting.fetches(), [0..MIB, 2 * MIB..3 * MIB]);
        for fetch in [0..MIB, 2 * MIB..3 * MIB] {
            let fetched = &data[fetch.start as usize..fetch.end as usize];
            block_on(cache.take_fetched(&mut waiting, fetch, fetched)).unwrap();
        }
        let left = block_on(work.finish(&mut waiting)).unwrap();
        assert!(left.is_empty(), "{left:?}");
        assert_eq!(waiting.into_bytes().unwrap(), data);
        cache.check_at_rest().unwrap();
    });
}

struct RacesInFlight;

impl ReadPolicy for RacesInFlight {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let mut gets: Vec<_> = read.remote_runs().map(PlannedGet::once).collect();
        for block in read.blocks() {
            if let Some(BlockSource::InFlight { requests }) = read.source(block) {
                let race = u8::try_from(requests + 1).unwrap();
                gets.push(PlannedGet::new(
                    block..block + 1,
                    NonZeroU8::new(race).unwrap(),
                ));
            }
        }
        ReadPlan::new(gets)
    }

    fn on_io(&self, _event: &IoEvent<'_>) {}
}

#[test]
fn a_racing_read_joins_a_fetch_in_flight() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(2, 1);
        let data = file_bytes(MIB);
        let file = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let mut first = block_on(cache.read(&file, 0..MIB, None, &RacesInFlight)).unwrap();
        let mut racing = block_on(cache.read(&file, 0..MIB, None, &RacesInFlight)).unwrap();
        assert_eq!(racing.fetches(), [0..MIB], "raced, not waited for");
        let fetch = cache.in_flight.get(key(&file, 0)).unwrap();
        assert_eq!(fetch.requests(), 3, "one in flight, two racing it");

        block_on(cache.take_fetched(&mut racing, 0..MIB, &data)).unwrap();
        assert!(
            cache.in_flight.get(key(&file, 0)).is_none(),
            "the first to land ends the fetch"
        );
        block_on(cache.take_fetched(&mut first, 0..MIB, &data)).unwrap();
        assert_eq!(fetch.requests(), 0);
        assert_eq!(cache.tiers.memory.free_count(), 1, "one copy cached");
        assert_eq!(first.into_bytes().unwrap(), data);
        assert_eq!(racing.into_bytes().unwrap(), data);
        cache.check_at_rest().unwrap();
    });
}

#[test]
fn a_plan_taken_that_the_cache_cant_carry_out_fetches_as_the_default() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (cache, _policy) = cache(4, 1);
        let reads = FetchRuns::default();
        let data = file_bytes(2 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        let (mut read, mut work) = cache.start_read(&file, 0..2 * MIB, None, &reads).unwrap();

        let held = work.locate(&mut read).unwrap().unwrap();
        assert_eq!(held.run().planned_by(&reads, &file, None).gets().len(), 1);
        work.take_gets(&mut read, held, &ReadPlan::default());
        assert!(work.locate(&mut read).unwrap().is_none());
        assert_eq!(read.fetches(), [0..2 * MIB], "the default's GET");

        block_on(cache.take_fetched(&mut read, 0..2 * MIB, &data)).unwrap();
        assert!(block_on(work.finish(&mut read)).unwrap().is_empty());
        assert_eq!(read.into_bytes().unwrap(), data);
        cache.check_at_rest().unwrap();
    });
}

fn disk_admitting_cache(
    file_system: &FileSystem,
    disk_chunks: u32,
    keep_free: u64,
) -> (ObjectCache, Arc<TestPolicy>) {
    let file = DiskFile::new("cache".into(), u64::from(disk_chunks) * MIB);
    let disk = CacheFiles::open(file_system, &[file], NonZeroU32::new(8).unwrap(), true)
        .unwrap()
        .keeping_free(keep_free);
    let policy = Arc::new(TestPolicy::new());
    policy.set_admit(Admit::Disk);
    let placement = Arc::clone(&policy) as Arc<dyn PlacementPolicy>;
    let cache = ObjectCache::new(NonZeroU32::new(4).unwrap(), disk, placement).unwrap();
    (cache, policy)
}

#[test]
fn a_cache_file_takes_room_as_blocks_are_written() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let file_system = FileSystem::default();
        let (cache, _policy) = disk_admitting_cache(&file_system, 8, 0);
        let path = Path::new("cache");
        let allocated = || file_system.allocated(path).unwrap();
        let size = file_system.open_existing(path, false).unwrap().size();
        assert_eq!(size.unwrap(), 8 * MIB, "sized");
        assert!(allocated() < MIB, "not allocated: {}", allocated());

        let data = file_bytes(3 * MIB);
        let file = cache.resolve(b"a", Some("v1"), 3 * MIB).unwrap();
        let (out, _) = read_through(&cache, &file, &data, 0..3 * MIB);
        assert_eq!(out, data);
        for index in 0..3 {
            disk_chunk(&cache, &file, index);
        }
        assert!(allocated() >= 3 * MIB, "{}", allocated());
    });
}

#[test]
fn a_drive_short_of_room_takes_no_block_and_evicts_none() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let file_system = FileSystem::default();
        let a = file_bytes(4 * MIB);
        {
            // A cache with room fills the tier, and keeps it as it drops.
            let (cache, _policy) = disk_admitting_cache(&file_system, 4, 0);
            let file = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
            read_through(&cache, &file, &a, 0..4 * MIB);
            assert_eq!(cache.tiers.disk.chunks().free_count(), 0);
        }
        let (cache, policy) = disk_admitting_cache(&file_system, 4, u64::MAX);
        let kept = policy.queued(Tier::Disk);
        assert_eq!(kept.len(), 4, "taken back");

        let b = file_bytes(MIB);
        let other = cache.resolve(b"b", Some("v1"), MIB).unwrap();
        let (out, _) = read_through(&cache, &other, &b, 0..MIB);
        assert_eq!(out, b);
        assert_eq!(location(&cache, &other, 0), None, "not cached");
        assert_eq!(policy.queued(Tier::Disk), kept, "no victim picked");

        let file = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        let (out, fetches) = read_through(&cache, &file, &a, 0..4 * MIB);
        assert_eq!(out, a);
        assert_eq!(fetches, Vec::<Range<u64>>::new(), "served from disk");
        cache.check_at_rest().unwrap();
    });
}
