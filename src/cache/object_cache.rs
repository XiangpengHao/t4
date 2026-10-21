//! [`ObjectCache`]: reads and fills over the index and the memory tier
//! (`doc/03-cache.md`). A read copies the blocks memory holds and names the
//! rest; the caller fetches those from the object store and hands them back
//! to be cached.
//!
//! The disk tier, the eviction policy and request coalescing come later:
//! until then a block is in memory or missing, and a chunk is evicted only
//! when the caller names it.

use std::num::NonZeroU32;
use std::ops::Range;

use super::block_index::{BlockTable, FileVersion, Location, PathTable, UncacheableObject};
use super::chunk_pool::memory_pool::{MemoryChunkReader, MemoryPool};
use super::{BLOCK_SIZE, BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::error::Result;

/// A cache of file versions' blocks in memory chunks, found through the
/// index.
pub(super) struct ObjectCache {
    paths: PathTable,
    blocks: BlockTable,
    memory: MemoryPool,
}

impl ObjectCache {
    /// A cache of `memory_chunks` chunks, all free, holding nothing.
    pub(super) fn new(memory_chunks: NonZeroU32) -> Result<Self> {
        Ok(Self {
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: MemoryPool::new(memory_chunks)?,
        })
    }

    /// The version of the file at `path` with `e_tag` and `size`, which
    /// reads and fills name; or why the cache can't hold the file, whose
    /// reads then go to the object store alone.
    pub(super) fn resolve(
        &self,
        path: &[u8],
        e_tag: Option<&str>,
        size: u64,
    ) -> std::result::Result<FileVersion, UncacheableObject> {
        self.paths.resolve(path, e_tag, size)
    }

    /// Reads `range` of `version` into `out`, which it sets to the bytes the
    /// read takes, the range clamped to the file: those of blocks memory
    /// holds copied in, the rest zeros. The [`PartialRead`] names what the
    /// caller fetches for the rest.
    pub(super) fn read(
        &self,
        version: FileVersion,
        range: Range<u64>,
        out: &mut Vec<u8>,
    ) -> PartialRead {
        let plan = self.blocks.plan_read(version, range);
        let bytes = plan.bytes();
        out.clear();
        out.resize(PartialRead::offset(bytes.end - bytes.start), 0);
        let mut fetches: Vec<Range<u64>> = Vec::new();
        for block in plan.blocks() {
            match self.hold_in_memory(block.key(), block.location()) {
                Some(reader) => {
                    let window = block.window().bytes();
                    let start = block.object_range().start + u64::from(window.start);
                    let at = PartialRead::offset(start - bytes.start);
                    let window = window.start as usize..window.end as usize;
                    out[at..at + window.len()].copy_from_slice(&reader.bytes()[window]);
                }
                None => {
                    let missing = block.object_range();
                    match fetches.last_mut() {
                        Some(run) if run.end == missing.start => run.end = missing.end,
                        _ => fetches.push(missing),
                    }
                }
            }
        }
        PartialRead { bytes, fetches }
    }

    /// Caches the blocks of `version` that `fetched` holds: the file's bytes
    /// from `start`, whole blocks, as a [`PartialRead`]'s fetches name them.
    /// A block a tier holds already stays as it is.
    ///
    /// # Errors
    ///
    /// [`MemoryFull`], if no memory chunk is free for a block: the blocks
    /// before it are cached, and the rest aren't.
    ///
    /// # Panics
    ///
    /// If `fetched` isn't whole blocks of the file from a block's start.
    pub(super) fn cache_fetched(
        &self,
        version: FileVersion,
        start: u64,
        fetched: &[u8],
    ) -> std::result::Result<(), MemoryFull> {
        assert!(
            start.is_multiple_of(BLOCK_SIZE_U64),
            "a fetch starts at a block"
        );
        let first =
            u32::try_from(start / BLOCK_SIZE_U64).expect("a file's block numbers fit a u32");
        for (index, block_bytes) in (first..).zip(fetched.chunks(BLOCK_SIZE)) {
            let block = version.block_range(index);
            assert_eq!(
                block_bytes.len() as u64,
                block.end - block.start,
                "a fetch holds whole blocks"
            );
            self.cache_block(BlockKey::new(version.id(), index), block_bytes)?;
        }
        Ok(())
    }

    /// Caches `bytes`, the whole of `key`'s block, in a memory chunk, unless
    /// a tier holds the block already.
    fn cache_block(&self, key: BlockKey, bytes: &[u8]) -> std::result::Result<(), MemoryFull> {
        if self.blocks.get(key).is_some() {
            return Ok(());
        }
        let mut writer = self.memory.allocate().ok_or(MemoryFull)?;
        writer.bytes_mut()[..bytes.len()].copy_from_slice(bytes);
        // Published before the index names it, so that a read that finds the
        // entry finds the chunk open.
        let reader = writer.publish(key);
        if !self
            .blocks
            .insert_if_absent(key, Location::Memory(reader.chunk()))
        {
            // Another fetch of the block cached it first. No entry names
            // this chunk: it goes back when `reader` drops.
            reader.unpublish();
        }
        Ok(())
    }

    /// Drops the block in memory chunk `chunk` from the cache, as eviction
    /// does once its policy picks the chunk: closes the chunk to new
    /// readers, then drops the index entry that names it. The chunk goes free
    /// when its last reader leaves. Whether this call evicted the block: of
    /// two evictions of one chunk, one does, and a free chunk has none.
    pub(super) fn evict_from_memory(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return false;
        };
        if !victim.unpublish() {
            return false;
        }
        // Closed first, so a read that finds the entry from now on finds the
        // chunk closed, and takes the block as missing.
        self.blocks
            .remove_if(victim.block(), Location::Memory(chunk));
        true
    }

    /// Holds the memory chunk with `key`'s block, the index having said it
    /// is at `location`; `None` if memory doesn't hold it.
    ///
    /// An entry may name a chunk that no longer holds the block: eviction
    /// closed the chunk, and is about to drop the entry, or the chunk
    /// already holds another block. So the index is read again, until it
    /// names the chunk that holds the block, or no change.
    fn hold_in_memory(
        &self,
        key: BlockKey,
        mut location: Option<Location>,
    ) -> Option<MemoryChunkReader<'_>> {
        loop {
            // Disk chunks are read once the disk tier is built; a block
            // being fetched is missing until it lands.
            let Some(Location::Memory(chunk)) = location else {
                return None;
            };
            if let Some(reader) = self.memory.read(chunk, key) {
                return Some(reader);
            }
            let again = self.blocks.get(key);
            if again == location {
                return None;
            }
            location = again;
        }
    }
}

/// A read that memory served in part: the bytes it takes, and the ranges of
/// the file to fetch for the rest, whole blocks, adjacent ones in one.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PartialRead {
    bytes: Range<u64>,
    fetches: Vec<Range<u64>>,
}

impl PartialRead {
    /// The file's bytes the read takes, which `out` holds.
    pub(super) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    /// The ranges of the file to fetch, one GET each, in order.
    pub(super) fn fetches(&self) -> &[Range<u64>] {
        &self.fetches
    }

    /// Copies the part of `fetched`, the file's bytes from `start`, that the
    /// read takes into `out`, the buffer [`ObjectCache::read`] filled.
    pub(super) fn fill(&self, out: &mut [u8], start: u64, fetched: &[u8]) {
        let fetched_end = start + fetched.len() as u64;
        let overlap = self.bytes.start.max(start)..self.bytes.end.min(fetched_end);
        if overlap.is_empty() {
            return;
        }
        let len = Self::offset(overlap.end - overlap.start);
        let at = Self::offset(overlap.start - self.bytes.start);
        let from = Self::offset(overlap.start - start);
        out[at..at + len].copy_from_slice(&fetched[from..from + len]);
    }

    /// A length or an offset within a read's bytes, as a `usize`: the
    /// bytes are in memory, so it fits.
    fn offset(within_read: u64) -> usize {
        usize::try_from(within_read).expect("a read's bytes fit in memory")
    }
}

/// No memory chunk was free for a block. Until eviction is built, the cache
/// holds what fits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MemoryFull;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

// `[0..MIB]` is a list of one fetch, not the range's offsets.
#[allow(clippy::single_range_in_vec_init)]
#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;

    const MIB: u64 = BLOCK_SIZE_U64;

    fn cache(memory_chunks: u32) -> ObjectCache {
        ObjectCache::new(NonZeroU32::new(memory_chunks).unwrap()).unwrap()
    }

    /// A file's bytes: each byte a function of its offset.
    fn file_bytes(size: u64) -> Vec<u8> {
        (0..size).map(|offset| (offset % 251) as u8).collect()
    }

    /// Reads `range`, then fetches and caches what the read names, from
    /// `file`, as the front end will; returns what the caller got, and the
    /// fetches the read named.
    fn read_through(
        cache: &ObjectCache,
        version: FileVersion,
        file: &[u8],
        range: Range<u64>,
    ) -> (Vec<u8>, Vec<Range<u64>>) {
        let mut out = Vec::new();
        let read = cache.read(version, range, &mut out);
        for fetch in read.fetches() {
            let fetched = &file[fetch.start as usize..fetch.end as usize];
            read.fill(&mut out, fetch.start, fetched);
            let _ = cache.cache_fetched(version, fetch.start, fetched);
        }
        (out, read.fetches().to_vec())
    }

    #[test]
    fn a_miss_fetches_whole_blocks_and_then_hits() {
        let cache = cache(4);
        let file = file_bytes(2 * MIB + MIB / 2);
        let version = cache
            .resolve(b"a.parquet", Some("v1"), 2 * MIB + MIB / 2)
            .unwrap();

        let range = 100..2 * MIB + 10;
        let (out, fetches) = read_through(&cache, version, &file, range.clone());
        assert_eq!(
            fetches,
            [0..2 * MIB + MIB / 2],
            "all three blocks in one GET"
        );
        assert_eq!(out, file[100..2 * MIB as usize + 10]);
        assert_eq!(cache.memory.free_count(), 1);

        let (out, fetches) = read_through(&cache, version, &file, range);
        assert!(fetches.is_empty());
        assert_eq!(out, file[100..2 * MIB as usize + 10]);

        // Past the end, the read is clamped to the file.
        let (out, fetches) = read_through(&cache, version, &file, 2 * MIB..u64::MAX);
        assert!(fetches.is_empty());
        assert_eq!(out, file[2 * MIB as usize..]);
    }

    #[test]
    fn a_read_fetches_only_the_blocks_memory_lacks() {
        let cache = cache(4);
        let file = file_bytes(4 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        cache
            .cache_fetched(version, MIB, &file[MIB as usize..2 * MIB as usize])
            .unwrap();
        cache
            .cache_fetched(version, 3 * MIB, &file[3 * MIB as usize..])
            .unwrap();

        let mut out = Vec::new();
        let read = cache.read(version, 10..4 * MIB - 10, &mut out);
        assert_eq!(read.bytes(), 10..4 * MIB - 10);
        assert_eq!(read.fetches(), [0..MIB, 2 * MIB..3 * MIB]);
        // The cached blocks are in, the missing ones zeros until filled.
        assert_eq!(
            out[MIB as usize - 10..2 * MIB as usize - 10],
            file[MIB as usize..2 * MIB as usize]
        );
        assert!(out[..MIB as usize - 10].iter().all(|&byte| byte == 0));
        for fetch in read.fetches() {
            read.fill(
                &mut out,
                fetch.start,
                &file[fetch.start as usize..fetch.end as usize],
            );
        }
        assert_eq!(out, file[10..4 * MIB as usize - 10]);
    }

    #[test]
    fn a_full_pool_caches_what_fits() {
        let cache = cache(1);
        let file = file_bytes(2 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        assert_eq!(cache.cache_fetched(version, 0, &file), Err(MemoryFull));

        let mut out = Vec::new();
        let read = cache.read(version, 0..2 * MIB, &mut out);
        assert_eq!(read.fetches(), [MIB..2 * MIB]);
        assert_eq!(out[..MIB as usize], file[..MIB as usize]);
    }

    #[test]
    fn a_block_fetched_twice_takes_one_chunk() {
        let cache = cache(4);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        assert_eq!(cache.memory.free_count(), 3);
    }

    #[test]
    fn racing_fetches_of_one_block_take_one_chunk() {
        const THREADS: usize = 8;
        let cache = cache(THREADS as u32);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let barrier = std::sync::Barrier::new(THREADS);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    barrier.wait();
                    cache.cache_fetched(version, 0, &file).unwrap();
                });
            }
        });
        assert_eq!(cache.memory.free_count(), THREADS as u32 - 1);
        let (out, fetches) = read_through(&cache, version, &file, 0..MIB);
        assert!(fetches.is_empty());
        assert_eq!(out, file);
    }

    #[test]
    fn a_stale_entry_reads_as_a_miss() {
        let cache = cache(1);
        let file = file_bytes(MIB);
        let old = cache.resolve(b"old", Some("v1"), MIB).unwrap();
        cache.cache_fetched(old, 0, &file).unwrap();
        let key = BlockKey::new(old.id(), 0);
        let Some(Location::Memory(chunk)) = cache.blocks.get(key) else {
            panic!("the block is in memory");
        };

        // Evicted, as eviction will: closed, but its entry not yet dropped.
        assert!(cache.memory.pin_for_eviction(chunk).unwrap().unpublish());
        let (_, fetches) = read_through(&cache, old, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);

        // Dropped, and the chunk reused for another file's block while a
        // read still holds the old entry.
        assert!(cache.blocks.remove_if(key, Location::Memory(chunk)));
        let other = cache.resolve(b"other", Some("v1"), MIB).unwrap();
        cache.cache_fetched(other, 0, &file_bytes(MIB)).unwrap();
        assert_eq!(
            cache.blocks.get(BlockKey::new(other.id(), 0)),
            Some(Location::Memory(chunk))
        );
        assert!(
            cache
                .hold_in_memory(key, Some(Location::Memory(chunk)))
                .is_none()
        );
    }

    #[test]
    fn an_evicted_block_misses_and_its_chunk_goes_free() {
        let cache = cache(1);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        let Some(Location::Memory(chunk)) = cache.blocks.get(BlockKey::new(version.id(), 0)) else {
            panic!("the block is in memory");
        };

        // A read in flight keeps the chunk, not the block.
        let reader = cache.memory.read(chunk, BlockKey::new(version.id(), 0));
        assert!(cache.evict_from_memory(chunk));
        assert!(!cache.evict_from_memory(chunk), "one eviction of a chunk");
        assert_eq!(cache.blocks.get(BlockKey::new(version.id(), 0)), None);
        assert_eq!(cache.memory.free_count(), 0, "the read is still in it");
        drop(reader);
        assert_eq!(cache.memory.free_count(), 1);

        let (out, fetches) = read_through(&cache, version, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, file);
    }

    #[test]
    fn a_new_version_misses_the_old_ones_blocks() {
        let cache = cache(4);
        let file = file_bytes(MIB);
        let v1 = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(v1, 0, &file).unwrap();
        let v2 = cache.resolve(b"a", Some("v2"), MIB).unwrap();
        let (_, fetches) = read_through(&cache, v2, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
    }

    #[test]
    fn fill_copies_only_the_overlap() {
        let read = PartialRead {
            bytes: 100..200,
            fetches: Vec::new(),
        };
        let mut out = vec![0; 100];
        read.fill(&mut out, 150, &[7; 100]);
        assert!(out[..50].iter().all(|&byte| byte == 0));
        assert!(out[50..].iter().all(|&byte| byte == 7));
        read.fill(&mut out, 0, &[9; 120]);
        assert!(out[..20].iter().all(|&byte| byte == 9));
        assert_eq!(out[20], 0);
        read.fill(&mut out, 300, &[1; 10]);
        read.fill(&mut out, 0, &[1; 100]);
        assert!(out[..20].iter().all(|&byte| byte == 9));
    }

    #[test]
    #[should_panic(expected = "a fetch holds whole blocks")]
    fn a_fetch_of_part_of_a_block_is_refused() {
        let cache = cache(1);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let _ = cache.cache_fetched(version, 0, &file_bytes(MIB / 2));
    }
}
