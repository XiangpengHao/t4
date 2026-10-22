//! [`ObjectCache`]: reads, fills and evictions over the index and the two
//! tiers (`doc/03-cache.md`). A read copies the blocks memory holds, reads
//! those on disk, and names the rest; the caller fetches those from the
//! object store and hands them back to be cached in memory. Eviction moves
//! a memory chunk's block to disk, or drops a chunk's block from its tier.
//!
//! The eviction policy and request coalescing come later: until then a
//! chunk is evicted only when the caller names it.

use std::num::NonZeroU32;
use std::ops::Range;

use super::block_index::{BlockTable, FileVersion, Location, PathTable, UncacheableObject};
use super::cache_file::CacheFile;
use super::chunk_pool::disk_chunks::DiskChunkReader;
use super::chunk_pool::memory_pool::{MemoryChunkReader, MemoryPool};
use super::{BLOCK_SIZE, BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::error::Result;
use crate::io::sync::Arc;

/// A cache of file versions' blocks in memory and disk chunks, found
/// through the index.
pub(super) struct ObjectCache {
    paths: PathTable,
    blocks: BlockTable,
    /// Shared with the writes in flight that lend its chunks.
    memory: Arc<MemoryPool>,
    disk: CacheFile,
}

impl ObjectCache {
    /// A cache of `memory_chunks` chunks of memory and the chunks of
    /// `disk`, all free, holding nothing.
    pub(super) fn new(memory_chunks: NonZeroU32, disk: CacheFile) -> Result<Self> {
        Ok(Self {
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: Arc::new(MemoryPool::new(memory_chunks)?),
            disk,
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
    /// holds copied in, those of blocks on disk read in, the rest zeros. The
    /// [`PartialRead`] names what the caller fetches for the rest.
    ///
    /// # Errors
    ///
    /// If a read from the cache file fails.
    pub(super) async fn read(
        &self,
        version: FileVersion,
        range: Range<u64>,
        out: &mut Vec<u8>,
    ) -> Result<PartialRead> {
        let plan = self.blocks.plan_read(version, range);
        let bytes = plan.bytes();
        out.clear();
        out.resize(PartialRead::offset(bytes.end - bytes.start), 0);
        let mut fetches: Vec<Range<u64>> = Vec::new();
        let mut disk_reads = Vec::new();
        for block in plan.blocks() {
            let window = block.window();
            let wanted = window.bytes();
            let start = block.object_range().start + u64::from(wanted.start);
            let at = PartialRead::offset(start - bytes.start);
            match self.hold(block.key(), block.location()) {
                HeldBlock::Memory(reader) => {
                    let wanted = wanted.start as usize..wanted.end as usize;
                    out[at..at + wanted.len()].copy_from_slice(&reader.bytes()[wanted]);
                }
                HeldBlock::Disk(reader) => {
                    let pages = self.disk.read_pages(&reader, window.pages())?;
                    disk_reads.push((reader, pages, at, window.bytes_in_pages()));
                }
                HeldBlock::Missing => {
                    let missing = block.object_range();
                    match fetches.last_mut() {
                        Some(run) if run.end == missing.start => run.end = missing.end,
                        _ => fetches.push(missing),
                    }
                }
            }
        }
        // Every disk read was submitted above, so they run at once. Each
        // keeps its chunk held until its bytes land.
        for (_held, pages, at, wanted) in disk_reads {
            let pages = pages.await?;
            let wanted = wanted.start as usize..wanted.end as usize;
            out[at..at + wanted.len()].copy_from_slice(&pages.as_slice()[wanted]);
        }
        Ok(PartialRead { bytes, fetches })
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

    /// Moves the block in memory chunk `chunk` to a free disk chunk, as
    /// eviction does once its policy picks the chunk: writes the whole chunk
    /// there, a short block's slack too (reads never look past a block's
    /// bytes, and the file's last block is the only short one), then
    /// points its entry at the disk chunk, and closes the memory chunk,
    /// which goes free when its last reader leaves. Readers go on finding
    /// the block in memory until the entry moves.
    ///
    /// The write lends the memory chunk's pages, not a copy, and holds both
    /// chunks until it is done, even if this future is dropped first.
    ///
    /// # Errors
    ///
    /// If the write fails; the block stays in memory.
    pub(super) async fn move_to_disk(&self, chunk: ChunkId) -> Result<MoveToDisk> {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return Ok(MoveToDisk::NoBlock);
        };
        let Some(writer) = self.disk.chunks().allocate() else {
            return Ok(MoveToDisk::DiskFull);
        };
        let block = victim.block();
        let pages = victim.lend_for_write(&self.memory, writer.hold_for_write(self.disk.chunks()));
        self.disk.write(&writer, pages).await?;
        let on_disk = writer.publish(block);
        let to_disk = Location::Disk(on_disk.chunk());
        if self
            .blocks
            .replace_if(block, Location::Memory(chunk), to_disk)
        {
            // New reads go to disk. Those that found the old entry finish in
            // memory, and the chunk goes free after them.
            victim.unpublish();
            Ok(MoveToDisk::Moved)
        } else {
            // The block left memory another way while it was written: the
            // copy on disk goes back.
            on_disk.unpublish();
            Ok(MoveToDisk::Superseded)
        }
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
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Memory(chunk))
    }

    /// [`Self::evict_from_memory`] for disk chunk `chunk`: the chunk goes
    /// free once the reads in flight from it are done.
    pub(super) fn evict_from_disk(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.disk.chunks().pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Disk(chunk))
    }

    /// Drops `block`'s entry if it names `location`, the chunk there just
    /// `closed` by this eviction; whether it was. Closed first, so a read
    /// that finds the entry from then on finds the chunk closed, and takes
    /// the block as missing.
    fn forget_if_closed(&self, closed: bool, block: BlockKey, location: Location) -> bool {
        if closed {
            self.blocks.remove_if(block, location);
        }
        closed
    }

    /// Caches `bytes`, the whole of `key`'s block, in a memory chunk, unless
    /// a tier holds the block already.
    fn cache_block(&self, key: BlockKey, bytes: &[u8]) -> std::result::Result<(), MemoryFull> {
        if self.blocks.get(key).is_some() {
            return Ok(());
        }
        let mut writer = self.memory.allocate().ok_or(MemoryFull)?;
        // A short block leaves the rest of the chunk as its last block did:
        // no read looks there.
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

    /// Holds the chunk with `key`'s block, the index having said it is at
    /// `location`.
    ///
    /// An entry may name a chunk that no longer holds the block: eviction
    /// closed the chunk, and is about to drop the entry, or moved the block
    /// to the other tier, or the chunk already holds another block. So the
    /// index is read again, until it names the chunk that holds the block,
    /// or no change.
    fn hold(&self, key: BlockKey, mut location: Option<Location>) -> HeldBlock<'_> {
        loop {
            let held = match location {
                Some(Location::Memory(chunk)) => {
                    self.memory.read(chunk, key).map(HeldBlock::Memory)
                }
                Some(Location::Disk(chunk)) => {
                    self.disk.chunks().read(chunk, key).map(HeldBlock::Disk)
                }
                // A block being fetched is missing until it lands.
                Some(Location::Fetching) | None => return HeldBlock::Missing,
            };
            if let Some(held) = held {
                return held;
            }
            let again = self.blocks.get(key);
            if again == location {
                return HeldBlock::Missing;
            }
            location = again;
        }
    }
}

/// A block a read holds while it takes its bytes, or found missing.
enum HeldBlock<'cache> {
    Memory(MemoryChunkReader<'cache>),
    Disk(DiskChunkReader<'cache>),
    Missing,
}

/// What [`ObjectCache::move_to_disk`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MoveToDisk {
    /// The block is on disk, and its memory chunk goes free.
    Moved,
    /// The memory chunk held no block, or another eviction had closed it.
    NoBlock,
    /// No disk chunk was free: the block stays in memory.
    DiskFull,
    /// The block left memory another way while it was written, and the
    /// copy on disk went back.
    Superseded,
}

/// A read that the cache served in part: the bytes it takes, and the ranges
/// of the file to fetch for the rest, whole blocks, adjacent ones in one.
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

/// No memory chunk was free for a block. Until the eviction policy is
/// built, the cache holds what fits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MemoryFull;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

// `[0..MIB]` is a list of one fetch, not the range's offsets.
#[allow(clippy::single_range_in_vec_init)]
#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use pollster::block_on;
    use tempfile::TempDir;

    use super::*;

    const MIB: u64 = BLOCK_SIZE_U64;

    /// A cache of `memory_chunks` chunks of memory and `disk_chunks` on
    /// disk, in a directory of its own that lives as long as the cache.
    fn cache(memory_chunks: u32, disk_chunks: u32) -> (TempDir, ObjectCache) {
        let dir = tempfile::tempdir().unwrap();
        let disk = CacheFile::open(
            &dir.path().join("cache"),
            NonZeroU32::new(disk_chunks).unwrap(),
            NonZeroU32::new(8).unwrap(),
            true,
        )
        .unwrap();
        let cache = ObjectCache::new(NonZeroU32::new(memory_chunks).unwrap(), disk).unwrap();
        (dir, cache)
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
        let read = block_on(cache.read(version, range, &mut out)).unwrap();
        for fetch in read.fetches() {
            let fetched = &file[fetch.start as usize..fetch.end as usize];
            read.fill(&mut out, fetch.start, fetched);
            let _ = cache.cache_fetched(version, fetch.start, fetched);
        }
        (out, read.fetches().to_vec())
    }

    fn location(cache: &ObjectCache, version: FileVersion, index: u32) -> Option<Location> {
        cache.blocks.get(BlockKey::new(version.id(), index))
    }

    fn memory_chunk(cache: &ObjectCache, version: FileVersion, index: u32) -> ChunkId {
        let Some(Location::Memory(chunk)) = location(cache, version, index) else {
            panic!("block {index} is in memory");
        };
        chunk
    }

    fn move_to_disk(cache: &ObjectCache, chunk: ChunkId) -> MoveToDisk {
        block_on(cache.move_to_disk(chunk)).unwrap()
    }

    #[test]
    fn a_miss_fetches_whole_blocks_and_then_hits() {
        let (_dir, cache) = cache(4, 1);
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
        let (_dir, cache) = cache(4, 1);
        let file = file_bytes(4 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        cache
            .cache_fetched(version, MIB, &file[MIB as usize..2 * MIB as usize])
            .unwrap();
        cache
            .cache_fetched(version, 3 * MIB, &file[3 * MIB as usize..])
            .unwrap();

        let mut out = Vec::new();
        let read = block_on(cache.read(version, 10..4 * MIB - 10, &mut out)).unwrap();
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
        let (_dir, cache) = cache(1, 1);
        let file = file_bytes(2 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        assert_eq!(cache.cache_fetched(version, 0, &file), Err(MemoryFull));

        let mut out = Vec::new();
        let read = block_on(cache.read(version, 0..2 * MIB, &mut out)).unwrap();
        assert_eq!(read.fetches(), [MIB..2 * MIB]);
        assert_eq!(out[..MIB as usize], file[..MIB as usize]);
    }

    #[test]
    fn a_block_fetched_twice_takes_one_chunk() {
        let (_dir, cache) = cache(4, 1);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        assert_eq!(cache.memory.free_count(), 3);
    }

    #[test]
    fn racing_fetches_of_one_block_take_one_chunk() {
        const THREADS: usize = 8;
        let (_dir, cache) = cache(THREADS as u32, 1);
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
        let (_dir, cache) = cache(1, 1);
        let file = file_bytes(MIB);
        let old = cache.resolve(b"old", Some("v1"), MIB).unwrap();
        cache.cache_fetched(old, 0, &file).unwrap();
        let key = BlockKey::new(old.id(), 0);
        let chunk = memory_chunk(&cache, old, 0);

        // Evicted, as eviction does: closed, but its entry not yet dropped.
        assert!(cache.memory.pin_for_eviction(chunk).unwrap().unpublish());
        let (_, fetches) = read_through(&cache, old, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);

        // Dropped, and the chunk reused for another file's block while a
        // read still holds the old entry.
        assert!(cache.blocks.remove_if(key, Location::Memory(chunk)));
        let other = cache.resolve(b"other", Some("v1"), MIB).unwrap();
        cache.cache_fetched(other, 0, &file_bytes(MIB)).unwrap();
        assert_eq!(location(&cache, other, 0), Some(Location::Memory(chunk)));
        assert!(matches!(
            cache.hold(key, Some(Location::Memory(chunk))),
            HeldBlock::Missing
        ));
    }

    #[test]
    fn an_evicted_block_misses_and_its_chunk_goes_free() {
        let (_dir, cache) = cache(1, 1);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        let chunk = memory_chunk(&cache, version, 0);

        // A read in flight keeps the chunk, not the block.
        let reader = cache.memory.read(chunk, BlockKey::new(version.id(), 0));
        assert!(cache.evict_from_memory(chunk));
        assert!(!cache.evict_from_memory(chunk), "one eviction of a chunk");
        assert_eq!(location(&cache, version, 0), None);
        assert_eq!(cache.memory.free_count(), 0, "the read is still in it");
        drop(reader);
        assert_eq!(cache.memory.free_count(), 1);

        let (out, fetches) = read_through(&cache, version, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, file);
    }

    #[test]
    fn blocks_moved_to_disk_read_back_from_disk() {
        let (_dir, cache) = cache(4, 4);
        let size = 2 * MIB + 5000;
        let file = file_bytes(size);
        let version = cache.resolve(b"a", Some("v1"), size).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        for index in 0..3 {
            let chunk = memory_chunk(&cache, version, index);
            assert_eq!(move_to_disk(&cache, chunk), MoveToDisk::Moved);
            assert!(matches!(
                location(&cache, version, index),
                Some(Location::Disk(_))
            ));
        }
        assert_eq!(cache.memory.free_count(), 4);
        assert_eq!(cache.disk.chunks().free_count(), 1);

        // Whole, within a page, across pages and blocks, and the short last
        // block: every read from disk, none fetched.
        for range in [0..size, 5000..5001, 4095..MIB + 4097, 2 * MIB + 10..size] {
            let (out, fetches) = read_through(&cache, version, &file, range.clone());
            assert!(fetches.is_empty(), "{range:?}");
            assert_eq!(out, file[range.start as usize..range.end as usize]);
        }
    }

    #[test]
    fn a_read_takes_each_block_from_where_it_is() {
        let (_dir, cache) = cache(4, 4);
        let file = file_bytes(4 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        cache
            .cache_fetched(version, 0, &file[..2 * MIB as usize])
            .unwrap();
        cache
            .cache_fetched(version, 3 * MIB, &file[3 * MIB as usize..])
            .unwrap();
        // Block 0 in memory, 1 and 3 on disk, 2 missing.
        for index in [1, 3] {
            let chunk = memory_chunk(&cache, version, index);
            assert_eq!(move_to_disk(&cache, chunk), MoveToDisk::Moved);
        }
        let (out, fetches) = read_through(&cache, version, &file, 100..4 * MIB - 100);
        assert_eq!(fetches, [2 * MIB..3 * MIB]);
        assert_eq!(out, file[100..4 * MIB as usize - 100]);
    }

    #[test]
    fn a_full_disk_keeps_the_block_in_memory() {
        let (_dir, cache) = cache(2, 1);
        let file = file_bytes(2 * MIB);
        let version = cache.resolve(b"a", Some("v1"), 2 * MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        let (first, second) = (
            memory_chunk(&cache, version, 0),
            memory_chunk(&cache, version, 1),
        );
        assert_eq!(move_to_disk(&cache, first), MoveToDisk::Moved);
        assert_eq!(move_to_disk(&cache, second), MoveToDisk::DiskFull);
        assert_eq!(location(&cache, version, 1), Some(Location::Memory(second)));
        let (out, fetches) = read_through(&cache, version, &file, 0..2 * MIB);
        assert!(fetches.is_empty());
        assert_eq!(out, file);
    }

    #[test]
    fn a_free_chunk_has_no_block_to_move_or_evict() {
        let (_dir, cache) = cache(1, 1);
        assert_eq!(move_to_disk(&cache, ChunkId::new(0)), MoveToDisk::NoBlock);
        assert!(!cache.evict_from_memory(ChunkId::new(0)));
        assert!(!cache.evict_from_disk(ChunkId::new(0)));
        assert_eq!(cache.disk.chunks().free_count(), 1);
    }

    #[test]
    fn a_block_evicted_from_disk_misses_and_its_chunk_goes_free() {
        let (_dir, cache) = cache(1, 1);
        let file = file_bytes(MIB);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        cache.cache_fetched(version, 0, &file).unwrap();
        let chunk = memory_chunk(&cache, version, 0);
        assert_eq!(move_to_disk(&cache, chunk), MoveToDisk::Moved);
        let Some(Location::Disk(on_disk)) = location(&cache, version, 0) else {
            panic!("the block is on disk");
        };

        assert!(cache.evict_from_disk(on_disk));
        assert!(!cache.evict_from_disk(on_disk), "one eviction of a chunk");
        assert_eq!(location(&cache, version, 0), None);
        assert_eq!(cache.disk.chunks().free_count(), 1);
        let (out, fetches) = read_through(&cache, version, &file, 0..MIB);
        assert_eq!(fetches, [0..MIB]);
        assert_eq!(out, file);
    }

    #[test]
    fn a_new_version_misses_the_old_ones_blocks() {
        let (_dir, cache) = cache(4, 1);
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
        let (_dir, cache) = cache(1, 1);
        let version = cache.resolve(b"a", Some("v1"), MIB).unwrap();
        let _ = cache.cache_fetched(version, 0, &file_bytes(MIB / 2));
    }
}
