use std::any::Any;
use std::num::NonZeroU32;
use std::ptr::NonNull;

use rustix::mm::{MapFlags, ProtFlags};

use super::chunks::{ChunkLease, ChunkPin, ChunkTable, ChunkTier};
use crate::PAGE_SIZE;
use crate::cache::{BLOCK_SIZE, BlockKey, ChunkId};
use crate::io::aligned_buf::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

pub(in crate::cache) struct MemoryPool {
    memory: PoolMemory,
    table: ChunkTable,
}

impl MemoryPool {
    pub(in crate::cache) fn new(chunk_count: NonZeroU32) -> Result<Self> {
        Ok(Self {
            memory: PoolMemory::map(chunk_count)?,
            table: ChunkTable::new(chunk_count.get()),
        })
    }

    pub(in crate::cache) fn chunk_count(&self) -> u32 {
        self.table.chunk_count()
    }

    #[cfg(test)]
    pub(in crate::cache) fn free_count(&self) -> u32 {
        self.table.free_count()
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn blocks_at_rest(
        &self,
    ) -> std::result::Result<Vec<Option<BlockKey>>, String> {
        self.table.blocks_at_rest()
    }

    pub(in crate::cache) fn allocate(&self) -> Option<MemoryChunkWriter<'_>> {
        Some(MemoryChunkWriter {
            lease: self.table.allocate()?,
            memory: &self.memory,
        })
    }

    pub(in crate::cache) fn read(
        &self,
        chunk: ChunkId,
        block: BlockKey,
    ) -> Option<MemoryChunkReader<'_>> {
        Some(MemoryChunkReader {
            pin: self.table.pin_block(chunk, block)?,
            memory: &self.memory,
        })
    }

    pub(in crate::cache) fn pin_for_eviction(
        &self,
        chunk: ChunkId,
    ) -> Option<MemoryChunkReader<'_>> {
        Some(MemoryChunkReader {
            pin: self.table.pin(chunk)?,
            memory: &self.memory,
        })
    }
}

impl ChunkTier for MemoryPool {
    fn chunk_table(&self) -> &ChunkTable {
        &self.table
    }
}

pub(in crate::cache) struct MemoryChunkWriter<'pool> {
    lease: ChunkLease<'pool>,
    memory: &'pool PoolMemory,
}

impl<'pool> MemoryChunkWriter<'pool> {
    #[cfg(test)]
    pub(in crate::cache) fn bytes_mut(&mut self) -> &mut [u8] {
        let start = self.memory.chunk_start(self.lease.chunk());
        // SAFETY: leased to this writer alone; the slice borrows it, so it ends before `publish`.
        unsafe { std::slice::from_raw_parts_mut(start.as_ptr(), BLOCK_SIZE) }
    }

    #[cfg(test)]
    pub(in crate::cache) fn publish(self, block: BlockKey) -> MemoryChunkReader<'pool> {
        MemoryChunkReader {
            pin: self.lease.publish(block),
            memory: self.memory,
        }
    }

    pub(in crate::cache) fn lend_for_read(
        self,
        pool: &Arc<MemoryPool>,
        len: NonZeroU32,
    ) -> (AlignedBuf, LentMemoryChunk<'pool>) {
        let len_bytes = len.get() as usize;
        assert!(
            len_bytes <= BLOCK_SIZE && len_bytes.is_multiple_of(PAGE_SIZE),
            "a read fills whole pages of one chunk"
        );
        let hold = self.lease.hold_owned(pool);
        let start = self.memory.chunk_start(self.lease.chunk());
        // SAFETY: page-aligned; unpublished, so only the read writes it; `hold` keeps it alive.
        let buffer = unsafe { AlignedBuf::lent(start, len, Arc::new(hold)) };
        let lent = LentMemoryChunk {
            lease: self.lease,
            memory: self.memory,
        };
        (buffer, lent)
    }
}

pub(in crate::cache) struct LentMemoryChunk<'pool> {
    lease: ChunkLease<'pool>,
    memory: &'pool PoolMemory,
}

impl<'pool> LentMemoryChunk<'pool> {
    pub(in crate::cache) fn publish(
        self,
        filled: AlignedBuf,
        block: BlockKey,
    ) -> MemoryChunkReader<'pool> {
        let start = self.memory.chunk_start(self.lease.chunk());
        assert_eq!(
            filled.as_ptr(),
            start.as_ptr().cast_const(),
            "a lent chunk publishes once its own buffer is back"
        );
        drop(filled);
        MemoryChunkReader {
            pin: self.lease.publish(block),
            memory: self.memory,
        }
    }
}

pub(in crate::cache) struct MemoryChunkReader<'pool> {
    pin: ChunkPin<'pool>,
    memory: &'pool PoolMemory,
}

impl MemoryChunkReader<'_> {
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.pin.chunk()
    }

    pub(in crate::cache) fn block(&self) -> BlockKey {
        self.pin.block()
    }

    pub(in crate::cache) fn lend_for_write(
        &self,
        pool: &Arc<MemoryPool>,
        with: impl Any + Send + Sync,
    ) -> AlignedBuf {
        let hold = self.pin.hold_owned(pool);
        let start = self.memory.chunk_start(self.pin.chunk());
        // SAFETY: page-aligned whole pages; `hold` keeps it unfreed and mapped; nothing writes it.
        unsafe { AlignedBuf::lent(start, PoolMemory::CHUNK_LEN, Arc::new((hold, with))) }
    }

    /// All `BLOCK_SIZE` bytes: only the block's length of them are `block`'s.
    pub(in crate::cache) fn bytes(&self) -> &[u8] {
        let start = self.memory.chunk_start(self.pin.chunk());
        // SAFETY: the pin keeps writers out, and saw the publish that followed their writes.
        unsafe { std::slice::from_raw_parts(start.as_ptr(), BLOCK_SIZE) }
    }

    pub(in crate::cache) fn unpublish(&self) -> bool {
        self.pin.unpublish()
    }
}

/// An anonymous mapping, not `std::alloc`, whose zeroing would fault in the whole pool at once.
/// Chunks start on a huge page: a cold pool faults 2 MiB at a time, not 4 KiB.
struct PoolMemory {
    mapping: NonNull<u8>,
    start: NonNull<u8>,
    chunk_count: u32,
}

// SAFETY: plain memory, valid until drop; `MemoryPool` hands out chunks under `ChunkTable`'s rules.
unsafe impl Send for PoolMemory {}
unsafe impl Sync for PoolMemory {}

impl PoolMemory {
    const CHUNK_LEN: NonZeroU32 = NonZeroU32::new(BLOCK_SIZE as u32).expect("a chunk has bytes");

    /// x86-64's, and arm64's with 4 KiB pages.
    const HUGE_PAGE: usize = 2 << 20;

    fn map(chunk_count: NonZeroU32) -> Result<Self> {
        let mapping_len = Self::mapping_len(chunk_count.get()).ok_or(Error::InvalidArgument(
            "memory pool larger than the address space",
        ))?;
        // SAFETY: a new private mapping, at an address the kernel picks, aliases nothing.
        let mapping = unsafe {
            rustix::mm::mmap_anonymous(
                std::ptr::null_mut(),
                mapping_len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }
        .map_err(std::io::Error::from)?;
        let mapping = NonNull::new(mapping.cast::<u8>()).expect("a mapping is never at address 0");
        let skip = mapping.as_ptr().align_offset(Self::HUGE_PAGE);
        // SAFETY: the mapping is a huge page longer than the chunks, and `skip` is less than one.
        let start = unsafe { mapping.add(skip) };
        let memory = Self {
            mapping,
            start,
            chunk_count: chunk_count.get(),
        };
        memory.ask_for_huge_pages();
        Ok(memory)
    }

    /// Advice only: if refused, the chunks take small pages.
    #[cfg(target_os = "linux")]
    fn ask_for_huge_pages(&self) {
        let len = Self::len_of(self.chunk_count).expect("`map` checked the length");
        // SAFETY: the range is the chunks', within this mapping; advice changes no byte.
        let _ = unsafe {
            rustix::mm::madvise(
                self.start.as_ptr().cast(),
                len,
                rustix::mm::Advice::LinuxHugepage,
            )
        };
    }

    #[cfg(not(target_os = "linux"))]
    fn ask_for_huge_pages(&self) {}

    fn len_of(chunk_count: u32) -> Option<usize> {
        usize::try_from(chunk_count).ok()?.checked_mul(BLOCK_SIZE)
    }

    /// A huge page more, to start the chunks at one's boundary.
    fn mapping_len(chunk_count: u32) -> Option<usize> {
        Self::len_of(chunk_count)?.checked_add(Self::HUGE_PAGE)
    }

    fn chunk_start(&self, chunk: ChunkId) -> NonNull<u8> {
        assert!(
            chunk.index() < self.chunk_count,
            "{chunk:?} is past the pool"
        );
        // SAFETY: the chunk lies within the mapping, whose length `map` checked fits a `usize`.
        unsafe { self.start.add(chunk.index() as usize * BLOCK_SIZE) }
    }
}

impl Drop for PoolMemory {
    fn drop(&mut self) {
        let len = Self::mapping_len(self.chunk_count).expect("`map` checked the length");
        // SAFETY: the mapping is this struct's, and every writer and reader borrows the pool.
        let _ = unsafe { rustix::mm::munmap(self.mapping.as_ptr().cast(), len) };
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;
    #[cfg(feature = "shuttle")]
    use crate::io::sync::Mutex;

    fn pool(chunk_count: u32) -> MemoryPool {
        MemoryPool::new(NonZeroU32::new(chunk_count).unwrap()).unwrap()
    }

    fn fill(writer: &mut MemoryChunkWriter<'_>, block: BlockKey) {
        let bytes = writer.bytes_mut();
        bytes.fill(block.file_version().get() as u8);
        bytes[..8].copy_from_slice(&block.file_version().get().to_le_bytes());
        bytes[8..12].copy_from_slice(&block.index().to_le_bytes());
    }

    fn holds_its_block(reader: &MemoryChunkReader<'_>) -> bool {
        let (block, bytes) = (reader.block(), reader.bytes());
        let tag = block.file_version().get() as u8;
        bytes[..8] == block.file_version().get().to_le_bytes()
            && bytes[8..12] == block.index().to_le_bytes()
            && bytes
                .chunks(PAGE_SIZE)
                .all(|page| page[PAGE_SIZE - 1] == tag)
    }

    fn publish(pool: &MemoryPool, block: BlockKey) -> ChunkId {
        let mut writer = pool.allocate().unwrap();
        fill(&mut writer, block);
        writer.publish(block).chunk()
    }

    #[test]
    fn the_chunks_start_at_a_huge_pages_boundary() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let pool = pool(3);
            let first = pool.memory.chunk_start(ChunkId::new(0)).as_ptr() as usize;
            assert!(first.is_multiple_of(PoolMemory::HUGE_PAGE));
        });
    }

    #[test]
    fn chunks_are_page_aligned_and_apart() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let pool = pool(4);
            let mut writers: Vec<_> = std::iter::from_fn(|| pool.allocate()).collect();
            assert_eq!(writers.len(), 4);
            let mut starts: Vec<usize> = writers
                .iter_mut()
                .map(|writer| writer.bytes_mut().as_ptr() as usize)
                .collect();
            assert!(starts.iter().all(|start| start.is_multiple_of(PAGE_SIZE)));
            starts.sort_unstable();
            assert!(
                starts
                    .windows(2)
                    .all(|pair| pair[1] - pair[0] >= BLOCK_SIZE)
            );
        });
    }

    #[test]
    fn a_published_chunk_reads_back_its_block() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let pool = pool(2);
            let block = BlockKey::new(FileVersionId::new(7), 3);
            let chunk = publish(&pool, block);
            let reader = pool.read(chunk, block).unwrap();
            assert_eq!(reader.bytes().len(), BLOCK_SIZE);
            assert!(holds_its_block(&reader));
            assert!(
                pool.read(chunk, BlockKey::new(FileVersionId::new(7), 4))
                    .is_none()
            );
        });
    }

    #[test]
    fn an_evicted_chunk_is_freed_after_its_last_reader() {
        crate::io::test_schedule::TestSchedule::once(|| {
            let pool = pool(1);
            let block = BlockKey::new(FileVersionId::new(1), 0);
            let chunk = publish(&pool, block);
            let reader = pool.read(chunk, block).unwrap();
            assert!(pool.pin_for_eviction(chunk).unwrap().unpublish());
            assert!(pool.allocate().is_none(), "the reader still copies from it");
            assert!(holds_its_block(&reader));
            drop(reader);
            assert_eq!(pool.free_count(), 1);
        });
    }

    /// A pool too small for every thread, so chunks are reused under readers with stale entries.
    #[cfg(feature = "shuttle")]
    #[test]
    fn concurrent_allocate_read_and_evict() {
        crate::io::test_schedule::TestSchedule::check(|| {
            const THREADS: u64 = 4;
            const ROUNDS: u32 = 3;
            let pool = pool(4);
            // Stands in for the index.
            let published = Mutex::new(Vec::<(ChunkId, BlockKey)>::new());
            crate::io::sync::thread::scope(|scope| {
                for thread in 0..THREADS {
                    let (pool, published) = (&pool, &published);
                    scope.spawn(move || {
                        for round in 0..ROUNDS {
                            let block = BlockKey::new(FileVersionId::new(thread), round);
                            if let Some(mut writer) = pool.allocate() {
                                fill(&mut writer, block);
                                let reader = writer.publish(block);
                                assert!(holds_its_block(&reader));
                                published.lock().unwrap().push((reader.chunk(), block));
                            }
                            let entry = {
                                let published = published.lock().unwrap();
                                published
                                    .get(round as usize % published.len().max(1))
                                    .copied()
                            };
                            // Maybe evicted, and its chunk reused, by now.
                            if let Some((chunk, block)) = entry
                                && let Some(reader) = pool.read(chunk, block)
                            {
                                assert!(holds_its_block(&reader));
                            }
                            if round % 2 == 1 {
                                let oldest = {
                                    let mut published = published.lock().unwrap();
                                    (!published.is_empty()).then(|| published.remove(0))
                                };
                                if let Some((chunk, block)) = oldest {
                                    let victim = pool.pin_for_eviction(chunk).unwrap();
                                    assert_eq!(victim.block(), block);
                                    assert!(victim.unpublish());
                                }
                            }
                        }
                    });
                }
            });
            for (chunk, block) in published.into_inner().unwrap() {
                assert!(pool.read(chunk, block).unwrap().unpublish());
            }
            assert_eq!(pool.free_count(), pool.chunk_count());
        });
    }
}
