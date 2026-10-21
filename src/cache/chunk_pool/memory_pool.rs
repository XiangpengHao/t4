use std::num::NonZeroU32;
use std::ptr::NonNull;

use rustix::mm::{MapFlags, ProtFlags};

use super::chunk_table::{ChunkLease, ChunkPin, ChunkTable};
use crate::PAGE_SIZE;
use crate::cache::{BLOCK_SIZE, BlockKey, ChunkId};
use crate::io::error::{Error, Result};

/// The memory tier (`doc/03-cache.md`, decision 2): chunks in one mapping,
/// taken when the pool is built and given back when it drops.
///
/// A chunk's bytes follow its [`ChunkTable`] round: its writer alone writes
/// them, through [`MemoryChunkWriter::bytes_mut`], until it publishes them;
/// readers then read them, through [`MemoryChunkReader::bytes`], and the
/// chunk goes to another writer only once the last reader drops.
pub(in crate::cache) struct MemoryPool {
    memory: PoolMemory,
    table: ChunkTable,
}

impl MemoryPool {
    /// A pool of `chunk_count` chunks, all free. Its memory is mapped now;
    /// each page takes room when it is first written.
    pub(in crate::cache) fn new(chunk_count: NonZeroU32) -> Result<Self> {
        Ok(Self {
            memory: PoolMemory::map(chunk_count)?,
            table: ChunkTable::new(chunk_count.get()),
        })
    }

    pub(in crate::cache) fn chunk_count(&self) -> u32 {
        self.table.chunk_count()
    }

    pub(in crate::cache) fn free_count(&self) -> u32 {
        self.table.free_count()
    }

    /// A free chunk for one writer to fill; `None` when every chunk is taken.
    pub(in crate::cache) fn allocate(&self) -> Option<MemoryChunkWriter<'_>> {
        Some(MemoryChunkWriter {
            lease: self.table.allocate()?,
            memory: &self.memory,
        })
    }

    /// Holds `chunk` to copy `block` from it, and marks it accessed. `None`
    /// if the chunk doesn't hold `block`: the index entry that named it is
    /// stale, and the caller looks again.
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

    /// Holds `chunk` with whatever block it holds, as eviction does: not an
    /// access. `None` if no block is published in it.
    pub(in crate::cache) fn pin_for_eviction(
        &self,
        chunk: ChunkId,
    ) -> Option<MemoryChunkReader<'_>> {
        Some(MemoryChunkReader {
            pin: self.table.pin(chunk)?,
            memory: &self.memory,
        })
    }

    /// Clears `chunk`'s access bit, and returns it: whether a read held the
    /// chunk since the last call.
    pub(in crate::cache) fn take_accessed(&self, chunk: ChunkId) -> bool {
        self.table.take_accessed(chunk)
    }
}

/// A free chunk, its one writer's to fill until [`Self::publish`]. Dropped
/// unpublished, it frees the chunk.
pub(in crate::cache) struct MemoryChunkWriter<'pool> {
    lease: ChunkLease<'pool>,
    memory: &'pool PoolMemory,
}

impl<'pool> MemoryChunkWriter<'pool> {
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.lease.chunk()
    }

    /// The chunk's `BLOCK_SIZE` bytes, as its last block left them: the
    /// writer writes what the new block needs, and readers read only within
    /// the block's length.
    pub(in crate::cache) fn bytes_mut(&mut self) -> &mut [u8] {
        let start = self.memory.chunk_start(self.lease.chunk());
        // SAFETY: the chunk is this writer's alone. The free bitmap leased it
        // to one writer, and no reader can pin it before `publish`, which
        // takes the writer; its last readers dropped before it was freed.
        // The slice borrows the writer, so it ends before `publish`.
        unsafe { std::slice::from_raw_parts_mut(start.as_ptr(), BLOCK_SIZE) }
    }

    /// Publishes the chunk as holding `block`: readers may hold it from now
    /// on, and the writer goes on as one of them. It starts unaccessed.
    pub(in crate::cache) fn publish(self, block: BlockKey) -> MemoryChunkReader<'pool> {
        MemoryChunkReader {
            pin: self.lease.publish(block),
            memory: self.memory,
        }
    }
}

/// A reader's hold on a published chunk: until it drops, the chunk is not
/// freed, so its bytes stay `block`'s.
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

    /// The chunk's `BLOCK_SIZE` bytes: `block`'s, within the block's length.
    pub(in crate::cache) fn bytes(&self) -> &[u8] {
        let start = self.memory.chunk_start(self.pin.chunk());
        // SAFETY: the pin keeps the chunk from being freed, so no writer
        // holds it while the slice lives, and its writer's writes came
        // before the publish the pin saw (`ChunkTable::pin`).
        unsafe { std::slice::from_raw_parts(start.as_ptr(), BLOCK_SIZE) }
    }

    /// Unpublishes the chunk, for eviction: no new reader holds it, and it
    /// is freed when its last reader drops, this one included. Whether this
    /// call did it: of two evictions of one chunk, one goes on.
    pub(in crate::cache) fn unpublish(&self) -> bool {
        self.pin.unpublish()
    }
}

/// The pool's bytes: `chunk_count` chunks of `BLOCK_SIZE`, back to back.
///
/// One anonymous mapping, not `std::alloc`: readers and writers see a chunk
/// as `[u8]`, so its bytes must start initialized, and `std::alloc` zeroes
/// a page-aligned allocation by writing every byte, faulting in a pool of
/// tens of GB at startup. A mapping's pages are zeros the kernel supplies on
/// first touch, it is page-aligned, so every chunk is, as `O_DIRECT` needs,
/// and it goes back to the kernel on drop, whatever the global allocator.
struct PoolMemory {
    start: NonNull<u8>,
    chunk_count: u32,
}

// SAFETY: the mapping is plain memory, valid until `PoolMemory` drops, and
// only reached through raw pointers here; `MemoryPool` hands out its chunks
// under the rules of `ChunkTable`.
unsafe impl Send for PoolMemory {}
unsafe impl Sync for PoolMemory {}

impl PoolMemory {
    fn map(chunk_count: NonZeroU32) -> Result<Self> {
        let len = Self::len_of(chunk_count.get()).ok_or(Error::InvalidArgument(
            "memory pool larger than the address space",
        ))?;
        // SAFETY: a new private mapping, at an address the kernel picks: it
        // aliases nothing.
        let start = unsafe {
            rustix::mm::mmap_anonymous(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }
        .map_err(std::io::Error::from)?;
        let start = NonNull::new(start.cast::<u8>()).expect("a mapping is never at address 0");
        debug_assert!((start.as_ptr() as usize).is_multiple_of(PAGE_SIZE));
        Ok(Self {
            start,
            chunk_count: chunk_count.get(),
        })
    }

    /// The bytes `chunk_count` chunks take; `None` if no mapping can be that
    /// large.
    fn len_of(chunk_count: u32) -> Option<usize> {
        usize::try_from(chunk_count).ok()?.checked_mul(BLOCK_SIZE)
    }

    /// Where `chunk` starts: page-aligned, `BLOCK_SIZE` bytes before the
    /// next chunk.
    fn chunk_start(&self, chunk: ChunkId) -> NonNull<u8> {
        assert!(
            chunk.index() < self.chunk_count,
            "{chunk:?} is past the pool"
        );
        // SAFETY: the chunk lies within the mapping, whose length `map`
        // checked fits a `usize`.
        unsafe { self.start.add(chunk.index() as usize * BLOCK_SIZE) }
    }
}

impl Drop for PoolMemory {
    fn drop(&mut self) {
        let len = Self::len_of(self.chunk_count).expect("`map` checked the length");
        // SAFETY: the mapping is this struct's, and nothing uses it any more:
        // every writer and reader borrows the pool.
        let _ = unsafe { rustix::mm::munmap(self.start.as_ptr().cast(), len) };
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileId;
    use crate::io::sync::Mutex;

    fn pool(chunk_count: u32) -> MemoryPool {
        MemoryPool::new(NonZeroU32::new(chunk_count).unwrap()).unwrap()
    }

    /// Writes `block`'s bytes into a chunk, as a fetch would: its key, then
    /// a byte of its file id throughout.
    fn fill(writer: &mut MemoryChunkWriter<'_>, block: BlockKey) {
        let bytes = writer.bytes_mut();
        bytes.fill(block.file().get() as u8);
        bytes[..8].copy_from_slice(&block.file().get().to_le_bytes());
        bytes[8..12].copy_from_slice(&block.index().to_le_bytes());
    }

    /// Whether `reader` sees what [`fill`] wrote for its block: its key, and
    /// the fill byte at the end of every page.
    fn holds_its_block(reader: &MemoryChunkReader<'_>) -> bool {
        let (block, bytes) = (reader.block(), reader.bytes());
        let tag = block.file().get() as u8;
        bytes[..8] == block.file().get().to_le_bytes()
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
    fn chunks_are_page_aligned_and_apart() {
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
    }

    #[test]
    fn a_published_chunk_reads_back_its_block() {
        let pool = pool(2);
        let block = BlockKey::new(FileId::new(7), 3);
        let chunk = publish(&pool, block);
        let reader = pool.read(chunk, block).unwrap();
        assert_eq!(reader.bytes().len(), BLOCK_SIZE);
        assert!(holds_its_block(&reader));
        assert!(pool.read(chunk, BlockKey::new(FileId::new(7), 4)).is_none());
    }

    #[test]
    fn an_evicted_chunk_is_freed_after_its_last_reader() {
        let pool = pool(1);
        let block = BlockKey::new(FileId::new(1), 0);
        let chunk = publish(&pool, block);
        let reader = pool.read(chunk, block).unwrap();
        assert!(pool.pin_for_eviction(chunk).unwrap().unpublish());
        assert!(pool.allocate().is_none(), "the reader still copies from it");
        assert!(holds_its_block(&reader));
        drop(reader);
        assert_eq!(pool.free_count(), 1);
    }

    /// Threads fill, publish, read and evict chunks of a pool too small for
    /// them all, so chunks are reused under readers holding stale entries.
    #[test]
    fn concurrent_allocate_read_and_evict() {
        const THREADS: u64 = 4;
        const ROUNDS: u32 = 200;
        let pool = pool(4);
        // What an index would hold: the published blocks, and their chunks.
        let published = Mutex::new(Vec::<(ChunkId, BlockKey)>::new());
        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let (pool, published) = (&pool, &published);
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        let block = BlockKey::new(FileId::new(thread), round);
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
    }
}
