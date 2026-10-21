use super::chunk_table::{ChunkLease, ChunkPin, ChunkTable};
use crate::PAGE_SIZE_U64;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::error::{Error, Result};

/// The disk tier (`doc/03-cache.md`, decision 2): the cache file's data
/// chunks, chunk `i` the `BLOCK_SIZE` bytes at `data_start + i *
/// BLOCK_SIZE`. The rest of the file, and its size, are its owner's.
///
/// A chunk follows the same [`ChunkTable`] round as a memory chunk: a writer
/// holds it while direct I/O writes its block, and each reader while direct
/// I/O reads from it, so no block is written over a read in flight.
pub(in crate::cache) struct DiskChunks {
    data_start: u64,
    table: ChunkTable,
}

impl DiskChunks {
    /// `chunk_count` chunks from `data_start` on, all free. `data_start` is
    /// a page boundary, as `O_DIRECT` needs.
    pub(in crate::cache) fn new(data_start: u64, chunk_count: u32) -> Result<Self> {
        if !data_start.is_multiple_of(PAGE_SIZE_U64) {
            return Err(Error::InvalidArgument(
                "disk chunks must start on a page boundary",
            ));
        }
        // At most 2^32 chunks of 2^20 bytes: no overflow.
        let data_len = u64::from(chunk_count) * BLOCK_SIZE_U64;
        if data_start.checked_add(data_len).is_none() {
            return Err(Error::InvalidArgument(
                "disk chunks end past the largest file offset",
            ));
        }
        Ok(Self {
            data_start,
            table: ChunkTable::new(chunk_count),
        })
    }

    pub(in crate::cache) fn chunk_count(&self) -> u32 {
        self.table.chunk_count()
    }

    pub(in crate::cache) fn free_count(&self) -> u32 {
        self.table.free_count()
    }

    /// A free chunk for one writer to fill; `None` when every chunk is taken.
    pub(in crate::cache) fn allocate(&self) -> Option<DiskChunkWriter<'_>> {
        let lease = self.table.allocate()?;
        Some(DiskChunkWriter {
            offset: self.offset(lease.chunk()),
            lease,
        })
    }

    /// Holds `chunk` to read `block` from it, and marks it accessed. `None`
    /// if the chunk doesn't hold `block`: the index entry that named it is
    /// stale, and the caller looks again.
    pub(in crate::cache) fn read(
        &self,
        chunk: ChunkId,
        block: BlockKey,
    ) -> Option<DiskChunkReader<'_>> {
        Some(DiskChunkReader {
            pin: self.table.pin_block(chunk, block)?,
            offset: self.offset(chunk),
        })
    }

    /// Holds `chunk` with whatever block it holds, as eviction does: not an
    /// access. `None` if no block is published in it.
    pub(in crate::cache) fn pin_for_eviction(&self, chunk: ChunkId) -> Option<DiskChunkReader<'_>> {
        Some(DiskChunkReader {
            pin: self.table.pin(chunk)?,
            offset: self.offset(chunk),
        })
    }

    /// Clears `chunk`'s access bit, and returns it: whether a read held the
    /// chunk since the last call.
    pub(in crate::cache) fn take_accessed(&self, chunk: ChunkId) -> bool {
        self.table.take_accessed(chunk)
    }

    /// Where `chunk` starts in the file. Only for a chunk the table has
    /// checked, by leasing or pinning it.
    fn offset(&self, chunk: ChunkId) -> u64 {
        self.data_start + u64::from(chunk.index()) * BLOCK_SIZE_U64
    }
}

/// A free chunk, its one writer's to fill until [`Self::publish`]. Dropped
/// unpublished, it frees the chunk.
pub(in crate::cache) struct DiskChunkWriter<'chunks> {
    lease: ChunkLease<'chunks>,
    offset: u64,
}

impl<'chunks> DiskChunkWriter<'chunks> {
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.lease.chunk()
    }

    /// Where the chunk starts in the file: a page boundary.
    pub(in crate::cache) fn offset(&self) -> u64 {
        self.offset
    }

    /// Publishes the chunk as holding `block`, once its write is done:
    /// readers may hold it from now on, and the writer goes on as one of
    /// them. It starts unaccessed.
    pub(in crate::cache) fn publish(self, block: BlockKey) -> DiskChunkReader<'chunks> {
        DiskChunkReader {
            pin: self.lease.publish(block),
            offset: self.offset,
        }
    }
}

/// A reader's hold on a published chunk: until it drops, the chunk is not
/// freed, so the bytes at its offset stay `block`'s.
pub(in crate::cache) struct DiskChunkReader<'chunks> {
    pin: ChunkPin<'chunks>,
    offset: u64,
}

impl DiskChunkReader<'_> {
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.pin.chunk()
    }

    pub(in crate::cache) fn block(&self) -> BlockKey {
        self.pin.block()
    }

    /// Where the chunk starts in the file: a page boundary.
    pub(in crate::cache) fn offset(&self) -> u64 {
        self.offset
    }

    /// Unpublishes the chunk, for eviction: no new reader holds it, and it
    /// is freed when its last reader drops, this one included. Whether this
    /// call did it: of two evictions of one chunk, one goes on.
    pub(in crate::cache) fn unpublish(&self) -> bool {
        self.pin.unpublish()
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;

    const DATA_START: u64 = 3 * PAGE_SIZE_U64;

    #[test]
    fn chunks_lie_back_to_back_from_the_data_start() {
        let chunks = DiskChunks::new(DATA_START, 3).unwrap();
        // Held, or each chunk would be freed as soon as it was allocated.
        let writers: Vec<_> = std::iter::from_fn(|| chunks.allocate()).collect();
        let mut offsets: Vec<u64> = writers.iter().map(DiskChunkWriter::offset).collect();
        offsets.sort_unstable();
        let block = BLOCK_SIZE_U64;
        assert_eq!(
            offsets,
            [DATA_START, DATA_START + block, DATA_START + 2 * block]
        );
    }

    #[test]
    fn rejects_a_misaligned_or_overflowing_data_region() {
        assert!(DiskChunks::new(DATA_START + 512, 1).is_err());
        assert!(DiskChunks::new(u64::MAX - PAGE_SIZE_U64 + 1, 1).is_err());
    }

    #[test]
    fn a_read_in_flight_keeps_its_chunk_from_reuse() {
        let chunks = DiskChunks::new(DATA_START, 1).unwrap();
        let block = BlockKey::new(FileVersionId::new(1), 0);
        let chunk = chunks.allocate().unwrap().publish(block).chunk();
        let reader = chunks.read(chunk, block).unwrap();
        assert_eq!(reader.offset(), DATA_START);

        assert!(chunks.pin_for_eviction(chunk).unwrap().unpublish());
        assert!(chunks.read(chunk, block).is_none());
        assert!(chunks.allocate().is_none(), "the read is still in flight");
        drop(reader);
        assert_eq!(chunks.allocate().unwrap().chunk(), chunk);
    }
}
