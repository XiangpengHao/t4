use std::any::Any;
use std::num::NonZeroU32;

use crate::cache::storage::chunks::{ChunkLease, ChunkPin, ChunkTable, ChunkTier};
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::error::{Error, Result};
use crate::io::io_task::FilePosition;
use crate::io::sync::Arc;

pub(in crate::cache) struct DiskChunks {
    files: NonZeroU32,
    table: ChunkTable,
}

impl DiskChunks {
    pub(in crate::cache) fn new(files: NonZeroU32, chunks_per_file: u32) -> Result<Self> {
        // A file holds at most 2^32 chunks of 2^20 bytes: no offset overflows.
        Ok(Self {
            files,
            table: ChunkTable::new(Self::total_chunks(files, chunks_per_file)?),
        })
    }

    fn total_chunks(files: NonZeroU32, chunks_per_file: u32) -> Result<u32> {
        files
            .get()
            .checked_mul(chunks_per_file)
            .ok_or(Error::InvalidArgument(
                "a disk tier holds 2^32 chunks or fewer",
            ))
    }

    pub(in crate::cache) fn chunk_count(&self) -> u32 {
        self.table.chunk_count()
    }

    #[cfg(test)]
    pub(in crate::cache) fn free_count(&self) -> u32 {
        self.table.free_count()
    }

    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn blocks_at_rest(
        &self,
    ) -> std::result::Result<Vec<Option<BlockKey>>, String> {
        self.table.blocks_at_rest()
    }

    pub(in crate::cache) fn restore(&self, chunk: ChunkId, block: BlockKey) {
        self.table.restore(chunk, block);
    }

    pub(in crate::cache) fn close(&self) {
        self.table.close();
    }

    pub(in crate::cache) fn is_closed(&self) -> bool {
        self.table.is_closed()
    }

    pub(in crate::cache) fn allocate(&self) -> Option<DiskChunkWriter<'_>> {
        let lease = self.table.allocate()?;
        Some(DiskChunkWriter {
            position: self.position(lease.chunk()),
            lease,
        })
    }

    pub(in crate::cache) fn read(
        &self,
        chunk: ChunkId,
        block: BlockKey,
    ) -> Option<DiskChunkReader<'_>> {
        Some(DiskChunkReader {
            pin: self.table.pin_block(chunk, block)?,
            position: self.position(chunk),
        })
    }

    pub(in crate::cache) fn pin_for_eviction(&self, chunk: ChunkId) -> Option<DiskChunkReader<'_>> {
        Some(DiskChunkReader {
            pin: self.table.pin(chunk)?,
            position: self.position(chunk),
        })
    }

    fn position(&self, chunk: ChunkId) -> FilePosition {
        let (index, files) = (chunk.index(), self.files.get());
        FilePosition::new(index % files, u64::from(index / files) * BLOCK_SIZE_U64)
    }
}

impl ChunkTier for DiskChunks {
    fn chunk_table(&self) -> &ChunkTable {
        &self.table
    }
}

pub(in crate::cache) struct DiskChunkWriter<'chunks> {
    lease: ChunkLease<'chunks>,
    position: FilePosition,
}

impl<'chunks> DiskChunkWriter<'chunks> {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.lease.chunk()
    }

    pub(in crate::cache) fn position(&self) -> FilePosition {
        self.position
    }

    pub(in crate::cache) fn hold_for_write(
        &self,
        chunks: &Arc<DiskChunks>,
    ) -> impl Any + Send + Sync + use<> {
        self.lease.hold_owned(chunks)
    }

    pub(in crate::cache) fn publish(self, block: BlockKey) -> DiskChunkReader<'chunks> {
        DiskChunkReader {
            pin: self.lease.publish(block),
            position: self.position,
        }
    }
}

pub(in crate::cache) struct DiskChunkReader<'chunks> {
    pin: ChunkPin<'chunks>,
    position: FilePosition,
}

impl DiskChunkReader<'_> {
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.pin.chunk()
    }

    pub(in crate::cache) fn block(&self) -> BlockKey {
        self.pin.block()
    }

    pub(in crate::cache) fn position(&self) -> FilePosition {
        self.position
    }

    pub(in crate::cache) fn unpublish(&self) -> bool {
        self.pin.unpublish()
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;

    const TWO: NonZeroU32 = NonZeroU32::new(2).unwrap();

    #[test]
    fn chunks_stripe_over_the_files() {
        let chunks = DiskChunks::new(TWO, 2).unwrap();
        // Held, or each chunk would be freed as soon as it was allocated.
        let writers: Vec<_> = std::iter::from_fn(|| chunks.allocate()).collect();
        let mut positions: Vec<_> = writers
            .iter()
            .map(|writer| (writer.chunk().index(), writer.position()))
            .collect();
        positions.sort_unstable_by_key(|&(chunk, _)| chunk);
        let block = BLOCK_SIZE_U64;
        assert_eq!(
            positions,
            [
                (0, FilePosition::new(0, 0)),
                (1, FilePosition::new(1, 0)),
                (2, FilePosition::new(0, block)),
                (3, FilePosition::new(1, block)),
            ]
        );
    }

    #[test]
    fn rejects_more_chunks_than_a_tier_numbers() {
        assert!(DiskChunks::new(TWO, u32::MAX).is_err());
        // Counted, not built: its chunk table would take 64 GiB, which CI's runners refuse.
        assert_eq!(
            DiskChunks::total_chunks(TWO, u32::MAX / 2).unwrap(),
            u32::MAX - 1
        );
    }

    #[test]
    fn a_read_in_flight_keeps_its_chunk_from_reuse() {
        let chunks = DiskChunks::new(NonZeroU32::MIN, 1).unwrap();
        let block = BlockKey::new(FileVersionId::new(1), 0);
        let chunk = chunks.allocate().unwrap().publish(block).chunk();
        let reader = chunks.read(chunk, block).unwrap();
        assert_eq!(reader.position(), FilePosition::new(0, 0));

        assert!(chunks.pin_for_eviction(chunk).unwrap().unpublish());
        assert!(chunks.read(chunk, block).is_none());
        assert!(chunks.allocate().is_none(), "the read is still in flight");
        drop(reader);
        assert_eq!(chunks.allocate().unwrap().chunk(), chunk);
    }
}
