use std::any::Any;
use std::num::NonZeroU32;

use super::chunk_table::{ChunkLease, ChunkPin, ChunkTable, ChunkTier};
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::io::error::{Error, Result};
use crate::io::io_task::FilePosition;
use crate::io::sync::Arc;

/// The disk tier (`doc/03-cache.md`, decision 2): its chunks, striped over
/// its cache files by number (`doc/04-multi-device.md`, decision 2). Chunk
/// `c` is file `c mod n`'s chunk `c / n`, the `BLOCK_SIZE` bytes at `(c / n)
/// * BLOCK_SIZE` in it: adjacent chunks sit on different files.
///
/// A chunk follows the same [`ChunkTable`] round as a memory chunk: a writer
/// holds it while direct I/O writes its block, and each reader while direct
/// I/O reads from it, so no block is written over a read in flight.
pub(in crate::cache) struct DiskChunks {
    files: NonZeroU32,
    table: ChunkTable,
}

impl DiskChunks {
    /// `chunks_per_file` chunks in each of `files` files, all free.
    pub(in crate::cache) fn new(files: NonZeroU32, chunks_per_file: u32) -> Result<Self> {
        let chunk_count =
            files
                .get()
                .checked_mul(chunks_per_file)
                .ok_or(Error::InvalidArgument(
                    "a disk tier holds 2^32 chunks or fewer",
                ))?;
        // A file holds at most 2^32 chunks of 2^20 bytes: no offset
        // overflows.
        Ok(Self {
            files,
            table: ChunkTable::new(chunk_count),
        })
    }

    pub(in crate::cache) fn chunk_count(&self) -> u32 {
        self.table.chunk_count()
    }

    #[cfg(test)]
    pub(in crate::cache) fn free_count(&self) -> u32 {
        self.table.free_count()
    }

    /// The block published in each chunk, once nothing holds any
    /// ([`ChunkTable::blocks_at_rest`]).
    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn blocks_at_rest(
        &self,
    ) -> std::result::Result<Vec<Option<BlockKey>>, String> {
        self.table.blocks_at_rest()
    }

    /// A free chunk for one writer to fill; `None` when every chunk is taken.
    pub(in crate::cache) fn allocate(&self) -> Option<DiskChunkWriter<'_>> {
        let lease = self.table.allocate()?;
        Some(DiskChunkWriter {
            position: self.position(lease.chunk()),
            lease,
        })
    }

    /// Holds `chunk` to read `block` from it. `None` if the chunk doesn't
    /// hold `block`: the index entry that named it is stale, and the caller
    /// looks again.
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

    /// Holds `chunk` with whatever block it holds, as eviction does. `None`
    /// if no block is published in it.
    pub(in crate::cache) fn pin_for_eviction(&self, chunk: ChunkId) -> Option<DiskChunkReader<'_>> {
        Some(DiskChunkReader {
            pin: self.table.pin(chunk)?,
            position: self.position(chunk),
        })
    }

    /// Where `chunk` starts: file `c mod n`, at `(c / n) * BLOCK_SIZE`. Only
    /// for a chunk the table has checked, by leasing or pinning it.
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

/// A free chunk, its one writer's to fill until [`Self::publish`]. Dropped
/// unpublished, it frees the chunk, once the writes it started are done.
pub(in crate::cache) struct DiskChunkWriter<'chunks> {
    lease: ChunkLease<'chunks>,
    position: FilePosition,
}

impl<'chunks> DiskChunkWriter<'chunks> {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn chunk(&self) -> ChunkId {
        self.lease.chunk()
    }

    /// Where the chunk starts: its file, and a page boundary in it.
    pub(in crate::cache) fn position(&self) -> FilePosition {
        self.position
    }

    /// A hold on the chunk for the direct I/O that writes it, to pass along
    /// with the write: until it drops, the chunk isn't freed, whatever
    /// becomes of this writer. `chunks` is the tier the chunk is in.
    pub(in crate::cache) fn hold_for_write(
        &self,
        chunks: &Arc<DiskChunks>,
    ) -> impl Any + Send + Sync + use<> {
        self.lease.hold_owned(chunks)
    }

    /// Publishes the chunk as holding `block`, once its write is done:
    /// readers may hold it from now on, and the writer goes on as one of
    /// them.
    pub(in crate::cache) fn publish(self, block: BlockKey) -> DiskChunkReader<'chunks> {
        DiskChunkReader {
            pin: self.lease.publish(block),
            position: self.position,
        }
    }
}

/// A reader's hold on a published chunk: until it drops, the chunk is not
/// freed, so the bytes at its offset stay `block`'s.
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

    /// Where the chunk starts: its file, and a page boundary in it.
    pub(in crate::cache) fn position(&self) -> FilePosition {
        self.position
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

    const TWO: NonZeroU32 = NonZeroU32::new(2).unwrap();

    /// Chunks alternate between the files, each file's back to back from
    /// its start.
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
        assert!(DiskChunks::new(TWO, u32::MAX / 2).is_ok());
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
