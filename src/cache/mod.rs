//! The object store cache (`doc/03-cache.md`): aligned blocks of objects,
//! held in memory chunks and disk chunks, found through an index on bbtree.
//!
//! - `chunk_pool`: each tier's chunks, which of them are free, and what
//!   each holds (build step 1).
//! - `block_index`: which blocks of which objects are cached, and where
//!   (build step 2).

mod block_index;
mod chunk_pool;

use std::fmt;

/// The bytes of a block, and of the chunk that holds it.
const BLOCK_SIZE: usize = 1 << 20;

/// A chunk, by its number in its tier: the memory pool's chunks, or the
/// file's data chunks. Which tier it is in is its holder's to say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ChunkId(u32);

impl ChunkId {
    fn new(index: u32) -> Self {
        Self(index)
    }

    fn index(self) -> u32 {
        self.0
    }
}

/// A version of an object: the id the cache gave its path and e_tag
/// (`doc/03-cache.md`, decision 5). Never reused, and below 2^40, as a
/// [`BlockKey`] keeps it in 40 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FileId(u64);

impl FileId {
    /// File ids are below this: 2^40, about 1.1 trillion versions.
    const LIMIT: u64 = 1 << (u64::BITS - BlockKey::INDEX_BITS);

    fn new(id: u64) -> Self {
        assert!(id < Self::LIMIT, "file ids ran out: 2^40 of them");
        Self(id)
    }

    fn get(self) -> u64 {
        self.0
    }
}

/// Block `index` of a file: its bytes `[index * BLOCK_SIZE, (index + 1) *
/// BLOCK_SIZE)`, fewer for an object's last block.
///
/// One `u64`: the file id in the high 40 bits, the block number in the low
/// 24 (decision 4). So it orders by file, then block, as the index keeps
/// its keys, and a chunk's owner is one atomic word.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct BlockKey(u64);

impl BlockKey {
    /// The bits of the block number.
    const INDEX_BITS: u32 = 24;
    /// Block numbers are below this: 2^24, so a file is under 16 TiB.
    const INDEX_LIMIT: u32 = 1 << Self::INDEX_BITS;

    fn new(file: FileId, index: u32) -> Self {
        assert!(index < Self::INDEX_LIMIT, "block numbers stop below 2^24");
        Self((file.0 << Self::INDEX_BITS) | u64::from(index))
    }

    /// The key [`Self::packed`] gave: every `u64` is one.
    fn from_packed(packed: u64) -> Self {
        Self(packed)
    }

    fn packed(self) -> u64 {
        self.0
    }

    fn file(self) -> FileId {
        FileId(self.0 >> Self::INDEX_BITS)
    }

    fn index(self) -> u32 {
        // The low 24 bits, which a `u32` holds.
        (self.0 & u64::from(Self::INDEX_LIMIT - 1)) as u32
    }
}

impl fmt::Debug for BlockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockKey")
            .field("file", &self.file().get())
            .field("index", &self.index())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_keys_order_by_file_then_block_and_unpack() {
        let last_file = FileId::LIMIT - 1;
        let last_index = BlockKey::INDEX_LIMIT - 1;
        let keys = [
            (0, 0),
            (0, 1),
            (0, last_index),
            (1, 0),
            (1, 7),
            (last_file, 0),
            (last_file, last_index),
        ]
        .map(|(file, index)| BlockKey::new(FileId::new(file), index));
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].packed() < pair[1].packed());
        }
        for key in keys {
            assert_eq!(BlockKey::from_packed(key.packed()), key);
        }
        let key = keys[6];
        assert_eq!((key.file().get(), key.index()), (last_file, last_index));
    }

    #[test]
    #[should_panic(expected = "file ids ran out")]
    fn file_ids_stop_below_their_limit() {
        FileId::new(FileId::LIMIT);
    }

    #[test]
    #[should_panic(expected = "block numbers stop")]
    fn block_numbers_stop_below_their_limit() {
        BlockKey::new(FileId::new(0), BlockKey::INDEX_LIMIT);
    }
}
