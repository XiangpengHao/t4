//! The object store cache (`doc/03-cache.md`): aligned blocks of objects,
//! held in memory chunks and disk chunks, found through an index on bbtree.
//!
//! - `chunk_pool`: each tier's chunks, which of them are free, and what
//!   each holds (build step 1).
//! - `block_index`: which blocks of which objects are cached, and where
//!   (build step 2).
//! - `cache_files`: the disk tier, its chunks striped over its cache files,
//!   read and written by direct I/O (`doc/04-multi-device.md`).
//! - `policy`: what the cache asks of its policy, and the default one.
//! - `in_flight`: the blocks being fetched, which a read that needs one
//!   waits for rather than fetching again.
//! - `object_cache`: reads, fills and evictions over them all.
//! - `read_output`: the buffer a read's bytes land in, the caller's as
//!   `Bytes` with no copy.
//! - `t4_caching`: the front end over another `ObjectStore`: files opened
//!   and read through the cache, and an `ObjectStore` itself.

mod block_index;
mod cache_files;
mod chunk_pool;
mod get_queue;
mod in_flight;
mod io_report;
mod object_cache;
pub mod policy;
mod read_output;
mod t4_caching;

pub use t4_caching::{CachedFile, T4Caching, T4CachingBuilder};

use std::fmt;

use bbtree::bbtree_map::InsertError;

/// The bytes of a block, and of the chunk that holds it.
const BLOCK_SIZE: usize = 1 << 20;
const BLOCK_SIZE_U64: u64 = BLOCK_SIZE as u64;

/// A chunk, by its number in its tier: the memory pool's chunks, or the
/// cache file's data chunks. Which tier it is in is its holder's to say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(u32);

impl ChunkId {
    /// Chunk `index` of its tier: below the tier's chunk count.
    pub fn new(index: u32) -> Self {
        Self(index)
    }

    pub fn index(self) -> u32 {
        self.0
    }
}

/// A file version: the id the cache gave a file's path and e_tag
/// (`doc/03-cache.md`, decision 5). Never reused, and below 2^40, as a
/// [`BlockKey`] keeps it in 40 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileVersionId(u64);

impl FileVersionId {
    /// File version ids are below this: 2^40, about 1.1 trillion versions.
    const LIMIT: u64 = 1 << (u64::BITS - BlockKey::INDEX_BITS);

    fn new(id: u64) -> Self {
        assert!(id < Self::LIMIT, "file version ids ran out: 2^40 of them");
        Self(id)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Block `index` of a file version: its bytes `[index * BLOCK_SIZE, (index
/// + 1) * BLOCK_SIZE)`, fewer for a file's last block.
///
/// One `u64`: the file version id in the high 40 bits, the block number in
/// the low 24 (decision 4). So it orders by file version, then block, as
/// the index keeps its keys, and a chunk's owner is one atomic word.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockKey(u64);

impl BlockKey {
    /// The bits of the block number.
    const INDEX_BITS: u32 = 24;
    /// Block numbers are below this: 2^24, so a file is under 16 TiB.
    const INDEX_LIMIT: u32 = 1 << Self::INDEX_BITS;

    fn new(version: FileVersionId, index: u32) -> Self {
        assert!(index < Self::INDEX_LIMIT, "block numbers stop below 2^24");
        Self((version.0 << Self::INDEX_BITS) | u64::from(index))
    }

    /// The key [`Self::packed`] gave: every `u64` is one.
    fn from_packed(packed: u64) -> Self {
        Self(packed)
    }

    fn packed(self) -> u64 {
        self.0
    }

    pub fn file_version(self) -> FileVersionId {
        FileVersionId(self.0 >> Self::INDEX_BITS)
    }

    /// The block's number in its file version.
    pub fn index(self) -> u32 {
        // The low 24 bits, which a `u32` holds.
        (self.0 & u64::from(Self::INDEX_LIMIT - 1)) as u32
    }
}

impl fmt::Debug for BlockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockKey")
            .field("file_version", &self.file_version().get())
            .field("index", &self.index())
            .finish()
    }
}

/// An insert into one of the cache's maps, which ends the process if the
/// global allocator has no page for it, as std's collections do. The maps
/// live on the global allocator, outside the cache's budget (decision 6),
/// like any other allocation of the process.
trait InsertOrAbort<T> {
    fn or_abort(self) -> T;
}

impl<T, K, V> InsertOrAbort<T> for Result<T, InsertError<K, V>> {
    fn or_abort(self) -> T {
        self.unwrap_or_else(|_| {
            eprintln!("t4: out of memory inserting into the cache index");
            std::process::abort()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_keys_order_by_file_then_block_and_unpack() {
        let last_file = FileVersionId::LIMIT - 1;
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
        .map(|(file, index)| BlockKey::new(FileVersionId::new(file), index));
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].packed() < pair[1].packed());
        }
        for key in keys {
            assert_eq!(BlockKey::from_packed(key.packed()), key);
        }
        let key = keys[6];
        assert_eq!(
            (key.file_version().get(), key.index()),
            (last_file, last_index)
        );
    }

    #[test]
    #[should_panic(expected = "file version ids ran out")]
    fn file_ids_stop_below_their_limit() {
        FileVersionId::new(FileVersionId::LIMIT);
    }

    #[test]
    #[should_panic(expected = "block numbers stop")]
    fn block_numbers_stop_below_their_limit() {
        BlockKey::new(FileVersionId::new(0), BlockKey::INDEX_LIMIT);
    }
}
