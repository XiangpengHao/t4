mod cache_stores;
mod io_report;
mod network;
pub mod policy;
mod read_output;
mod storage;
mod t4_caching;

pub use t4_caching::{FileHandle, T4Caching, T4CachingBuilder};

use std::fmt;

use bbtree::bbtree_map::InsertError;

const BLOCK_SIZE: usize = 1 << 20;
const BLOCK_SIZE_U64: u64 = BLOCK_SIZE as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(u32);

impl ChunkId {
    pub fn new(index: u32) -> Self {
        Self(index)
    }

    pub fn index(self) -> u32 {
        self.0
    }
}

/// The id the cache gave a file's path and e_tag; never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileVersionId(u64);

impl FileVersionId {
    const LIMIT: u64 = 1 << (u64::BITS - BlockKey::INDEX_BITS);

    fn new(id: u64) -> Self {
        assert!(id < Self::LIMIT, "file version ids ran out: 2^40 of them");
        Self(id)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Block `index` of a file version, packed in one `u64` so a chunk's owner
/// is one atomic word.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockKey(u64);

impl BlockKey {
    const INDEX_BITS: u32 = 24;
    const INDEX_LIMIT: u32 = 1 << Self::INDEX_BITS;

    fn new(version: FileVersionId, index: u32) -> Self {
        assert!(index < Self::INDEX_LIMIT, "block numbers stop below 2^24");
        Self((version.0 << Self::INDEX_BITS) | u64::from(index))
    }

    fn from_packed(packed: u64) -> Self {
        Self(packed)
    }

    fn packed(self) -> u64 {
        self.0
    }

    pub fn file_version(self) -> FileVersionId {
        FileVersionId(self.0 >> Self::INDEX_BITS)
    }

    pub fn index(self) -> u32 {
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

/// Aborts on OOM as std's collections do: the maps are outside the cache's budget.
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
