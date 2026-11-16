mod cache_stores;
mod io_report;
mod network;
pub mod policy;
mod read_output;
mod storage;
mod t4_caching;

#[cfg(all(feature = "__fuzz", feature = "shuttle", t4_simulated))]
pub use cache_stores::{
    FuzzedHello, FuzzedPath, FuzzedRead, FuzzedSession, FuzzedSpan, FuzzedStep,
};
pub use t4_caching::{FileHandle, T4Caching, T4CachingBuilder};
#[cfg(all(feature = "__fuzz", feature = "shuttle", t4_simulated))]
pub use t4_caching::{ScenarioRead, SharedOperation, SharedScenario, SharedSettings};
#[cfg(feature = "__fuzz")]
pub use {
    cache_stores::FuzzedFrame,
    storage::{FuzzedLogBase, FuzzedVersion},
};

use std::fmt;

use bbtree::bbtree_map::InsertError;
use vstd::prelude::*;

verus! {

// The constants Verus reasons about are literals: it doesn't evaluate shifts.
const BLOCK_SIZE_U64: u64 = 1024 * 1024;

} // verus!

const BLOCK_SIZE: usize = BLOCK_SIZE_U64 as usize;

verus! {

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(u32);

impl View for ChunkId {
    type V = u32;

    closed spec fn view(&self) -> u32 {
        self.0
    }
}

impl ChunkId {
    pub fn new(index: u32) -> (chunk: Self)
        ensures
            chunk@ == index,
    {
        Self(index)
    }

    pub fn index(self) -> (index: u32)
        ensures
            index == self@,
    {
        self.0
    }
}

/// The id the cache gave a file's path and e_tag; never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileVersionId(u64);

impl FileVersionId {
    const LIMIT: u64 = 0x100_0000_0000;

    #[verifier::type_invariant]
    spec fn below_limit(self) -> bool {
        self.0 < Self::LIMIT
    }

    fn new(id: u64) -> Self
        requires
            id < Self::LIMIT,
    {
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

impl View for BlockKey {
    type V = (FileVersionId, u32);

    closed spec fn view(&self) -> (FileVersionId, u32) {
        let limit = Self::INDEX_LIMIT as u64;
        (FileVersionId(self.0 / limit), (self.0 % limit) as u32)
    }
}

impl BlockKey {
    const INDEX_LIMIT: u32 = 0x100_0000;

    fn new(version: FileVersionId, index: u32) -> (key: Self)
        requires
            index < Self::INDEX_LIMIT,
        ensures
            key@ == (version, index),
    {
        assert!(index < Self::INDEX_LIMIT, "block numbers stop below 2^24");
        proof {
            use_type_invariant(version);
        }
        Self(version.0 * u64::from(Self::INDEX_LIMIT) + u64::from(index))
    }

    /// A scan of the index between two keys of a version finds that
    /// version's blocks between them, and nothing else.
    proof fn lemma_scan_between(start: BlockKey, end: BlockKey, key: BlockKey)
        requires
            start@.0 == end@.0,
        ensures
            (start.0 <= key.0 < end.0) <==> (key@.0 == start@.0 && start@.1 <= key@.1 < end@.1),
    {
        let limit = Self::INDEX_LIMIT as u64;
        assert(key@.1 == key.0 % limit && start@.1 == start.0 % limit && end@.1 == end.0 % limit);
    }

    fn from_packed(packed: u64) -> Self {
        Self(packed)
    }

    fn packed(self) -> u64 {
        self.0
    }

    pub fn file_version(self) -> (version: FileVersionId)
        ensures
            version == self@.0,
    {
        FileVersionId(self.0 / u64::from(Self::INDEX_LIMIT))
    }

    pub fn index(self) -> (index: u32)
        ensures
            index == self@.1,
    {
        (self.0 % u64::from(Self::INDEX_LIMIT)) as u32
    }
}

} // verus!

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
