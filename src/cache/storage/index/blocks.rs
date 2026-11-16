use std::ops::{ControlFlow, Range};

use bbtree::BBTreeMap;

use super::paths::FileVersion;
use super::read_plan::{ReadPlan, ReadPlanner};
use crate::cache::InsertOrAbort;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) enum Location {
    Memory(ChunkId),
    Disk(ChunkId),
}

/// Packed `u64` keys, not an `InlineKey` impl: `get_copied` reads them without a latch,
/// which it does for no key under `WithInlineKey`.
pub(in crate::cache) struct BlockTable {
    blocks: BBTreeMap<u64, Location>,
}

impl BlockTable {
    pub(in crate::cache) fn new() -> Self {
        Self {
            blocks: BBTreeMap::new(),
        }
    }

    pub(in crate::cache) fn get(&self, key: BlockKey) -> Option<Location> {
        self.blocks.get_copied(&key.packed())
    }

    #[cfg(test)]
    pub(in crate::cache) fn insert(&self, key: BlockKey, location: Location) -> Option<Location> {
        self.blocks.insert(key.packed(), location).or_abort()
    }

    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn entries(&self) -> Vec<(BlockKey, Location)> {
        let mut entries = Vec::new();
        self.blocks.scan::<u64, ()>(.., |key, location| {
            entries.push((BlockKey::from_packed(*key), *location));
            ControlFlow::Continue(())
        });
        entries
    }

    pub(in crate::cache) fn insert_if_absent(&self, key: BlockKey, location: Location) -> bool {
        self.blocks
            .update_or_insert(key.packed(), location, |_, _, _| ())
            .or_abort()
            .is_none()
    }

    pub(in crate::cache) fn replace_if(
        &self,
        key: BlockKey,
        expected: Location,
        new: Location,
    ) -> bool {
        self.blocks
            .update(&key.packed(), |_, location| {
                let at_expected = *location == expected;
                if at_expected {
                    *location = new;
                }
                at_expected
            })
            .unwrap_or(false)
    }

    pub(in crate::cache) fn remove_if(&self, key: BlockKey, expected: Location) -> bool {
        self.blocks
            .remove_if(&key.packed(), |_, location| *location == expected)
            .is_some()
    }

    pub(in crate::cache) fn on_disk(&self, version: FileVersion) -> Vec<(u32, ChunkId)> {
        // The block one past the file's last is a block number too (`FileVersion::MAX_SIZE`).
        let blocks = version.size().div_ceil(BLOCK_SIZE_U64) as u32;
        let keys =
            BlockKey::new(version.id(), 0).packed()..BlockKey::new(version.id(), blocks).packed();
        let mut on_disk = Vec::new();
        self.blocks.scan(keys, |key, location| {
            if let Location::Disk(chunk) = *location {
                on_disk.push((BlockKey::from_packed(*key).index(), chunk));
            }
            ControlFlow::<()>::Continue(())
        });
        on_disk
    }

    /// Neither a snapshot nor a hold: a block may move or leave as eviction goes on.
    pub(in crate::cache) fn plan_read(&self, version: FileVersion, range: Range<u64>) -> ReadPlan {
        let mut planner = ReadPlanner::new(version, range);
        let keys = planner.keys();
        if !keys.is_empty() {
            let scanned = keys.start.packed()..keys.end.packed();
            self.blocks.scan(scanned, |key, location| {
                planner.found(BlockKey::from_packed(*key), *location);
                ControlFlow::<()>::Continue(())
            });
        }
        planner.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;

    fn key(file: u64, index: u32) -> BlockKey {
        BlockKey::new(FileVersionId::new(file), index)
    }

    fn memory(chunk: u32) -> Location {
        Location::Memory(ChunkId::new(chunk))
    }

    fn disk(chunk: u32) -> Location {
        Location::Disk(ChunkId::new(chunk))
    }

    #[test]
    fn insert_replaces_and_returns_the_old_location() {
        let table = BlockTable::new();
        assert_eq!(table.get(key(1, 0)), None);
        assert_eq!(table.insert(key(1, 0), disk(5)), None);
        assert_eq!(table.insert(key(1, 0), memory(3)), Some(disk(5)));
        assert_eq!(table.get(key(1, 0)), Some(memory(3)));
        assert_eq!(table.get(key(1, 1)), None);
        assert_eq!(table.get(key(2, 0)), None);
    }

    #[test]
    fn insert_if_absent_keeps_the_first() {
        let table = BlockTable::new();
        assert!(table.insert_if_absent(key(1, 0), memory(3)));
        assert!(!table.insert_if_absent(key(1, 0), memory(4)));
        assert_eq!(table.get(key(1, 0)), Some(memory(3)));
        assert!(table.insert_if_absent(key(1, 1), memory(4)));
    }

    #[test]
    fn replace_if_moves_a_block_only_from_where_it_is_expected() {
        let table = BlockTable::new();
        assert!(!table.replace_if(key(1, 0), memory(3), disk(4)));
        assert_eq!(table.get(key(1, 0)), None);

        table.insert(key(1, 0), memory(3));
        // Same chunk number, other tier: not the expected location.
        assert!(!table.replace_if(key(1, 0), disk(3), disk(4)));
        assert_eq!(table.get(key(1, 0)), Some(memory(3)));
        assert!(table.replace_if(key(1, 0), memory(3), disk(4)));
        assert_eq!(table.get(key(1, 0)), Some(disk(4)));
    }

    #[test]
    fn remove_if_forgets_a_block_only_where_it_is_expected() {
        let table = BlockTable::new();
        assert!(!table.remove_if(key(1, 0), disk(4)));

        table.insert(key(1, 0), disk(4));
        assert!(!table.remove_if(key(1, 0), disk(3)));
        assert_eq!(table.get(key(1, 0)), Some(disk(4)));
        assert!(table.remove_if(key(1, 0), disk(4)));
        assert_eq!(table.get(key(1, 0)), None);
        assert!(!table.remove_if(key(1, 0), disk(4)));
    }
}
