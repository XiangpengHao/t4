use std::ops::Range;

use vstd::prelude::*;

use super::blocks::Location;
use super::page_window::{PageWindow, RangeRequestU32};
use super::paths::FileVersion;
use crate::cache::{BLOCK_SIZE_U64, BlockKey};

#[derive(Debug)]
pub(in crate::cache) struct ReadPlan {
    bytes: Range<u64>,
    blocks: Vec<PlannedBlock>,
}

impl ReadPlan {
    pub(in crate::cache) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn blocks(&self) -> &[PlannedBlock] {
        &self.blocks
    }

    pub(in crate::cache) fn into_blocks(self) -> Vec<PlannedBlock> {
        self.blocks
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct PlannedBlock {
    key: BlockKey,
    len: u32,
    location: Option<Location>,
    window: BlockWindow,
}

impl PlannedBlock {
    pub(in crate::cache) fn key(&self) -> BlockKey {
        self.key
    }

    pub(in crate::cache) fn location(&self) -> Option<Location> {
        self.location
    }

    pub(in crate::cache) fn relocated(&self, location: Option<Location>) -> Self {
        Self { location, ..*self }
    }

    pub(in crate::cache) fn window(&self) -> BlockWindow {
        self.window
    }

    pub(in crate::cache) fn object_range(&self) -> Range<u64> {
        let start = u64::from(self.key.index()) * BLOCK_SIZE_U64;
        start..start + u64::from(self.len)
    }
}

verus! {

/// A short last block's pages may end past its bytes, never past its chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct BlockWindow {
    start: u32,
    pages: PageWindow,
}

impl BlockWindow {
    #[verifier::type_invariant]
    spec fn pages_hold_bytes_within_chunk(self) -> bool {
        &&& self.pages.first_byte() <= self.start
        &&& self.start + self.pages.len() <= self.pages.first_byte() + self.pages.buffer_len()
        &&& self.pages.first_byte() + self.pages.buffer_len() <= BLOCK_SIZE_U64
    }

    fn new(bytes: Range<u32>, block_len: u32) -> (window: Self)
        requires
            bytes.start < bytes.end <= block_len <= BLOCK_SIZE_U64,
        ensures
            window.start == bytes.start,
            window.pages.len() == bytes.end - bytes.start,
    {
        let range = RangeRequestU32::from_u32(bytes.start, bytes.end - bytes.start).expect(
            "a range of u32s ends within u32",
        ).checked_against(block_len).expect("a read's bytes in a block lie in the block");
        let pages = PageWindow::of(range).expect("a block's pages end within u32");
        Self { start: bytes.start, pages }
    }

    pub(in crate::cache) fn bytes(&self) -> Range<u32> {
        proof {
            use_type_invariant(self);
        }
        self.start..self.start + self.pages.len()
    }

    pub(in crate::cache) fn pages(&self) -> Range<u32> {
        proof {
            use_type_invariant(self);
        }
        let first = self.pages.first_byte();
        first..first + self.pages.buffer_len()
    }
}

/// The read planner's arithmetic, apart from its `Vec`, so Verus can prove it.
struct ReadBlocks {
    version: FileVersion,
    bytes: Range<u64>,
    unplanned: Range<u32>,
}

impl ReadBlocks {
    spec fn holds_bytes(self, index: int) -> bool {
        &&& self.bytes.start < self.bytes.end
        &&& index * BLOCK_SIZE_U64 < self.bytes.end
        &&& self.bytes.start < (index + 1) * BLOCK_SIZE_U64
    }

    spec fn first_block(self) -> int {
        (self.bytes.start / BLOCK_SIZE_U64) as int
    }

    spec fn wf(self) -> bool {
        &&& self.bytes.start <= self.bytes.end <= self.version.size()
        &&& self.first_block() <= self.unplanned.start <= self.unplanned.end
            < BlockKey::INDEX_LIMIT
        &&& forall|index: int|
            self.first_block() <= index < self.unplanned.end <==> #[trigger] self.holds_bytes(index)
    }

    fn new(version: FileVersion, range: Range<u64>) -> (blocks: Self)
        ensures
            blocks.wf(),
            blocks.version == version,
            blocks.bytes.end == vstd::math::min(range.end as int, version.size() as int),
            blocks.bytes.start == vstd::math::min(range.start as int, blocks.bytes.end as int),
            blocks.unplanned.start == blocks.first_block(),
    {
        let end = range.end.min(version.size());
        let start = range.start.min(end);
        let first_block = start / BLOCK_SIZE_U64;
        let end_block = if start == end {
            first_block
        } else {
            // `end.div_ceil(BLOCK_SIZE_U64)`, which Verus has no spec for.
            (end - 1) / BLOCK_SIZE_U64 + 1
        };
        Self {
            version,
            bytes: start..end,
            unplanned: Self::narrow(first_block)..Self::narrow(end_block),
        }
    }

    fn keys(&self) -> (keys: Range<BlockKey>)
        requires
            self.wf(),
        ensures
            keys.start@ == (self.version.id(), self.unplanned.start),
            keys.end@ == (self.version.id(), self.unplanned.end),
    {
        BlockKey::new(self.version.id(), self.unplanned.start)..BlockKey::new(
            self.version.id(),
            self.unplanned.end,
        )
    }

    fn take_through(&mut self, key: BlockKey) -> (not_found: Range<u32>)
        requires
            old(self).wf(),
            key@.0 == old(self).version.id(),
            old(self).unplanned.start <= key@.1 < old(self).unplanned.end,
        ensures
            final(self).wf(),
            final(self).version == old(self).version,
            final(self).bytes == old(self).bytes,
            not_found.start == old(self).unplanned.start && not_found.end == key@.1,
            final(self).unplanned.start == key@.1 + 1,
            final(self).unplanned.end == old(self).unplanned.end,
    {
        let index = key.index();
        let not_found = self.unplanned.start..index;
        self.unplanned.start = index + 1;
        assert(forall|index: int| #[trigger]
            self.holds_bytes(index) == old(self).holds_bytes(index));
        not_found
    }

    fn block(&self, index: u32) -> (block: (BlockKey, u32, BlockWindow))
        requires
            self.wf(),
            self.holds_bytes(index as int),
        ensures
            block.0@ == (self.version.id(), index),
            index * BLOCK_SIZE_U64 + block.1 == vstd::math::min(
                (index + 1) * BLOCK_SIZE_U64,
                self.version.size() as int,
            ),
            index * BLOCK_SIZE_U64 + block.2.start == vstd::math::max(
                self.bytes.start as int,
                index * BLOCK_SIZE_U64,
            ),
            index * BLOCK_SIZE_U64 + block.2.start + block.2.pages.len() == vstd::math::min(
                self.bytes.end as int,
                (index + 1) * BLOCK_SIZE_U64,
            ),
    {
        let block_range = self.version.block_range(index);
        let needed_start = self.bytes.start.max(block_range.start) - block_range.start;
        let needed_end = self.bytes.end.min(block_range.end) - block_range.start;
        let len = Self::narrow(block_range.end - block_range.start);
        let window = BlockWindow::new(Self::narrow(needed_start)..Self::narrow(needed_end), len);
        (BlockKey::new(self.version.id(), index), len, window)
    }

    fn narrow(value: u64) -> (narrowed: u32)
        requires
            value <= u32::MAX,
        ensures
            narrowed == value,
    {
        u32::try_from(value).expect("block numbers and offsets in a block fit in u32")
    }
}

} // verus!

impl BlockWindow {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn bytes_in_pages(&self) -> Range<u32> {
        self.pages.start()..self.pages.start() + self.pages.len()
    }
}

pub(super) struct ReadPlanner {
    blocks: ReadBlocks,
    planned: Vec<PlannedBlock>,
}

impl ReadPlanner {
    pub(super) fn new(version: FileVersion, range: Range<u64>) -> Self {
        let blocks = ReadBlocks::new(version, range);
        Self {
            planned: Vec::with_capacity(blocks.unplanned.len()),
            blocks,
        }
    }

    pub(super) fn keys(&self) -> Range<BlockKey> {
        self.blocks.keys()
    }

    /// Called in key order, within [`Self::keys`].
    pub(super) fn found(&mut self, key: BlockKey, location: Location) {
        debug_assert!(
            key.file_version() == self.blocks.version.id()
                && self.blocks.unplanned.contains(&key.index())
        );
        for remote in self.blocks.take_through(key) {
            self.plan(remote, None);
        }
        self.plan(key.index(), Some(location));
    }

    pub(super) fn finish(mut self) -> ReadPlan {
        for remote in self.blocks.unplanned.clone() {
            self.plan(remote, None);
        }
        ReadPlan {
            bytes: self.blocks.bytes,
            blocks: self.planned,
        }
    }

    fn plan(&mut self, index: u32, location: Option<Location>) {
        let (key, len, window) = self.blocks.block(index);
        self.planned.push(PlannedBlock {
            key,
            len,
            location,
            window,
        });
    }
}

// Outside shuttle, whose atomics `PathTable` uses.
// `[MIB..2 * MIB]` is a list of one block's bytes, not the range's offsets.
#[allow(clippy::single_range_in_vec_init)]
#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::ChunkId;
    use crate::cache::storage::index::{BlockTable, PathTable};

    const MIB: u64 = BLOCK_SIZE_U64;
    const MIB_U32: u32 = crate::cache::BLOCK_SIZE as u32;
    const PAGE: u32 = crate::PAGE_SIZE_U32;

    fn file(size: u64) -> FileVersion {
        PathTable::new().resolve(b"p", Some("v1"), size).unwrap()
    }

    fn summary(plan: &ReadPlan) -> Vec<(u32, Option<Location>, Range<u32>)> {
        plan.blocks()
            .iter()
            .map(|block| {
                (
                    block.key().index(),
                    block.location(),
                    block.window().bytes(),
                )
            })
            .collect()
    }

    fn remote(plan: &ReadPlan) -> Vec<Range<u64>> {
        plan.blocks()
            .iter()
            .filter(|block| block.location().is_none())
            .map(PlannedBlock::object_range)
            .collect()
    }

    fn memory(chunk: u32) -> Location {
        Location::Memory(ChunkId::new(chunk))
    }

    fn disk(chunk: u32) -> Location {
        Location::Disk(ChunkId::new(chunk))
    }

    #[test]
    fn a_range_inside_one_block() {
        let blocks = BlockTable::new();
        let plan = blocks.plan_read(file(3 * MIB), MIB + 100..MIB + 200);
        assert_eq!(plan.bytes(), MIB + 100..MIB + 200);
        assert_eq!(summary(&plan), [(1, None, 100..200)]);
        assert_eq!(remote(&plan), [MIB..2 * MIB]);
    }

    #[test]
    fn a_range_spanning_blocks() {
        let blocks = BlockTable::new();
        let plan = blocks.plan_read(file(4 * MIB), 100..3 * MIB + 5);
        assert_eq!(
            summary(&plan),
            [
                (0, None, 100..MIB_U32),
                (1, None, 0..MIB_U32),
                (2, None, 0..MIB_U32),
                (3, None, 0..5),
            ]
        );
        assert_eq!(
            remote(&plan),
            [0..MIB, MIB..2 * MIB, 2 * MIB..3 * MIB, 3 * MIB..4 * MIB]
        );
    }

    #[test]
    fn the_last_block_is_short() {
        let blocks = BlockTable::new();
        let plan = blocks.plan_read(file(2 * MIB + 1000), 0..u64::MAX);
        assert_eq!(plan.bytes(), 0..2 * MIB + 1000);
        assert_eq!(
            summary(&plan),
            [
                (0, None, 0..MIB_U32),
                (1, None, 0..MIB_U32),
                (2, None, 0..1000)
            ]
        );
        let last = plan.blocks()[2];
        assert_eq!(last.object_range(), 2 * MIB..2 * MIB + 1000);
        assert_eq!(last.window().pages(), 0..PAGE);

        let plan = blocks.plan_read(file(10), 0..10);
        assert_eq!(summary(&plan), [(0, None, 0..10)]);
        assert_eq!(remote(&plan), [0..10]);
    }

    #[test]
    fn reads_past_the_end_are_clamped() {
        let blocks = BlockTable::new();
        let plan = blocks.plan_read(file(1000), 500..5000);
        assert_eq!(plan.bytes(), 500..1000);
        assert_eq!(summary(&plan), [(0, None, 500..1000)]);

        for past_the_end in [1000..2000, 5000..6000, MIB..2 * MIB] {
            let plan = blocks.plan_read(file(1000), past_the_end);
            assert!(plan.bytes().is_empty());
            assert!(plan.blocks().is_empty());
        }
    }

    #[test]
    fn an_empty_range_plans_nothing() {
        let blocks = BlockTable::new();
        blocks.insert(BlockKey::new(file(0).id(), 0), memory(1));
        #[allow(clippy::reversed_empty_ranges)]
        let empty = [(3 * MIB, 100..100), (3 * MIB, 200..100), (0, 0..10)];
        for (size, range) in empty {
            let plan = blocks.plan_read(file(size), range);
            assert!(plan.bytes().is_empty());
            assert!(plan.blocks().is_empty());
        }
    }

    #[test]
    fn disk_windows_cover_whole_pages() {
        let window = |size, range| {
            let plan = BlockTable::new().plan_read(file(size), range);
            assert_eq!(plan.blocks().len(), 1);
            let window = plan.blocks()[0].window();
            (window.pages(), window.bytes_in_pages())
        };
        let page = u64::from(PAGE);
        assert_eq!(window(MIB, 5000..5001), (PAGE..2 * PAGE, 904..905));
        assert_eq!(
            window(MIB, page - 1..page + 1),
            (0..2 * PAGE, PAGE - 1..PAGE + 1)
        );
        assert_eq!(window(MIB, page..2 * page), (PAGE..2 * PAGE, 0..PAGE));
        assert_eq!(
            window(MIB, MIB - 1..MIB),
            (MIB_U32 - PAGE..MIB_U32, PAGE - 1..PAGE)
        );
        assert_eq!(window(MIB + 10, MIB + 3..MIB + 10), (0..PAGE, 3..10));
        assert_eq!(
            window(MIB + page + 10, MIB + page..u64::MAX),
            (PAGE..2 * PAGE, 0..10)
        );
    }

    #[test]
    fn a_plan_finds_each_block_where_it_is() {
        let blocks = BlockTable::new();
        let file = file(6 * MIB);
        let key = |index| BlockKey::new(file.id(), index);
        blocks.insert(key(2), memory(7));
        blocks.insert(key(4), disk(9));

        let plan = blocks.plan_read(file, 0..6 * MIB);
        let locations: Vec<_> = plan.blocks().iter().map(PlannedBlock::location).collect();
        assert_eq!(
            locations,
            [None, None, Some(memory(7)), None, Some(disk(9)), None]
        );
        assert_eq!(
            remote(&plan),
            [0..MIB, MIB..2 * MIB, 3 * MIB..4 * MIB, 5 * MIB..6 * MIB]
        );

        let plan = blocks.plan_read(file, 2 * MIB + 1..5 * MIB);
        assert_eq!(
            summary(&plan),
            [
                (2, Some(memory(7)), 1..MIB_U32),
                (3, None, 0..MIB_U32),
                (4, Some(disk(9)), 0..MIB_U32),
            ]
        );
        assert_eq!(remote(&plan), [3 * MIB..4 * MIB]);
    }

    #[test]
    fn a_plan_sees_only_its_files_blocks() {
        let paths = PathTable::new();
        let before = paths.resolve(b"a", Some("v1"), 4 * MIB).unwrap();
        let file = paths.resolve(b"b", Some("v1"), 2 * MIB).unwrap();
        let after = paths.resolve(b"c", Some("v1"), 4 * MIB).unwrap();
        let blocks = BlockTable::new();
        for neighbor in [before, after] {
            for index in 0..4 {
                blocks.insert(BlockKey::new(neighbor.id(), index), memory(index));
            }
        }
        blocks.insert(BlockKey::new(file.id(), 1), disk(5));

        let plan = blocks.plan_read(file, 0..u64::MAX);
        assert_eq!(
            summary(&plan),
            [(0, None, 0..MIB_U32), (1, Some(disk(5)), 0..MIB_U32)]
        );
        assert!(
            plan.blocks()
                .iter()
                .all(|block| block.key().file_version() == file.id())
        );
    }
}
