//! Range planning (`doc/03-cache.md`, decisions 1 and 3): the blocks a read
//! of a byte range covers, where each one is, and the part of each the read
//! needs.

use std::ops::Range;

use super::blocks::Location;
use super::paths::ResolvedFile;
use crate::cache::{BLOCK_SIZE, BlockKey};
use crate::disk::ValueWindow;
use crate::range::RangeRequestU32;

/// [`BLOCK_SIZE`], for offsets in an object.
const BLOCK_BYTES: u64 = BLOCK_SIZE as u64;

/// How to read a byte range of an object: the blocks it covers, in order,
/// each where it was when planned, with the part of it the read needs.
#[derive(Debug)]
pub(in crate::cache) struct ReadPlan {
    bytes: Range<u64>,
    blocks: Vec<PlannedBlock>,
}

impl ReadPlan {
    /// The bytes of the object the read takes: the range asked for, clamped
    /// to the object. The blocks' windows hold them, in order.
    pub(in crate::cache) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    /// The blocks the read covers, in order.
    pub(in crate::cache) fn blocks(&self) -> &[PlannedBlock] {
        &self.blocks
    }

    /// The runs of consecutive blocks that no tier held, in order: each is
    /// one GET from the object store.
    pub(in crate::cache) fn missing_runs(&self) -> impl Iterator<Item = MissingRun<'_>> {
        self.blocks
            .chunk_by(|block, next| block.is_missing() == next.is_missing())
            .filter(|run| run.first().is_some_and(PlannedBlock::is_missing))
            .map(|blocks| MissingRun { blocks })
    }
}

/// A block a read covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct PlannedBlock {
    key: BlockKey,
    /// The object's bytes in the block: `BLOCK_SIZE`, fewer in the object's
    /// last block.
    len: u32,
    /// Where the block was when planned; `None` if no tier held it.
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

    /// The part of the block the read needs.
    pub(in crate::cache) fn window(&self) -> BlockWindow {
        self.window
    }

    /// The block's bytes in the object: what fetching it takes.
    pub(in crate::cache) fn object_range(&self) -> Range<u64> {
        let start = u64::from(self.key.index()) * BLOCK_BYTES;
        start..start + u64::from(self.len)
    }

    fn is_missing(&self) -> bool {
        self.location.is_none()
    }
}

/// Consecutive blocks of a plan that no tier held: one GET from the object
/// store fetches them all.
#[derive(Clone, Copy, Debug)]
pub(in crate::cache) struct MissingRun<'p> {
    /// Never empty.
    blocks: &'p [PlannedBlock],
}

impl<'p> MissingRun<'p> {
    pub(in crate::cache) fn blocks(&self) -> &'p [PlannedBlock] {
        self.blocks
    }

    /// The bytes of the object the GET fetches: its blocks, whole.
    pub(in crate::cache) fn object_range(&self) -> Range<u64> {
        let first = self.blocks.first().expect("a run has a block");
        let last = self.blocks.last().expect("a run has a block");
        first.object_range().start..last.object_range().end
    }
}

/// The part of a block a read needs, as offsets in the block: the bytes
/// asked for, and the 4 KiB pages around them that a read from a disk chunk
/// takes with `O_DIRECT` (decision 3). The pages of a short last block may
/// end past its bytes, but never past its chunk, a whole block's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct BlockWindow {
    /// Where the bytes asked for start in the block.
    start: u32,
    /// The pages around them, as a value's are laid out: a chunk, like a
    /// value, starts on a page boundary.
    pages: ValueWindow,
}

impl BlockWindow {
    /// The window for `bytes` of a block that holds `block_len` of them.
    fn new(bytes: Range<u32>, block_len: u32) -> Self {
        let range = RangeRequestU32::from_u32(bytes.start, bytes.end - bytes.start)
            .and_then(|range| range.checked_against(block_len))
            .expect("a read's bytes in a block lie in the block");
        let pages = ValueWindow::of(range).expect("a block's pages end within u32");
        Self {
            start: bytes.start,
            pages,
        }
    }

    /// The bytes asked for.
    pub(in crate::cache) fn bytes(&self) -> Range<u32> {
        self.start..self.start + self.pages.len()
    }

    /// The whole pages around them: what a read from a disk chunk takes.
    pub(in crate::cache) fn pages(&self) -> Range<u32> {
        // `ValueWindow` keeps where its pages start to itself: where the
        // bytes start, less their offset in the pages.
        let first = self.start - self.pages.start();
        first..first + self.pages.buffer_len()
    }

    /// Where the bytes asked for lie in a buffer that holds [`Self::pages`].
    pub(in crate::cache) fn bytes_in_pages(&self) -> Range<u32> {
        self.pages.start()..self.pages.start() + self.pages.len()
    }
}

/// A plan being made. Told a read's cached blocks in order, it takes the
/// blocks between them as missing.
pub(super) struct ReadPlanner {
    file: ResolvedFile,
    /// The bytes the read takes: the range asked for, clamped to the object.
    bytes: Range<u64>,
    /// The blocks the read covers that the plan has yet to take.
    unplanned: Range<u32>,
    planned: Vec<PlannedBlock>,
}

impl ReadPlanner {
    /// A plan for `range` of `file`, with no block taken yet.
    pub(super) fn new(file: ResolvedFile, range: Range<u64>) -> Self {
        let end = range.end.min(file.size());
        let start = range.start.min(end);
        let first_block = start / BLOCK_BYTES;
        let end_block = if start == end {
            first_block
        } else {
            end.div_ceil(BLOCK_BYTES)
        };
        let unplanned = Self::narrow(first_block)..Self::narrow(end_block);
        Self {
            file,
            bytes: start..end,
            planned: Vec::with_capacity(unplanned.len()),
            unplanned,
        }
    }

    /// The keys of the blocks the read covers: what the scan that finds the
    /// cached ones goes over.
    pub(super) fn keys(&self) -> Range<BlockKey> {
        BlockKey::new(self.file.id(), self.unplanned.start)
            ..BlockKey::new(self.file.id(), self.unplanned.end)
    }

    /// Takes `key`'s block, found at `location`, and the blocks before it
    /// that were not found, as missing. Blocks are found in key order, within
    /// [`Self::keys`].
    pub(super) fn found(&mut self, key: BlockKey, location: Location) {
        let index = key.index();
        debug_assert!(key.file() == self.file.id() && self.unplanned.contains(&index));
        for missing in self.unplanned.start..index {
            self.plan(missing, None);
        }
        self.plan(index, Some(location));
        // Below `unplanned.end`, a `u32`: no overflow.
        self.unplanned.start = index + 1;
    }

    /// The plan, the blocks not found taken as missing.
    pub(super) fn finish(mut self) -> ReadPlan {
        for missing in self.unplanned.clone() {
            self.plan(missing, None);
        }
        ReadPlan {
            bytes: self.bytes,
            blocks: self.planned,
        }
    }

    fn plan(&mut self, index: u32, location: Option<Location>) {
        let block_start = u64::from(index) * BLOCK_BYTES;
        let block_end = (block_start + BLOCK_BYTES).min(self.file.size());
        let needed_start = self.bytes.start.max(block_start) - block_start;
        let needed_end = self.bytes.end.min(block_end) - block_start;
        let len = Self::narrow(block_end - block_start);
        self.planned.push(PlannedBlock {
            key: BlockKey::new(self.file.id(), index),
            len,
            location,
            window: BlockWindow::new(Self::narrow(needed_start)..Self::narrow(needed_end), len),
        });
    }

    /// A block number or an offset in a block, as a `u32`: `ResolvedFile`
    /// keeps an object's block numbers below the last a `BlockKey` holds, so
    /// the number past its last fits too.
    fn narrow(value: u64) -> u32 {
        u32::try_from(value).expect("block indices and offsets in a block fit in u32")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ChunkId;
    use crate::cache::block_index::{BlockTable, PathTable};

    const MIB: u64 = BLOCK_BYTES;
    const MIB_U32: u32 = BLOCK_SIZE as u32;
    const PAGE: u32 = crate::PAGE_SIZE_U32;

    fn file(size: u64) -> ResolvedFile {
        PathTable::new().resolve(b"p", Some("v1"), size).unwrap()
    }

    /// Each planned block's index, location and bytes asked for.
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

    fn run_ranges(plan: &ReadPlan) -> Vec<Range<u64>> {
        plan.missing_runs().map(|run| run.object_range()).collect()
    }

    /// The bytes the plan's one missing run fetches.
    fn only_run(plan: &ReadPlan) -> Range<u64> {
        let runs = run_ranges(plan);
        assert_eq!(runs.len(), 1, "{runs:?}");
        runs[0].clone()
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
        assert_eq!(plan.blocks()[0].object_range(), MIB..2 * MIB);
        assert_eq!(only_run(&plan), MIB..2 * MIB);
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
        assert_eq!(only_run(&plan), 0..4 * MIB);
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
        assert_eq!(only_run(&plan), 0..2 * MIB + 1000);

        // An object smaller than a block is one short block.
        let plan = blocks.plan_read(file(10), 0..10);
        assert_eq!(summary(&plan), [(0, None, 0..10)]);
        assert_eq!(only_run(&plan), 0..10);
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
            assert_eq!(plan.missing_runs().count(), 0);
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
            assert_eq!(plan.missing_runs().count(), 0);
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
        // In a short last block, the pages run past its bytes, within its chunk.
        assert_eq!(window(MIB + 10, MIB + 3..MIB + 10), (0..PAGE, 3..10));
        assert_eq!(
            window(MIB + page + 10, MIB + page..u64::MAX),
            (PAGE..2 * PAGE, 0..10)
        );
    }

    #[test]
    fn missing_runs_lie_between_hits() {
        let blocks = BlockTable::new();
        let file = file(8 * MIB);
        let key = |index| BlockKey::new(file.id(), index);
        blocks.insert(key(2), memory(7));
        blocks.insert(key(4), disk(9));
        blocks.insert(key(5), Location::Fetching);

        let plan = blocks.plan_read(file, 0..8 * MIB);
        let locations: Vec<_> = plan.blocks().iter().map(PlannedBlock::location).collect();
        assert_eq!(
            locations,
            [
                None,
                None,
                Some(memory(7)),
                None,
                Some(disk(9)),
                Some(Location::Fetching),
                None,
                None,
            ]
        );
        // A block on its way is no one's to fetch again.
        assert_eq!(
            run_ranges(&plan),
            [0..2 * MIB, 3 * MIB..4 * MIB, 6 * MIB..8 * MIB]
        );
        let run_blocks: Vec<Vec<u32>> = plan
            .missing_runs()
            .map(|run| {
                run.blocks()
                    .iter()
                    .map(|block| block.key().index())
                    .collect()
            })
            .collect();
        assert_eq!(run_blocks, [vec![0, 1], vec![3], vec![6, 7]]);

        // A read from the middle sees only its own blocks.
        let plan = blocks.plan_read(file, 2 * MIB + 1..5 * MIB);
        assert_eq!(
            summary(&plan),
            [
                (2, Some(memory(7)), 1..MIB_U32),
                (3, None, 0..MIB_U32),
                (4, Some(disk(9)), 0..MIB_U32),
            ]
        );
        assert_eq!(only_run(&plan), 3 * MIB..4 * MIB);
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
                .all(|block| block.key().file() == file.id())
        );
    }
}
