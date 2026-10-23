//! [`ReadPolicy`]: how the cache serves a read (`doc/06-read-modes.md`):
//! which GETs fetch the blocks only the remote holds, and how many race
//! for each. It
//! hears every I/O the cache makes ([`IoEvent`]), to learn from.
//!
//! The policy decides; the cache carries the decision out, or, given a plan
//! it can't carry out, fetches the read as [`FetchRuns`] does.

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use crate::cache::BLOCK_SIZE_U64;

/// What a read is for, as its caller says, through `GetOptions::extensions`;
/// a policy guesses where it isn't said.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadIntent {
    /// The answer soonest: metadata, a footer.
    Latency,
    /// The most bytes a second: a scan.
    Throughput,
    /// The fewest requests: warming the cache.
    Warm,
}

/// How the cache serves a read. Every hook takes `&self`: a policy keeps
/// what state it wants, with what concurrency it wants.
pub trait ReadPolicy: Send + Sync {
    /// The GETs that fetch `read`'s remote blocks, those neither memory nor
    /// disk holds. They must cover every one, each block in one GET at most,
    /// within the file; they may cover more (widened), split a run of remote
    /// blocks, or span runs (merged, the memory and disk blocks between
    /// fetched and their bytes dropped). Asked of every read with a remote
    /// block.
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan;

    /// An I/O the cache made started, was answered, finished, failed or
    /// was given up on.
    fn on_io(&self, event: &IoEvent<'_>);
}

/// A read, each of its blocks in memory, on disk or remote: what a
/// [`ReadPolicy`] plans by.
#[derive(Clone, Debug)]
pub struct LocatedRead<'r> {
    path: &'r [u8],
    file_size: u64,
    asked: Range<u64>,
    intent: Option<ReadIntent>,
    /// The read's first block's number.
    first: u32,
    /// Where each of the read's blocks is, the first block's first.
    sources: &'r [BlockSource],
}

impl<'r> LocatedRead<'r> {
    pub(in crate::cache) fn new(
        path: &'r [u8],
        file_size: u64,
        asked: Range<u64>,
        intent: Option<ReadIntent>,
        first: u32,
        sources: &'r [BlockSource],
    ) -> Self {
        Self {
            path,
            file_size,
            asked,
            intent,
            first,
            sources,
        }
    }

    /// The file's path.
    pub fn path(&self) -> &'r [u8] {
        self.path
    }

    /// The file's size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The file's bytes the read asked for, clamped to the file.
    pub fn asked(&self) -> Range<u64> {
        self.asked.clone()
    }

    /// What the read is for, if its caller said.
    pub fn intent(&self) -> Option<ReadIntent> {
        self.intent
    }

    /// The numbers of the blocks the read takes bytes of.
    pub fn blocks(&self) -> Range<u32> {
        let count = u32::try_from(self.sources.len()).expect("a file's block numbers fit a u32");
        self.first..self.first + count
    }

    /// Where `block`, one of the read's, is; `None` if it isn't one of them.
    pub fn source(&self, block: u32) -> Option<BlockSource> {
        let index = block.checked_sub(self.first)?;
        self.sources.get(index as usize).copied()
    }

    /// The number of blocks in the file: a GET may widen up to it.
    pub fn file_blocks(&self) -> u32 {
        u32::try_from(self.file_size.div_ceil(BLOCK_SIZE_U64))
            .expect("a file's block numbers fit a u32")
    }

    /// The file's bytes block `block` holds.
    pub fn block_bytes(&self, block: u32) -> Range<u64> {
        let start = u64::from(block) * BLOCK_SIZE_U64;
        start.min(self.file_size)..(start + BLOCK_SIZE_U64).min(self.file_size)
    }

    /// Whether the cache can carry `plan` out for this read: every GET
    /// within the file and of a block at least, no two sharing a block, and
    /// together covering every remote block.
    pub(in crate::cache) fn accepts(&self, plan: &ReadPlan) -> bool {
        let mut gets: Vec<Range<u32>> = plan.gets().iter().map(PlannedGet::blocks).collect();
        gets.sort_unstable_by_key(|blocks| blocks.start);
        let in_file = gets
            .iter()
            .all(|blocks| blocks.start < blocks.end && blocks.end <= self.file_blocks());
        let apart = gets.windows(2).all(|pair| pair[0].end <= pair[1].start);
        let covered = self.remote_runs().flatten().all(|block| {
            let at = gets.partition_point(|blocks| blocks.start <= block);
            at > 0 && gets[at - 1].contains(&block)
        });
        in_file && apart && covered
    }

    /// The runs of the read's adjacent remote blocks, in order.
    pub fn remote_runs(&self) -> impl Iterator<Item = Range<u32>> + '_ {
        let mut blocks = self.blocks().zip(self.sources.iter()).peekable();
        std::iter::from_fn(move || {
            let (start, _) = blocks.find(|(_, source)| **source == BlockSource::Remote)?;
            let mut end = start + 1;
            while blocks
                .next_if(|(_, source)| **source == BlockSource::Remote)
                .is_some()
            {
                end += 1;
            }
            Some(start..end)
        })
    }
}

/// Where one of a read's blocks is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockSource {
    Memory,
    Disk,
    /// Neither memory nor disk holds it: the remote serves it.
    Remote,
}

/// A [`ReadPolicy`]'s answer: the GETs that fetch a read's remote blocks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadPlan {
    gets: Vec<PlannedGet>,
}

impl ReadPlan {
    pub fn new(gets: Vec<PlannedGet>) -> Self {
        Self { gets }
    }

    pub fn gets(&self) -> &[PlannedGet] {
        &self.gets
    }
}

/// One GET of a plan: the blocks it fetches, and how many identical
/// requests race for them, sent at once, the first response taken and the
/// others dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedGet {
    blocks: Range<u32>,
    race: NonZeroU8,
}

impl PlannedGet {
    pub fn new(blocks: Range<u32>, race: NonZeroU8) -> Self {
        Self { blocks, race }
    }

    /// A GET of `blocks`, sent once.
    pub fn once(blocks: Range<u32>) -> Self {
        Self::new(blocks, NonZeroU8::MIN)
    }

    /// The numbers of the blocks it fetches.
    pub fn blocks(&self) -> Range<u32> {
        self.blocks.clone()
    }

    /// How many requests race for them.
    pub fn race(&self) -> NonZeroU8 {
        self.race
    }
}

/// The default: each run of remote blocks fetched in one GET, sent once.
#[derive(Clone, Copy, Debug, Default)]
pub struct FetchRuns;

impl ReadPolicy for FetchRuns {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        ReadPlan::new(read.remote_runs().map(PlannedGet::once).collect())
    }

    fn on_io(&self, _event: &IoEvent<'_>) {}
}

/// Something that happened to an I/O the cache made.
#[derive(Clone, Debug)]
pub struct IoEvent<'e> {
    io: Io<'e>,
    phase: IoPhase,
}

impl<'e> IoEvent<'e> {
    pub(in crate::cache) fn new(io: Io<'e>, phase: IoPhase) -> Self {
        Self { io, phase }
    }

    pub fn io(&self) -> &Io<'e> {
        &self.io
    }

    pub fn phase(&self) -> IoPhase {
        self.phase
    }
}

/// An I/O the cache makes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Io<'e> {
    /// A GET of `bytes` of the file at `path`; `racer` numbers it among
    /// those racing for them, from 0.
    Get {
        path: &'e [u8],
        bytes: Range<u64>,
        racer: u8,
    },
    /// A read of `bytes` from the cache file on drive `drive`, numbered as
    /// the cache's files were given, from 0.
    DiskRead { drive: u32, bytes: u32 },
}

impl Io<'_> {
    /// The bytes it moves, all of it done.
    pub fn bytes(&self) -> u64 {
        match self {
            Self::Get { bytes, .. } => bytes.end - bytes.start,
            Self::DiskRead { bytes, .. } => u64::from(*bytes),
        }
    }
}

/// Where an I/O is, and how long since it started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoPhase {
    Started,
    /// The answer began: a GET's response came, its body to follow.
    Answered {
        after: Duration,
    },
    Finished {
        bytes: u64,
        after: Duration,
    },
    Failed {
        after: Duration,
    },
    /// Given up on before it ended, as a race's losers are.
    Cancelled {
        after: Duration,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = BLOCK_SIZE_U64;

    fn read(sources: &[BlockSource], first: u32) -> LocatedRead<'_> {
        let size = 10 * MIB + MIB / 2;
        LocatedRead::new(b"f", size, 0..size, None, first, sources)
    }

    #[test]
    fn remote_runs_are_the_reads_adjacent_remote_blocks() {
        use BlockSource::{Disk, Memory, Remote};
        let sources = [Remote, Remote, Memory, Remote, Disk, Remote, Remote];
        assert_eq!(
            read(&sources, 3).remote_runs().collect::<Vec<_>>(),
            [3..5, 6..7, 8..10]
        );
        assert_eq!(read(&[Memory, Disk], 0).remote_runs().count(), 0);
    }

    #[test]
    fn the_default_fetches_each_run_once() {
        use BlockSource::{Memory, Remote};
        let sources = [Remote, Memory, Remote, Remote];
        let plan = FetchRuns.plan(&read(&sources, 0));
        assert_eq!(
            plan.gets(),
            [PlannedGet::once(0..1), PlannedGet::once(2..4)]
        );
    }

    // `[4..7]` is a plan of one GET, not the GET's blocks.
    #[allow(clippy::single_range_in_vec_init)]
    #[test]
    fn a_plan_must_cover_every_remote_block_once_within_the_file() {
        use BlockSource::{Memory, Remote};
        let sources = [Remote, Memory, Remote];
        let located = read(&sources, 4);
        let accepts = |gets: &[Range<u32>]| {
            let gets = gets.iter().cloned().map(PlannedGet::once).collect();
            located.accepts(&ReadPlan::new(gets))
        };
        assert!(accepts(&[4..5, 6..7]), "the runs");
        assert!(accepts(&[4..7]), "merged over the hit");
        assert!(accepts(&[0..5, 6..11]), "widened to the file's ends");
        assert!(accepts(&[6..7, 4..5]), "in any order");
        assert!(!accepts(&[4..5]), "a remote block left out");
        assert!(!accepts(&[4..6, 5..7]), "a block fetched twice");
        assert!(!accepts(&[4..5, 6..12]), "past the file");
        assert!(!accepts(&[4..5, 5..5, 6..7]), "an empty GET");
    }

    #[test]
    fn the_last_block_is_the_files_rest() {
        let located = read(&[BlockSource::Remote], 10);
        assert_eq!(located.file_blocks(), 11);
        assert_eq!(located.block_bytes(10), 10 * MIB..10 * MIB + MIB / 2);
        assert_eq!(located.source(10), Some(BlockSource::Remote));
        assert_eq!(located.source(9), None);
    }
}
