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
    /// disk holds, nor another read is fetching. They must cover every one,
    /// each block in one GET at most, within the file; they may cover more
    /// (widened), split a run of remote blocks, or span runs (merged, the
    /// memory and disk blocks between fetched and their bytes dropped). A
    /// block in flight they may cover only racing it, with more requests
    /// than it has in flight; the read waits for one they leave out. Asked
    /// of every read with a remote block or one in flight.
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
    /// within the file and of a block at least, no two sharing a block,
    /// together covering every remote block, and racing every block in
    /// flight it covers with more requests than the block has in flight.
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
        let races_in_flight = plan.gets().iter().all(|get| {
            get.blocks().all(|block| match self.source(block) {
                Some(BlockSource::InFlight { requests }) => requests < u32::from(get.race().get()),
                _ => true,
            })
        });
        in_file && apart && covered && races_in_flight
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
    /// Neither memory nor disk holds it, nor is another read fetching it:
    /// the read fetches it.
    Remote,
    /// Another read is fetching it, `requests` in flight for it: the read
    /// waits for that fetch, unless a GET of its own races it.
    InFlight {
        requests: u32,
    },
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

/// One GET of a plan: the blocks it fetches, how many identical requests
/// race for them, sent at once, the first response taken and the others
/// dropped; and whether one more races it late, if it hasn't finished.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedGet {
    blocks: Range<u32>,
    race: NonZeroU8,
    late: Option<Duration>,
}

impl PlannedGet {
    pub fn new(blocks: Range<u32>, race: NonZeroU8) -> Self {
        Self {
            blocks,
            race,
            late: None,
        }
    }

    /// The GET, raced once more if its body isn't all in `after` since it
    /// was sent: one more request, for the bytes not in yet, the first to
    /// have them all taken and the other dropped. The cache holds the late
    /// request's bytes until it has them all, and sends it only if it was
    /// given a timer (`T4CachingBuilder::with_sleep`).
    #[must_use]
    pub fn with_late_race(self, after: Duration) -> Self {
        Self {
            late: Some(after),
            ..self
        }
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

    /// When one more request races it, if it hasn't finished.
    pub fn late_race(&self) -> Option<Duration> {
        self.late
    }
}

/// The default: each run of remote blocks fetched in GETs sent once, but a
/// footer's. A normal read's, one whose intent isn't said, are of
/// [`Self::NORMAL_GET_MOST`] bytes at most, so that no one GET holds the
/// read up for long; and the blocks holding a Parquet or Vortex file's
/// last [`Self::FOOTER_BYTES`], where its metadata is, it fetches in a GET
/// of their own, [`Self::FOOTER_RACE`] requests racing for them unless
/// [`Self::racing_footers`] says otherwise. A read with an intent fetches
/// each run in one GET.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchRuns {
    footer_race: NonZeroU8,
}

impl Default for FetchRuns {
    fn default() -> Self {
        Self::racing_footers(Self::FOOTER_RACE)
    }
}

impl FetchRuns {
    /// The bytes a normal read's GET holds at most.
    pub const NORMAL_GET_MOST: u64 = 4 << 20;
    /// The bytes at a Parquet or Vortex file's end that a normal read
    /// races for.
    pub const FOOTER_BYTES: u64 = 2 << 20;
    /// The requests that race for them by default.
    pub const FOOTER_RACE: NonZeroU8 = NonZeroU8::new(2).unwrap();
    /// The endings of the paths of files with a footer.
    const FOOTED: [&[u8]; 2] = [b".parquet", b".vortex"];

    /// Fetches as the default does, `race` requests racing for a footer's
    /// blocks: with one, its GET of their own is sent once.
    pub fn racing_footers(race: NonZeroU8) -> Self {
        Self { footer_race: race }
    }

    /// The first block a normal read of a Parquet or Vortex file races for,
    /// the first holding its last [`Self::FOOTER_BYTES`]; for another read,
    /// the file's end.
    fn footer_start(read: &LocatedRead<'_>) -> u32 {
        let footed = Self::FOOTED
            .iter()
            .any(|ending| read.path().ends_with(ending));
        if !footed || read.intent().is_some() {
            return read.file_blocks();
        }
        let footer = read.file_size().saturating_sub(Self::FOOTER_BYTES);
        u32::try_from(footer / BLOCK_SIZE_U64).expect("a file's block numbers fit a u32")
    }
}

impl ReadPolicy for FetchRuns {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let most = match read.intent() {
            None => u32::try_from((Self::NORMAL_GET_MOST / BLOCK_SIZE_U64).max(1))
                .expect("a GET's blocks fit a u32"),
            Some(_) => u32::MAX,
        };
        let footer_start = Self::footer_start(read);
        ReadPlan::new(
            read.remote_runs()
                .flat_map(|run| {
                    let body_end = footer_start.clamp(run.start, run.end);
                    let body = (run.start..body_end)
                        .step_by(most as usize)
                        .map(move |start| {
                            PlannedGet::once(start..start.saturating_add(most).min(body_end))
                        });
                    let footer = (body_end < run.end)
                        .then(|| PlannedGet::new(body_end..run.end, self.footer_race));
                    body.chain(footer)
                })
                .collect(),
        )
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
    /// A GET's blocks are cached where they landed, once its body was all
    /// in: in memory, or written to disk. How its time went, since it
    /// started: its answer began after `answered` and was all in after
    /// `received`, of which `landing` went to landing its pieces in the
    /// cache as they came rather than to waiting for them; its blocks were
    /// cached after `after`. Before it started, it waited `waited` for its
    /// turn, the cap on bytes in flight reached (`doc/06-read-modes.md`,
    /// decision 11).
    Landed {
        answered: Duration,
        received: Duration,
        landing: Duration,
        after: Duration,
        waited: Duration,
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
        let plan = FetchRuns::default().plan(&read(&sources, 0));
        assert_eq!(
            plan.gets(),
            [PlannedGet::once(0..1), PlannedGet::once(2..4)]
        );
    }

    /// A normal read's run longer than 4 MiB is fetched in GETs of 4 MiB
    /// at most; a read warming the cache fetches it in one.
    #[allow(clippy::single_range_in_vec_init)]
    #[test]
    fn the_default_splits_a_normal_reads_long_run() {
        let sources = [BlockSource::Remote; 10];
        let normal = FetchRuns::default().plan(&read(&sources, 0));
        assert_eq!(
            normal.gets(),
            [
                PlannedGet::once(0..4),
                PlannedGet::once(4..8),
                PlannedGet::once(8..10)
            ]
        );
        let size = 10 * MIB + MIB / 2;
        let warm = LocatedRead::new(b"f", size, 0..size, Some(ReadIntent::Warm), 0, &sources);
        assert_eq!(
            FetchRuns::default().plan(&warm).gets(),
            [PlannedGet::once(0..10)]
        );
    }

    /// A normal read of a Parquet or Vortex file fetches the blocks holding
    /// its last 2 MiB in a GET of their own, two requests racing unless
    /// told otherwise; of another file, or with an intent, it races
    /// nothing.
    #[test]
    fn the_default_races_a_footer() {
        let raced = |blocks| PlannedGet::new(blocks, FetchRuns::FOOTER_RACE);
        let plan_by = |fetches: FetchRuns,
                       path: &'static [u8],
                       size,
                       intent,
                       first,
                       sources: &[BlockSource]| {
            let read = LocatedRead::new(path, size, 0..size, intent, first, sources);
            fetches.plan(&read).gets().to_vec()
        };
        let plan = |path, size, intent, first, sources: &[BlockSource]| {
            plan_by(FetchRuns::default(), path, size, intent, first, sources)
        };
        let size = 9 * MIB + MIB / 2;
        let whole = [BlockSource::Remote; 10];
        assert_eq!(
            plan(b"t/a.parquet", size, None, 0, &whole),
            [PlannedGet::once(0..4), PlannedGet::once(4..7), raced(7..10)],
            "the blocks holding the last 2 MiB, 7.5 MiB on"
        );
        assert_eq!(
            plan(b"t/a.vortex", size, None, 9, &[BlockSource::Remote]),
            [raced(9..10)]
        );
        assert_eq!(
            plan(b"t/a.vortex", MIB, None, 0, &[BlockSource::Remote]),
            [raced(0..1)],
            "a file smaller than its footer"
        );
        assert_eq!(
            plan(b"t/a.csv", size, None, 0, &whole),
            [
                PlannedGet::once(0..4),
                PlannedGet::once(4..8),
                PlannedGet::once(8..10)
            ]
        );
        assert_eq!(
            plan(b"t/a.parquet", size, Some(ReadIntent::Warm), 0, &whole),
            [PlannedGet::once(0..10)]
        );
        let unraced = FetchRuns::racing_footers(NonZeroU8::MIN);
        assert_eq!(
            plan_by(unraced, b"t/a.parquet", size, None, 0, &whole),
            [
                PlannedGet::once(0..4),
                PlannedGet::once(4..7),
                PlannedGet::once(7..10)
            ],
            "the footer apart, sent once"
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

    /// A block in flight is waited for, or raced with more requests than it
    /// has in flight.
    #[test]
    fn a_plan_covers_a_block_in_flight_only_racing_it() {
        use BlockSource::{InFlight, Remote};
        let sources = [Remote, InFlight { requests: 1 }, Remote];
        let located = read(&sources, 0);
        let accepts = |gets: &[(Range<u32>, u8)]| {
            let gets = gets
                .iter()
                .map(|(blocks, race)| {
                    PlannedGet::new(blocks.clone(), NonZeroU8::new(*race).unwrap())
                })
                .collect();
            located.accepts(&ReadPlan::new(gets))
        };
        assert!(accepts(&[(0..1, 1), (2..3, 1)]), "the remote runs, waiting");
        assert!(!accepts(&[(0..3, 1)]), "merged over it, a second request");
        assert!(!accepts(&[(1..2, 1)]), "a remote block left out");
        assert!(accepts(&[(0..1, 1), (1..2, 2), (2..3, 1)]), "raced");
        assert!(!accepts(&[(0..1, 1), (1..3, 1)]), "as many as in flight");
        assert_eq!(
            FetchRuns::default().plan(&located).gets().len(),
            2,
            "the default waits"
        );
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
