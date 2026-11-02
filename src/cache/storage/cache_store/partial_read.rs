//! [`PartialRead`]: a read the cache served in part, its output, and the
//! GETs that fetch the rest, with what becomes of each block they fetch.

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;

use crate::cache::policy::{Admit, PlannedGet};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::in_flight::FetchClaim;
use crate::cache::storage::index::{FileVersion, PlannedBlock};
use crate::io::error::Result;

use super::fill::FetchedOutput;

/// A read that the cache served in part: the bytes it takes, the output they
/// land in, and the GETs that fetch the rest, as the read policy planned
/// them, with what becomes of each block they fetch.
#[derive(Debug)]
pub(in crate::cache) struct PartialRead {
    pub(super) version: FileVersion,
    /// Where the read's bytes land, the caller's once filled.
    pub(super) output: ReadOutput,
    pub(super) fetches: Vec<PlannedFetch>,
}

impl PartialRead {
    pub(super) fn new(version: FileVersion, output: ReadOutput) -> Self {
        Self {
            version,
            output,
            fetches: Vec::new(),
        }
    }

    /// The file's bytes the read takes, which its output holds.
    pub(in crate::cache) fn bytes(&self) -> Range<u64> {
        self.output.asked()
    }

    /// The file's bytes each GET fetches, in the order planned.
    pub(in crate::cache) fn fetches(&self) -> Vec<Range<u64>> {
        self.fetches
            .iter()
            .map(|planned| planned.bytes.clone())
            .collect()
    }

    /// The read's output over the pages of `block`, the file's bytes of a
    /// block it asked for, lent for the block's fetched bytes to land in;
    /// `None` if the output covers none of them.
    pub(super) fn lend_for_block(&mut self, block: Range<u64>) -> Result<Option<FetchedOutput>> {
        let Some(pages) = self.output.pages_over(block) else {
            return Ok(None);
        };
        Ok(Some(FetchedOutput {
            start: pages.start,
            pages: self.output.lend(pages)?,
        }))
    }

    /// The read's bytes, once the caller has filled what came remote: `None`
    /// if a read from disk it started is still writing them, as after one
    /// failed.
    pub(in crate::cache) fn into_bytes(self) -> Option<Bytes> {
        self.output.into_bytes()
    }

    /// Whether the read's bytes are all in its output, in place, once the
    /// caller has filled what came remote: false while a read from disk it
    /// started is still writing them. For an output over a span of a
    /// client's region, which the client then takes.
    pub(in crate::cache) fn filled_in_place(self) -> bool {
        self.output.filled_in_place()
    }

    /// Copies the part of `block` the read takes from `chunk`, the whole of
    /// the chunk that holds it, into its output.
    pub(super) fn place_from_chunk(&mut self, block: &PlannedBlock, chunk: &[u8]) {
        let wanted = block.window().bytes();
        let at = block.object_range().start + u64::from(wanted.start);
        self.output
            .copy_in(at, &chunk[wanted.start as usize..wanted.end as usize]);
    }

    /// A length or an offset within a read's bytes, or a fetch's, as a
    /// `usize`: the bytes are in memory, so it fits.
    pub(super) fn offset(within_read: u64) -> usize {
        usize::try_from(within_read).expect("a read's bytes fit in memory")
    }
}

/// One GET of a read's plan: the file's bytes it fetches, whole blocks,
/// how many requests race for them, at once and late, and what becomes of
/// each block.
#[derive(Debug)]
pub(super) struct PlannedFetch {
    pub(super) bytes: Range<u64>,
    pub(super) race: NonZeroU8,
    pub(super) late: Option<Duration>,
    pub(super) first: u32,
    pub(super) blocks: Vec<FetchBlockPlan>,
}

impl PlannedFetch {
    /// `get` of `version`, what becomes of its blocks `blocks`, the first
    /// block's first.
    pub(super) fn new(
        version: &FileVersion,
        get: &PlannedGet,
        blocks: Vec<FetchBlockPlan>,
    ) -> Self {
        let range = get.blocks();
        Self {
            bytes: version.block_range(range.start).start..version.block_range(range.end - 1).end,
            race: get.race(),
            late: get.late_race(),
            first: range.start,
            blocks,
        }
    }
}

/// What becomes of a block a GET fetches, and the requests the read holds
/// in the block's fetch, if it is to be cached.
#[derive(Debug)]
pub(super) struct FetchBlockPlan {
    pub(super) fate: BlockFate,
    pub(super) claim: Option<FetchClaim>,
}

impl FetchBlockPlan {
    pub(super) fn new(fate: BlockFate, claim: Option<FetchClaim>) -> Self {
        Self { fate, claim }
    }

    /// Its bytes dropped, the block cached already or another's to fetch.
    pub(super) fn dropped() -> Self {
        Self::new(BlockFate::Dropped, None)
    }
}

/// What becomes of a block a GET fetches.
#[derive(Clone, Copy, Debug)]
pub(super) enum BlockFate {
    /// The read asked for it: into the read's output, and the cache, where
    /// the placement policy said.
    Asked(Admit),
    /// A GET widened past the read fetched it: into the cache alone.
    Widened(Admit),
    /// A tier holds it already, or another read is fetching it: its bytes
    /// dropped.
    Dropped,
}
