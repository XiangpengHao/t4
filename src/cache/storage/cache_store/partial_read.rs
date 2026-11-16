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

#[derive(Debug)]
pub(in crate::cache) struct PartialRead {
    pub(super) version: FileVersion,
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

    pub(in crate::cache) fn bytes(&self) -> Range<u64> {
        self.output.asked()
    }

    pub(in crate::cache) fn fetches(&self) -> Vec<Range<u64>> {
        self.fetches
            .iter()
            .map(|planned| planned.bytes.clone())
            .collect()
    }

    pub(super) fn lend_for_block(&mut self, block: Range<u64>) -> Result<Option<FetchedOutput>> {
        let Some(pages) = self.output.pages_over(block) else {
            return Ok(None);
        };
        Ok(Some(FetchedOutput {
            start: pages.start,
            pages: self.output.lend(pages)?,
        }))
    }

    /// `None` while a read from disk it started still writes them, as after one failed.
    pub(in crate::cache) fn into_bytes(self) -> Option<Bytes> {
        self.output.into_bytes()
    }

    pub(in crate::cache) fn filled_in_place(self) -> bool {
        self.output.filled_in_place()
    }

    pub(super) fn place_from_chunk(&mut self, block: &PlannedBlock, chunk: &[u8]) {
        let wanted = block.window().bytes();
        let at = block.object_range().start + u64::from(wanted.start);
        self.output
            .copy_in(at, &chunk[wanted.start as usize..wanted.end as usize]);
    }

    pub(super) fn offset(within_read: u64) -> usize {
        usize::try_from(within_read).expect("a read's bytes fit in memory")
    }
}

#[derive(Debug)]
pub(super) struct PlannedFetch {
    pub(super) bytes: Range<u64>,
    pub(super) race: NonZeroU8,
    pub(super) late: Option<Duration>,
    pub(super) first: u32,
    pub(super) blocks: Vec<FetchBlockPlan>,
}

impl PlannedFetch {
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

#[derive(Debug)]
pub(super) struct FetchBlockPlan {
    pub(super) fate: BlockFate,
    pub(super) claim: Option<FetchClaim>,
}

impl FetchBlockPlan {
    pub(super) fn new(fate: BlockFate, claim: Option<FetchClaim>) -> Self {
        Self { fate, claim }
    }

    pub(super) fn dropped() -> Self {
        Self::new(BlockFate::Dropped, None)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum BlockFate {
    Asked(Admit),
    Widened(Admit),
    Dropped,
}
