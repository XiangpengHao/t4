//! [`LocatedRun`]: a run of a read's blocks as a read policy plans its
//! GETs by, data alone, so that a client's crosses the socket.

use std::ops::Range;

use crate::cache::policy::{BlockSource, LocatedRead, ReadIntent, ReadPlan, ReadPolicy};

use super::CacheableFile;

/// A run of a read's adjacent blocks that the tiers don't all hold, located
/// ([`TierWork::locate_next_run`](super::TierWork::locate_next_run)): where
/// each block is, which its caller's read policy plans the run's GETs by
/// ([`Self::planned_by`]). Data alone, so that a client's crosses the
/// socket (`doc/11-shared-mode.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache) struct LocatedRun {
    /// The number of the run's first block.
    pub(super) first: u32,
    /// The file's bytes the read asked for within the run.
    pub(super) asked: Range<u64>,
    /// Where each block is, the run's first block's first.
    pub(super) sources: Vec<BlockSource>,
}

impl LocatedRun {
    /// A run of `sources`, from block `first`, the read's `asked` bytes of
    /// it: as a client takes one its owner located.
    pub(in crate::cache) fn new(first: u32, asked: Range<u64>, sources: Vec<BlockSource>) -> Self {
        Self {
            first,
            asked,
            sources,
        }
    }

    /// The number of the run's first block.
    pub(in crate::cache) fn first(&self) -> u32 {
        self.first
    }

    /// The file's bytes the read asked for within the run.
    pub(in crate::cache) fn asked(&self) -> Range<u64> {
        self.asked.clone()
    }

    /// Where each block is, the run's first block's first.
    pub(in crate::cache) fn sources(&self) -> &[BlockSource] {
        &self.sources
    }

    /// The GETs `policy` plans for the run, of a read of `file` for
    /// `intent`; the cache checks them as it takes them
    /// ([`TierWork::take_gets`](super::TierWork::take_gets)).
    pub(in crate::cache) fn planned_by(
        &self,
        policy: &dyn ReadPolicy,
        file: &CacheableFile<'_>,
        intent: Option<ReadIntent>,
    ) -> ReadPlan {
        policy.plan(&self.located(file, intent))
    }

    /// The run as a read policy sees it, of a read of `file` for `intent`.
    pub(super) fn located<'r>(
        &'r self,
        file: &CacheableFile<'r>,
        intent: Option<ReadIntent>,
    ) -> LocatedRead<'r> {
        LocatedRead::new(
            file.path,
            file.version.size(),
            self.asked.clone(),
            intent,
            self.first,
            &self.sources,
        )
    }
}
