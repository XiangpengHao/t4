//! [`LocatedRun`]: a run of a read's blocks as a read policy plans its
//! GETs by, data alone, so that a client's crosses the socket.

use std::ops::Range;

use crate::cache::BlockKey;
use crate::cache::policy::{BlockSource, LocatedRead, ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::sharing::{FrameReader, FrameWriter};
use crate::io::error::Result;

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
    /// it: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn new(first: u32, asked: Range<u64>, sources: Vec<BlockSource>) -> Self {
        Self {
            first,
            asked,
            sources,
        }
    }

    /// Writes the run into `frame`, for a client to plan its GETs by.
    pub(in crate::cache) fn write_to(&self, frame: &mut FrameWriter) {
        frame.u32(self.first);
        frame.range(&self.asked);
        frame.sources(&self.sources);
    }

    /// The run `frame` holds next, as [`Self::write_to`] wrote it.
    ///
    /// # Errors
    ///
    /// If the frame ends first, or the run holds no block or one past the
    /// last a file has.
    pub(in crate::cache) fn read_from(frame: &mut FrameReader) -> Result<Self> {
        let first = frame.u32()?;
        let asked = frame.range()?;
        let sources = frame.sources()?;
        let blocks = u32::try_from(sources.len()).ok();
        let end = blocks.and_then(|blocks| first.checked_add(blocks));
        if sources.is_empty() || end.is_none_or(|end| end > BlockKey::INDEX_LIMIT) {
            return Err(FrameReader::malformed("a located run's blocks"));
        }
        Ok(Self {
            first,
            asked,
            sources,
        })
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
