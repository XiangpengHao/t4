use std::ops::Range;

use crate::cache::policy::{BlockSource, LocatedRead, ReadIntent, ReadPlan, ReadPolicy};

use super::CacheableFile;

/// Data alone, so that a client's crosses the socket (`doc/11-shared-mode.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache) struct LocatedRun {
    pub(super) first: u32,
    pub(super) asked: Range<u64>,
    pub(super) sources: Vec<BlockSource>,
}

impl LocatedRun {
    pub(in crate::cache) fn new(first: u32, asked: Range<u64>, sources: Vec<BlockSource>) -> Self {
        Self {
            first,
            asked,
            sources,
        }
    }

    pub(in crate::cache) fn first(&self) -> u32 {
        self.first
    }

    pub(in crate::cache) fn asked(&self) -> Range<u64> {
        self.asked.clone()
    }

    pub(in crate::cache) fn sources(&self) -> &[BlockSource] {
        &self.sources
    }

    pub(in crate::cache) fn planned_by(
        &self,
        policy: &dyn ReadPolicy,
        file: &CacheableFile<'_>,
        intent: Option<ReadIntent>,
    ) -> ReadPlan {
        policy.plan(&self.located(file, intent))
    }

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
