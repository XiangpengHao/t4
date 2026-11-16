//! Tiers are exclusive: a block is in one at a time, and the index names it there.

use std::num::NonZeroU32;
use std::ops::Range;

use super::disk::{CacheFiles, FileIdentity};
use super::in_flight::InFlight;
use super::index::{BlockTable, FileVersion, PathTable, PlannedBlock, UncacheableObject};
use super::memory::MemoryPool;
use crate::cache::policy::{PlacementPolicy, ReadIntent, ReadPolicy};
use crate::cache::read_output::ReadOutput;
use crate::io::error::Result;
use crate::io::sync::Arc;
use disk_writer::{DiskWriter, PendingDisk};

#[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
mod at_rest;
mod disk_writer;
mod eviction;
mod fill;
#[cfg(feature = "__fuzz")]
mod fuzzing;
mod locate;
mod located_run;
mod partial_read;
mod restart;
mod tier_work;

pub(in crate::cache) use fill::FetchedBlocks;
#[cfg(feature = "__fuzz")]
pub use fuzzing::{FuzzedSnapshot, FuzzedVersion};
pub(in crate::cache) use located_run::LocatedRun;
pub(in crate::cache) use partial_read::PartialRead;
pub(in crate::cache) use tier_work::TierWork;

pub(in crate::cache) struct ObjectCache {
    clock: crate::io::time::Clock,
    state: Arc<CacheState>,
    writer: Option<DiskWriter>,
}

pub(in crate::cache) struct CacheState {
    pending: Arc<PendingDisk>,
    paths: PathTable,
    blocks: BlockTable,
    memory: Arc<MemoryPool>,
    disk: CacheFiles,
    placement: Arc<dyn PlacementPolicy>,
    in_flight: Arc<InFlight>,
}

impl std::ops::Deref for ObjectCache {
    type Target = CacheState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl ObjectCache {
    #[cfg(any(test, feature = "__fuzz"))]
    pub(in crate::cache) fn wait_for_disk_writes(&self) {
        self.pending.wait_for_idle();
    }

    pub(in crate::cache) fn with_clock(mut self, clock: crate::io::time::Clock) -> Self {
        self.clock = clock;
        self
    }

    #[cfg(test)]
    pub(in crate::cache) fn waiting_reads(&self) -> usize {
        self.in_flight.waiting_reads()
    }

    pub(in crate::cache) fn new(
        memory_chunks: NonZeroU32,
        disk: CacheFiles,
        placement: Arc<dyn PlacementPolicy>,
    ) -> Result<Self> {
        let kept = match disk.snapshot() {
            Some(snapshot) => snapshot.take()?,
            None => None,
        };
        let state = Arc::new(CacheState {
            pending: Arc::new(PendingDisk::new(memory_chunks.get())),
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: Arc::new(MemoryPool::new(memory_chunks)?),
            disk,
            placement,
            in_flight: Arc::default(),
        });
        let writer = DiskWriter::start(Arc::clone(&state))?;
        let cache = Self {
            clock: Default::default(),
            state,
            writer: Some(writer),
        };
        if let Some(kept) = kept {
            cache.restore(&kept);
        }
        Ok(cache)
    }

    /// Names its owner's address (`doc/11-shared-mode.md`, decision 6).
    pub(in crate::cache) fn first_file(&self) -> Option<FileIdentity> {
        self.disk.first_file()
    }

    pub(in crate::cache) fn resolve<'p>(
        &self,
        path: &'p [u8],
        e_tag: Option<&str>,
        size: u64,
    ) -> std::result::Result<CacheableFile<'p>, UncacheableObject> {
        let version = self.paths.resolve(path, e_tag, size)?;
        Ok(CacheableFile { path, version })
    }

    pub(in crate::cache) fn start_read<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
    ) -> Result<(PartialRead, TierWork<'c>)> {
        self.start_read_into(file, range, intent, reads, ReadOutput::new)
    }

    pub(in crate::cache) fn start_read_into<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
        output: impl FnOnce(Range<u64>) -> Result<ReadOutput>,
    ) -> Result<(PartialRead, TierWork<'c>)> {
        let plan = self.blocks.plan_read(file.version, range.clone());
        let read = PartialRead::new(file.version, output(plan.bytes())?);
        let work = TierWork::new(
            self,
            reads,
            *file,
            range,
            intent,
            [plan.into_blocks()].into(),
        );
        Ok((read, work))
    }

    pub(in crate::cache) fn retry<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        blocks: BlocksToFetch,
        reads: &'c dyn ReadPolicy,
    ) -> TierWork<'c> {
        let relocated: Vec<PlannedBlock> = blocks
            .0
            .iter()
            .map(|block| block.relocated(self.blocks.get(block.key())))
            .collect();
        let runs = relocated
            .chunk_by(|a, b| a.key().index() + 1 == b.key().index())
            .map(<[PlannedBlock]>::to_vec)
            .collect();
        TierWork::new(self, reads, *file, range, intent, runs)
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn start_planned_read<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
    ) -> Result<(PartialRead, TierWork<'c>)> {
        let (mut read, mut work) = self.start_read(file, range, intent, reads)?;
        Self::plan_gets(&mut read, &mut work, file, intent, reads)?;
        Ok((read, work))
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn retry_planned<'c>(
        &'c self,
        read: &mut PartialRead,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        blocks: BlocksToFetch,
        reads: &'c dyn ReadPolicy,
    ) -> Result<TierWork<'c>> {
        let mut work = self.retry(file, range, intent, blocks, reads);
        Self::plan_gets(read, &mut work, file, intent, reads)?;
        Ok(work)
    }

    #[cfg(all(test, feature = "shuttle"))]
    fn plan_gets(
        read: &mut PartialRead,
        work: &mut TierWork<'_>,
        file: &CacheableFile<'_>,
        intent: Option<ReadIntent>,
        reads: &dyn ReadPolicy,
    ) -> Result<()> {
        while let Some(run) = work.locate_next_run(read)? {
            let gets = run.planned_by(reads, file, intent);
            work.take_gets(read, &gets);
        }
        Ok(())
    }

    /// Blocks until another test thread fills any fetch this read waits for.
    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) async fn read(
        &self,
        file: &CacheableFile<'_>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &dyn ReadPolicy,
    ) -> Result<PartialRead> {
        let (mut read, mut work) = self.start_planned_read(file, range.clone(), intent, reads)?;
        loop {
            let retry = work.finish(&mut read).await?;
            if retry.is_empty() {
                return Ok(read);
            }
            work = self.retry_planned(&mut read, file, range.clone(), intent, retry, reads)?;
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::cache) struct CacheableFile<'p> {
    path: &'p [u8],
    version: FileVersion,
}

impl<'p> CacheableFile<'p> {
    pub(in crate::cache) fn named(path: &'p [u8], version: FileVersion) -> Self {
        Self { path, version }
    }

    pub(in crate::cache) fn version(&self) -> FileVersion {
        self.version
    }
}

/// Its caller learns only whether any is left: the cache finds each block again.
#[derive(Debug)]
pub(in crate::cache) struct BlocksToFetch(Vec<PlannedBlock>);

impl BlocksToFetch {
    pub(in crate::cache) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod test_policy;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

// `[0..MIB]` is a list of one fetch, not the range's offsets.
#[allow(clippy::single_range_in_vec_init)]
#[cfg(all(test, feature = "shuttle"))]
mod tests;
