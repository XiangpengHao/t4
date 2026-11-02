//! [`ObjectCache`]: reads, fills and evictions over the index and the two
//! tiers, as its policies decide (`doc/03-cache.md`, `doc/06-read-modes.md`).
//!
//! - **A read** copies the blocks memory holds, and reads those on disk,
//!   moving one to memory if the placement policy says so. It waits for
//!   those another read is fetching, and takes each from where it lands, or
//!   from the fetch itself, for one no tier keeps.
//!   For the rest it locates each block, claimed for the read or in flight,
//!   run by run; the caller's read policy plans each run's GETs, and the
//!   read takes them: what becomes of each block they fetch, where the
//!   placement policy said it goes, the read's part into its output, a
//!   block a tier holds already dropped.
//! - **A fill** takes the caller's fetch of those blocks as its bytes come
//!   in, into the read's output and each block where the read said.
//! - **A tier with no free chunk** for a block evicts the victims the policy
//!   picks: memory moves its victims to disk or drops them, the disk drops
//!   its own.
//!
//! Tiers are exclusive: a block is in one at a time, and the index names it
//! there. A block is fetched once (`in_flight`): a read that needs one being
//! fetched waits for that fetch, unless it races, and fetches the block
//! itself if the fetch ends without caching it.
//!
//! Here, how a read starts, and what its parts share. Each part on its own:
//!
//! - `locate`: where a block a read needs is, and the chunk that holds it.
//! - `tier_work`: a read's work on the tiers, run by run ([`TierWork`]).
//! - `located_run`: a run as the read policy plans its GETs by, and as it
//!   crosses the socket ([`LocatedRun`]).
//! - `partial_read`: the read's output, and what its GETs will do with
//!   each block ([`PartialRead`]).
//! - `fill`: a fetch's bytes landing, in the output and the cache
//!   ([`FetchedBlocks`]).
//! - `eviction`: room made in a full tier, and blocks moved between tiers.
//! - `restart`: the disk tier kept as the cache drops, and restored as the
//!   next cache on its files opens.
//! - `at_rest`: whether the index and the tiers agree, for tests.

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

#[cfg(any(test, feature = "__fuzz"))]
mod at_rest;
mod eviction;
mod fill;
mod locate;
mod located_run;
mod partial_read;
mod restart;
mod tier_work;

pub(in crate::cache) use fill::FetchedBlocks;
pub(in crate::cache) use located_run::LocatedRun;
pub(in crate::cache) use partial_read::PartialRead;
pub(in crate::cache) use tier_work::TierWork;

/// A cache of file versions' blocks in memory and disk chunks, found
/// through the index, placed and evicted by its policy.
pub(in crate::cache) struct ObjectCache {
    paths: PathTable,
    blocks: BlockTable,
    /// Shared with the I/O in flight that lends its chunks.
    memory: Arc<MemoryPool>,
    disk: CacheFiles,
    placement: Arc<dyn PlacementPolicy>,
    /// The blocks being fetched, shared with the claims on them.
    in_flight: Arc<InFlight>,
}

impl ObjectCache {
    /// A cache of `memory_chunks` chunks of memory and the chunks of
    /// `disk`: its blocks placed by `placement`, which is built for both
    /// counts. Memory holds nothing; the disk holds what the snapshot the
    /// last cache on its files kept says, if it matches them, and nothing
    /// otherwise (`restart`).
    ///
    /// # Errors
    ///
    /// If the memory can't be mapped, or a snapshot is there and can't be
    /// removed.
    pub(in crate::cache) fn new(
        memory_chunks: NonZeroU32,
        disk: CacheFiles,
        placement: Arc<dyn PlacementPolicy>,
    ) -> Result<Self> {
        let kept = match disk.snapshot() {
            Some(snapshot) => snapshot.take()?,
            None => None,
        };
        let cache = Self {
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: Arc::new(MemoryPool::new(memory_chunks)?),
            disk,
            placement,
            in_flight: Arc::default(),
        };
        if let Some(kept) = kept {
            cache.restore(&kept);
        }
        Ok(cache)
    }

    /// The identity of its first cache file, which names its owner's
    /// address (`doc/11-shared-mode.md`, decision 6); `None` for simulated
    /// files.
    pub(in crate::cache) fn first_file(&self) -> Option<FileIdentity> {
        self.disk.first_file()
    }

    /// The file at `path`, the version of it with `e_tag` and `size`, as
    /// reads name it; or why the cache can't hold the file, whose reads then
    /// go to the object store alone.
    pub(in crate::cache) fn resolve<'p>(
        &self,
        path: &'p [u8],
        e_tag: Option<&str>,
        size: u64,
    ) -> std::result::Result<CacheableFile<'p>, UncacheableObject> {
        let version = self.paths.resolve(path, e_tag, size)?;
        Ok(CacheableFile { path, version })
    }

    /// Starts a read of `range` of `file`, clamped to the file: its output,
    /// and its work on the tiers ([`TierWork`]), which locates the read's
    /// blocks for the caller to plan its GETs by
    /// ([`TierWork::locate_next_run`]): the bytes of blocks memory holds
    /// copied into the output, the pages of blocks on disk submitted to be
    /// read straight into it, the blocks another read is fetching to be
    /// waited for, and the rest claimed for the read's GETs, which the
    /// caller plans with its read policy, `reads`, the work takes
    /// ([`TierWork::take_gets`]), and the caller makes and the read takes
    /// ([`Self::receive`]). The caller awaits the work beside its GETs: a
    /// read's time on the network overlaps its time on the drives, the
    /// evictions its promotions make, and its waits.
    ///
    /// The placement policy hears of every block: a hit in either tier,
    /// where a disk hit goes, a remote read and where its block goes once
    /// fetched, and where each block a GET fetches past the read goes.
    /// `reads` hears of every read from disk the work makes, as it goes.
    ///
    /// # Errors
    ///
    /// If the read's output can't be made.
    pub(in crate::cache) fn start_read<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
    ) -> Result<(PartialRead, TierWork<'c>)> {
        self.start_read_into(file, range, intent, reads, ReadOutput::new)
    }

    /// [`Self::start_read`], the read's output made by `output` for the
    /// file's bytes it takes: one over a span of a client's region, say
    /// (`doc/11-shared-mode.md`, decision 5).
    ///
    /// # Errors
    ///
    /// If `output` fails.
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

    /// The work on the tiers for `blocks` of a read of `range` of `file`,
    /// again: those its work waited for, and no fetch cached
    /// ([`TierWork::finish`]). As [`Self::start_read`]'s work locates a
    /// read, each run of adjacent ones as a read of its own: some may be
    /// cached by now, or in flight again, and the read fetches the rest,
    /// its GETs the caller's to plan and make, its reads from disk heard by
    /// `reads`.
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

    /// [`Self::start_read`] with its GETs planned by `reads`, as a caller
    /// plans them: for tests.
    ///
    /// # Errors
    ///
    /// If the read's output can't be made, or a read from the cache file
    /// can't be submitted.
    #[cfg(test)]
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

    /// [`Self::retry`] with its GETs planned by `reads`: for tests.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted.
    #[cfg(test)]
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

    /// Plans `read`'s GETs with `reads`, run by run as `work` locates them,
    /// as `T4Caching` plans them with its own: for tests.
    #[cfg(test)]
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

    /// [`Self::start_planned_read`] and its work on the tiers, one after the
    /// other, and so again for each block it waited for that no fetch
    /// cached: for tests, whose fetches come after. A read that waits for a
    /// block another read is fetching waits until another test thread fills
    /// that read.
    #[cfg(test)]
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

/// A file the cache can hold: its path, and the version of it reads name.
#[derive(Clone, Copy, Debug)]
pub(in crate::cache) struct CacheableFile<'p> {
    path: &'p [u8],
    version: FileVersion,
}

impl<'p> CacheableFile<'p> {
    /// The file at `path`, its version as an owner named it to its client
    /// (`doc/11-shared-mode.md`).
    pub(in crate::cache) fn named(path: &'p [u8], version: FileVersion) -> Self {
        Self { path, version }
    }

    pub(in crate::cache) fn version(&self) -> FileVersion {
        self.version
    }
}

/// The blocks a read waited for that no fetch cached, or that left the
/// cache before the read took them ([`TierWork::finish`]): the read fetches
/// those itself, its work on the tiers made again for them
/// ([`ObjectCache::retry`]). Its caller learns only whether any is left;
/// where each block was stays with the cache, which finds it again.
#[derive(Debug)]
pub(in crate::cache) struct BlocksToFetch(Vec<PlannedBlock>);

impl BlocksToFetch {
    pub(in crate::cache) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod test_policy;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

// `[0..MIB]` is a list of one fetch, not the range's offsets.
#[allow(clippy::single_range_in_vec_init)]
#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
