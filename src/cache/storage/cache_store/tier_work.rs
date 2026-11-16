//! A read's work on the tiers ([`TierWork`]): its blocks located run by
//! run, its hits taken, its page reads, its promotions, and its waits for
//! the blocks other reads fetch.

use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU8;
use std::ops::Range;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, join, join_all};

use crate::cache::BlockKey;
use crate::cache::io_report::IoReport;
use crate::cache::policy::{
    Admit, BlockRead, BlockSource, DiskHit, FetchRuns, Io, ReadIntent, ReadPlan, ReadPolicy,
};
use crate::cache::storage::disk::DiskChunkReader;
use crate::cache::storage::in_flight::{BlockFetch, FetchClaim, FetchEnd, InFlight};
use crate::cache::storage::index::PlannedBlock;
use crate::cache::storage::memory::MemoryChunkReader;
use crate::io::error::Result;
use crate::io::sync::Arc;

use super::eviction::Promotion;
use super::locate::{HeldBlock, Located};
use super::located_run::LocatedRun;
use super::partial_read::{BlockFate, FetchBlockPlan, PartialRead, PlannedFetch};
use super::{BlocksToFetch, CacheableFile, ObjectCache};

impl ObjectCache {
    /// Reads the pages of `block` that `read` needs from the disk chunk
    /// `on_disk` holds, straight into the read's output. Submitted now, so
    /// reads started together run at once; done when the future is.
    fn read_pages(
        &self,
        read: &mut PartialRead,
        on_disk: &DiskChunkReader<'_>,
        block: &PlannedBlock,
    ) -> Result<impl Future<Output = Result<()>> + use<>> {
        let pages = block.window().pages();
        let start = block.object_range().start;
        let into = read
            .output
            .lend(start + u64::from(pages.start)..start + u64::from(pages.end))?;
        self.disk.read_into(on_disk, pages.start, into)
    }
}

/// A read's work on the tiers once it is started
/// ([`ObjectCache::start_read`]): the runs of its blocks it locates, one at
/// a time; its page reads, submitted as they are located, the blocks it
/// moves to memory, and those it waits for another read to fetch. Its
/// caller awaits it beside the read's GETs.
pub(in crate::cache) struct TierWork<'c> {
    cache: &'c ObjectCache,
    /// The caller's read policy, which hears of the read's reads from disk
    /// as they go.
    reads: &'c dyn ReadPolicy,
    /// The read's file, the bytes it asked for and what for: what the
    /// placement policy hears of each block.
    file: CacheableFile<'c>,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    /// The runs of the read's adjacent blocks not located yet, in order:
    /// each located once the GETs of the one before have claimed the
    /// blocks they widen to.
    unlocated: VecDeque<Vec<PlannedBlock>>,
    /// Each read of a disk chunk's pages into the read's output, the chunk
    /// held until its bytes land.
    page_reads: Vec<(DiskChunkReader<'c>, BoxFuture<'c, Result<()>>)>,
    /// The blocks the placement policy moves to memory.
    promotions: Vec<(DiskChunkReader<'c>, PlannedBlock)>,
    /// The blocks another read is fetching, each with its fetch.
    waits: Vec<(PlannedBlock, Arc<BlockFetch>)>,
    /// The run located last, held until its GETs are taken.
    held: Option<HeldRun>,
}

impl<'c> TierWork<'c> {
    /// No work yet, for a read of `range` of `file` whose blocks are the
    /// runs `unlocated`, its reads from disk heard by `reads`.
    pub(super) fn new(
        cache: &'c ObjectCache,
        reads: &'c dyn ReadPolicy,
        file: CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        unlocated: VecDeque<Vec<PlannedBlock>>,
    ) -> Self {
        Self {
            cache,
            reads,
            file,
            range,
            intent,
            unlocated,
            page_reads: Vec::new(),
            promotions: Vec::new(),
            waits: Vec::new(),
            held: None,
        }
    }

    /// Locates the read's next run of adjacent blocks: takes each a tier
    /// holds as a hit, and returns where each of the run's blocks is, for
    /// the caller's read policy to plan its GETs by; the work holds the
    /// claims and fetches in flight those GETs take ([`Self::take_gets`]),
    /// which come before the next run is located. A run the tiers hold
    /// whole is taken and skipped. `None` once every run is located.
    ///
    /// # Errors
    ///
    /// If a read from the cache file can't be submitted.
    ///
    /// # Panics
    ///
    /// If the GETs of the run located last weren't taken.
    pub(in crate::cache) fn locate_next_run(
        &mut self,
        read: &mut PartialRead,
    ) -> Result<Option<LocatedRun>> {
        assert!(
            self.held.is_none(),
            "a run's GETs are taken before the next run is located"
        );
        let cache = self.cache;
        while let Some(blocks) = self.unlocated.pop_front() {
            let mut sources = Vec::with_capacity(blocks.len());
            let mut remote = Vec::with_capacity(blocks.len());
            for block in &blocks {
                let context = self.context(block.key(), true);
                let (source, fetch) = match cache.locate(block, &context) {
                    Located::Memory(reader) => {
                        self.take_memory_hit(read, block, &reader);
                        (BlockSource::Memory, None)
                    }
                    Located::Disk(reader) => {
                        self.take_disk_hit(read, block, reader)?;
                        (BlockSource::Disk, None)
                    }
                    Located::InFlight(fetch) => (
                        BlockSource::InFlight {
                            requests: fetch.requests(),
                        },
                        Some(RemoteBlock::InFlight(fetch)),
                    ),
                    Located::Remote { admit, claim } => (
                        BlockSource::Remote,
                        Some(RemoteBlock::Fetched { admit, claim }),
                    ),
                };
                sources.push(source);
                remote.push(fetch);
            }
            if remote.iter().any(Option::is_some) {
                let located = LocatedRun {
                    first: blocks[0].key().index(),
                    asked: Self::asked_within(read.bytes(), &blocks),
                    sources,
                };
                self.held = Some(HeldRun {
                    located: located.clone(),
                    blocks,
                    remote,
                });
                return Ok(Some(located));
            }
        }
        Ok(None)
    }

    /// Takes `gets`, the GETs planned for the run of the read
    /// [`Self::locate_next_run`] located last; or the default's, if the
    /// cache can't carry them out: a read policy's mistake costs a read
    /// nothing, wherever the policy ran. Names each GET for the read, with
    /// what becomes of each block it fetches: one of the run's it fetches,
    /// one a tier holds merged over and dropped, and one past the run
    /// claimed for it, or dropped if a tier holds it or another read is
    /// fetching it. The read waits for each of the run's blocks in flight
    /// that no GET races.
    ///
    /// # Panics
    ///
    /// If no run is located, or its GETs were taken already.
    pub(in crate::cache) fn take_gets(&mut self, read: &mut PartialRead, gets: &ReadPlan) {
        let cache = self.cache;
        let HeldRun {
            located: run,
            blocks,
            mut remote,
        } = self
            .held
            .take()
            .expect("GETs are taken for the run located last, once");
        let first = run.first;
        let located = run.located(&self.file, self.intent);
        let default;
        let gets = if located.accepts(gets) {
            gets
        } else {
            default = FetchRuns::default().plan(&located);
            &default
        };
        for get in gets.gets() {
            let fetched = get
                .blocks()
                .map(|index| match located.source(index) {
                    Some(BlockSource::Memory | BlockSource::Disk) => FetchBlockPlan::dropped(),
                    Some(BlockSource::Remote | BlockSource::InFlight { .. }) => remote
                        [(index - first) as usize]
                        .take()
                        .expect("a plan fetches a block in one GET at most")
                        .fetched_by(&cache.in_flight, get.race()),
                    None => {
                        cache.widened_block(&self.file, &self.range, self.intent, index, get.race())
                    }
                })
                .collect();
            read.fetches
                .push(PlannedFetch::new(&read.version, get, fetched));
        }
        // The blocks in flight that no GET races: the read waits for them.
        for (block, fetch) in blocks.iter().zip(remote) {
            if let Some(RemoteBlock::InFlight(fetch)) = fetch {
                self.waits.push((*block, fetch));
            }
        }
    }

    /// Takes `block`, a hit in the memory chunk `reader` holds: its part of
    /// the read copied into `read`'s output.
    fn take_memory_hit(
        &self,
        read: &mut PartialRead,
        block: &PlannedBlock,
        reader: &MemoryChunkReader<'_>,
    ) {
        let context = self.context(block.key(), true);
        self.cache.placement.on_memory_hit(reader.chunk(), &context);
        read.place_from_chunk(block, reader.bytes());
    }

    /// Takes `block`, a hit in the disk chunk `reader` holds: its pages read
    /// into `read`'s output, submitted now, or the block moved to memory
    /// first, as the placement policy says.
    ///
    /// # Errors
    ///
    /// If the read can't be submitted.
    fn take_disk_hit(
        &mut self,
        read: &mut PartialRead,
        block: &PlannedBlock,
        reader: DiskChunkReader<'c>,
    ) -> Result<()> {
        let context = self.context(block.key(), true);
        match self.cache.placement.on_disk_hit(reader.chunk(), &context) {
            DiskHit::Stay => {
                let report = self.report_disk_read(&reader, block);
                let pages = self.cache.read_pages(read, &reader, block)?;
                self.page_reads
                    .push((reader, report.awaiting(pages).boxed()));
            }
            DiskHit::Promote => self.promotions.push((reader, *block)),
        }
        Ok(())
    }

    /// Reports a read of `block`'s pages from the disk chunk `on_disk`
    /// holds, started, to the read's policy.
    fn report_disk_read(
        &self,
        on_disk: &DiskChunkReader<'_>,
        block: &PlannedBlock,
    ) -> IoReport<'c> {
        let pages = block.window().pages();
        IoReport::start(
            self.reads,
            Io::DiskRead {
                drive: on_disk.position().file(),
                bytes: pages.end - pages.start,
            },
        )
    }

    /// What the placement policy hears of the read's block `key`: whether
    /// the read `asked` for it, or a GET fetches it past the read.
    fn context(&self, key: BlockKey, asked: bool) -> BlockRead<'c> {
        BlockRead::new(
            key,
            self.file.version.size(),
            self.range.clone(),
            self.file.path,
            self.intent,
            asked,
        )
    }

    /// `bytes`, a read's, within `blocks`, adjacent blocks of it.
    fn asked_within(bytes: Range<u64>, blocks: &[PlannedBlock]) -> Range<u64> {
        let start = blocks[0].object_range().start;
        let end = blocks[blocks.len() - 1].object_range().end;
        bytes.start.max(start)..bytes.end.min(end)
    }

    /// Waits for the page reads and for the blocks other reads fetch, takes
    /// each of those from where its fetch cached it, then moves each block
    /// promoted to memory and copies its part of `read`'s output from
    /// there; a block that finds no room in memory stays, and the read
    /// takes its pages from disk after all. Returns the blocks waited for
    /// that no fetch cached, or that left the cache before the read took
    /// them: the read fetches those itself ([`ObjectCache::retry`]).
    ///
    /// # Errors
    ///
    /// If a read from the cache file fails.
    pub(in crate::cache) async fn finish(
        mut self,
        read: &mut PartialRead,
    ) -> Result<BlocksToFetch> {
        let waits = std::mem::take(&mut self.waits);
        let page_reads = Self::page_reads_done(std::mem::take(&mut self.page_reads));
        let ends = join_all(waits.iter().map(|(_, fetch)| fetch.ended()));
        let (pages_read, ends) = join(page_reads, ends).await;
        pages_read?;
        let mut retry = Vec::new();
        for ((block, _), end) in waits.iter().zip(ends) {
            let held = match end {
                FetchEnd::Cached(location) => self.cache.hold(block.key(), Some(location)),
                FetchEnd::Passed(bytes) => {
                    read.place_from_chunk(block, &bytes);
                    continue;
                }
                FetchEnd::Abandoned => HeldBlock::Remote,
            };
            match held {
                HeldBlock::Memory(reader) => self.take_memory_hit(read, block, &reader),
                HeldBlock::Disk(reader) => self.take_disk_hit(read, block, reader)?,
                HeldBlock::Remote => retry.push(*block),
            }
        }
        Self::page_reads_done(std::mem::take(&mut self.page_reads)).await?;
        let promotions = std::mem::take(&mut self.promotions);
        let (cache, reads) = (self.cache, self.reads);
        let promoted = join_all(promotions.into_iter().map(|(on_disk, block)| async move {
            (cache.promote(on_disk, &block, reads).await, block)
        }))
        .await;
        for (promotion, block) in promoted {
            match promotion? {
                Promotion::InMemory(reader) => read.place_from_chunk(&block, reader.bytes()),
                Promotion::NoRoom(on_disk) => {
                    let report = self.report_disk_read(&on_disk, &block);
                    let pages = cache.read_pages(read, &on_disk, &block)?;
                    report.awaiting(pages).await?;
                }
            }
        }
        Ok(BlocksToFetch(retry))
    }

    /// Waits for `page_reads`, each chunk held until its bytes land. They
    /// were submitted as they were planned, so they run at once, each
    /// straight into the read's output; awaited together, each reports its
    /// end as its bytes land, not once those before it have.
    async fn page_reads_done(
        page_reads: Vec<(DiskChunkReader<'c>, BoxFuture<'c, Result<()>>)>,
    ) -> Result<()> {
        join_all(page_reads.into_iter().map(|(held, pages)| async move {
            let _held = held;
            pages.await
        }))
        .await
        .into_iter()
        .collect()
    }
}

/// The run a read's work located last, held until its GETs are taken
/// ([`TierWork::take_gets`]): what its caller was told, its blocks, and the
/// claims and fetches in flight its GETs take. Dropped untaken, its claims
/// end, and the reads waiting on them fetch for themselves.
struct HeldRun {
    located: LocatedRun,
    /// The run's blocks, in order.
    blocks: Vec<PlannedBlock>,
    /// For each block no tier holds, the read's claim on it, or the fetch
    /// in flight it waits for unless a GET races it; `None` for a hit.
    remote: Vec<Option<RemoteBlock>>,
}

/// A block a read needs that no tier holds: one the read fetches, claimed
/// if it is to be cached, or one another read is fetching.
enum RemoteBlock {
    Fetched {
        admit: Admit,
        claim: Option<FetchClaim>,
    },
    InFlight(Arc<BlockFetch>),
}

impl RemoteBlock {
    /// What becomes of the block, a GET of the read racing `race` fetching
    /// it: into the read's output, and the cache where it goes. The GET's
    /// racers are the read's requests in the block's fetch: those of its
    /// claim, or, for a block another read is fetching, a racer's joined.
    fn fetched_by(self, in_flight: &Arc<InFlight>, race: NonZeroU8) -> FetchBlockPlan {
        match self {
            Self::Fetched { admit, claim } => {
                let claim = claim.map(|mut claim| {
                    claim.fetched_by(race);
                    claim
                });
                FetchBlockPlan::new(BlockFate::Asked(admit), claim)
            }
            Self::InFlight(fetch) => {
                let racer = fetch.join(in_flight, u32::from(race.get()));
                FetchBlockPlan::new(BlockFate::Asked(fetch.admit()), Some(racer))
            }
        }
    }
}
