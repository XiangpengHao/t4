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
    /// Submitted now, so reads started together run at once.
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
            .lend_to_disk_read(start + u64::from(pages.start)..start + u64::from(pages.end))?;
        self.tiers.disk.read_into(on_disk, pages.start, into)
    }
}

pub(in crate::cache) struct TierWork<'c> {
    cache: &'c ObjectCache,
    reads: &'c dyn ReadPolicy,
    file: CacheableFile<'c>,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    /// In order, with gaps where an earlier round served the read: one run, so no GET of one
    /// run widens to blocks another has claimed.
    unlocated: Vec<PlannedBlock>,
    page_reads: Vec<(DiskChunkReader<'c>, BoxFuture<'c, Result<()>>)>,
    promotions: Vec<(DiskChunkReader<'c>, PlannedBlock)>,
    waits: Vec<(PlannedBlock, Arc<BlockFetch>)>,
}

impl<'c> TierWork<'c> {
    pub(super) fn new(
        cache: &'c ObjectCache,
        reads: &'c dyn ReadPolicy,
        file: CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        unlocated: Vec<PlannedBlock>,
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
        }
    }

    /// Takes the round's hits; the run is `None` if it leaves nothing to fetch. Once a round:
    /// later calls find no blocks.
    pub(in crate::cache) fn locate(&mut self, read: &mut PartialRead) -> Result<Option<HeldRun>> {
        let blocks = std::mem::take(&mut self.unlocated);
        let (Some(first), Some(last)) = (blocks.first(), blocks.last()) else {
            return Ok(None);
        };
        let first = first.key().index();
        let span = (last.key().index() - first + 1) as usize;
        // A gap is a memory hit to the policy: the read holds its bytes already.
        let mut sources = vec![BlockSource::Memory; span];
        let mut remote: Vec<_> = std::iter::repeat_with(|| None).take(span).collect();
        let cache = self.cache;
        for block in &blocks {
            let context = self.context(block.key(), true);
            let at = (block.key().index() - first) as usize;
            (sources[at], remote[at]) = match cache.locate(block, &context) {
                Located::Memory(reader) => {
                    self.take_memory_hit(read, block, &reader);
                    (BlockSource::Memory, None)
                }
                Located::Disk(reader) => {
                    self.take_disk_hit(read, block, reader)?;
                    (BlockSource::Disk, None)
                }
                Located::Landed(bytes) => {
                    read.place_from_chunk(block, &bytes);
                    (BlockSource::Memory, None)
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
        }
        if remote.iter().all(Option::is_none) {
            return Ok(None);
        }
        let located = LocatedRun {
            first,
            asked: Self::asked_within(read.bytes(), &blocks),
            sources,
        };
        Ok(Some(HeldRun {
            located,
            blocks,
            remote,
        }))
    }

    /// Falls back to the default's GETs: a policy's mistake, wherever it ran, costs a read nothing.
    pub(in crate::cache) fn take_gets(
        &mut self,
        read: &mut PartialRead,
        held: HeldRun,
        gets: &ReadPlan,
    ) {
        let cache = self.cache;
        let HeldRun {
            located: run,
            blocks,
            mut remote,
        } = held;
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
        for block in &blocks {
            let at = (block.key().index() - first) as usize;
            if let Some(RemoteBlock::InFlight(fetch)) = remote[at].take() {
                self.waits.push((*block, fetch));
            }
        }
    }

    fn take_memory_hit(
        &self,
        read: &mut PartialRead,
        block: &PlannedBlock,
        reader: &MemoryChunkReader<'_>,
    ) {
        let context = self.context(block.key(), true);
        self.cache
            .tiers
            .placement
            .on_memory_hit(reader.chunk(), &context);
        read.place_from_chunk(block, reader.bytes());
    }

    fn take_disk_hit(
        &mut self,
        read: &mut PartialRead,
        block: &PlannedBlock,
        reader: DiskChunkReader<'c>,
    ) -> Result<()> {
        let context = self.context(block.key(), true);
        match self
            .cache
            .tiers
            .placement
            .on_disk_hit(reader.chunk(), &context)
        {
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
            &self.cache.clock,
        )
    }

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

    fn asked_within(bytes: Range<u64>, blocks: &[PlannedBlock]) -> Range<u64> {
        let start = blocks[0].object_range().start;
        let end = blocks[blocks.len() - 1].object_range().end;
        bytes.start.max(start)..bytes.end.min(end)
    }

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

    /// Awaited together, so each reports its end as its bytes land, not after those before it.
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

/// A located run and the claims on its remote blocks, held until its GETs are taken.
pub(in crate::cache) struct HeldRun {
    located: LocatedRun,
    blocks: Vec<PlannedBlock>,
    remote: Vec<Option<RemoteBlock>>,
}

impl HeldRun {
    pub(in crate::cache) fn run(&self) -> &LocatedRun {
        &self.located
    }
}

enum RemoteBlock {
    Fetched {
        admit: Admit,
        claim: Option<FetchClaim>,
    },
    InFlight(Arc<BlockFetch>),
}

impl RemoteBlock {
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
