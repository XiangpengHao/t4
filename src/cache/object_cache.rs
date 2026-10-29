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

use std::collections::VecDeque;
use std::future::Future;
use std::num::{NonZeroU8, NonZeroU32};
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, join, join_all};

use super::block_index::{
    BlockTable, FileVersion, Location, PathTable, PlannedBlock, UncacheableObject,
};
use super::cache_files::CacheFiles;
use super::chunk_pool::disk_chunks::{DiskChunkReader, DiskChunkWriter};
use super::chunk_pool::memory_pool::{
    LentMemoryChunk, MemoryChunkReader, MemoryChunkWriter, MemoryPool,
};
use super::in_flight::{BlockFetch, FetchClaim, FetchEnd, InFlight};
use super::io_report::IoReport;
use super::policy::{
    Admit, BlockRead, BlockSource, DiskHit, FetchRuns, Io, LocatedRead, MemoryVictim,
    PlacementPolicy, PlannedGet, ReadIntent, ReadPlan, ReadPolicy, Tier, VictimAction,
};
use super::read_output::ReadOutput;
use super::sharing::{FrameReader, FrameWriter};
use super::{BLOCK_SIZE_U64, BlockKey, ChunkId, FileVersionId};
use crate::PAGE_SIZE_U32;
use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

/// A cache of file versions' blocks in memory and disk chunks, found
/// through the index, placed and evicted by its policy.
pub(super) struct ObjectCache {
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
    /// The most victims a full tier gives up at once: one eviction makes
    /// room for the insertions after it too.
    const EVICTION_BATCH: usize = 8;

    /// How many times a full tier evicts for one insertion before the block
    /// goes uncached: a victim's chunk goes free only once its readers have
    /// left.
    const EVICTION_ROUNDS: usize = 4;

    /// A cache of `memory_chunks` chunks of memory and the chunks of
    /// `disk`, all free, holding nothing: its blocks placed by `placement`,
    /// which is built for both counts.
    pub(super) fn new(
        memory_chunks: NonZeroU32,
        disk: CacheFiles,
        placement: Arc<dyn PlacementPolicy>,
    ) -> Result<Self> {
        Ok(Self {
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: Arc::new(MemoryPool::new(memory_chunks)?),
            disk,
            placement,
            in_flight: Arc::default(),
        })
    }

    /// The file at `path`, the version of it with `e_tag` and `size`, as
    /// reads name it; or why the cache can't hold the file, whose reads then
    /// go to the object store alone.
    pub(super) fn resolve<'p>(
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
    pub(super) fn start_read<'c>(
        &'c self,
        file: &CacheableFile<'c>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
    ) -> Result<(PartialRead, TierWork<'c>)> {
        let plan = self.blocks.plan_read(file.version, range.clone());
        let read = PartialRead::new(file.version, plan.bytes())?;
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
    pub(super) fn retry<'c>(
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
    pub(super) fn start_planned_read<'c>(
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
    pub(super) fn retry_planned<'c>(
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
    pub(super) async fn read(
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

    /// Where `block` is, for a read of `context`: held in a tier, being
    /// fetched by another read, or remote, claimed for the read's own fetch.
    fn locate(&self, block: &PlannedBlock, context: &BlockRead<'_>) -> Located<'_> {
        let key = block.key();
        let mut location = block.location();
        loop {
            match self.hold(key, location) {
                HeldBlock::Memory(reader) => return Located::Memory(reader),
                HeldBlock::Disk(reader) => return Located::Disk(reader),
                HeldBlock::Remote => {}
            }
            if let Some(fetch) = self.in_flight.get(key) {
                return Located::InFlight(fetch);
            }
            let admit = self.placement.on_remote_read(context);
            let claim = match self.in_flight.claim(key, admit) {
                Ok(claim) => claim,
                Err(fetch) => return Located::InFlight(fetch),
            };
            // A fetch may have cached the block since the index said it was
            // remote, where the index names it now: not the entry just found
            // stale, a chunk an eviction closed. The claim ends with the
            // block there, for any read that joined it meanwhile, and this
            // read takes it from there too.
            match self.blocks.get(key) {
                Some(cached) if Some(cached) != location => {
                    claim.ended(Some(cached));
                    location = Some(cached);
                }
                _ => {
                    return Located::Remote {
                        admit,
                        claim: Some(claim),
                    };
                }
            }
        }
    }

    /// What becomes of block `index` of `file`, fetched by a GET racing
    /// `race`, widened past the read of `range`: dropped if a tier holds it
    /// or another read is fetching it, else placed as the placement policy
    /// says, and claimed for the GET.
    fn widened_block(
        &self,
        file: &CacheableFile<'_>,
        range: &Range<u64>,
        intent: Option<ReadIntent>,
        index: u32,
        race: NonZeroU8,
    ) -> FetchBlockPlan {
        let key = BlockKey::new(file.version.id(), index);
        if self.blocks.get(key).is_some() || self.in_flight.get(key).is_some() {
            return FetchBlockPlan::dropped();
        }
        let context = BlockRead::new(
            key,
            file.version.size(),
            range.clone(),
            file.path,
            intent,
            false,
        );
        let admit = self.placement.on_remote_read(&context);
        match self.in_flight.claim(key, admit) {
            Err(_) => FetchBlockPlan::dropped(),
            Ok(mut claim) => match self.blocks.get(key) {
                Some(cached) => {
                    claim.ended(Some(cached));
                    FetchBlockPlan::dropped()
                }
                None => {
                    claim.fetched_by(race);
                    FetchBlockPlan::new(BlockFate::Widened(admit), Some(claim))
                }
            },
        }
    }

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

    /// Takes `fetch`, the bytes of one of `read`'s GETs, as they come in
    /// ([`FetchedBlocks`]): each block the read asked for into its output,
    /// over the pages of it the block covers, and each block whole into its
    /// place in the cache, where the placement policy said it goes; the
    /// bytes of blocks the cache holds already, or another read is fetching,
    /// dropped. Each fetch is taken once.
    ///
    /// # Errors
    ///
    /// If the output's pages can't be lent.
    ///
    /// # Panics
    ///
    /// If `fetch` isn't the bytes of one of the read's GETs not taken yet.
    pub(super) fn receive(
        &self,
        read: &mut PartialRead,
        fetch: Range<u64>,
    ) -> Result<FetchedBlocks<'_>> {
        let at = read
            .fetches
            .iter()
            .position(|planned| planned.bytes == fetch)
            .expect("a fetch is one the read planned, taken once");
        let PlannedFetch {
            race,
            late,
            first,
            blocks: planned,
            ..
        } = read.fetches.remove(at);
        let mut blocks = Vec::with_capacity(planned.len());
        for (index, FetchBlockPlan { fate, claim }) in (first..).zip(planned) {
            let (landing, output) = match fate {
                BlockFate::Asked(admit) => (
                    Landing::Due(admit),
                    read.lend_for_block(read.version.block_range(index))?,
                ),
                BlockFate::Widened(admit) => (Landing::Due(admit), None),
                BlockFate::Dropped => (Landing::Nowhere, None),
            };
            blocks.push(FetchedBlock {
                landing,
                output,
                claim,
            });
        }
        Ok(FetchedBlocks {
            cache: self,
            version: read.version,
            range: fetch,
            race,
            late,
            received: 0,
            first,
            blocks,
        })
    }

    /// Takes `fetched`, the file's bytes of `fetch`, in one piece, as a
    /// fetch whose answer comes in one does: for tests, which have the
    /// bytes at hand.
    #[cfg(test)]
    pub(super) async fn take_fetched(
        &self,
        read: &mut PartialRead,
        fetch: Range<u64>,
        fetched: &[u8],
    ) -> Result<()> {
        let mut blocks = self.receive(read, fetch)?;
        blocks.land(fetched).await?;
        blocks.finish().await
    }

    /// Where `key`'s block, `len` bytes, lands as it is fetched: a chunk of
    /// the tier `admit` names, lent to take its bytes; a buffer, for the
    /// reads waiting on its fetch, if the policy discards it. Nowhere if a
    /// tier holds it already, or that tier has no room even after evicting.
    async fn landing(&self, key: BlockKey, admit: Admit, len: usize) -> Landing<'_> {
        if self.blocks.get(key).is_some() {
            return Landing::Nowhere;
        }
        let pages = u32::try_from(len.next_multiple_of(PAGE_SIZE_U32 as usize))
            .ok()
            .and_then(NonZeroU32::new)
            .expect("a block is a chunk's bytes or fewer, one or more");
        match admit {
            Admit::Memory => match self.allocate_memory().await {
                Ok(Some(writer)) => {
                    let (chunk, lent) = writer.lend_for_read(&self.memory, pages);
                    Landing::Memory { chunk, lent }
                }
                // No room, or an eviction's write failed: the block goes
                // uncached, as one the cache fails to keep.
                Ok(None) | Err(_) => Landing::Nowhere,
            },
            Admit::Disk => {
                let Some(writer) = self.allocate_disk() else {
                    return Landing::Nowhere;
                };
                let hold = writer.hold_for_write(self.disk.chunks());
                match AlignedBuf::new_zeroed(pages) {
                    Ok(pages) => Landing::Disk {
                        pages: pages.holding(Arc::new(hold)),
                        writer,
                    },
                    Err(_) => Landing::Nowhere,
                }
            }
            Admit::Discard => Landing::Passing(Vec::with_capacity(len)),
        }
    }

    /// Caches `key`'s block where it landed, all of it in: publishes its
    /// memory chunk, or writes its disk chunk and then publishes that. A
    /// block that a tier came to hold meanwhile, or whose write fails, goes
    /// uncached.
    async fn cache_landed(&self, key: BlockKey, landing: Landing<'_>) {
        match landing {
            Landing::Memory { chunk, lent } => {
                // Published before the index names it, so that a read that
                // finds the entry finds the chunk open.
                let reader = lent.publish(chunk, key);
                if self
                    .blocks
                    .insert_if_absent(key, Location::Memory(reader.chunk()))
                {
                    self.placement.on_admit(Tier::Memory, reader.chunk(), key);
                } else {
                    // Another fetch of the block cached it first. No entry
                    // names this chunk: it goes back when `reader` drops.
                    reader.unpublish();
                }
            }
            Landing::Disk { pages, writer } => {
                if self.disk.write(&writer, pages).await.is_err() {
                    return;
                }
                let reader = writer.publish(key);
                if self
                    .blocks
                    .insert_if_absent(key, Location::Disk(reader.chunk()))
                {
                    self.placement.on_admit(Tier::Disk, reader.chunk(), key);
                } else {
                    reader.unpublish();
                }
            }
            Landing::Due(_) | Landing::Passing(_) | Landing::Nowhere => {}
        }
    }

    /// Moves `block`, in disk chunk `on_disk`, to a memory chunk: reads its
    /// pages into one, a read `reads` hears of, then points its entry at
    /// it, and closes the disk chunk, which goes free when its last reader
    /// leaves. Gives the disk chunk back if memory has no room even after
    /// evicting.
    ///
    /// The read fills a lent memory chunk, which no reader sees until it is
    /// done. If the block left disk another way meanwhile, the memory copy
    /// still serves the caller, and goes back after it.
    async fn promote<'c>(
        &'c self,
        on_disk: DiskChunkReader<'c>,
        block: &PlannedBlock,
        reads: &dyn ReadPolicy,
    ) -> Result<Promotion<'c>> {
        let Some(writer) = self.allocate_memory().await? else {
            return Ok(Promotion::NoRoom(on_disk));
        };
        let range = block.object_range();
        let pages = u32::try_from(range.end - range.start)
            .ok()
            .and_then(|len| NonZeroU32::new(len.next_multiple_of(PAGE_SIZE_U32)))
            .expect("a block is a chunk's bytes or fewer, one or more");
        let (buffer, lent) = writer.lend_for_read(&self.memory, pages);
        let report = IoReport::start(
            reads,
            Io::DiskRead {
                drive: on_disk.position().file(),
                bytes: pages.get(),
            },
        );
        let filled = report
            .awaiting(self.disk.read_chunk_start(&on_disk, buffer))
            .await?;
        let block = on_disk.block();
        let in_memory = lent.publish(filled, block);
        let (from, to) = (
            Location::Disk(on_disk.chunk()),
            Location::Memory(in_memory.chunk()),
        );
        if self.blocks.replace_if(block, from, to) {
            on_disk.unpublish();
            self.placement
                .on_admit(Tier::Memory, in_memory.chunk(), block);
        } else {
            in_memory.unpublish();
        }
        Ok(Promotion::InMemory(in_memory))
    }

    /// A free memory chunk, evicting the policy's victims for one if memory
    /// is full; `None` if none goes free, or the policy picks none.
    async fn allocate_memory(&self) -> Result<Option<MemoryChunkWriter<'_>>> {
        for _ in 0..Self::EVICTION_ROUNDS {
            if let Some(writer) = self.memory.allocate() {
                return Ok(Some(writer));
            }
            let batch = Self::eviction_batch(self.memory.chunk_count());
            let victims = self.placement.pick_memory_victims(batch);
            if victims.is_empty() {
                return Ok(None);
            }
            self.evict_memory_victims(&victims).await?;
        }
        Ok(self.memory.allocate())
    }

    /// [`Self::allocate_memory`] for a disk chunk. Disk victims are dropped:
    /// no I/O.
    fn allocate_disk(&self) -> Option<DiskChunkWriter<'_>> {
        for _ in 0..Self::EVICTION_ROUNDS {
            if let Some(writer) = self.disk.chunks().allocate() {
                return Some(writer);
            }
            let batch = Self::eviction_batch(self.disk.chunks().chunk_count());
            let victims = self.placement.pick_disk_victims(batch);
            if victims.is_empty() {
                return None;
            }
            for chunk in victims {
                self.evict_from_disk(chunk);
            }
        }
        self.disk.chunks().allocate()
    }

    /// The victims a full tier of `chunk_count` chunks gives up at once:
    /// [`Self::EVICTION_BATCH`], but never more than a sixteenth of the tier,
    /// so a small one isn't emptied for one block.
    fn eviction_batch(chunk_count: u32) -> usize {
        (chunk_count as usize / 16).clamp(1, Self::EVICTION_BATCH)
    }

    /// Drops or moves each of `victims` as the policy said. The moves' writes
    /// run at once.
    async fn evict_memory_victims(&self, victims: &[MemoryVictim]) -> Result<()> {
        let mut moves = Vec::new();
        for victim in victims {
            match victim.action() {
                VictimAction::Drop => {
                    self.evict_from_memory(victim.chunk());
                }
                VictimAction::ToDisk => moves.push(self.move_to_disk(victim.chunk())),
            }
        }
        for moved in join_all(moves).await {
            moved?;
        }
        Ok(())
    }

    /// Moves the block in memory chunk `chunk` to a disk chunk, evicting the
    /// disk's victims for one if the disk is full: writes the whole chunk
    /// there, a short block's slack too (no read looks past a block's
    /// bytes), then points the block's entry at the disk chunk, and closes
    /// the memory chunk, which goes free when its last reader leaves.
    /// Readers find the block in memory until the entry moves. A block that
    /// stays, for want of room on disk, a failed write, or a caller who gave
    /// up on the move, goes back to the policy.
    ///
    /// The write lends the memory chunk's pages, not a copy, and holds both
    /// chunks until it is done, even if this future is dropped first.
    async fn move_to_disk(&self, chunk: ChunkId) -> Result<MoveToDisk> {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return Ok(MoveToDisk::NoBlock);
        };
        let block = victim.block();
        let picked = PickedVictim::new(self.placement.as_ref(), chunk, block);
        let Some(writer) = self.allocate_disk() else {
            return Ok(MoveToDisk::DiskFull);
        };
        let hold = writer.hold_for_write(self.disk.chunks());
        let pages = victim.lend_for_write(&self.memory, hold);
        self.disk.write(&writer, pages).await?;
        picked.settle();
        let on_disk = writer.publish(block);
        let to_disk = Location::Disk(on_disk.chunk());
        if self
            .blocks
            .replace_if(block, Location::Memory(chunk), to_disk)
        {
            // New reads go to disk. Those that found the old entry finish in
            // memory, and the chunk goes free after them.
            victim.unpublish();
            self.placement.on_admit(Tier::Disk, on_disk.chunk(), block);
            Ok(MoveToDisk::Moved)
        } else {
            // The block left memory another way while it was written: the
            // copy on disk goes back.
            on_disk.unpublish();
            Ok(MoveToDisk::Superseded)
        }
    }

    /// Drops the block in memory chunk `chunk` from the cache: closes the
    /// chunk to new readers, then drops the index entry that names it. The
    /// chunk goes free when its last reader leaves. Whether this call
    /// evicted the block: of two evictions of one chunk, one does, and a
    /// free chunk has none.
    fn evict_from_memory(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Memory(chunk))
    }

    /// [`Self::evict_from_memory`] for disk chunk `chunk`: the chunk goes
    /// free once the reads in flight from it are done.
    fn evict_from_disk(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.disk.chunks().pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Disk(chunk))
    }

    /// Drops `block`'s entry if it names `location`, the chunk there just
    /// `closed` by this eviction; whether it was. Closed first, so a read
    /// that finds the entry from then on finds the chunk closed, and takes
    /// the block as remote.
    fn forget_if_closed(&self, closed: bool, block: BlockKey, location: Location) -> bool {
        if closed {
            self.blocks.remove_if(block, location);
        }
        closed
    }

    /// Whether the index and both tiers agree, as they do once every read,
    /// fill and eviction is done, and the I/O each started: no block in
    /// flight, each entry names
    /// a chunk that holds its block, each chunk that holds a block has an
    /// entry naming it, and every other chunk is free. If so, the chunks
    /// that hold a block, each with its tier; `Err` says where they don't.
    #[cfg(any(test, feature = "__fuzz"))]
    pub(super) fn check_at_rest(&self) -> std::result::Result<Vec<(Tier, ChunkId)>, String> {
        if !self.in_flight.is_empty() {
            return Err("a block is still in flight".to_owned());
        }
        let mut memory =
            (self.memory.blocks_at_rest()).map_err(|error| format!("memory {error}"))?;
        let mut disk =
            (self.disk.chunks().blocks_at_rest()).map_err(|error| format!("disk {error}"))?;
        let mut in_use = Vec::new();
        for (block, location) in self.blocks.entries() {
            let (chunks, tier, chunk) = match location {
                Location::Memory(chunk) => (&mut memory, Tier::Memory, chunk),
                Location::Disk(chunk) => (&mut disk, Tier::Disk, chunk),
            };
            in_use.push((tier, chunk));
            // Taken, so that a second entry naming the chunk finds none.
            let held = chunks
                .get_mut(chunk.index() as usize)
                .and_then(Option::take);
            if held != Some(block) {
                return Err(format!(
                    "{block:?} is at {location:?}, which holds {held:?}"
                ));
            }
        }
        for (tier, chunks) in [("memory", memory), ("disk", disk)] {
            if let Some((index, block)) = (0..)
                .zip(chunks)
                .find_map(|(index, block)| Some((index, block?)))
            {
                return Err(format!(
                    "{tier} chunk {index} holds {block:?}, which no entry names"
                ));
            }
        }
        Ok(in_use)
    }

    /// Holds the chunk with `key`'s block, the index having said it is at
    /// `location`.
    ///
    /// An entry may name a chunk that no longer holds the block: eviction
    /// closed the chunk, and is about to drop the entry, or moved the block
    /// to the other tier, or the chunk already holds another block. So the
    /// index is read again, until it names the chunk that holds the block,
    /// or no change.
    fn hold(&self, key: BlockKey, mut location: Option<Location>) -> HeldBlock<'_> {
        loop {
            let held = match location {
                Some(Location::Memory(chunk)) => {
                    self.memory.read(chunk, key).map(HeldBlock::Memory)
                }
                Some(Location::Disk(chunk)) => {
                    self.disk.chunks().read(chunk, key).map(HeldBlock::Disk)
                }
                None => return HeldBlock::Remote,
            };
            if let Some(held) = held {
                return held;
            }
            let again = self.blocks.get(key);
            if again == location {
                return HeldBlock::Remote;
            }
            location = again;
        }
    }
}

/// A file the cache can hold: its path, and the version of it reads name.
#[derive(Clone, Copy, Debug)]
pub(super) struct CacheableFile<'p> {
    path: &'p [u8],
    version: FileVersion,
}

impl<'p> CacheableFile<'p> {
    /// The file at `path`, its version as an owner named it to its client
    /// (`doc/11-shared-mode.md`).
    pub(super) fn named(path: &'p [u8], version: FileVersion) -> Self {
        Self { path, version }
    }

    pub(super) fn version(&self) -> FileVersion {
        self.version
    }
}

/// A block a read holds while it takes its bytes, or found remote: in
/// neither memory nor disk.
enum HeldBlock<'cache> {
    Memory(MemoryChunkReader<'cache>),
    Disk(DiskChunkReader<'cache>),
    Remote,
}

/// What became of a promotion.
enum Promotion<'cache> {
    /// The block is in this memory chunk, which holds it for the read.
    InMemory(MemoryChunkReader<'cache>),
    /// Memory had no room: the block is in this disk chunk still.
    NoRoom(DiskChunkReader<'cache>),
}

/// A memory victim being moved to disk. The policy forgot its chunk when it
/// picked it, so a move that leaves the block in memory, for want of room
/// on disk, a failed write, or a dropped future, must give the chunk back,
/// or the policy never picks it again. Dropping this gives it back, unless
/// the move [settled](Self::settle) first.
struct PickedVictim<'c> {
    policy: &'c dyn PlacementPolicy,
    chunk: ChunkId,
    block: BlockKey,
    settled: bool,
}

impl<'c> PickedVictim<'c> {
    fn new(policy: &'c dyn PlacementPolicy, chunk: ChunkId, block: BlockKey) -> Self {
        Self {
            policy,
            chunk,
            block,
            settled: false,
        }
    }

    /// The block's copy is written: the block moves to disk, which the
    /// policy hears of, or it left memory another way meanwhile. Either way,
    /// its memory chunk doesn't go back to the policy.
    fn settle(mut self) {
        self.settled = true;
    }
}

impl Drop for PickedVictim<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.policy.on_admit(Tier::Memory, self.chunk, self.block);
        }
    }
}

/// What became of a move to disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MoveToDisk {
    /// The block is on disk, and its memory chunk goes free.
    Moved,
    /// The memory chunk held no block, or another eviction had closed it.
    NoBlock,
    /// No disk chunk went free: the block stays in memory.
    DiskFull,
    /// The block left memory another way while it was written, and the
    /// copy on disk went back.
    Superseded,
}

/// A read's work on the tiers once it is started
/// ([`ObjectCache::start_read`]): the runs of its blocks it locates, one at
/// a time; its page reads, submitted as they are located, the blocks it
/// moves to memory, and those it waits for another read to fetch. Its
/// caller awaits it beside the read's GETs.
pub(super) struct TierWork<'c> {
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
    fn new(
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
    pub(super) fn locate_next_run(&mut self, read: &mut PartialRead) -> Result<Option<LocatedRun>> {
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
    pub(super) fn take_gets(&mut self, read: &mut PartialRead, gets: &ReadPlan) {
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
    pub(super) async fn finish(mut self, read: &mut PartialRead) -> Result<BlocksToFetch> {
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

/// A read that the cache served in part: the bytes it takes, the output they
/// land in, and the GETs that fetch the rest, as the read policy planned
/// them, with what becomes of each block they fetch.
#[derive(Debug)]
pub(super) struct PartialRead {
    version: FileVersion,
    /// Where the read's bytes land, the caller's once filled.
    output: ReadOutput,
    fetches: Vec<PlannedFetch>,
}

impl PartialRead {
    fn new(version: FileVersion, bytes: Range<u64>) -> Result<Self> {
        Ok(Self {
            version,
            output: ReadOutput::new(bytes)?,
            fetches: Vec::new(),
        })
    }

    /// The file's bytes the read takes, which its output holds.
    pub(super) fn bytes(&self) -> Range<u64> {
        self.output.asked()
    }

    /// The file's bytes each GET fetches, in the order planned.
    pub(super) fn fetches(&self) -> Vec<Range<u64>> {
        self.fetches
            .iter()
            .map(|planned| planned.bytes.clone())
            .collect()
    }

    /// The read's output over the pages of `block`, the file's bytes of a
    /// block it asked for, lent for the block's fetched bytes to land in;
    /// `None` if the output covers none of them.
    fn lend_for_block(&mut self, block: Range<u64>) -> Result<Option<FetchedOutput>> {
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
    pub(super) fn into_bytes(self) -> Option<Bytes> {
        self.output.into_bytes()
    }

    /// Copies the part of `block` the read takes from `chunk`, the whole of
    /// the chunk that holds it, into its output.
    fn place_from_chunk(&mut self, block: &PlannedBlock, chunk: &[u8]) {
        let wanted = block.window().bytes();
        let at = block.object_range().start + u64::from(wanted.start);
        self.output
            .copy_in(at, &chunk[wanted.start as usize..wanted.end as usize]);
    }

    /// A length or an offset within a read's bytes, or a fetch's, as a
    /// `usize`: the bytes are in memory, so it fits.
    fn offset(within_read: u64) -> usize {
        usize::try_from(within_read).expect("a read's bytes fit in memory")
    }
}

/// One GET of a read's plan: the file's bytes it fetches, whole blocks,
/// how many requests race for them, at once and late, and what becomes of
/// each block.
#[derive(Debug)]
struct PlannedFetch {
    bytes: Range<u64>,
    race: NonZeroU8,
    late: Option<Duration>,
    first: u32,
    blocks: Vec<FetchBlockPlan>,
}

impl PlannedFetch {
    /// `get` of `version`, what becomes of its blocks `blocks`, the first
    /// block's first.
    fn new(version: &FileVersion, get: &PlannedGet, blocks: Vec<FetchBlockPlan>) -> Self {
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
struct FetchBlockPlan {
    fate: BlockFate,
    claim: Option<FetchClaim>,
}

impl FetchBlockPlan {
    fn new(fate: BlockFate, claim: Option<FetchClaim>) -> Self {
        Self { fate, claim }
    }

    /// Its bytes dropped, the block cached already or another's to fetch.
    fn dropped() -> Self {
        Self::new(BlockFate::Dropped, None)
    }
}

/// What becomes of a block a GET fetches.
#[derive(Clone, Copy, Debug)]
enum BlockFate {
    /// The read asked for it: into the read's output, and the cache, where
    /// the placement policy said.
    Asked(Admit),
    /// A GET widened past the read fetched it: into the cache alone.
    Widened(Admit),
    /// A tier holds it already, or another read is fetching it: its bytes
    /// dropped.
    Dropped,
}

/// Where a block a read needs is ([`ObjectCache::locate`]).
enum Located<'c> {
    Memory(MemoryChunkReader<'c>),
    Disk(DiskChunkReader<'c>),
    /// Another read is fetching it.
    InFlight(Arc<BlockFetch>),
    /// No tier holds it, nor is another read fetching it: the read fetches
    /// it, claimed if it is to be cached.
    Remote {
        admit: Admit,
        claim: Option<FetchClaim>,
    },
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

/// The blocks a read waited for that no fetch cached, or that left the
/// cache before the read took them ([`TierWork::finish`]): the read fetches
/// those itself, its work on the tiers made again for them
/// ([`ObjectCache::retry`]). Its caller learns only whether any is left;
/// where each block was stays with the cache, which finds it again.
#[derive(Debug)]
pub(super) struct BlocksToFetch(Vec<PlannedBlock>);

impl BlocksToFetch {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}

/// A run of a read's adjacent blocks that the tiers don't all hold, located
/// ([`TierWork::locate_next_run`]): where each block is, which its caller's
/// read policy plans the run's GETs by ([`Self::planned_by`]). Data alone,
/// so that a client's crosses the socket (`doc/11-shared-mode.md`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LocatedRun {
    /// The number of the run's first block.
    first: u32,
    /// The file's bytes the read asked for within the run.
    asked: Range<u64>,
    /// Where each block is, the run's first block's first.
    sources: Vec<BlockSource>,
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

impl LocatedRun {
    /// A run of `sources`, from block `first`, the read's `asked` bytes of
    /// it: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn new(first: u32, asked: Range<u64>, sources: Vec<BlockSource>) -> Self {
        Self {
            first,
            asked,
            sources,
        }
    }

    /// Writes the run into `frame`, for a client to plan its GETs by.
    pub(super) fn write_to(&self, frame: &mut FrameWriter) {
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
    pub(super) fn read_from(frame: &mut FrameReader) -> Result<Self> {
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
    /// ([`TierWork::take_gets`]).
    pub(super) fn planned_by(
        &self,
        policy: &dyn ReadPolicy,
        file: &CacheableFile<'_>,
        intent: Option<ReadIntent>,
    ) -> ReadPlan {
        policy.plan(&self.located(file, intent))
    }

    /// The run as a read policy sees it, of a read of `file` for `intent`.
    fn located<'r>(
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

/// A fetch's bytes as they come in ([`ObjectCache::receive`]), each put
/// where it goes: into the read's output, over the pages of it the fetch
/// covers; and each block's into its place in the cache, which it takes as
/// the block's first byte comes. [`Self::finish`] caches the blocks once
/// every byte is in. Dropped before, as when the fetch fails, it caches
/// nothing, and gives the chunks it took back.
///
/// Each byte is copied twice: into the output, and into the cache. The
/// output is the caller's, never the cache's memory.
pub(super) struct FetchedBlocks<'c> {
    cache: &'c ObjectCache,
    version: FileVersion,
    /// The file's bytes the fetch holds: whole blocks, from a block's start.
    range: Range<u64>,
    race: NonZeroU8,
    /// When one more request races the fetch, if it hasn't finished.
    late: Option<Duration>,
    /// How many of them have come in.
    received: u64,
    /// The first block's number.
    first: u32,
    /// Where each block's bytes go, the first block's first.
    blocks: Vec<FetchedBlock<'c>>,
}

/// Where one fetched block's bytes go: its place in the cache, and the
/// read's output over its pages, if the read asked for it; and the requests
/// the read holds in its fetch, if it is to be cached.
struct FetchedBlock<'c> {
    landing: Landing<'c>,
    output: Option<FetchedOutput>,
    claim: Option<FetchClaim>,
}

impl FetchedBlocks<'_> {
    /// The file's bytes the fetch holds.
    pub(super) fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// The version of the file they are of.
    pub(super) fn file(&self) -> FileVersionId {
        self.version.id()
    }

    /// How many identical requests race for them.
    pub(super) fn race(&self) -> NonZeroU8 {
        self.race
    }

    /// When one more request races them, if they aren't all in.
    pub(super) fn late_race(&self) -> Option<Duration> {
        self.late
    }

    /// Takes `piece`, the fetch's next bytes.
    ///
    /// # Errors
    ///
    /// If the fetch brings more bytes than it holds.
    pub(super) async fn land(&mut self, mut piece: &[u8]) -> Result<()> {
        let len = self.range.end - self.range.start;
        if self.received + piece.len() as u64 > len {
            return Err(Self::wrong_length(&self.range, "more"));
        }
        while !piece.is_empty() {
            let index = PartialRead::offset(self.received / BLOCK_SIZE_U64);
            let within = PartialRead::offset(self.received % BLOCK_SIZE_U64);
            if within == 0 {
                self.decide_landing(index).await;
            }
            let taken = piece.len().min(self.block_len(index) - within);
            let at = self.range.start + self.received;
            let block = &mut self.blocks[index];
            if let Some(output) = &mut block.output {
                output.copy_in(at, &piece[..taken]);
            }
            block.landing.copy_in(within, &piece[..taken]);
            piece = &piece[taken..];
            self.received += taken as u64;
        }
        Ok(())
    }

    /// Caches the blocks where they landed, once every byte is in, and ends
    /// the read's requests in their fetches.
    ///
    /// # Errors
    ///
    /// If the fetch brought fewer bytes than it holds: nothing is cached.
    pub(super) async fn finish(self) -> Result<()> {
        if self.received != self.range.end - self.range.start {
            return Err(Self::wrong_length(&self.range, "fewer"));
        }
        let Self {
            cache,
            version,
            first,
            blocks,
            ..
        } = self;
        let cached = (first..).zip(blocks).map(|(index, block)| async move {
            let FetchedBlock {
                landing,
                output,
                claim,
            } = block;
            // The read's output over the block is whole: the read may take
            // its bytes.
            drop(output);
            let key = BlockKey::new(version.id(), index);
            let passed = match landing {
                Landing::Passing(bytes) => Some(Bytes::from(bytes)),
                landing => {
                    cache.cache_landed(key, landing).await;
                    None
                }
            };
            // The fetch ends with the block where a tier holds it, whether
            // this fetch or another put it there, or, one no tier keeps, with
            // its bytes, for the reads waiting.
            if let Some(claim) = claim {
                match passed {
                    Some(bytes) => claim.passed(bytes),
                    None => claim.ended(cache.blocks.get(key)),
                }
            }
        });
        join_all(cached).await;
        Ok(())
    }

    /// Decides where block `index`, its first byte come, lands.
    async fn decide_landing(&mut self, index: usize) {
        if let Landing::Due(admit) = self.blocks[index].landing {
            let number = self.first + u32::try_from(index).expect("a fetch's blocks fit a u32");
            let key = BlockKey::new(self.version.id(), number);
            let landing = self.cache.landing(key, admit, self.block_len(index)).await;
            self.blocks[index].landing = landing;
        }
    }

    /// Block `index`'s bytes: a block's, but for the file's last, which may
    /// be shorter.
    fn block_len(&self, index: usize) -> usize {
        let start = index as u64 * BLOCK_SIZE_U64;
        PartialRead::offset((self.range.end - self.range.start - start).min(BLOCK_SIZE_U64))
    }

    fn wrong_length(range: &Range<u64>, more_or_fewer: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("the fetch of {range:?} brought {more_or_fewer} bytes than it holds"),
        ))
    }
}

/// The pages of a read's output a fetch covers, lent to it, and the file's
/// byte they start at.
struct FetchedOutput {
    pages: AlignedBuf,
    start: u64,
}

impl FetchedOutput {
    /// Copies what of `bytes`, the file's from `at`, falls in these pages.
    fn copy_in(&mut self, at: u64, bytes: &[u8]) {
        let end = self.start + self.pages.len() as u64;
        let overlap = at.max(self.start)..(at + bytes.len() as u64).min(end);
        if overlap.is_empty() {
            return;
        }
        let from = PartialRead::offset(overlap.start - at);
        let len = PartialRead::offset(overlap.end - overlap.start);
        self.pages.copy_in(
            PartialRead::offset(overlap.start - self.start),
            &bytes[from..from + len],
        );
    }
}

/// Where one fetched block's bytes land.
enum Landing<'c> {
    /// Not yet decided: where the read said the block goes.
    Due(Admit),
    /// A memory chunk, lent as a buffer until the block is in.
    Memory {
        chunk: AlignedBuf,
        lent: LentMemoryChunk<'c>,
    },
    /// A buffer the write to a disk chunk takes once the block is in.
    Disk {
        pages: AlignedBuf,
        writer: DiskChunkWriter<'c>,
    },
    /// A buffer the bytes of a block no tier keeps collect in, for the
    /// reads waiting on its fetch to take theirs from; the block goes
    /// uncached.
    Passing(Vec<u8>),
    /// Nowhere: the block goes uncached.
    Nowhere,
}

impl Landing<'_> {
    /// Copies `bytes` into the block's place from its byte `at`. A block's
    /// bytes come in order.
    fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        match self {
            Self::Memory { chunk, .. } => chunk.copy_in(at, bytes),
            Self::Disk { pages, .. } => pages.copy_in(at, bytes),
            Self::Passing(passing) => {
                debug_assert_eq!(passing.len(), at, "a block's bytes come in order");
                passing.extend_from_slice(bytes);
            }
            Self::Due(_) | Self::Nowhere => {}
        }
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
