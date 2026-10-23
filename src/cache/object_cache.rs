//! [`ObjectCache`]: reads, fills and evictions over the index and the two
//! tiers, as its policy decides (`doc/03-cache.md`).
//!
//! - **A read** copies the blocks memory holds, and reads those on disk,
//!   moving one to memory if the policy says so. It names the rest, each
//!   with where the policy said it goes.
//! - **A fill** takes the caller's fetch of those blocks as its bytes come
//!   in, into the read's output and each block where the read said.
//! - **A tier with no free chunk** for a block evicts the victims the policy
//!   picks: memory moves its victims to disk or drops them, the disk drops
//!   its own.
//!
//! Tiers are exclusive: a block is in one at a time, and the index names it
//! there. Request coalescing comes later.

use std::future::Future;
use std::num::NonZeroU32;
use std::ops::Range;

use bytes::Bytes;
use futures_util::future::join_all;

use super::block_index::{
    BlockTable, FileVersion, Location, PathTable, PlannedBlock, UncacheableObject,
};
use super::cache_files::CacheFiles;
use super::chunk_pool::disk_chunks::{DiskChunkReader, DiskChunkWriter};
use super::chunk_pool::memory_pool::{
    LentMemoryChunk, MemoryChunkReader, MemoryChunkWriter, MemoryPool,
};
use super::policy::{Admit, BlockRead, CachePolicy, DiskHit, MemoryVictim, Tier, VictimAction};
use super::read_output::ReadOutput;
use super::{BLOCK_SIZE_U64, BlockKey, ChunkId};
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
    policy: Arc<dyn CachePolicy>,
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
    /// `disk`, all free, holding nothing, run by `policy`, which is built
    /// for both counts.
    pub(super) fn new(
        memory_chunks: NonZeroU32,
        disk: CacheFiles,
        policy: Arc<dyn CachePolicy>,
    ) -> Result<Self> {
        Ok(Self {
            paths: PathTable::new(),
            blocks: BlockTable::new(),
            memory: Arc::new(MemoryPool::new(memory_chunks)?),
            disk,
            policy,
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

    /// Reads `range` of `file`, clamped to the file, into the output of the
    /// [`PartialRead`] it returns: the bytes of blocks memory holds copied
    /// in, those of blocks on disk read straight in. The read names what the
    /// caller fetches for the rest, and takes it ([`PartialRead::fill`]).
    ///
    /// The policy hears of every block: a hit in either tier, where a disk
    /// hit goes, a miss and where its block goes once fetched.
    ///
    /// # Errors
    ///
    /// If a read from the cache file fails.
    pub(super) async fn read(
        &self,
        file: &CacheableFile<'_>,
        range: Range<u64>,
    ) -> Result<PartialRead> {
        let plan = self.blocks.plan_read(file.version, range.clone());
        let mut read = PartialRead::new(file.version, plan.bytes())?;
        let mut page_reads = Vec::new();
        let mut promotions = Vec::new();
        for &block in plan.blocks() {
            let context =
                BlockRead::new(block.key(), file.version.size(), range.clone(), file.path);
            match self.hold(block.key(), block.location()) {
                HeldBlock::Memory(reader) => {
                    self.policy.on_memory_hit(reader.chunk(), &context);
                    read.place_from_chunk(&block, reader.bytes());
                }
                HeldBlock::Disk(reader) => {
                    match self.policy.on_disk_hit(reader.chunk(), &context) {
                        DiskHit::Stay => {
                            let pages = self.read_pages(&mut read, &reader, &block)?;
                            page_reads.push((reader, pages));
                        }
                        DiskHit::Promote => promotions.push((reader, block)),
                    }
                }
                HeldBlock::Missing => read.miss(&block, self.policy.on_miss(&context)),
            }
        }
        // Every page read was submitted above, so they run at once, each
        // straight into the read's output. Each keeps its chunk held until
        // its bytes land.
        for (_held, pages) in page_reads {
            pages.await?;
        }
        let promoted =
            join_all(promotions.into_iter().map(|(on_disk, block)| async move {
                (self.promote(on_disk, &block).await, block)
            }))
            .await;
        for (promotion, block) in promoted {
            match promotion? {
                Promotion::InMemory(reader) => read.place_from_chunk(&block, reader.bytes()),
                // No room in memory: the block stays, and the read takes its
                // pages after all.
                Promotion::NoRoom(on_disk) => self.read_pages(&mut read, &on_disk, &block)?.await?,
            }
        }
        Ok(read)
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

    /// Takes `fetch`, one of `read`'s fetches, as its bytes come in
    /// ([`FetchedBlocks`]): each into the read's output, over the pages of
    /// it the fetch covers, and each block whole into its place in the
    /// cache, where the read said it goes.
    ///
    /// # Errors
    ///
    /// If the output's pages can't be lent.
    ///
    /// # Panics
    ///
    /// If `fetch` isn't blocks the read missed, from a block's start.
    pub(super) fn receive(
        &self,
        read: &mut PartialRead,
        fetch: Range<u64>,
    ) -> Result<FetchedBlocks<'_>> {
        assert!(
            fetch.start.is_multiple_of(BLOCK_SIZE_U64),
            "a fetch starts at a block"
        );
        let first =
            u32::try_from(fetch.start / BLOCK_SIZE_U64).expect("a file's block numbers fit a u32");
        let count = u32::try_from((fetch.end - fetch.start).div_ceil(BLOCK_SIZE_U64))
            .expect("a file's block numbers fit a u32");
        let landings = (first..first + count)
            .map(|index| Landing::Due(read.admission(index)))
            .collect();
        Ok(FetchedBlocks {
            cache: self,
            version: read.version,
            output: read.lend_for_fetch(&fetch)?,
            range: fetch,
            received: 0,
            first,
            landings,
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
    /// the tier `admit` names, lent to take its bytes. Nowhere if the policy
    /// discards it, a tier holds it already, or that tier has no room even
    /// after evicting.
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
            Admit::Discard => Landing::Nowhere,
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
                    self.policy.on_admit(Tier::Memory, reader.chunk(), key);
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
                    self.policy.on_admit(Tier::Disk, reader.chunk(), key);
                } else {
                    reader.unpublish();
                }
            }
            Landing::Due(_) | Landing::Nowhere => {}
        }
    }

    /// Moves `block`, in disk chunk `on_disk`, to a memory chunk: reads its
    /// pages into one, then points its entry at it, and closes the disk
    /// chunk, which goes free when its last reader leaves. Gives the disk
    /// chunk back if memory has no room even after evicting.
    ///
    /// The read fills a lent memory chunk, which no reader sees until it is
    /// done. If the block left disk another way meanwhile, the memory copy
    /// still serves the caller, and goes back after it.
    async fn promote<'c>(
        &'c self,
        on_disk: DiskChunkReader<'c>,
        block: &PlannedBlock,
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
        let filled = self.disk.read_chunk_start(&on_disk, buffer).await?;
        let block = on_disk.block();
        let in_memory = lent.publish(filled, block);
        let (from, to) = (
            Location::Disk(on_disk.chunk()),
            Location::Memory(in_memory.chunk()),
        );
        if self.blocks.replace_if(block, from, to) {
            on_disk.unpublish();
            self.policy.on_admit(Tier::Memory, in_memory.chunk(), block);
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
            let victims = self.policy.pick_memory_victims(batch);
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
            let victims = self.policy.pick_disk_victims(batch);
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
        let picked = PickedVictim::new(self.policy.as_ref(), chunk, block);
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
            self.policy.on_admit(Tier::Disk, on_disk.chunk(), block);
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
    /// the block as missing.
    fn forget_if_closed(&self, closed: bool, block: BlockKey, location: Location) -> bool {
        if closed {
            self.blocks.remove_if(block, location);
        }
        closed
    }

    /// Whether the index and both tiers agree, as they do once every read,
    /// fill and eviction is done, and the I/O each started: each entry names
    /// a chunk that holds its block, each chunk that holds a block has an
    /// entry naming it, and every other chunk is free. If so, the chunks
    /// that hold a block, each with its tier; `Err` says where they don't.
    #[cfg(any(test, feature = "__fuzz"))]
    pub(super) fn check_at_rest(&self) -> std::result::Result<Vec<(Tier, ChunkId)>, String> {
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
                None => return HeldBlock::Missing,
            };
            if let Some(held) = held {
                return held;
            }
            let again = self.blocks.get(key);
            if again == location {
                return HeldBlock::Missing;
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

impl CacheableFile<'_> {
    #[cfg(test)]
    pub(super) fn version(&self) -> FileVersion {
        self.version
    }
}

/// A block a read holds while it takes its bytes, or found missing.
enum HeldBlock<'cache> {
    Memory(MemoryChunkReader<'cache>),
    Disk(DiskChunkReader<'cache>),
    Missing,
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
    policy: &'c dyn CachePolicy,
    chunk: ChunkId,
    block: BlockKey,
    settled: bool,
}

impl<'c> PickedVictim<'c> {
    fn new(policy: &'c dyn CachePolicy, chunk: ChunkId, block: BlockKey) -> Self {
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

/// A read that the cache served in part: the bytes it takes, the output they
/// land in, the ranges of the file to fetch for the rest, whole blocks,
/// adjacent ones in one, and where each block fetched goes.
#[derive(Debug)]
pub(super) struct PartialRead {
    version: FileVersion,
    /// Where the read's bytes land, the caller's once filled.
    output: ReadOutput,
    fetches: Vec<Range<u64>>,
    /// The blocks the read missed, by number, in order, and where each goes
    /// once fetched, as the policy said.
    missed: Vec<(u32, Admit)>,
}

impl PartialRead {
    fn new(version: FileVersion, bytes: Range<u64>) -> Result<Self> {
        Ok(Self {
            version,
            output: ReadOutput::new(bytes)?,
            fetches: Vec::new(),
            missed: Vec::new(),
        })
    }

    /// The file's bytes the read takes, which its output holds.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn bytes(&self) -> Range<u64> {
        self.output.asked()
    }

    /// The ranges of the file to fetch, one GET each, in order.
    pub(super) fn fetches(&self) -> &[Range<u64>] {
        &self.fetches
    }

    /// The read's output over the pages `fetch` covers, lent for the
    /// fetch's bytes to land in; `None` if it covers none.
    fn lend_for_fetch(&mut self, fetch: &Range<u64>) -> Result<Option<FetchedOutput>> {
        let Some(pages) = self.output.pages_over(fetch.clone()) else {
            return Ok(None);
        };
        Ok(Some(FetchedOutput {
            start: pages.start,
            pages: self.output.lend(pages)?,
        }))
    }

    /// The read's bytes, once the caller has filled what it missed: `None`
    /// if a read from disk it started is still writing them, as after one
    /// failed.
    pub(super) fn into_bytes(self) -> Option<Bytes> {
        self.output.into_bytes()
    }

    /// Notes that the read missed `block`, which goes where `admit` says
    /// once fetched: it joins the fetch before it if they're adjacent.
    fn miss(&mut self, block: &PlannedBlock, admit: Admit) {
        self.missed.push((block.key().index(), admit));
        let missing = block.object_range();
        match self.fetches.last_mut() {
            Some(run) if run.end == missing.start => run.end = missing.end,
            _ => self.fetches.push(missing),
        }
    }

    /// Where block `index`, which the read missed, goes once fetched.
    fn admission(&self, index: u32) -> Admit {
        let at = self
            .missed
            .binary_search_by_key(&index, |&(missed, _)| missed)
            .expect("a fetch holds blocks the read missed");
        self.missed[at].1
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
    /// How many of them have come in.
    received: u64,
    /// The read's output over the pages the fetch covers, lent.
    output: Option<FetchedOutput>,
    /// The first block's number.
    first: u32,
    /// Where each block's bytes land, the first block's first.
    landings: Vec<Landing<'c>>,
}

impl FetchedBlocks<'_> {
    /// The file's bytes the fetch holds.
    pub(super) fn range(&self) -> Range<u64> {
        self.range.clone()
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
        if let Some(output) = &mut self.output {
            output.copy_in(self.range.start + self.received, piece);
        }
        while !piece.is_empty() {
            let index = PartialRead::offset(self.received / BLOCK_SIZE_U64);
            let within = PartialRead::offset(self.received % BLOCK_SIZE_U64);
            if within == 0 {
                self.decide_landing(index).await;
            }
            let taken = piece.len().min(self.block_len(index) - within);
            self.landings[index].copy_in(within, &piece[..taken]);
            piece = &piece[taken..];
            self.received += taken as u64;
        }
        Ok(())
    }

    /// Caches the blocks where they landed, once every byte is in.
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
            landings,
            output,
            ..
        } = self;
        // The read's output is whole: the read may take its bytes.
        drop(output);
        let cached = (first..).zip(landings).map(|(index, landing)| {
            cache.cache_landed(BlockKey::new(version.id(), index), landing)
        });
        join_all(cached).await;
        Ok(())
    }

    /// Decides where block `index`, its first byte come, lands.
    async fn decide_landing(&mut self, index: usize) {
        if let Landing::Due(admit) = self.landings[index] {
            let number = self.first + u32::try_from(index).expect("a fetch's blocks fit a u32");
            let key = BlockKey::new(self.version.id(), number);
            self.landings[index] = self.cache.landing(key, admit, self.block_len(index)).await;
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
    /// Nowhere: the block goes uncached.
    Nowhere,
}

impl Landing<'_> {
    /// Copies `bytes` into the block's place from its byte `at`.
    fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        match self {
            Self::Memory { chunk, .. } => chunk.copy_in(at, bytes),
            Self::Disk { pages, .. } => pages.copy_in(at, bytes),
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
