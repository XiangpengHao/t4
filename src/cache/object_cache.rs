//! [`ObjectCache`]: reads, fills and evictions over the index and the two
//! tiers, as its policy decides (`doc/03-cache.md`).
//!
//! - **A read** copies the blocks memory holds, and reads those on disk,
//!   moving one to memory if the policy says so. It names the rest, each
//!   with where the policy said it goes.
//! - **A fill** takes the caller's fetch of those blocks, and puts each where
//!   the read said.
//! - **A tier with no free chunk** for a block evicts the victims the policy
//!   picks: memory moves its victims to disk or drops them, the disk drops
//!   its own.
//!
//! Tiers are exclusive: a block is in one at a time, and the index names it
//! there. Request coalescing comes later.

use std::num::NonZeroU32;
use std::ops::Range;

use futures_util::future::join_all;

use super::block_index::{
    BlockTable, FileVersion, Location, PathTable, PlannedBlock, UncacheableObject,
};
use super::cache_file::CacheFile;
use super::chunk_pool::disk_chunks::{DiskChunkReader, DiskChunkWriter};
use super::chunk_pool::memory_pool::{MemoryChunkReader, MemoryChunkWriter, MemoryPool};
use super::policy::{Admit, BlockRead, CachePolicy, DiskHit, MemoryVictim, Tier, VictimAction};
use super::{BLOCK_SIZE, BLOCK_SIZE_U64, BlockKey, ChunkId};
use crate::PAGE_SIZE_U32;
use crate::buffer::AlignedBuf;
use crate::io::error::Result;
use crate::io::sync::Arc;

/// A cache of file versions' blocks in memory and disk chunks, found
/// through the index, placed and evicted by its policy.
pub(super) struct ObjectCache {
    paths: PathTable,
    blocks: BlockTable,
    /// Shared with the I/O in flight that lends its chunks.
    memory: Arc<MemoryPool>,
    disk: CacheFile,
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
        disk: CacheFile,
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

    /// Reads `range` of `file` into `out`, which it sets to the bytes the
    /// read takes, the range clamped to the file: those of blocks memory
    /// holds copied in, those of blocks on disk read in, the rest zeros. The
    /// [`PartialRead`] names what the caller fetches for the rest.
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
        out: &mut Vec<u8>,
    ) -> Result<PartialRead> {
        let plan = self.blocks.plan_read(file.version, range.clone());
        let mut read = PartialRead::new(file.version, plan.bytes());
        out.clear();
        out.resize(PartialRead::offset(read.bytes.end - read.bytes.start), 0);
        let mut page_reads = Vec::new();
        let mut promotions = Vec::new();
        for &block in plan.blocks() {
            let context =
                BlockRead::new(block.key(), file.version.size(), range.clone(), file.path);
            match self.hold(block.key(), block.location()) {
                HeldBlock::Memory(reader) => {
                    self.policy.on_memory_hit(reader.chunk(), &context);
                    read.place_from_chunk(out, &block, reader.bytes());
                }
                HeldBlock::Disk(reader) => {
                    match self.policy.on_disk_hit(reader.chunk(), &context) {
                        DiskHit::Stay => {
                            let pages = self.disk.read_pages(&reader, block.window().pages())?;
                            page_reads.push((reader, pages, block));
                        }
                        DiskHit::Promote => promotions.push((reader, block)),
                    }
                }
                HeldBlock::Missing => read.miss(&block, self.policy.on_miss(&context)),
            }
        }
        // Every page read was submitted above, so they run at once. Each
        // keeps its chunk held until its bytes land.
        for (_held, pages, block) in page_reads {
            read.place_from_pages(out, &block, pages.await?.as_slice());
        }
        let promoted =
            join_all(promotions.into_iter().map(|(on_disk, block)| async move {
                (self.promote(on_disk, &block).await, block)
            }))
            .await;
        for (promotion, block) in promoted {
            match promotion? {
                Promotion::InMemory(reader) => read.place_from_chunk(out, &block, reader.bytes()),
                // No room in memory: the block stays, and the read takes its
                // pages after all.
                Promotion::NoRoom(on_disk) => {
                    let pages = self.disk.read_pages(&on_disk, block.window().pages())?;
                    read.place_from_pages(out, &block, pages.await?.as_slice());
                }
            }
        }
        Ok(read)
    }

    /// Caches the blocks that `fetched` holds, the file's bytes from
    /// `start`, each where `read`, which missed it, said it goes: whole
    /// blocks, as the read's fetches name them. A tier with no free chunk
    /// evicts for it; a block that still finds no room, or that a tier holds
    /// already, isn't cached.
    ///
    /// # Errors
    ///
    /// If a write to the cache file fails, the block's or an eviction's.
    ///
    /// # Panics
    ///
    /// If `fetched` isn't whole blocks the read missed, from a block's start.
    pub(super) async fn cache_fetched(
        &self,
        read: &PartialRead,
        start: u64,
        fetched: &[u8],
    ) -> Result<()> {
        assert!(
            start.is_multiple_of(BLOCK_SIZE_U64),
            "a fetch starts at a block"
        );
        let first =
            u32::try_from(start / BLOCK_SIZE_U64).expect("a file's block numbers fit a u32");
        for (index, block_bytes) in (first..).zip(fetched.chunks(BLOCK_SIZE)) {
            let block = read.version.block_range(index);
            assert_eq!(
                block_bytes.len() as u64,
                block.end - block.start,
                "a fetch holds whole blocks"
            );
            let key = BlockKey::new(read.version.id(), index);
            match read.admission(index) {
                Admit::Memory => self.cache_in_memory(key, block_bytes).await?,
                Admit::Disk => self.cache_on_disk(key, block_bytes).await?,
                Admit::Discard => {}
            }
        }
        Ok(())
    }

    /// Caches `bytes`, the whole of `key`'s block, in a memory chunk, unless
    /// a tier holds the block already or memory has no room even after
    /// evicting.
    async fn cache_in_memory(&self, key: BlockKey, bytes: &[u8]) -> Result<()> {
        if self.blocks.get(key).is_some() {
            return Ok(());
        }
        let Some(mut writer) = self.allocate_memory().await? else {
            return Ok(());
        };
        // A short block leaves the rest of the chunk as its last block did:
        // no read looks there.
        writer.bytes_mut()[..bytes.len()].copy_from_slice(bytes);
        // Published before the index names it, so that a read that finds the
        // entry finds the chunk open.
        let reader = writer.publish(key);
        let in_memory = Location::Memory(reader.chunk());
        if self.blocks.insert_if_absent(key, in_memory) {
            self.policy.on_admit(Tier::Memory, reader.chunk(), key);
        } else {
            // Another fetch of the block cached it first. No entry names
            // this chunk: it goes back when `reader` drops.
            reader.unpublish();
        }
        Ok(())
    }

    /// [`Self::cache_in_memory`] for a disk chunk: the block is written from
    /// a copy, which holds the chunk until the write is done.
    async fn cache_on_disk(&self, key: BlockKey, bytes: &[u8]) -> Result<()> {
        if self.blocks.get(key).is_some() {
            return Ok(());
        }
        let Some(writer) = self.allocate_disk() else {
            return Ok(());
        };
        let hold = writer.hold_for_write(self.disk.chunks());
        let pages = AlignedBuf::from_padded_slice(bytes)?.holding(Arc::new(hold));
        self.disk.write(&writer, pages).await?;
        let on_disk = writer.publish(key);
        if self
            .blocks
            .insert_if_absent(key, Location::Disk(on_disk.chunk()))
        {
            self.policy.on_admit(Tier::Disk, on_disk.chunk(), key);
        } else {
            on_disk.unpublish();
        }
        Ok(())
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
    /// stays, for want of room on disk or a failed write, goes back to the
    /// policy.
    ///
    /// The write lends the memory chunk's pages, not a copy, and holds both
    /// chunks until it is done, even if this future is dropped first.
    async fn move_to_disk(&self, chunk: ChunkId) -> Result<MoveToDisk> {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return Ok(MoveToDisk::NoBlock);
        };
        let block = victim.block();
        let Some(writer) = self.allocate_disk() else {
            self.policy.on_admit(Tier::Memory, chunk, block);
            return Ok(MoveToDisk::DiskFull);
        };
        let hold = writer.hold_for_write(self.disk.chunks());
        let pages = victim.lend_for_write(&self.memory, hold);
        if let Err(error) = self.disk.write(&writer, pages).await {
            self.policy.on_admit(Tier::Memory, chunk, block);
            return Err(error);
        }
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
    /// entry naming it, and every other chunk is free. `Err` says where they
    /// don't.
    #[cfg(any(test, feature = "__fuzz"))]
    pub(super) fn check_at_rest(&self) -> std::result::Result<(), String> {
        let mut memory =
            (self.memory.blocks_at_rest()).map_err(|error| format!("memory {error}"))?;
        let mut disk =
            (self.disk.chunks().blocks_at_rest()).map_err(|error| format!("disk {error}"))?;
        for (block, location) in self.blocks.entries() {
            let (chunks, chunk) = match location {
                Location::Memory(chunk) => (&mut memory, chunk),
                Location::Disk(chunk) => (&mut disk, chunk),
            };
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
        Ok(())
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

/// A read that the cache served in part: the bytes it takes, the ranges of
/// the file to fetch for the rest, whole blocks, adjacent ones in one, and
/// where each block fetched goes.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PartialRead {
    version: FileVersion,
    bytes: Range<u64>,
    fetches: Vec<Range<u64>>,
    /// The blocks the read missed, by number, in order, and where each goes
    /// once fetched, as the policy said.
    missed: Vec<(u32, Admit)>,
}

impl PartialRead {
    fn new(version: FileVersion, bytes: Range<u64>) -> Self {
        Self {
            version,
            bytes,
            fetches: Vec::new(),
            missed: Vec::new(),
        }
    }

    /// The file's bytes the read takes, which `out` holds.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    /// The ranges of the file to fetch, one GET each, in order.
    pub(super) fn fetches(&self) -> &[Range<u64>] {
        &self.fetches
    }

    /// Copies the part of `fetched`, the file's bytes from `start`, that the
    /// read takes into `out`, the buffer [`ObjectCache::read`] filled.
    pub(super) fn fill(&self, out: &mut [u8], start: u64, fetched: &[u8]) {
        let fetched_end = start + fetched.len() as u64;
        let overlap = self.bytes.start.max(start)..self.bytes.end.min(fetched_end);
        if overlap.is_empty() {
            return;
        }
        let len = Self::offset(overlap.end - overlap.start);
        let at = Self::offset(overlap.start - self.bytes.start);
        let from = Self::offset(overlap.start - start);
        out[at..at + len].copy_from_slice(&fetched[from..from + len]);
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
    /// the chunk that holds it, into `out`.
    fn place_from_chunk(&self, out: &mut [u8], block: &PlannedBlock, chunk: &[u8]) {
        let wanted = block.window().bytes();
        self.place(
            out,
            block,
            &chunk[wanted.start as usize..wanted.end as usize],
        );
    }

    /// Copies the part of `block` the read takes from `pages`, the block's
    /// pages around it, into `out`.
    fn place_from_pages(&self, out: &mut [u8], block: &PlannedBlock, pages: &[u8]) {
        let wanted = block.window().bytes_in_pages();
        self.place(
            out,
            block,
            &pages[wanted.start as usize..wanted.end as usize],
        );
    }

    /// Copies `wanted`, the bytes of `block` the read takes, into `out`.
    fn place(&self, out: &mut [u8], block: &PlannedBlock, wanted: &[u8]) {
        let start = block.object_range().start + u64::from(block.window().bytes().start);
        let at = Self::offset(start - self.bytes.start);
        out[at..at + wanted.len()].copy_from_slice(wanted);
    }

    /// A length or an offset within a read's bytes, as a `usize`: the
    /// bytes are in memory, so it fits.
    fn offset(within_read: u64) -> usize {
        usize::try_from(within_read).expect("a read's bytes fit in memory")
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
