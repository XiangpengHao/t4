//! A tier with no free chunk for a block evicts the victims the placement
//! policy picks: memory moves its victims to disk or drops them, the disk
//! drops its own. And a block moves from disk to memory, promoted, as the
//! policy says.

use std::num::NonZeroU32;

use futures_util::future::join_all;

use crate::PAGE_SIZE_U32;
use crate::cache::io_report::IoReport;
use crate::cache::policy::{Io, MemoryVictim, PlacementPolicy, ReadPolicy, Tier, VictimAction};
use crate::cache::storage::disk::{DiskChunkReader, DiskChunkWriter};
use crate::cache::storage::index::{Location, PlannedBlock};
use crate::cache::storage::memory::{MemoryChunkReader, MemoryChunkWriter};
use crate::cache::{BlockKey, ChunkId};
use crate::io::error::Result;

use super::ObjectCache;

impl ObjectCache {
    /// The most victims a full tier gives up at once: one eviction makes
    /// room for the insertions after it too.
    const EVICTION_BATCH: usize = 8;

    /// How many times a full tier evicts for one insertion before the block
    /// goes uncached: a victim's chunk goes free only once its readers have
    /// left.
    const EVICTION_ROUNDS: usize = 4;

    /// Moves `block`, in disk chunk `on_disk`, to a memory chunk: reads its
    /// pages into one, a read `reads` hears of, then points its entry at
    /// it, and closes the disk chunk, which goes free when its last reader
    /// leaves. Gives the disk chunk back if memory has no room even after
    /// evicting.
    ///
    /// The read fills a lent memory chunk, which no reader sees until it is
    /// done. If the block left disk another way meanwhile, the memory copy
    /// still serves the caller, and goes back after it.
    pub(super) async fn promote<'c>(
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
    pub(super) async fn allocate_memory(&self) -> Result<Option<MemoryChunkWriter<'_>>> {
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
    /// no I/O. `None` at once if a drive of files that grow is short of
    /// room: evicting would give it none back, as a freed chunk keeps its
    /// room in the file.
    pub(super) fn allocate_disk(&self) -> Option<DiskChunkWriter<'_>> {
        if !self.disk.has_room_for_a_block() {
            return None;
        }
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
    pub(super) async fn move_to_disk(&self, chunk: ChunkId) -> Result<MoveToDisk> {
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
    pub(super) fn evict_from_memory(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Memory(chunk))
    }

    /// [`Self::evict_from_memory`] for disk chunk `chunk`: the chunk goes
    /// free once the reads in flight from it are done.
    pub(super) fn evict_from_disk(&self, chunk: ChunkId) -> bool {
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
}

/// What became of a promotion.
pub(super) enum Promotion<'cache> {
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
pub(super) enum MoveToDisk {
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
