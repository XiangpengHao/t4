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
    /// Batched: one eviction makes room for the insertions after it too.
    const EVICTION_BATCH: usize = 8;

    /// More than one: a victim's chunk goes free only once its readers have left.
    const EVICTION_ROUNDS: usize = 4;

    /// If the block left disk meanwhile, the memory copy still serves the caller, then goes back.
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
            &self.clock,
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

    /// Evicts nothing if the tier takes no block: a freed chunk keeps its room in the file.
    pub(super) fn allocate_disk(&self) -> Option<DiskChunkWriter<'_>> {
        if !self.disk.takes_a_block() {
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

    /// At most a sixteenth of the tier, so a small one isn't emptied for one block.
    fn eviction_batch(chunk_count: u32) -> usize {
        (chunk_count as usize / 16).clamp(1, Self::EVICTION_BATCH)
    }

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

    /// The write holds both chunks until it is done, even if this future is dropped first.
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
            victim.unpublish();
            self.placement.on_admit(Tier::Disk, on_disk.chunk(), block);
            Ok(MoveToDisk::Moved)
        } else {
            on_disk.unpublish();
            Ok(MoveToDisk::Superseded)
        }
    }

    pub(super) fn evict_from_memory(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.memory.pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Memory(chunk))
    }

    pub(super) fn evict_from_disk(&self, chunk: ChunkId) -> bool {
        let Some(victim) = self.disk.chunks().pin_for_eviction(chunk) else {
            return false;
        };
        self.forget_if_closed(victim.unpublish(), victim.block(), Location::Disk(chunk))
    }

    /// Closed first, so a read that finds the entry from then on finds the chunk closed.
    fn forget_if_closed(&self, closed: bool, block: BlockKey, location: Location) -> bool {
        if closed {
            self.blocks.remove_if(block, location);
        }
        closed
    }
}

pub(super) enum Promotion<'cache> {
    InMemory(MemoryChunkReader<'cache>),
    NoRoom(DiskChunkReader<'cache>),
}

/// The policy forgot the chunk when it picked it: a move that leaves the block
/// in memory must give it back, or the policy never picks it again.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MoveToDisk {
    Moved,
    NoBlock,
    DiskFull,
    Superseded,
}
