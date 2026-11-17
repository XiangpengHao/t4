use std::num::NonZeroU32;

use crate::PAGE_SIZE_U32;
use crate::cache::io_report::IoReport;
use crate::cache::policy::{Io, MemoryVictim, ReadPolicy, Tier, VictimAction};
use crate::cache::storage::disk::{DiskChunkReader, DiskChunkWriter};
use crate::cache::storage::index::{Location, PlannedBlock};
use crate::cache::storage::memory::{MemoryChunkReader, MemoryChunkWriter};
use crate::cache::{BlockKey, ChunkId};
use crate::io::error::Result;

use super::disk_writer::{LandedBlock, PendingWrite};
use super::{ObjectCache, Tiers};

impl ObjectCache {
    /// If the block left disk meanwhile, the memory copy still serves the caller, then goes back.
    pub(super) async fn promote<'c>(
        &'c self,
        on_disk: DiskChunkReader<'c>,
        block: &PlannedBlock,
        reads: &dyn ReadPolicy,
    ) -> Result<Promotion<'c>> {
        let tiers = &*self.tiers;
        let Some(writer) = tiers.allocate_memory().await? else {
            return Ok(Promotion::NoRoom(on_disk));
        };
        let range = block.object_range();
        let pages = u32::try_from(range.end - range.start)
            .ok()
            .and_then(|len| NonZeroU32::new(len.next_multiple_of(PAGE_SIZE_U32)))
            .expect("a block is a chunk's bytes or fewer, one or more");
        let (buffer, lent) = writer.lend_for_read(&tiers.memory, pages);
        let report = IoReport::start(
            reads,
            Io::DiskRead {
                drive: on_disk.position().file(),
                bytes: pages.get(),
            },
            &self.clock,
        );
        let filled = report
            .awaiting(tiers.disk.read_chunk_start(&on_disk, buffer))
            .await?;
        let block = on_disk.block();
        let in_memory = lent.publish(filled, block);
        let (from, to) = (
            Location::Disk(on_disk.chunk()),
            Location::Memory(in_memory.chunk()),
        );
        let mut changes = tiers.disk.log().map(|log| log.changes());
        if tiers.blocks.replace_if(block, from, to) {
            if let Some(log) = &mut changes {
                log.free(on_disk.chunk());
            }
            on_disk.unpublish();
            tiers
                .placement
                .on_admit(Tier::Memory, in_memory.chunk(), block);
        } else {
            in_memory.unpublish();
        }
        Ok(Promotion::InMemory(in_memory))
    }
}

impl Tiers {
    /// Batched: one eviction makes room for the insertions after it too.
    const EVICTION_BATCH: usize = 8;

    /// More than one: a victim's chunk goes free only once its readers have left.
    const EVICTION_ROUNDS: usize = 4;

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
        for victim in victims {
            match victim.action() {
                VictimAction::Drop => {
                    self.evict_from_memory(victim.chunk());
                }
                VictimAction::ToDisk => {
                    if let Some(held) = self.memory.pin_for_eviction(victim.chunk())
                        && !self.pending.enqueue(held.chunk(), held.block())
                    {
                        self.placement
                            .on_admit(Tier::Memory, held.chunk(), held.block());
                    }
                }
            }
        }
        Ok(())
    }

    /// Gives back the victims it couldn't move, for memory to keep.
    pub(super) async fn write_disk_batch(
        &self,
        batch: Vec<PendingWrite>,
    ) -> Vec<(ChunkId, BlockKey)> {
        let mut moves = Vec::new();
        let mut writes = Vec::new();
        let mut kept = Vec::new();
        for write in batch {
            let moving = match write {
                PendingWrite::Victim { chunk, block } => match self.memory.read(chunk, block) {
                    Some(victim) => DiskMove::Victim(victim),
                    None => continue,
                },
                PendingWrite::Landed(landed) if self.blocks.get(landed.block()).is_some() => {
                    self.leave_uncached(landed);
                    continue;
                }
                PendingWrite::Landed(landed) => DiskMove::Landed(landed),
            };
            let Some(writer) = self.allocate_disk() else {
                self.leave_unmoved(moving, &mut kept);
                continue;
            };
            let hold = writer.hold_for_write(self.disk.chunks());
            let buf = match &moving {
                DiskMove::Victim(victim) => victim.lend_for_write(&self.memory, hold),
                DiskMove::Landed(landed) => landed.write_buffer(hold),
            };
            writes.push(crate::io::io_task::PageWrite {
                buf,
                at: writer.position(),
            });
            moves.push((moving, writer));
        }
        let frees_logged = self
            .disk
            .log()
            .is_none_or(|log| log.changes().flush().is_ok());
        if !frees_logged {
            self.disk.chunks().close();
        }
        if !frees_logged || writes.is_empty() || self.disk.write_batch(writes).await.is_err() {
            for (moving, _) in moves {
                self.leave_unmoved(moving, &mut kept);
            }
            return kept;
        }
        let mut changes = self.disk.log().map(|log| log.changes());
        for (moving, writer) in moves {
            let block = moving.block();
            let on_disk = writer.publish(block);
            let to = Location::Disk(on_disk.chunk());
            let cached = match &moving {
                DiskMove::Victim(victim) => {
                    self.blocks
                        .replace_if(block, Location::Memory(victim.chunk()), to)
                }
                DiskMove::Landed(_) => self.blocks.insert_if_absent(block, to),
            };
            if cached {
                if let Some(log) = &mut changes {
                    log.add(block, on_disk.chunk());
                }
                if let DiskMove::Victim(victim) = &moving {
                    victim.unpublish();
                }
                self.placement.on_admit(Tier::Disk, on_disk.chunk(), block);
            } else {
                on_disk.unpublish();
            }
            if let DiskMove::Landed(landed) = moving {
                self.leave_uncached(landed);
            }
        }
        kept
    }

    fn leave_unmoved(&self, moving: DiskMove<'_>, kept: &mut Vec<(ChunkId, BlockKey)>) {
        match moving {
            DiskMove::Victim(victim) => kept.push((victim.chunk(), victim.block())),
            DiskMove::Landed(landed) => self.leave_uncached(landed),
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
        let mut changes = self.disk.log().map(|log| log.changes());
        let closed = victim.unpublish();
        if closed && let Some(log) = &mut changes {
            log.free(chunk);
        }
        self.forget_if_closed(closed, victim.block(), Location::Disk(chunk))
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

enum DiskMove<'c> {
    Victim(MemoryChunkReader<'c>),
    Landed(LandedBlock),
}

impl DiskMove<'_> {
    fn block(&self) -> BlockKey {
        match self {
            Self::Victim(victim) => victim.block(),
            Self::Landed(landed) => landed.block(),
        }
    }
}
