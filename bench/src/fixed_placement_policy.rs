//! [`FixedPlacementPolicy`]: a cache policy that places every fetched block
//! alike, in memory, on disk or nowhere, so that a pass measures one tier's
//! hits alone, or t4 with no tier at all.

use t4::policy::{
    Admit, BlockKey, BlockRead, ChunkId, ClockPolicy, DiskHit, MemoryVictim, PlacementPolicy, Tier,
    TierSizes,
};

/// A [`ClockPolicy`] that admits every fetched block as one [`Admit`] says,
/// and never moves a disk hit to memory. Once its tier holds the working
/// set, every read is a hit there: all memory hits, or all disk hits. With
/// [`Admit::Discard`], every read goes to the remote, through t4.
pub struct FixedPlacementPolicy {
    clock: ClockPolicy,
    admit: Admit,
}

impl FixedPlacementPolicy {
    /// Places every block as `admit` says, over tiers of `sizes`.
    pub fn new(sizes: TierSizes, admit: Admit) -> Self {
        Self {
            clock: ClockPolicy::new(sizes.memory_chunks(), sizes.disk_chunks()),
            admit,
        }
    }
}

impl PlacementPolicy for FixedPlacementPolicy {
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) {
        self.clock.on_memory_hit(chunk, read);
    }

    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit {
        // The CLOCK marks the chunk referenced; its answer is ignored.
        self.clock.on_disk_hit(chunk, read);
        DiskHit::Stay
    }

    fn on_remote_read(&self, _read: &BlockRead<'_>) -> Admit {
        self.admit
    }

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey) {
        self.clock.on_admit(tier, chunk, block);
    }

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim> {
        self.clock.pick_memory_victims(count)
    }

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId> {
        self.clock.pick_disk_victims(count)
    }
}
