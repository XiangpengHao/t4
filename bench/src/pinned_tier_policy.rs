//! [`PinnedTierPolicy`]: a cache policy that keeps every block in one
//! tier, so that a warm pass measures that tier's hits alone.

use t4::policy::{
    Admit, BlockKey, BlockRead, CachePolicy, ChunkId, ClockPolicy, DiskHit, MemoryVictim, Tier,
    TierSizes,
};

/// A [`ClockPolicy`] that admits every fetched block to one tier, and never
/// moves a disk hit to memory. Once its tier holds the working set, every
/// read is a hit there: all memory hits, or all disk hits.
pub struct PinnedTierPolicy {
    clock: ClockPolicy,
    tier: Tier,
}

impl PinnedTierPolicy {
    /// Keeps every block in `tier`, over tiers of `sizes`.
    pub fn new(sizes: TierSizes, tier: Tier) -> Self {
        Self {
            clock: ClockPolicy::new(sizes.memory_chunks(), sizes.disk_chunks()),
            tier,
        }
    }
}

impl CachePolicy for PinnedTierPolicy {
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) {
        self.clock.on_memory_hit(chunk, read);
    }

    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit {
        // The CLOCK marks the chunk referenced; its answer is ignored.
        self.clock.on_disk_hit(chunk, read);
        DiskHit::Stay
    }

    fn on_miss(&self, _read: &BlockRead<'_>) -> Admit {
        match self.tier {
            Tier::Memory => Admit::Memory,
            Tier::Disk => Admit::Disk,
        }
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
