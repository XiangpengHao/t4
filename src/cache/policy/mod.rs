//! Policies a user may replace: [`PlacementPolicy`] (where blocks live) and [`ReadPolicy`]
//! (how a read is served). Tiers are exclusive: a block is in one tier at a time.

mod clock;
mod fixed_placement;
mod read;
mod stragglers;

pub use super::{BlockKey, ChunkId, FileVersionId};
pub use clock::ClockPolicy;
pub use fixed_placement::FixedPlacementPolicy;
pub use read::{
    BlockSource, FetchRuns, Io, IoEvent, IoPhase, LocatedRead, PlannedGet, ReadIntent, ReadPlan,
    ReadPolicy,
};
pub use stragglers::RaceStragglers;

use std::num::NonZeroU32;
use std::ops::Range;

/// Every victim picked, the cache frees or gives back with [`Self::on_admit`], so a policy may
/// forget a chunk when it picks it. A picked chunk may hold no block by then; the cache skips it.
pub trait PlacementPolicy: Send + Sync {
    /// On the hit path: keep it cheap.
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>);

    /// Asked before the read, so a move reads the chunk straight into memory in one I/O.
    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit;

    /// Also asked of blocks a GET fetches past the read ([`BlockRead::asked`]).
    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit;

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey);

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim>;

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId>;
}

/// Every chunk a policy hears of or picks is below its tier's count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TierSizes {
    memory_chunks: NonZeroU32,
    disk_chunks: NonZeroU32,
}

impl TierSizes {
    pub(in crate::cache) fn new(memory_chunks: NonZeroU32, disk_chunks: NonZeroU32) -> Self {
        Self {
            memory_chunks,
            disk_chunks,
        }
    }

    pub fn memory_chunks(&self) -> NonZeroU32 {
        self.memory_chunks
    }

    pub fn disk_chunks(&self) -> NonZeroU32 {
        self.disk_chunks
    }
}

#[derive(Clone, Debug)]
pub struct BlockRead<'a> {
    block: BlockKey,
    file_size: u64,
    read: Range<u64>,
    path: &'a [u8],
    intent: Option<ReadIntent>,
    asked: bool,
}

impl<'a> BlockRead<'a> {
    pub(in crate::cache) fn new(
        block: BlockKey,
        file_size: u64,
        read: Range<u64>,
        path: &'a [u8],
        intent: Option<ReadIntent>,
        asked: bool,
    ) -> Self {
        Self {
            block,
            file_size,
            read,
            path,
            intent,
            asked,
        }
    }

    pub fn intent(&self) -> Option<ReadIntent> {
        self.intent
    }

    /// False for a block a GET fetches past the read's bytes, widened.
    pub fn asked(&self) -> bool {
        self.asked
    }

    pub fn block(&self) -> BlockKey {
        self.block
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn read(&self) -> Range<u64> {
        self.read.clone()
    }

    pub fn path(&self) -> &'a [u8] {
        self.path
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Memory,
    Disk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskHit {
    Stay,
    Promote,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admit {
    Memory,
    Disk,
    Discard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryVictim {
    chunk: ChunkId,
    action: VictimAction,
}

impl MemoryVictim {
    pub fn new(chunk: ChunkId, action: VictimAction) -> Self {
        Self { chunk, action }
    }

    pub fn chunk(&self) -> ChunkId {
        self.chunk
    }

    pub fn action(&self) -> VictimAction {
        self.action
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VictimAction {
    ToDisk,
    Drop,
}
