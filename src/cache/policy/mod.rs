//! [`CachePolicy`]: what the cache asks of its policy (`doc/03-cache.md`,
//! decisions 7 and 8): where a fetched block goes, whether a block read
//! from disk moves to memory, and which chunks a full tier gives up.
//!
//! Tiers are exclusive: a block is in one tier at a time, and the index
//! names it there. The cache keeps no access state of its own: a policy
//! hears every hit, and keeps what it wants, with what concurrency it wants,
//! as every hook takes `&self`.
//!
//! - `clock`: the default, a lock-free CLOCK for each tier.

mod clock;

pub use super::{BlockKey, ChunkId, FileVersionId};
pub use clock::ClockPolicy;

use std::num::NonZeroU32;
use std::ops::Range;

/// The cache's policy. One serves both tiers, built for their chunk counts:
/// every chunk it hears of or picks is below its tier's count.
///
/// Every chunk the policy picks as a victim, the cache frees, or gives back
/// with [`Self::on_admit`]: so a policy that forgets a chunk when it picks it
/// never loses one that still holds a block. A chunk it picks may hold no
/// block by then; the cache skips it.
pub trait CachePolicy: Send + Sync {
    /// A read found its block in memory chunk `chunk`. On the hit path:
    /// keep it cheap.
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>);

    /// A read found its block in disk chunk `chunk`: whether it stays there
    /// or moves to memory. Asked before the read, so that a move reads the
    /// whole chunk straight into memory, one I/O for both.
    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit;

    /// A read found its block in neither tier: where the block goes once
    /// fetched. Asked as the read plans its fetches, so a policy hears every
    /// miss, those it doesn't cache too.
    fn on_miss(&self, read: &BlockRead<'_>) -> Admit;

    /// `chunk` of `tier` took `block`: a fill, a move, a block given back.
    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey);

    /// Memory is full: up to `count` chunks to free, each moved to disk or
    /// dropped.
    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim>;

    /// The disk is full: up to `count` chunks to drop.
    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId>;
}

/// The chunks of each tier, which a policy is built for: every chunk it
/// hears of or picks is below its tier's count.
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

/// A block a read takes bytes of, and the read around it: what a policy
/// decides by.
#[derive(Clone, Debug)]
pub struct BlockRead<'a> {
    block: BlockKey,
    file_size: u64,
    read: Range<u64>,
    path: &'a [u8],
}

impl<'a> BlockRead<'a> {
    pub(in crate::cache) fn new(
        block: BlockKey,
        file_size: u64,
        read: Range<u64>,
        path: &'a [u8],
    ) -> Self {
        Self {
            block,
            file_size,
            read,
            path,
        }
    }

    /// The block: its file version and its number.
    pub fn block(&self) -> BlockKey {
        self.block
    }

    /// The file's size: which block is its last, how large it is.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The bytes of the file the read asked for: a point read, or a scan.
    pub fn read(&self) -> Range<u64> {
        self.read.clone()
    }

    /// The file's path.
    pub fn path(&self) -> &'a [u8] {
        self.path
    }
}

/// One of the cache's tiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    Memory,
    Disk,
}

/// Where a block read from disk goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskHit {
    /// It stays on disk; the read takes the pages it needs.
    Stay,
    /// It moves to memory, the disk chunk going free; the read takes it
    /// from there.
    Promote,
}

/// Where a block fetched from the object store goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admit {
    Memory,
    Disk,
    /// Nowhere: the read takes its bytes, and the cache keeps none.
    Discard,
}

/// A memory chunk to free, and what becomes of its block.
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

/// What becomes of a memory victim's block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VictimAction {
    /// It moves to a disk chunk, which the disk frees one for if full.
    ToDisk,
    /// It leaves the cache.
    Drop,
}
