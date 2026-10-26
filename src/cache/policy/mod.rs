//! What the cache asks of its policies (`doc/06-read-modes.md`), which a
//! user may replace, each apart:
//!
//! - [`PlacementPolicy`], where blocks live (`doc/03-cache.md`, decisions 7
//!   and 8): where a fetched block goes, whether a block read from disk
//!   moves to memory, and which chunks a full tier gives up. The default,
//!   `clock`, a lock-free CLOCK for each tier.
//! - [`ReadPolicy`], how a read is served (`read`): which GETs fetch what
//!   only the remote holds, how many race. The default, [`FetchRuns`].
//!
//! Tiers are exclusive: a block is in one tier at a time, and the index
//! names it there. The cache keeps no access state of its own: a policy
//! hears every hit, and keeps what it wants, with what concurrency it wants,
//! as every hook takes `&self`.

mod clock;
mod read;
mod stragglers;

pub use super::{BlockKey, ChunkId, FileVersionId};
pub use clock::ClockPolicy;
pub use read::{
    BlockSource, FetchRuns, Io, IoEvent, IoPhase, LocatedRead, PlannedGet, ReadIntent, ReadPlan,
    ReadPolicy,
};
pub use stragglers::RaceStragglers;

use std::num::NonZeroU32;
use std::ops::Range;

/// Where blocks live. One policy serves both tiers, built for their chunk
/// counts: every chunk it hears of or picks is below its tier's count.
///
/// Every chunk the policy picks as a victim, the cache frees, or gives back
/// with [`Self::on_admit`], even if the read that evicts it is given up on
/// midway: so a policy that forgets a chunk when it picks it never loses one
/// that still holds a block. A chunk it picks may hold no block by then; the
/// cache skips it.
pub trait PlacementPolicy: Send + Sync {
    /// A read found its block in memory chunk `chunk`. On the hit path:
    /// keep it cheap.
    fn on_memory_hit(&self, chunk: ChunkId, read: &BlockRead<'_>);

    /// A read found its block in disk chunk `chunk`: whether it stays there
    /// or moves to memory. Asked before the read, so that a move reads the
    /// whole chunk straight into memory, one I/O for both.
    fn on_disk_hit(&self, chunk: ChunkId, read: &BlockRead<'_>) -> DiskHit;

    /// A read found its block in neither memory nor disk, so the remote
    /// serves it, or a GET fetches it past what the read asked for
    /// ([`BlockRead::asked`]): where the block goes once fetched. Asked as
    /// the read plans its fetches, so a policy hears of every block read
    /// from the remote, those it doesn't cache too.
    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit;

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

/// A block a read takes bytes of, or that a GET of it fetches past them,
/// and the read around it: what a policy decides by.
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

    /// What the read is for, if its caller said.
    pub fn intent(&self) -> Option<ReadIntent> {
        self.intent
    }

    /// Whether the read takes bytes of the block; not, if a GET of the read
    /// fetches it past them, widened.
    pub fn asked(&self) -> bool {
        self.asked
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
