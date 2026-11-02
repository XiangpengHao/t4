//! The storage half of the cache (`doc/11-shared-mode.md`, decision 2):
//! what is cached where, the bytes themselves, and the blocks being
//! fetched. The process that owns the cache runs it; its clients reach it
//! through their owner (`cache_stores`).
//!
//! - `cache_store`: the one way into the tiers: every read of memory
//!   and disk, fills, promotions and evictions, over the index and the
//!   blocks in flight.
//! - `index`: which blocks of which objects are cached, and where.
//! - `in_flight`: the blocks being fetched, which a read that needs one
//!   waits for rather than fetching again.
//! - `memory`: the memory tier: chunks in one mapping, written and read as
//!   bytes.
//! - `disk`: the disk tier: chunks in the cache files, written and read by
//!   direct I/O.
//! - `chunks`: the rules both tiers share for their chunks.

mod cache_store;
mod chunks;
mod disk;
mod in_flight;
mod index;
mod memory;

pub(in crate::cache) use disk::{CacheFiles, DiskFile, FileIdentity};

pub(in crate::cache) use cache_store::{
    CacheableFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead, TierWork,
};
pub(in crate::cache) use index::FileVersion;
