//! The storage half of the cache (`doc/11-shared-mode.md`, decision 2):
//! what is cached where, the bytes themselves, and the blocks being
//! fetched. The process that owns the cache runs it; its clients reach it
//! through their owner (`storage_link`).
//!
//! - `object_cache`: reads, fills and evictions over the tiers.
//! - `index`: which blocks of which objects are cached, and where.
//! - `in_flight`: the blocks being fetched, which a read that needs one
//!   waits for rather than fetching again.

mod in_flight;
mod index;
mod object_cache;

pub(in crate::cache) use index::FileVersion;
pub(in crate::cache) use object_cache::{
    CacheableFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead, TierWork,
};
