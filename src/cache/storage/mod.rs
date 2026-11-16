mod cache_store;
mod chunks;
mod disk;
mod in_flight;
mod index;
mod memory;
mod snapshot;

#[cfg(feature = "__fuzz")]
pub use cache_store::{FuzzedSnapshot, FuzzedVersion};

pub(in crate::cache) use disk::{CacheFiles, DiskFile, FileAllocation, FileIdentity};

pub(in crate::cache) use cache_store::{
    CacheableFile, FetchedBlocks, LocatedRun, ObjectCache, PartialRead, TierWork,
};
pub(in crate::cache) use index::FileVersion;
