//! Which blocks of which objects are cached, and where (`doc/03-cache.md`,
//! decisions 1, 3, 4 and 5). Build step 2.
//!
//! - `paths`: each path's file, the version of its object the cache holds
//!   blocks of, under an id never reused.
//! - `blocks`: where each cached block of each file is.
//! - `read_plan`: the blocks a read of a byte range covers, where each one
//!   is, and the part of each the read needs.

mod blocks;
mod paths;
mod read_plan;

pub(super) use blocks::{BlockTable, Location};
pub(super) use paths::{FileVersion, PathTable, UncacheableObject};
pub(super) use read_plan::PlannedBlock;
