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
// What a plan holds, which `ObjectCache` reads without naming the types.
#[allow(unused_imports)]
pub(super) use read_plan::{BlockWindow, MissingRun, PlannedBlock, ReadPlan};

use bbtree::bbtree_map::InsertError;

/// An insert into one of the index's maps, which ends the process if the
/// global allocator has no page for it, as std's collections do. The index
/// lives on the global allocator, outside the cache's budget (decision 6),
/// like any other allocation of the process.
trait InsertOrAbort<T> {
    fn or_abort(self) -> T;
}

impl<T, K, V> InsertOrAbort<T> for Result<T, InsertError<K, V>> {
    fn or_abort(self) -> T {
        self.unwrap_or_else(|_| {
            eprintln!("t4: out of memory inserting into the cache index");
            std::process::abort()
        })
    }
}
