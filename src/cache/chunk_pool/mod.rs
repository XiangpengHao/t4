//! Each tier's chunks, which of them are free, and what each holds
//! (`doc/03-cache.md`, decisions 2, 3 and 8). Build step 1.
//!
//! - `free_chunks`: a bit per chunk, set while it is taken.
//! - `chunk_table`: a tier's chunks, free or holding a block, and the
//!   writer or readers holding each: the rules both tiers share.
//! - `memory_pool`: the memory tier: chunks in one mapping, written and read
//!   as bytes.
//! - `disk_chunks`: the disk tier: the file's data chunks, written and read
//!   by direct I/O at their offsets.

mod chunk_table;
pub(super) mod disk_chunks;
mod free_chunks;
pub(super) mod memory_pool;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
