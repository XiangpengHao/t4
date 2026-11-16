//! The rules both tiers share for their chunks (`doc/03-cache.md`,
//! decisions 2, 3 and 8): which are free, and what each holds.
//!
//! - `free_chunks`: a bit per chunk, set while it is taken.
//! - `chunk_table`: a tier's chunks, free or holding a block, and the
//!   writer or readers holding each.

mod chunk_table;
mod free_chunks;

pub(super) use chunk_table::{ChunkLease, ChunkPin, ChunkTable, ChunkTier};
