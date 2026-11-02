//! The disk tier (`doc/03-cache.md`, `doc/04-multi-device.md`): its
//! chunks striped over its cache files, which a lock gives one cache at a
//! time, read and written by direct I/O.
//!
//! - `cache_files`: the files, their lock, and the I/O over them.
//! - `disk_chunks`: the files' data chunks, written and read at their
//!   offsets.

mod cache_files;
mod disk_chunks;

pub(in crate::cache) use cache_files::{CacheFiles, DiskFile};
pub(super) use disk_chunks::{DiskChunkReader, DiskChunkWriter};
