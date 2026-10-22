//! `t4`: a cache for object stores, in memory and on a local NVMe drive.
//!
//! [`T4Caching`] is an [`object_store::ObjectStore`] over another, which
//! [`T4Caching::builder`] makes: it keeps what is read in two tiers of fixed
//! size, and fetches what it misses itself. [`policy`] holds what decides
//! what it keeps: the [`policy::CachePolicy`] trait, and its default, a
//! lock-free CLOCK.

mod buffer;
// The object store cache (`doc/03-cache.md`). Linux and macOS: its memory
// pool is an anonymous mapping.
#[cfg(unix)]
mod cache;
mod io;
mod range;

use std::num::NonZeroU32;

use vstd::prelude::*;

#[cfg(unix)]
pub use cache::{T4Caching, T4CachingBuilder, policy};
pub use io::error::{Error, Result};

#[cfg(feature = "__bench")]
#[doc(hidden)]
pub mod __bench {
    pub use crate::buffer::AlignedBuf;
    pub use crate::io::io_task::PageWrite;
    pub use crate::io::io_worker::IoWorker;
}

verus! {

pub const PAGE_SIZE: usize = 4096;

} // verus!

pub const PAGE_SIZE_U32: u32 = PAGE_SIZE as u32;
pub const PAGE_SIZE_NZ_U32: NonZeroU32 = match NonZeroU32::new(PAGE_SIZE_U32) {
    Some(value) => value,
    None => panic!("PAGE_SIZE must be non-zero"),
};
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;
