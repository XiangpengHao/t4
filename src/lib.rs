//! `t4`: a cache for object stores, in memory and on a local NVMe drive.
//! Start from [`T4Caching::builder`].

#[cfg(unix)]
mod cache;
mod io;

use std::num::NonZeroU32;

use vstd::prelude::*;

#[cfg(unix)]
pub use cache::{FileHandle, T4Caching, T4CachingBuilder, policy};
pub use io::error::{Error, Result};

#[cfg(feature = "__bench")]
#[doc(hidden)]
pub mod __bench {
    pub use crate::io::aligned_buf::AlignedBuf;
    pub use crate::io::io_task::{FilePosition, PageWrite};
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
