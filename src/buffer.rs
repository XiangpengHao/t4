use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::any::Any;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::Arc;

use vstd::prelude::*;

use crate::PAGE_SIZE;
use crate::io::error::{Error, Result};

verus! {

/// Rounds `value` down to the nearest multiple of `alignment`.
/// `alignment` must be a power of two.
pub fn align_down_u64(value: u64, alignment: u64) -> (result: u64)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0,  // power of two

    ensures
        result <= value,
        result & sub(alignment, 1) == 0,
        value - result < alignment,
        // idempotent on aligned values
        value & sub(alignment, 1) == 0 ==> result == value,
{
    let mask = alignment - 1;
    let result = value & !mask;

    proof {
        assert(result <= value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1),
        ;
        assert(result & sub(alignment, 1) == 0u64) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1),
        ;
        assert(value - result < alignment) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1),
                result <= value,
        ;
        assert(value & sub(alignment, 1) == 0 ==> result == value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1),
        ;
    }

    result
}

/// Rounds `value` up to the nearest multiple of `alignment`.
/// Returns `None` on overflow.
/// `alignment` must be a power of two.
pub fn align_up_u64(value: u64, alignment: u64) -> (result: Option<u64>)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0,
    ensures
        result.is_some() ==> result.unwrap() >= value,
        result.is_some() ==> result.unwrap() & sub(alignment, 1) == 0,
        result.is_some() ==> result.unwrap() - value < alignment,
        result.is_some() ==> (value & sub(alignment, 1) == 0 ==> result.unwrap() == value),
        result.is_none() ==> value + alignment > u64::MAX,
{
    let mask = alignment - 1;
    let sum = value.checked_add(mask)?;
    let result = sum & !mask;

    proof {
        assert(result >= value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1),
        ;
        assert(result & sub(alignment, 1) == 0u64) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == sum & !sub(alignment, 1),
        ;
        assert(result - value < alignment) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1),
                result >= value,
        ;
        assert(value & sub(alignment, 1) == 0 ==> result == value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1),
        ;
    }

    Some(result)
}

} // verus!

/// A page-aligned buffer: what direct I/O reads into and writes from.
#[derive(Debug)]
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len_u32: NonZeroU32,
    memory: BufMemory,
    /// What the buffer keeps alive besides its bytes, until it drops: what
    /// the I/O it goes to holds ([`Self::holding`]).
    held: Option<Arc<dyn Any + Send + Sync>>,
}

/// Where an [`AlignedBuf`]'s bytes live.
#[derive(Debug)]
enum BufMemory {
    /// Allocated by the buffer with this layout, and freed when it drops.
    Owned(Layout),
    /// Lent by a mapping the guard keeps mapped until the buffer drops: a
    /// chunk of the cache's memory pool.
    Lent { _guard: Arc<dyn Any + Send + Sync> },
}

// An owned buffer is mutated only through `&mut self`, so moving it across
// threads preserves Rust's aliasing guarantees. A lent one adds a guard that
// is itself `Send`.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    pub fn new_zeroed(len_u32: NonZeroU32) -> Result<Self> {
        let len = len_u32.get() as usize;
        let layout = Layout::from_size_align(len, PAGE_SIZE)
            .map_err(|_| Error::InvalidArgument("invalid aligned buffer layout"))?;
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "aligned allocation failed",
            ))
        })?;
        Ok(Self {
            ptr,
            len_u32,
            memory: BufMemory::Owned(layout),
            held: None,
        })
    }

    /// A buffer over `len_u32` bytes at `ptr`, lent by a mapping `guard`
    /// keeps mapped.
    ///
    /// # Safety
    ///
    /// `ptr` is page-aligned, and valid for reads and writes of `len_u32`
    /// bytes for as long as `guard` lives. While the buffer lives, nothing
    /// but the I/O it goes to writes those bytes, and nothing reads them
    /// while that I/O writes them. Others may read them while an I/O reads
    /// them too, so the buffer is for direct I/O, through raw pointers, and
    /// never for [`Self::as_slice`] or [`Self::as_mut_slice`].
    pub(crate) unsafe fn lent(
        ptr: NonNull<u8>,
        len_u32: NonZeroU32,
        guard: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        debug_assert!((ptr.as_ptr() as usize).is_multiple_of(PAGE_SIZE));
        Self {
            ptr,
            len_u32,
            memory: BufMemory::Lent { _guard: guard },
            held: None,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len_u32.get() as usize
    }

    pub(crate) fn len_u32(&self) -> u32 {
        self.len_u32.get()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[cfg(test)]
    pub fn as_slice(&self) -> &[u8] {
        debug_assert!(
            self.is_owned(),
            "a lent buffer is only touched by raw pointers"
        );
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[cfg(test)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        debug_assert!(
            self.is_owned(),
            "a lent buffer is only touched by raw pointers"
        );
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    /// Copies `bytes` into the buffer from its byte `at`, through raw
    /// pointers, as the I/O a lent buffer goes to writes it: what the buffer
    /// held before need not be initialized, and nothing of it is read.
    ///
    /// # Panics
    ///
    /// If `bytes` run past the buffer's end.
    pub(crate) fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        assert!(
            at.checked_add(bytes.len())
                .is_some_and(|end| end <= self.len()),
            "{} bytes at {at} fit in {}",
            bytes.len(),
            self.len()
        );
        // SAFETY: the bytes lie within the buffer, as asserted, and `&mut
        // self` makes this the buffer's one writer; a lent buffer's lender
        // promises no one else writes them meanwhile ([`Self::lent`]).
        unsafe {
            self.as_mut_ptr()
                .add(at)
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }

    /// The buffer, keeping `held` alive too until it drops: so an I/O it
    /// goes to holds what the I/O needs, after the caller gave up on it.
    pub(crate) fn holding(mut self, held: Arc<dyn Any + Send + Sync>) -> Self {
        assert!(self.held.is_none(), "a buffer holds one thing");
        self.held = Some(held);
        self
    }

    #[cfg(test)]
    fn is_owned(&self) -> bool {
        matches!(self.memory, BufMemory::Owned(_))
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // A lent buffer's guard drops with it.
        if let BufMemory::Owned(layout) = self.memory {
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}
