#[cfg(any(test, feature = "__bench"))]
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::any::Any;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::PAGE_SIZE;
#[cfg(any(test, feature = "__bench"))]
use crate::io::error::{Error, Result};

#[derive(Debug)]
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len_u32: NonZeroU32,
    memory: BufMemory,
}

#[derive(Debug)]
enum BufMemory {
    #[cfg(any(test, feature = "__bench"))]
    Owned(Layout),
    Lent {
        _guard: Arc<dyn Any + Send + Sync>,
    },
}

// SAFETY: the bytes are mutated only through `&mut self`, and a lent buffer's guard is `Send`.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    #[cfg(any(test, feature = "__bench"))]
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
        })
    }

    /// Safety: `ptr` is page-aligned and valid for `len_u32` bytes while `guard` lives; only the
    /// buffer's I/O writes them, but others may read them alongside its reads, so never slice it.
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

    pub(crate) fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        assert!(
            at.checked_add(bytes.len())
                .is_some_and(|end| end <= self.len()),
            "{} bytes at {at} fit in {}",
            bytes.len(),
            self.len()
        );
        // SAFETY: in bounds as asserted; `&mut self` and the lender's promise make this the one writer.
        unsafe {
            self.as_mut_ptr()
                .add(at)
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }

    #[cfg(test)]
    fn is_owned(&self) -> bool {
        matches!(self.memory, BufMemory::Owned(_))
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        match self.memory {
            #[cfg(any(test, feature = "__bench"))]
            BufMemory::Owned(layout) => unsafe { dealloc(self.ptr.as_ptr(), layout) },
            BufMemory::Lent { .. } => {}
        }
    }
}
