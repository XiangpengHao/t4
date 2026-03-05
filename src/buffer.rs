use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::num::NonZeroU32;
use std::ptr::NonNull;

use crate::error::{Error, Result};
use crate::{PAGE_SIZE, PAGE_SIZE_NZ_U32};

pub use verified::{align_down_u64, align_up_u64};

pub fn align_up_u32(value: NonZeroU32, alignment: NonZeroU32) -> Result<NonZeroU32> {
    let value = value.get();
    let alignment = alignment.get();
    debug_assert!(alignment.is_power_of_two());
    let aligned = verified::align_up_u32(value, alignment)
        .ok_or(Error::InvalidArgument("value overflow while aligning"))?;
    NonZeroU32::new(aligned).ok_or(Error::InvalidArgument("aligned value unexpectedly zero"))
}

#[derive(Debug)]
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len_u32: NonZeroU32,
    layout: Layout,
}

// `AlignedBuf` owns its allocation and exposes mutation only through `&mut self`,
// so moving it across threads preserves Rust's aliasing guarantees.
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
            layout,
        })
    }

    pub fn from_padded_slice(src: &[u8]) -> Result<Self> {
        if src.is_empty() {
            return Err(Error::InvalidArgument(
                "padded buffer source must be non-empty",
            ));
        }
        let src_len_u32: u32 = src
            .len()
            .try_into()
            .map_err(|_| Error::InvalidArgument("aligned buffer length exceeds u32"))?;
        let src_len_u32 = NonZeroU32::new(src_len_u32)
            .ok_or(Error::InvalidArgument("aligned buffer length must be > 0"))?;
        let padded_len_u32 = align_up_u32(src_len_u32, PAGE_SIZE_NZ_U32)?;
        let mut buf = Self::new_zeroed(padded_len_u32)?;
        let src_len = src_len_u32.get() as usize;
        buf.as_mut_slice()[..src_len].copy_from_slice(src);
        Ok(buf)
    }

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

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    pub fn try_into_boxed_array(self) -> Result<Box<[u8; PAGE_SIZE]>> {
        if self.len() != PAGE_SIZE {
            return Err(Error::InvalidArgument("invalid aligned buffer layout"));
        }
        if self.layout.align() != PAGE_SIZE {
            return Err(Error::InvalidArgument("invalid aligned buffer layout"));
        }
        let ptr = self.ptr.as_ptr();
        let boxed = unsafe { Box::from_raw(ptr as *mut [u8; PAGE_SIZE]) };
        std::mem::forget(self);
        Ok(boxed)
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}
