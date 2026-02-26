use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;

use crate::error::{Error, Result};
use crate::format::PAGE_SIZE;

pub fn align_up_u64(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    (value + (alignment - 1)) & !(alignment - 1)
}

pub fn align_down_u64(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    value & !(alignment - 1)
}

pub fn align_up_usize(value: usize, alignment: usize) -> usize {
    debug_assert!(alignment.is_power_of_two());
    (value + (alignment - 1)) & !(alignment - 1)
}

#[derive(Debug)]
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    layout: Option<Layout>,
}

impl AlignedBuf {
    pub fn new_zeroed(len: usize) -> Result<Self> {
        if len == 0 {
            return Ok(Self {
                ptr: NonNull::dangling(),
                len: 0,
                layout: None,
            });
        }
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
            len,
            layout: Some(layout),
        })
    }

    pub fn from_padded_slice(src: &[u8]) -> Result<Self> {
        let padded_len = if src.is_empty() {
            0
        } else {
            align_up_usize(src.len(), PAGE_SIZE)
        };
        let mut buf = Self::new_zeroed(padded_len)?;
        if !src.is_empty() {
            buf.as_mut_slice()[..src.len()].copy_from_slice(src);
        }
        Ok(buf)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.len == 0 {
            return &mut [];
        }
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if let Some(layout) = self.layout {
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}
