use std::{
    alloc::Layout,
    ptr::{NonNull, copy_nonoverlapping},
};

use crate::art::ptr::TaggedPointer;

pub struct ArtIndex {
    root: TaggedPointer,
}

impl ArtIndex {
    pub(crate) fn new() -> Self {
        Self {
            root: TaggedPointer::default(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {}

    pub fn get(&self, key: &[u8]) -> Option<KVPair> {
        todo!()
    }
}

pub struct KVPair {
    key_len: u8,
    value_len: u8,
    data: NonNull<u8>,
}

impl KVPair {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let total_size = key.len() + value.len() + 2;
        let layout = Layout::from_size_align(total_size, 8).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) };
        let ptr = NonNull::new(ptr).unwrap();

        let key_ptr = ptr.as_ptr() as *mut u8;
        unsafe { copy_nonoverlapping(key.as_ptr(), key_ptr, key.len()) };
        let value_ptr = unsafe { key_ptr.add(key.len() as usize) as *mut u8 };
        unsafe { copy_nonoverlapping(value.as_ptr(), value_ptr, value.len()) };

        Self {
            data: ptr,
            key_len: key.len() as u8,
            value_len: value.len() as u8,
        }
    }

    pub fn key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.key_len as usize) }
    }

    pub fn value(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().add(self.key_len as usize),
                self.value_len as usize,
            )
        }
    }
}
