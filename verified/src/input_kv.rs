use std::borrow::Borrow;

use vstd::{prelude::*, slice::slice_to_vec};

use crate::{PAGE_SIZE, align_up_u64};

verus! {

#[derive(Debug)]
pub enum InputError {
    KeyTooLarge(usize),
    ValueTooLarge(usize),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct T4Key(Vec<u8>);

impl T4Key {
    #[verifier::type_invariant]
    spec fn type_inv(&self) -> bool {
        self.0.len() <= u8::MAX as usize
    }

    pub fn as_bytes(&self) -> (result: &[u8])
        ensures
            result.len() <= u8::MAX as usize,
    {
        proof {
            use_type_invariant(&*self);
        }
        self.0.as_slice()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[allow(clippy::should_implement_trait)]
    // verus doesn't allow inherit clone
    pub fn clone(&self) -> Self {
        proof {
            use_type_invariant(&*self);
        }
        Self(self.0.clone())
    }

    pub fn try_from_vec(value: Vec<u8>) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }

    pub fn try_from_slice(value: &[u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
            result.is_err() ==> value.len() > u8::MAX as usize,
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(slice_to_vec(value)))
    }
}

impl AsRef<[u8]> for T4Key {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for T4Key {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct T4KeyRef<'a>(&'a [u8]);

impl<'a> T4KeyRef<'a> {
    pub fn as_bytes(self) -> (result: &'a [u8])
        ensures
            result.len() <= u8::MAX as usize,
    {
        proof {
            use_type_invariant(&self);
        }
        self.0
    }

    pub fn len(self) -> usize {
        self.0.len()
    }

    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }

    #[verifier::type_invariant]
    pub closed spec fn wf(self) -> bool {
        self.0.len() <= u8::MAX as usize
    }

    pub fn from_slice(value: &'a [u8]) -> (result: Self)
        requires
            value.len() <= u8::MAX as usize,
        ensures
            result.wf(),
    {
        Self(value)
    }

    pub fn try_from_slice(value: &'a [u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
            result.is_ok() ==> result.unwrap().wf(),
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }
}

impl<'a> AsRef<[u8]> for T4KeyRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct T4Value {
    bytes: Vec<u8>,
    len_u32: u32,
}

impl T4Value {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn len_u32(&self) -> u32 {
        self.len_u32
    }

    pub fn is_empty(&self) -> bool {
        self.len_u32 == 0
    }

    #[allow(clippy::should_implement_trait)]
    // verus doesn't allow inherit clone
    pub fn clone(&self) -> Self {
        Self { bytes: self.bytes.clone(), len_u32: self.len_u32 }
    }

    pub fn try_from_vec(value: Vec<u8>) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u32::MAX as usize,
    {
        if value.len() > u32::MAX as usize {
            return Err(InputError::ValueTooLarge(value.len()));
        }
        let len_u32: u32 = value.len() as u32;
        Ok(Self { bytes: value, len_u32 })
    }

    pub fn try_from_slice(value: &[u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u32::MAX as usize,
    {
        if value.len() > u32::MAX as usize {
            return Err(InputError::ValueTooLarge(value.len()));
        }
        Ok(Self { bytes: slice_to_vec(value), len_u32: value.len() as u32 })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Logical location of a live value.
///
/// Empty values are represented as `(offset = 0, length = 0)`. Non-empty values
/// have a page-aligned data offset and a page-padded physical extent that fits
/// in the file address space.
pub struct ValueRef {
    offset: u64,
    length: u32,
}

impl ValueRef {
    closed spec fn padded_extent_wf(length: u32, padded: u64) -> bool {
        padded >= length as u64 && padded & sub(PAGE_SIZE as u64, 1) == 0 && padded
            - (length as u64) < (PAGE_SIZE as u64)
    }

    pub closed spec fn wf(self) -> bool {
        self.length == 0 && self.offset == 0 || self.length != 0
            && self.offset >= PAGE_SIZE as u64
            && self.offset & sub(PAGE_SIZE as u64, 1) == 0
            && exists|padded: u64|
                Self::padded_extent_wf(self.length, padded)
                    && self.offset as int + padded as int <= u64::MAX as int
    }

    #[verifier::type_invariant]
    spec fn type_inv(&self) -> bool {
        self.wf()
    }

    pub fn empty() -> (result: Self)
        ensures
            result.wf(),
    {
        Self { offset: 0, length: 0 }
    }

    pub fn try_new(offset: u64, length: u32) -> (result: Option<Self>)
        ensures
            result.is_some() ==> result.unwrap().wf(),
    {
        if length == 0 {
            if offset == 0 {
                return Some(Self::empty());
            }
            return None;
        }
        if offset < PAGE_SIZE as u64 {
            return None;
        }
        proof {
            assert(PAGE_SIZE as u64 & sub(PAGE_SIZE as u64, 1) == 0u64) by (bit_vector);
        }
        if offset & (PAGE_SIZE as u64 - 1) != 0 {
            return None;
        }
        let padded = align_up_u64(length as u64, PAGE_SIZE as u64).unwrap();
        match offset.checked_add(padded) {
            Some(_) => {
                proof {
                    assert(Self::padded_extent_wf(length, padded));
                    assert(offset as int + padded as int <= u64::MAX as int);
                    assert(exists|padded_witness: u64|
                        Self::padded_extent_wf(length, padded_witness)
                            && offset as int + padded_witness as int <= u64::MAX as int
                    ) by {
                        let padded_witness = padded;
                    };
                }
                Some(Self { offset, length })
            },
            None => None,
        }
    }

    pub fn offset(self) -> u64 {
        self.offset
    }

    pub fn length(self) -> u32 {
        self.length
    }

    pub fn is_empty(self) -> bool {
        self.length == 0
    }

    pub fn padded_length(self) -> u64 {
        if self.length == 0 {
            return 0;
        }
        proof {
            assert(PAGE_SIZE as u64 & sub(PAGE_SIZE as u64, 1) == 0u64) by (bit_vector);
        }
        align_up_u64(self.length as u64, PAGE_SIZE as u64).unwrap()
    }

    pub fn file_hole(self) -> Option<FileHole> {
        if self.is_empty() {
            None
        } else {
            Some(FileHole { offset: self.offset, length: self.padded_length() })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Physical free extent in the store file.
///
/// Unlike `ValueRef::length`, this length is the page-padded number of bytes
/// available for reuse.
pub struct FileHole {
    pub offset: u64,
    pub length: u64,
}

} // verus!
