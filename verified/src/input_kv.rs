use std::borrow::Borrow;
use std::collections::BTreeMap;

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
        padded >= length as u64 && padded & sub(PAGE_SIZE as u64, 1) == 0 && padded - (
        length as u64) < (PAGE_SIZE as u64)
    }

    pub closed spec fn wf(self) -> bool {
        self.length == 0 && self.offset == 0 || self.length != 0 && self.offset >= PAGE_SIZE as u64
            && self.offset & sub(PAGE_SIZE as u64, 1) == 0 && exists|padded: u64|
            Self::padded_extent_wf(self.length, padded) && self.offset as int + padded as int
                <= u64::MAX as int
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
                        Self::padded_extent_wf(length, padded_witness) && offset as int
                            + padded_witness as int <= u64::MAX as int) by {
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

    pub fn file_hole(self) -> (result: Option<FileHole>)
        ensures
            result.is_some() ==> result.unwrap().wf(),
    {
        if self.is_empty() {
            None
        } else {
            FileHole::new(self.offset, self.padded_length())
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

impl FileHole {
    pub closed spec fn wf(self) -> bool {
        self.length > 0 && self.offset as int + self.length as int <= u64::MAX as int
    }

    #[allow(clippy::manual_map)]
    pub fn new(offset: u64, length: u64) -> (result: Option<Self>)
        ensures
            result.is_some() ==> result.unwrap().wf(),
    {
        if length == 0 {
            return None;
        }
        match offset.checked_add(length) {
            Some(_) => Some(Self { offset, length }),
            None => None,
        }
    }
}

#[derive(Debug)]
/// Free space tracker bucketed by exact padded length.
///
/// `by_size[len]` holds the offsets of released extents whose padded length is
/// `len`. `reserve(len)` first looks for an exact-size bucket; if none, it
/// scans the smallest bucket in `(len, 2*len]`, pops an offset, and pushes the
/// `size - len` remainder back as a smaller hole. `consume` mirrors this so
/// WAL replay can subtract a Live entry that originally came from a larger
/// hole. There is no coalescing; unreusable fragments accumulate until GC.
pub struct FileHoles {
    by_size: BTreeMap<u64, Vec<u64>>,
}

/// Smallest size key in `[lo, hi]` whose bucket is non-empty.
#[verifier::external_body]
fn smallest_size_in_range(
    by_size: &BTreeMap<u64, Vec<u64>>,
    lo: u64,
    hi: u64,
) -> (result: Option<u64>)
    ensures
        result.is_some() ==> by_size@.contains_key(result.unwrap()) && by_size@[result.unwrap()]@.len() > 0,
        result.is_some() ==> lo <= result.unwrap() <= hi,
{
    by_size.range(lo..=hi).find(|(_, v)| !v.is_empty()).map(|(k, _)| *k)
}

/// Smallest size key in `[lo, hi]` whose bucket contains `offset`.
#[verifier::external_body]
fn size_holding_offset(
    by_size: &BTreeMap<u64, Vec<u64>>,
    lo: u64,
    hi: u64,
    offset: u64,
) -> (result: Option<u64>)
    ensures
        result.is_some() ==> by_size@.contains_key(result.unwrap()) && by_size@[result.unwrap()]@.len() > 0,
        result.is_some() ==> lo <= result.unwrap() <= hi,
{
    by_size.range(lo..=hi).find_map(
        |(k, v)| { if v.contains(&offset) { Some(*k) } else { None } },
    )
}

impl FileHoles {
    pub closed spec fn bucket_wf(size: u64, offsets: Seq<u64>) -> bool {
        size > 0 && (forall|i: int|
            0 <= i < offsets.len() ==> #[trigger] offsets[i] as int + size as int
                <= u64::MAX as int)
    }

    pub closed spec fn wf(self) -> bool {
        forall|k: u64| #[trigger]
            self.by_size@.contains_key(k) ==> Self::bucket_wf(k, self.by_size@[k]@)
    }

    /// Offsets currently parked in the `size` bucket (empty if none).
    pub closed spec fn bucket(self, size: u64) -> Seq<u64> {
        if self.by_size@.contains_key(size) {
            self.by_size@[size]@
        } else {
            Seq::empty()
        }
    }

    pub fn empty() -> (result: Self)
        ensures
            result.wf(),
    {
        Self { by_size: BTreeMap::new() }
    }

    pub fn release_hole(&mut self, hole: FileHole)
        requires
            old(self).wf(),
            hole.wf(),
        ensures
            final(self).wf(),
            final(self).bucket(hole.length).contains(hole.offset),
    {
        let mut bucket = match self.by_size.remove(&hole.length) {
            Some(b) => b,
            None => Vec::new(),
        };
        bucket.push(hole.offset);
        proof {
            assert(bucket@.last() == hole.offset);
        }
        self.by_size.insert(hole.length, bucket);
    }

    pub fn release_value(&mut self, value: ValueRef)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
    {
        match value.file_hole() {
            Some(hole) => self.release_hole(hole),
            None => {},
        }
    }

    /// Try to claim `len` padded bytes of free space.
    ///
    /// First looks for a hole whose padded length is exactly `len`. If none,
    /// scans the smallest bucket in `(len, 2*len]` and splits it, pushing the
    /// `size - len` leftover back as a smaller hole. Returns the offset of
    /// the claimed extent, or `None` when no usable hole exists — in which
    /// case the caller is expected to extend the file tail. `len == 0` is a
    /// no-op that returns `None`.
    pub fn reserve(&mut self, len: u32) -> (result: Option<u64>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            result.is_some() ==> len > 0,
            result.is_some() ==> result.unwrap() as int + len as int <= u64::MAX as int,
    {
        let len_u64 = len as u64;
        if len_u64 == 0 {
            return None;
        }
        if self.by_size.contains_key(&len_u64) {
            let mut bucket = self.by_size.remove(&len_u64).unwrap();
            let popped = bucket.pop();
            if !bucket.is_empty() {
                self.by_size.insert(len_u64, bucket);
            }
            if popped.is_some() {
                return popped;
            }
        }
        // No exact-size match: try the smallest bucket in (len, 2*len].
        // `len: u32` so `len_u64 * 2 <= 2 * u32::MAX < u64::MAX`.
        let lo = len_u64 + 1;
        let hi = len_u64 * 2;
        let size = smallest_size_in_range(&self.by_size, lo, hi)?;
        let mut bucket = self.by_size.remove(&size).unwrap();
        proof {
            assert(Self::bucket_wf(size, bucket@));
        }
        let offset = bucket.pop()?;
        if !bucket.is_empty() {
            self.by_size.insert(size, bucket);
        }
        // Push the leftover (offset + len, size - len) back as a smaller hole.
        // Overflow check is defensive — bucket_wf guarantees offset + size
        // <= u64::MAX, and len_u64 < size.
        let remainder_offset = offset.checked_add(len_u64)?;
        let remainder_len = size - len_u64;
        let new_hole = FileHole::new(remainder_offset, remainder_len)?;
        self.release_hole(new_hole);
        Some(offset)
    }

    /// Mirror image of `reserve` for WAL replay: subtract a Live entry's
    /// extent from the free list.
    ///
    /// Looks for `offset` in the exact-size bucket first, then in any larger
    /// bucket up to `2 * length` — that covers the case where the original
    /// `reserve` was satisfied by splitting a bigger hole. When found in a
    /// larger bucket, the `size - length` leftover is pushed back as a new
    /// smaller hole. A miss is a no-op (the extent must have come from
    /// extending the file tail, not from a hole). `length == 0` is a no-op.
    pub fn consume(&mut self, offset: u64, length: u64)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
    {
        if length == 0 {
            return;
        }
        // 1. Exact-size bucket: remove `offset` if present.
        if self.by_size.contains_key(&length) {
            let mut bucket = self.by_size.remove(&length).unwrap();
            let mut found = false;
            let mut idx = 0;
            while idx < bucket.len()
                invariant
                    idx <= bucket.len(),
                    Self::bucket_wf(length, bucket@),
                decreases bucket.len() - idx,
            {
                if bucket[idx] == offset {
                    bucket.swap_remove(idx);
                    found = true;
                    break;
                }
                idx = idx + 1;
            }
            if !bucket.is_empty() {
                self.by_size.insert(length, bucket);
            }
            if found {
                return;
            }
        }
        // 2. Larger bucket in (length, 2*length]: the offset originally came
        //    from a hole that reserve split. Mirror that split.
        let hi = match length.checked_mul(2) {
            Some(v) => v,
            None => return,
        };
        let lo = match length.checked_add(1) {
            Some(v) => v,
            None => return,
        };
        let size = match size_holding_offset(&self.by_size, lo, hi, offset) {
            Some(s) => s,
            None => return,
        };
        let mut bucket = self.by_size.remove(&size).unwrap();
        proof {
            assert(Self::bucket_wf(size, bucket@));
        }
        let mut idx = 0;
        while idx < bucket.len()
            invariant
                idx <= bucket.len(),
                Self::bucket_wf(size, bucket@),
            decreases bucket.len() - idx,
        {
            if bucket[idx] == offset {
                bucket.swap_remove(idx);
                break;
            }
            idx = idx + 1;
        }
        if !bucket.is_empty() {
            self.by_size.insert(size, bucket);
        }
        // Push the remainder back. Overflow check is defensive — bucket_wf
        // guarantees offset + size <= u64::MAX, and length < size.
        let remainder_offset = match offset.checked_add(length) {
            Some(v) => v,
            None => return,
        };
        let remainder_len = size - length;
        let new_hole = match FileHole::new(remainder_offset, remainder_len) {
            Some(h) => h,
            None => return,
        };
        self.release_hole(new_hole);
    }
}

} // verus!
