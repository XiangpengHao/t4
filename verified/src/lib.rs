pub mod input_kv;
pub mod wal;

pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: [u8; 4] = *b"BTF4";
pub const VERSION: u16 = 3;

use vstd::prelude::*;

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

/// Rounds `value` up to the nearest multiple of `alignment`.
/// Returns `None` on overflow.
/// `alignment` must be a power of two.
pub fn align_up_u32(value: u32, alignment: u32) -> (result: Option<u32>)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0,
    ensures
        result.is_some() ==> result.unwrap() >= value,
        result.is_some() ==> result.unwrap() & sub(alignment, 1) == 0,
        result.is_some() ==> result.unwrap() - value < alignment,
        result.is_some() ==> (value & sub(alignment, 1) == 0 ==> result.unwrap() == value),
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
        assert(result & sub(alignment, 1) == 0u32) by (bit_vector)
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

/// Mirrors key-length checks in `t4::types`.
pub fn key_len_fits_u16(len: usize) -> (fits: bool)
    ensures
        fits <==> len <= u16::MAX as usize,
{
    len <= u16::MAX as usize
}

/// Mirrors value-length checks in `t4::types`.
pub fn value_len_fits_u32(len: usize) -> (fits: bool)
    ensures
        fits <==> len <= u32::MAX as usize,
{
    len <= u32::MAX as usize
}

/// Checked conversion used by range planning.
pub fn u64_to_u32_checked(value: u64) -> (result: Option<u32>)
    ensures
        result.is_some() ==> value <= u32::MAX as u64,
        result.is_none() ==> value > u32::MAX as u64,
{
    if value > u32::MAX as u64 {
        None
    } else {
        Some(value as u32)
    }
}

/// Arithmetic model of `RangeRequest::new`.
#[derive(Clone, Copy)]
pub struct RangeRequestU32 {
    pub start: u32,
    pub len: u32,
    pub end: u32,
}

impl RangeRequestU32 {
    pub fn from_u32(start: u32, len: u32) -> (result: Option<Self>)
        ensures
            result.is_some() ==> result.unwrap().start == start,
            result.is_some() ==> result.unwrap().len == len,
            result.is_some() ==> result.unwrap().end >= start,
            result.is_some() ==> result.unwrap().end - start == len,
    {
        let end = start.checked_add(len)?;

        proof {
            assert(end >= start) by (bit_vector)
                requires
                    end == add(start, len),
                    end >= start,
            ;
            assert(end - start == len) by (bit_vector)
                requires
                    end == add(start, len),
                    end >= start,
            ;
        }

        Some(Self { start, len, end })
    }

    pub fn from_u64(start: u64, len: u64) -> (result: Option<Self>)
        ensures
            result.is_some() ==> result.unwrap().end >= result.unwrap().start,
            result.is_some() ==> result.unwrap().end - result.unwrap().start == result.unwrap().len,
    {
        let start = u64_to_u32_checked(start)?;
        let len = u64_to_u32_checked(len)?;
        Self::from_u32(start, len)
    }

    pub fn checked_against(self, upper_bound: u32) -> (result: Option<CheckedRangeU32>)
        ensures
            result.is_some() ==> result.unwrap().start == self.start,
            result.is_some() ==> result.unwrap().len == self.len,
            result.is_some() ==> result.unwrap().end == self.end,
            result.is_some() ==> result.unwrap().end <= upper_bound,
            result.is_none() ==> self.end > upper_bound,
    {
        if self.end > upper_bound {
            return None;
        }
        Some(CheckedRangeU32 { start: self.start, len: self.len, end: self.end })
    }

    pub fn start(self) -> (result: u32)
        ensures
            result == self.start,
    {
        self.start
    }

    pub fn len(self) -> (result: u32)
        ensures
            result == self.len,
    {
        self.len
    }

    pub fn is_empty(self) -> (result: bool)
        ensures
            result <==> self.len == 0,
    {
        self.len == 0
    }

    pub fn end(self) -> (result: u32)
        ensures
            result == self.end,
    {
        self.end
    }
}

#[derive(Clone, Copy)]
pub struct CheckedRangeU32 {
    pub start: u32,
    pub len: u32,
    pub end: u32,
}

impl CheckedRangeU32 {
    pub fn start(self) -> (result: u32)
        ensures
            result == self.start,
    {
        self.start
    }

    pub fn len(self) -> (result: u32)
        ensures
            result == self.len,
    {
        self.len
    }

    pub fn end(self) -> (result: u32)
        ensures
            result == self.end,
    {
        self.end
    }

    pub fn is_empty(self) -> (result: bool)
        ensures
            result <==> self.len == 0,
    {
        self.len == 0
    }
}

#[derive(Clone, Copy)]
pub struct SpaceReservation {
    pub offset: u64,
    pub next_tail: u64,
    pub len: u32,
}

/// Arithmetic model of `Wal::reserve_space_locked`.
pub fn reserve_space(file_tail: u64, len: u32) -> (result: Option<SpaceReservation>)
    ensures
        result.is_some() ==> result.unwrap().offset == file_tail,
        result.is_some() ==> result.unwrap().len == len,
        result.is_some() ==> result.unwrap().next_tail >= file_tail,
        result.is_some() ==> result.unwrap().next_tail - file_tail == len as u64,
{
    let next_tail = file_tail.checked_add(len as u64)?;

    proof {
        assert(next_tail >= file_tail) by (bit_vector)
            requires
                next_tail == add(file_tail, len as u64),
                next_tail >= file_tail,
        ;
        assert(next_tail - file_tail == len as u64) by (bit_vector)
            requires
                next_tail == add(file_tail, len as u64),
                next_tail >= file_tail,
        ;
    }

    Some(SpaceReservation { offset: file_tail, next_tail, len })
}

/// Arithmetic model for advancing `next_lsn`.
pub fn allocate_next_lsn(next_lsn: u64) -> (result: Option<u64>)
    ensures
        result.is_some() ==> result.unwrap() > next_lsn,
        result.is_some() ==> result.unwrap() - next_lsn == 1u64,
        result.is_none() ==> next_lsn == u64::MAX,
{
    let next = next_lsn.checked_add(1)?;

    proof {
        assert(next > next_lsn) by (bit_vector)
            requires
                next == add(next_lsn, 1),
                next > next_lsn,
        ;
        assert(next - next_lsn == 1u64) by (bit_vector)
            requires
                next == add(next_lsn, 1),
                next > next_lsn,
        ;
    }

    Some(next)
}

} // verus!
