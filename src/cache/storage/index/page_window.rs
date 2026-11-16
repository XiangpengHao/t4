use vstd::prelude::*;

use crate::PAGE_SIZE_U64;
use crate::io::error::{Error, Result};

verus! {

/// Rounds `value` down to the nearest multiple of `alignment`.
/// `alignment` must be a power of two.
fn align_down_u64(value: u64, alignment: u64) -> (result: u64)
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
fn align_up_u64(value: u64, alignment: u64) -> (result: Option<u64>)
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

/// A byte range, `start` and `len`, whose `end` fits a `u32`.
#[derive(Clone, Copy)]
pub(super) struct RangeRequestU32 {
    pub(super) start: u32,
    pub(super) len: u32,
    pub(super) end: u32,
}

impl RangeRequestU32 {
    pub(super) fn from_u32(start: u32, len: u32) -> (result: Option<Self>)
        ensures
            result.is_some() ==> result.unwrap().start == start,
            result.is_some() ==> result.unwrap().len == len,
            result.is_some() ==> result.unwrap().end >= start,
            result.is_some() ==> result.unwrap().end - start == len,
    {
        let end = start.checked_add(len)?;
        Some(Self { start, len, end })
    }

    pub(super) fn checked_against(self, upper_bound: u32) -> (result: Option<CheckedRangeU32>)
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
}

#[derive(Clone, Copy)]
pub(super) struct CheckedRangeU32 {
    pub(super) start: u32,
    pub(super) len: u32,
    pub(super) end: u32,
}

impl CheckedRangeU32 {
    fn start(self) -> (result: u32)
        ensures
            result == self.start,
    {
        self.start
    }

    fn len(self) -> (result: u32)
        ensures
            result == self.len,
    {
        self.len
    }

    fn end(self) -> (result: u32)
        ensures
            result == self.end,
    {
        self.end
    }

    fn is_empty(self) -> (result: bool)
        ensures
            result <==> self.len == 0,
    {
        self.len == 0
    }
}

} // verus!

/// The pages one read covers. The bytes asked for, a range of a page-aligned
/// extent (a chunk), lie inside a page-aligned window of it, read whole into
/// a buffer of exactly the window's size, as direct I/O needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageWindow {
    /// Where the window starts in the extent: a page boundary.
    first_byte: u32,
    /// The window's size, in whole pages; 0 when nothing is asked for.
    buffer_len: u32,
    /// Where the bytes asked for start in the window.
    start: u32,
    len: u32,
}

impl PageWindow {
    /// The window for `range` of an extent.
    pub(super) fn of(range: CheckedRangeU32) -> Result<Self> {
        if range.is_empty() {
            return Ok(Self {
                first_byte: 0,
                buffer_len: 0,
                start: 0,
                len: 0,
            });
        }
        let first_byte = align_down_u64(u64::from(range.start()), PAGE_SIZE_U64);
        let last_byte =
            align_up_u64(u64::from(range.end()), PAGE_SIZE_U64).ok_or(Error::RangeOutOfBounds)?;
        let buffer_len =
            u32::try_from(last_byte - first_byte).map_err(|_| Error::RangeOutOfBounds)?;
        // `first_byte` is at most `range.start()`, a `u32`.
        let first_byte = first_byte as u32;
        Ok(Self {
            first_byte,
            buffer_len,
            start: range.start() - first_byte,
            len: range.len(),
        })
    }

    /// Where the window starts in the extent: a page boundary.
    pub(super) fn first_byte(&self) -> u32 {
        self.first_byte
    }

    pub(super) fn buffer_len(&self) -> u32 {
        self.buffer_len
    }

    /// Where the bytes asked for start in the window: for the read plan's
    /// tests, which `shuttle` builds leave out.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn start(&self) -> u32 {
        self.start
    }

    pub(super) fn len(&self) -> u32 {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PAGE_SIZE;

    const PAGE: u32 = PAGE_SIZE as u32;

    fn window(start: u32, len: u32) -> PageWindow {
        let range = RangeRequestU32::from_u32(start, len).unwrap();
        PageWindow::of(range.checked_against(u32::MAX).unwrap()).unwrap()
    }

    #[test]
    fn windows_cover_whole_pages_around_the_range() {
        let expect = |first_byte, buffer_len, start, len| PageWindow {
            first_byte,
            buffer_len,
            start,
            len,
        };
        assert_eq!(window(0, 0), expect(0, 0, 0, 0));
        assert_eq!(window(9000, 0), expect(0, 0, 0, 0));
        assert_eq!(window(0, 1), expect(0, PAGE, 0, 1));
        assert_eq!(window(0, PAGE), expect(0, PAGE, 0, PAGE));
        assert_eq!(window(PAGE - 1, 2), expect(0, 2 * PAGE, PAGE - 1, 2));
        assert_eq!(window(PAGE + 5, 10), expect(PAGE, PAGE, 5, 10));
        assert_eq!(window(PAGE + 5, 10).first_byte(), PAGE);
    }
}
