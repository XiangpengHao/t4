use vstd::prelude::*;

#[cfg(verus_only)]
use crate::PAGE_SIZE_U32;
use crate::PAGE_SIZE_U64;

verus! {

fn align_down_u64(value: u64, alignment: u64) -> (result: u64)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0,

    ensures
        result <= value,
        result & sub(alignment, 1) == 0,
        value - result < alignment,
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
            result.is_none() ==> start + len > u32::MAX,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageWindow {
    first_byte: u32,
    buffer_len: u32,
    start: u32,
    len: u32,
}

impl PageWindow {
    pub(super) closed spec fn spec_first_byte(&self) -> u32 {
        self.first_byte
    }

    pub(super) closed spec fn spec_buffer_len(&self) -> u32 {
        self.buffer_len
    }

    pub(super) closed spec fn spec_len(&self) -> u32 {
        self.len
    }

    pub(super) fn of(range: CheckedRangeU32) -> (window: Option<Self>)
        requires
            range.end == range.start + range.len,
        ensures
            range.end + PAGE_SIZE_U32 <= u32::MAX + 1 ==> window is Some,
            window is Some ==> window->0.len() == range.len,
            window is Some && range.len > 0 ==> {
                let pages = window->0;
                &&& pages.first_byte() % PAGE_SIZE_U32 == 0
                &&& pages.buffer_len() % PAGE_SIZE_U32 == 0
                &&& pages.first_byte() <= range.start
                &&& range.end <= pages.first_byte() + pages.buffer_len() < range.end + PAGE_SIZE_U32
            },
    {
        if range.is_empty() {
            return Some(Self { first_byte: 0, buffer_len: 0, start: 0, len: 0 });
        }
        proof {
            assert(PAGE_SIZE_U64 & sub(PAGE_SIZE_U64, 1) == 0) by (compute_only);
        }
        let first_byte = align_down_u64(u64::from(range.start()), PAGE_SIZE_U64);
        let last_byte = align_up_u64(u64::from(range.end()), PAGE_SIZE_U64)?;
        let buffer_len = u32::try_from(last_byte - first_byte).ok()?;
        proof {
            assert(first_byte % 4096 == 0 && last_byte % 4096 == 0) by (bit_vector)
                requires
                    first_byte & 4095 == 0,
                    last_byte & 4095 == 0,
            ;
        }
        // `first_byte` is at most `range.start()`, a `u32`.
        let first_byte = first_byte as u32;
        Some(Self { first_byte, buffer_len, start: range.start() - first_byte, len: range.len() })
    }

    #[verifier::when_used_as_spec(spec_first_byte)]
    pub(super) fn first_byte(&self) -> (first_byte: u32)
        ensures
            first_byte == self.spec_first_byte(),
    {
        self.first_byte
    }

    #[verifier::when_used_as_spec(spec_buffer_len)]
    pub(super) fn buffer_len(&self) -> (buffer_len: u32)
        ensures
            buffer_len == self.spec_buffer_len(),
    {
        self.buffer_len
    }

    #[verifier::when_used_as_spec(spec_len)]
    pub(super) fn len(&self) -> (len: u32)
        ensures
            len == self.spec_len(),
    {
        self.len
    }
}

} // verus!

impl PageWindow {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn start(&self) -> u32 {
        self.start
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
