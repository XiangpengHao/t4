use vstd::prelude::*;

verus! {

/// Checked conversion used by range planning.
fn u64_to_u32_checked(value: u64) -> (result: Option<u32>)
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

} // verus!
