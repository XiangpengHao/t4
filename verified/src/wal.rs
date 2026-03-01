use vstd::prelude::*;
use vstd::slice::slice_subrange;

verus! {

pub const ENTRY_HEADER_SIZE: usize = 24;

#[derive(Debug)]
pub enum WalEntryDecodeError {
    Truncated,
    KeyTruncated,
}

fn u16_from_le_bytes(bytes: &[u8]) -> (result: u16)
    requires
        bytes.len() == 2,
    ensures
        result == (bytes[0] as usize | (bytes[1] as usize) << 8) as u16,
{
    (bytes[0] as usize | (bytes[1] as usize) << 8) as u16
}

#[verifier::external_body]
fn u64_from_le_bytes(bytes: &[u8]) -> (result: u64)
    requires
        bytes.len() == 8,
{
    u64::from_le_bytes(bytes.try_into().expect("entry bytes must be present"))
}

#[verifier::external_body]
fn u32_from_le_bytes(bytes: &[u8]) -> (result: u32)
    requires
        bytes.len() == 4,
{
    u32::from_le_bytes(bytes.try_into().expect("entry bytes must be present"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEntryRef<'a> {
    bytes: &'a [u8],
}

impl<'a> WalEntryRef<'a> {
    pub fn decode_from(src: &'a [u8]) -> (result: Result<(Self, usize), WalEntryDecodeError>)
        ensures
            result.is_ok() ==> {
                let consumed = result.unwrap().1;
                consumed >= ENTRY_HEADER_SIZE && consumed <= src.len() && result.unwrap().0.wf()
            },
            result.is_err() ==> true,
    {
        if src.len() < ENTRY_HEADER_SIZE {
            return Err(WalEntryDecodeError::Truncated);
        }
        let key_len = u16_from_le_bytes(slice_subrange(src, 0, 2)) as usize;
        let total = ENTRY_HEADER_SIZE + key_len;
        if src.len() < total {
            return Err(WalEntryDecodeError::KeyTruncated);
        }
        Ok((Self { bytes: slice_subrange(src, 0, total) }, total))
    }

    pub closed spec fn wf(self) -> bool {
        self.bytes@.len() >= ENTRY_HEADER_SIZE as int
    }

    pub fn flags(self) -> u8
        requires
            self.wf(),
    {
        self.bytes[2]
    }

    pub fn offset(self) -> u64
        requires
            self.wf(),
    {
        u64_from_le_bytes(slice_subrange(self.bytes, 4, 12))
    }

    pub fn length(self) -> u32
        requires
            self.wf(),
    {
        u32_from_le_bytes(slice_subrange(self.bytes, 12, 16))
    }

    pub fn lsn(self) -> u64
        requires
            self.wf(),
    {
        u64_from_le_bytes(slice_subrange(self.bytes, 16, 24))
    }

    pub fn key_bytes(self) -> &'a [u8]
        requires
            self.wf(),
    {
        slice_subrange(self.bytes, ENTRY_HEADER_SIZE, self.bytes.len())
    }
}

} // verus!
