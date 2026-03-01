use std::ptr::slice_from_raw_parts;

use vstd::prelude::*;
use vstd::slice::slice_subrange;

use crate::input_kv::T4Key;
use crate::PAGE_SIZE;

const _: [(); WAL_PAGE_HEADER_SIZE] = [(); std::mem::size_of::<WalPageHeader>()];

verus! {

const FLAG_LIVE: u8 = 0;

const FLAG_TOMBSTONE: u8 = 1;

pub const ENTRY_HEADER_SIZE: usize = 24;

const WAL_PAGE_HEADER_SIZE: usize = 32;

pub const MAGIC: [u8; 4] = [0x42, 0x54, 0x46, 0x34];

pub const VERSION: u16 = 3;

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

pub enum AppendEntry {
    Live { key: T4Key, offset: u64, length: u32 },
    Tombstone { key: T4Key },
}

impl AppendEntry {
    pub fn encoded_len(&self) -> usize
        requires
            match self {
                Self::Live { key, .. } => key.wf(),
                Self::Tombstone { key } => key.wf(),
            },
    {
        ENTRY_HEADER_SIZE + self.key_bytes().len()
    }

    pub fn key_bytes(&self) -> (result: &[u8])
        requires
            match self {
                Self::Live { key, .. } => key.wf(),
                Self::Tombstone { key } => key.wf(),
            },
        ensures
            result.len() <= u16::MAX as usize,
    {
        match self {
            Self::Live { key, .. } | Self::Tombstone { key } => key.as_bytes(),
        }
    }

    pub fn flags(&self) -> u8 {
        match self {
            Self::Live { .. } => FLAG_LIVE,
            Self::Tombstone { .. } => FLAG_TOMBSTONE,
        }
    }

    pub fn offset(&self) -> u64 {
        match self {
            Self::Live { offset, .. } => *offset,
            Self::Tombstone { .. } => 0,
        }
    }

    pub fn length(&self) -> u32 {
        match self {
            Self::Live { length, .. } => *length,
            Self::Tombstone { .. } => 0,
        }
    }
}

#[repr(C)]
struct WalPageHeader {
    magic: [u8; 4],
    version: u16,
    next_page: u64,
    entry_count: u32,
    used_bytes: u32,
    lsn: u64,
}

impl WalPageHeader {
    pub fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            next_page: 0,
            entry_count: 0,
            used_bytes: WAL_PAGE_HEADER_SIZE as u32,
            lsn: 0,
        }
    }
}

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_NEXT_PAGE: usize = 8;
const OFF_ENTRY_COUNT: usize = 16;
const OFF_USED_BYTES: usize = 20;
const OFF_LSN: usize = 24;



fn write_u16_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u16)
    requires
        off + 1 < PAGE_SIZE,
{
    bytes[off] = (v & 0x00ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x00ff) as u8;
}

fn write_u32_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u32)
    requires
        off + 3 < PAGE_SIZE,
{
    bytes[off] = (v & 0x000000ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x000000ff) as u8;
    bytes[off + 2] = ((v >> 16) & 0x000000ff) as u8;
    bytes[off + 3] = ((v >> 24) & 0x000000ff) as u8;
}

fn write_u64_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u64)
    requires
        off + 7 < PAGE_SIZE,
{
    bytes[off] = (v & 0x00000000000000ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x00000000000000ff) as u8;
    bytes[off + 2] = ((v >> 16) & 0x00000000000000ff) as u8;
    bytes[off + 3] = ((v >> 24) & 0x00000000000000ff) as u8;
    bytes[off + 4] = ((v >> 32) & 0x00000000000000ff) as u8;
    bytes[off + 5] = ((v >> 40) & 0x00000000000000ff) as u8;
    bytes[off + 6] = ((v >> 48) & 0x00000000000000ff) as u8;
    bytes[off + 7] = ((v >> 56) & 0x00000000000000ff) as u8;
}

pub struct WalPage {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl WalPage {
    pub fn empty() -> Self {
        let mut page = Self { bytes: Box::new([0_u8;PAGE_SIZE]) };
        let header = WalPageHeader::new();
        page.encode_header(&header);
        page
    }

    fn encode_header(&mut self, h: &WalPageHeader) {
        self.bytes[OFF_MAGIC + 0] = h.magic[0];
        self.bytes[OFF_MAGIC + 1] = h.magic[1];
        self.bytes[OFF_MAGIC + 2] = h.magic[2];
        self.bytes[OFF_MAGIC + 3] = h.magic[3];
        write_u16_le(&mut self.bytes, OFF_VERSION, h.version);
        write_u64_le(&mut self.bytes, OFF_NEXT_PAGE, h.next_page);
        write_u32_le(&mut self.bytes, OFF_ENTRY_COUNT, h.entry_count);
        write_u32_le(&mut self.bytes, OFF_USED_BYTES, h.used_bytes);
        write_u64_le(&mut self.bytes, OFF_LSN, h.lsn);
    }

    fn decode_header(&self) -> WalPageHeader {
        WalPageHeader {
            magic: [self.bytes[OFF_MAGIC + 0], self.bytes[OFF_MAGIC + 1], self.bytes[OFF_MAGIC + 2], self.bytes[OFF_MAGIC + 3]],
            version: u16_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_VERSION, OFF_VERSION + 2)),
            next_page: u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_NEXT_PAGE, OFF_NEXT_PAGE + 8)),
            entry_count: u32_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_ENTRY_COUNT, OFF_ENTRY_COUNT + 4)),
            used_bytes: u32_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_USED_BYTES, OFF_USED_BYTES + 4)),
            lsn: u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_LSN, OFF_LSN + 8)),
        }
    }
}

} // verus!
