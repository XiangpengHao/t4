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
    InvalidPageLayout,
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
    ensures
        result == u32_from_le_bytes_seq(bytes@),
{
    u32::from_le_bytes(bytes.try_into().expect("entry bytes must be present"))
}

spec fn u32_from_le_bytes_seq(bytes: Seq<u8>) -> u32
    recommends
        bytes.len() == 4,
{
    (bytes[0] as u32)
        | ((bytes[1] as u32) << 8)
        | ((bytes[2] as u32) << 16)
        | ((bytes[3] as u32) << 24)
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
            result.len() <= u8::MAX as usize,
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
    fn new() -> (result: Self)
        ensures
            result.used_bytes == WAL_PAGE_HEADER_SIZE as u32,
            result.used_bytes <= PAGE_SIZE as u32,
    {
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

fn magic_matches(bytes: &[u8; 4]) -> bool {
    bytes[0] == MAGIC[0] && bytes[1] == MAGIC[1] && bytes[2] == MAGIC[2] && bytes[3] == MAGIC[3]
}

pub struct WalPage {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl WalPage {
    #[verifier::type_invariant]
    spec fn wf(self) -> bool {
        {
            let used = u32_from_le_bytes_seq(
                self.bytes@.subrange(OFF_USED_BYTES as int, OFF_USED_BYTES as int + 4),
            );
            WAL_PAGE_HEADER_SIZE as u32 <= used && used <= PAGE_SIZE as u32
        }
    }

    pub fn empty() -> (result: Result<Self, WalEntryDecodeError>) {
        let mut bytes = Box::new([0_u8;PAGE_SIZE]);
        let header = WalPageHeader::new();

        bytes[OFF_MAGIC + 0] = header.magic[0];
        bytes[OFF_MAGIC + 1] = header.magic[1];
        bytes[OFF_MAGIC + 2] = header.magic[2];
        bytes[OFF_MAGIC + 3] = header.magic[3];
        write_u16_le(&mut bytes, OFF_VERSION, header.version);
        write_u64_le(&mut bytes, OFF_NEXT_PAGE, header.next_page);
        write_u32_le(&mut bytes, OFF_ENTRY_COUNT, header.entry_count);
        write_u32_le(&mut bytes, OFF_USED_BYTES, WAL_PAGE_HEADER_SIZE as u32);
        write_u64_le(&mut bytes, OFF_LSN, header.lsn);

        Self::from_bytes(bytes)
    }

    pub fn from_bytes(src: Box<[u8; PAGE_SIZE]>) -> (result: Result<Self, WalEntryDecodeError>)
        requires
            src.len() == PAGE_SIZE,
    {
        let used_bytes = u32_from_le_bytes(
            slice_subrange(src.as_slice(), OFF_USED_BYTES, OFF_USED_BYTES + 4),
        );
        if used_bytes < WAL_PAGE_HEADER_SIZE as u32 || used_bytes > PAGE_SIZE as u32 {
            return Err(WalEntryDecodeError::InvalidPageLayout);
        }

        let page = Self { bytes: src };

        let magic = page.magic();
        if !magic_matches(&magic) {
            return Err(WalEntryDecodeError::InvalidPageLayout);
        }

        let version = page.version();
        if version != VERSION {
            return Err(WalEntryDecodeError::InvalidPageLayout);
        }

        let used_bytes = page.used_bytes() as usize;
        let entry_count = page.entry_count();
        let mut cursor = WAL_PAGE_HEADER_SIZE;

        let mut i = 0;
        while i < entry_count
            invariant
                WAL_PAGE_HEADER_SIZE <= cursor <= used_bytes,
                used_bytes <= page.bytes@.len(),
            decreases entry_count - i,
        {
            let tail = slice_subrange(page.bytes.as_slice(), cursor, used_bytes);
            let (entry, consumed) = WalEntryRef::decode_from(tail)?;

            cursor = cursor + consumed;
            if cursor > used_bytes {
                return Err(WalEntryDecodeError::InvalidPageLayout);
            }
            let _ = entry;
            i = i + 1;
        }

        if cursor != used_bytes {
            return Err(WalEntryDecodeError::InvalidPageLayout);
        }

        Ok(page)
    }

    fn decode_header(&self) -> WalPageHeader {
        let used_bytes = self.used_bytes();
        let version = self.version();
        let magic = self.magic();
        let entry_count = self.entry_count();
        let lsn = self.lsn();
        let next_page = self.next_page();
        WalPageHeader {
            magic: magic,
            version: version,
            next_page: next_page,
            entry_count: entry_count,
            used_bytes: used_bytes,
            lsn: lsn,
        }
    }

    fn next_page(&self) -> u64 {
        u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_NEXT_PAGE, OFF_NEXT_PAGE + 8))
    }

    fn lsn(&self) -> u64 {
        u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_LSN, OFF_LSN + 8))
    }

    fn entry_count(&self) -> u32 {
        u32_from_le_bytes(
            slice_subrange(self.bytes.as_slice(), OFF_ENTRY_COUNT, OFF_ENTRY_COUNT + 4),
        )
    }

    fn magic(&self) -> [u8; 4] {
        [
            self.bytes[OFF_MAGIC + 0],
            self.bytes[OFF_MAGIC + 1],
            self.bytes[OFF_MAGIC + 2],
            self.bytes[OFF_MAGIC + 3],
        ]
    }

    fn used_bytes(&self) -> (result: u32)
    ensures
        WAL_PAGE_HEADER_SIZE as u32 <= result,
        result <= PAGE_SIZE as u32,
    {
        proof { use_type_invariant(&*self); }
        u32_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_USED_BYTES, OFF_USED_BYTES + 4))
    }

    fn version(&self) -> u16 {
        u16_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_VERSION, OFF_VERSION + 2))
    }

    // fn append(&mut self, entry: &AppendEntry, lsn: u64) -> Result<bool, WalEntryDecodeError>
    //     requires
    //         match entry {
    //             AppendEntry::Live { key, .. } => key.wf(),
    //             AppendEntry::Tombstone { key } => key.wf(),
    //         },
    // {
    //     let start = self.used_bytes() as usize;
    //     let end = start + entry.encoded_len();
    //     if end > PAGE_SIZE {
    //         return Ok(false);
    //     }
    //     let key = entry.key_bytes();
    //     let key_len = key.len() as u16;

    //     write_u16_le(&mut self.bytes, start, key_len);
    //     self.bytes[start + 2] = entry.flags();
    //     self.bytes[start + 3] = 0;
    //     write_u64_le(&mut self.bytes, start + 4, entry.offset());
    //     write_u32_le(&mut self.bytes, start + 12, entry.length());
    //     write_u64_le(&mut self.bytes, start + 16, lsn);
    //     copy_into_page(&mut self.bytes, start + 24, key);

    //     let next_entry_count = self.entry_count() + 1;
    //     self.set_entry_count(next_entry_count);
    //     self.set_used_bytes(end as u32);

    //     Ok(true)
    // }
}

// fn copy_into_page(bytes: &mut [u8; PAGE_SIZE], off: usize, src: &[u8])
//     requires
//         off + src.len() <= PAGE_SIZE,
// {
//     let mut i = 0;

//     while i < src.len()
//         invariant
//             off + src.len() <= PAGE_SIZE,
//             i <= src.len(),
//         decreases src.len() - i,
//     {
//         bytes[off + i] = src[i];
//         i = i + 1;
//     }
// }

} // verus!
