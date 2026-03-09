use vstd::prelude::*;
use vstd::slice::slice_subrange;

use crate::input_kv::{T4Key, T4KeyRef};
#[cfg(verus_only)]
use crate::le_bytes::u32_from_4;
use crate::le_bytes::{
    u16_from_le_bytes, u32_from_le_bytes, u64_from_le_bytes, write_u16_le, write_u32_le,
    write_u64_le,
};
use crate::{PAGE_SIZE, copy_into_page};

const _: [(); WAL_PAGE_HEADER_SIZE] = [(); std::mem::size_of::<WalPageHeader>()];

verus! {

pub const FLAG_LIVE: u8 = 0;

pub const FLAG_TOMBSTONE: u8 = 1;

pub const ENTRY_HEADER_SIZE: usize = 24;

pub const WAL_PAGE_HEADER_SIZE: usize = 32;

pub const MAGIC: [u8; 4] = [0x42, 0x54, 0x46, 0x34];

pub const VERSION: u16 = 3;

#[derive(Debug)]
pub enum WalError {
    Truncated,
    KeyTruncated,
    KeyTooLarge,
    InvalidPageLayout,
    InsufficientSpace,
    InvalidFlags,
}

pub(crate) enum WalEntryState {
    Live,
    Tombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEntryRef<'a> {
    pub(crate) flags: u8,
    pub(crate) offset: u64,
    pub(crate) value_length: u32,
    pub(crate) lsn: u64,
    pub(crate) key: T4KeyRef<'a>,
}

impl<'a> WalEntryRef<'a> {
    pub open spec fn flags_wf(flags: u8) -> bool {
        flags == FLAG_LIVE || flags == FLAG_TOMBSTONE
    }

    pub open spec fn encoded_entry_wf(bytes: Seq<u8>, cursor: int, used: int) -> bool {
        &&cursor + ENTRY_HEADER_SIZE as int <= used <= bytes.len() && Self::key_len_of(
            bytes.subrange(cursor, used),
        ) as int <= u8::MAX as int && cursor + ENTRY_HEADER_SIZE as int + Self::key_len_of(
            bytes.subrange(cursor, used),
        ) as int <= used && Self::flags_wf(bytes[cursor + 2])
    }

    pub open spec fn encoded_entry_len(bytes: Seq<u8>, cursor: int, used: int) -> int
        recommends
            Self::encoded_entry_wf(bytes, cursor, used),
    {
        ENTRY_HEADER_SIZE as int + Self::key_len_of(bytes.subrange(cursor, used)) as int
    }

    pub fn try_decode_from(src: &'a [u8]) -> (result: Result<(Self, usize), WalError>)
        ensures
            result.is_ok() ==> {
                let consumed = result.unwrap().1;
                consumed >= ENTRY_HEADER_SIZE && consumed <= src.len() && result.unwrap().0.wf()
            },
    {
        if src.len() < ENTRY_HEADER_SIZE {
            return Err(WalError::Truncated);
        }
        let key_len = u16_from_le_bytes(slice_subrange(src, 0, 2)) as usize;
        let total = ENTRY_HEADER_SIZE + key_len;
        if src.len() < total {
            return Err(WalError::KeyTruncated);
        }
        if key_len > u8::MAX as usize {
            return Err(WalError::KeyTooLarge);
        }
        let flags = src[2];
        if flags != FLAG_LIVE && flags != FLAG_TOMBSTONE {
            return Err(WalError::InvalidFlags);
        }
        Ok(Self::decode_from(src))
    }

    #[verifier::type_invariant]
    pub closed spec fn wf(self) -> bool {
        self.key.wf() && Self::flags_wf(self.flags)
    }

    pub closed spec fn key_len_of(src: Seq<u8>) -> u16
        recommends
            src.len() >= 2,
    {
        ((src[0] as usize) | ((src[1] as usize) << 8)) as u16
    }

    pub(crate) fn decode_from(src: &'a [u8]) -> (result: (Self, usize))
        requires
            src@.len() >= ENTRY_HEADER_SIZE as int,
            Self::key_len_of(src@) as int <= u8::MAX as int,
            ENTRY_HEADER_SIZE as int + Self::key_len_of(src@) as int <= src@.len(),
            Self::flags_wf(src@[2]),
        ensures
            result.0.wf(),
            result.1 as int == ENTRY_HEADER_SIZE as int + Self::key_len_of(src@) as int,
            result.1 >= ENTRY_HEADER_SIZE,
            result.1 <= src.len(),
    {
        let key_len = u16_from_le_bytes(slice_subrange(src, 0, 2)) as usize;
        let total = ENTRY_HEADER_SIZE + key_len;
        let flags = src[2];
        let offset = u64_from_le_bytes(slice_subrange(src, 4, 12));
        let value_length = u32_from_le_bytes(slice_subrange(src, 12, 16));
        let lsn = u64_from_le_bytes(slice_subrange(src, 16, 24));
        let key = T4KeyRef::from_slice(slice_subrange(src, ENTRY_HEADER_SIZE, total));
        (Self { flags, offset, value_length, lsn, key }, total)
    }

    pub(crate) fn state(&self) -> WalEntryState {
        proof {
            use_type_invariant(&self);
        }
        if self.flags == FLAG_LIVE {
            WalEntryState::Live
        } else {
            assert(self.flags == FLAG_TOMBSTONE);
            WalEntryState::Tombstone
        }
    }
}

pub enum AppendEntry {
    Live { key: T4Key, offset: u64, length: u32 },
    Tombstone { key: T4Key },
}

impl AppendEntry {
    pub fn encoded_len(&self) -> (result: usize)
        ensures
            ENTRY_HEADER_SIZE <= result && result <= PAGE_SIZE,
    {
        ENTRY_HEADER_SIZE + self.key_bytes().len()
    }

    pub fn key_bytes(&self) -> (result: &[u8])
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
            result.entry_count == 0,
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

pub const OFF_ENTRY_COUNT: usize = 16;

pub const OFF_USED_BYTES: usize = 20;

const OFF_LSN: usize = 24;

fn magic_matches(bytes: &[u8; 4]) -> bool {
    bytes[0] == MAGIC[0] && bytes[1] == MAGIC[1] && bytes[2] == MAGIC[2] && bytes[3] == MAGIC[3]
}

#[derive(Debug)]
pub struct WalPage {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl WalPage {
    pub closed spec fn entry_count_spec(self) -> u32 {
        u32_from_4(
            self.bytes@[OFF_ENTRY_COUNT as int],
            self.bytes@[OFF_ENTRY_COUNT as int + 1],
            self.bytes@[OFF_ENTRY_COUNT as int + 2],
            self.bytes@[OFF_ENTRY_COUNT as int + 3],
        )
    }

    pub closed spec fn used_bytes_spec(self) -> u32 {
        u32_from_4(
            self.bytes@[OFF_USED_BYTES as int],
            self.bytes@[OFF_USED_BYTES as int + 1],
            self.bytes@[OFF_USED_BYTES as int + 2],
            self.bytes@[OFF_USED_BYTES as int + 3],
        )
    }

    pub closed spec fn wf(self) -> bool {
        let used = self.used_bytes_spec() as int;
        WAL_PAGE_HEADER_SIZE as int <= used <= PAGE_SIZE as int && self.entry_count_spec() as int
            * ENTRY_HEADER_SIZE as int <= used && Self::entries_wf(
            self.bytes@,
            WAL_PAGE_HEADER_SIZE as int,
            self.entry_count_spec() as nat,
            used,
        )
    }

    spec fn iter_suffix_wf(self, cursor: int, remaining: nat) -> bool {
        let used = self.used_bytes_spec() as int;
        cursor <= used <= PAGE_SIZE as int && Self::entries_wf(self.bytes@, cursor, remaining, used)
    }

    pub open spec fn entries_wf(bytes: Seq<u8>, cursor: int, remaining: nat, used: int) -> bool
        decreases remaining,
    {
        if remaining == 0 {
            cursor == used
        } else {
            WalEntryRef::encoded_entry_wf(bytes, cursor, used) && Self::entries_wf(
                bytes,
                cursor + WalEntryRef::encoded_entry_len(bytes, cursor, used),
                (remaining - 1) as nat,
                used,
            )
        }
    }

    fn entries_wf_exec(bytes: &[u8], cursor: usize, remaining: u32, used: usize) -> (ok: bool)
        requires
            used <= bytes.len(),
            cursor <= used,
        ensures
            ok ==> Self::entries_wf(bytes@, cursor as int, remaining as nat, used as int),
        decreases remaining,
    {
        if remaining == 0 {
            return cursor == used;
        }
        if used - cursor < ENTRY_HEADER_SIZE {
            return false;
        }
        let tail = slice_subrange(bytes, cursor, used);
        let key_bytes = slice_subrange(tail, 0, 2);
        let key_len = u16_from_le_bytes(key_bytes) as usize;
        if key_len > u8::MAX as usize {
            return false;
        }
        if tail[2] != FLAG_LIVE && tail[2] != FLAG_TOMBSTONE {
            return false;
        }
        let consumed = ENTRY_HEADER_SIZE + key_len;
        if consumed > tail.len() {
            return false;
        }
        let next = cursor + consumed;
        let ok = Self::entries_wf_exec(bytes, next, remaining - 1, used);
        if !ok {
            return false;
        }
        true
    }

    pub fn empty() -> (result: Self)
        ensures
            result.wf(),
    {
        let mut bytes = Box::new([0_u8;PAGE_SIZE]);
        let header = WalPageHeader::new();

        bytes[OFF_MAGIC] = header.magic[0];
        bytes[OFF_MAGIC + 1] = header.magic[1];
        bytes[OFF_MAGIC + 2] = header.magic[2];
        bytes[OFF_MAGIC + 3] = header.magic[3];
        write_u16_le(&mut bytes, OFF_VERSION, header.version);
        write_u64_le(&mut bytes, OFF_NEXT_PAGE, header.next_page);
        write_u64_le(&mut bytes, OFF_LSN, header.lsn);
        write_u32_le(&mut bytes, OFF_USED_BYTES, WAL_PAGE_HEADER_SIZE as u32);
        write_u32_le(&mut bytes, OFF_ENTRY_COUNT, header.entry_count);
        Self { bytes }
    }

    pub fn from_bytes(src: Box<[u8; PAGE_SIZE]>) -> (result: Result<Self, WalError>)
        ensures
            result.is_ok() ==> result.unwrap().wf(),
            result.is_ok() ==> result.unwrap().entry_count_spec() == u32_from_4(
                src@[OFF_ENTRY_COUNT as int],
                src@[OFF_ENTRY_COUNT as int + 1],
                src@[OFF_ENTRY_COUNT as int + 2],
                src@[OFF_ENTRY_COUNT as int + 3],
            ),
            result.is_ok() ==> result.unwrap().used_bytes_spec() == u32_from_4(
                src@[OFF_USED_BYTES as int],
                src@[OFF_USED_BYTES as int + 1],
                src@[OFF_USED_BYTES as int + 2],
                src@[OFF_USED_BYTES as int + 3],
            ),
    {
        let page = Self { bytes: src };

        let used_bytes_u32 = u32_from_le_bytes(
            slice_subrange(page.bytes.as_slice(), OFF_USED_BYTES, OFF_USED_BYTES + 4),
        );
        if used_bytes_u32 < WAL_PAGE_HEADER_SIZE as u32 || used_bytes_u32 > PAGE_SIZE as u32 {
            return Err(WalError::InvalidPageLayout);
        }
        let magic = page.magic();
        if !magic_matches(&magic) {
            return Err(WalError::InvalidPageLayout);
        }
        let version = page.version();
        if version != VERSION {
            return Err(WalError::InvalidPageLayout);
        }
        let used_bytes = used_bytes_u32 as usize;
        let entry_count = u32_from_le_bytes(
            slice_subrange(page.bytes.as_slice(), OFF_ENTRY_COUNT, OFF_ENTRY_COUNT + 4),
        );
        if entry_count as usize > used_bytes / ENTRY_HEADER_SIZE {
            return Err(WalError::InvalidPageLayout);
        }
        let ok = Self::entries_wf_exec(
            page.bytes.as_slice(),
            WAL_PAGE_HEADER_SIZE,
            entry_count,
            used_bytes,
        );
        if !ok {
            return Err(WalError::InvalidPageLayout);
        }
        Ok(page)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    #[allow(dead_code)]
    fn decode_header(&self) -> WalPageHeader
        requires
            self.wf(),
    {
        let used_bytes = self.used_bytes();
        let version = self.version();
        let magic = self.magic();
        let entry_count = self.entry_count();
        let lsn = self.lsn();
        let next_page = self.next_page();
        WalPageHeader { magic, version, next_page, entry_count, used_bytes, lsn }
    }

    pub(crate) fn next_page(&self) -> u64 {
        u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_NEXT_PAGE, OFF_NEXT_PAGE + 8))
    }

    pub fn set_next_page(&mut self, next_page: u64) {
        write_u64_le(&mut self.bytes, OFF_NEXT_PAGE, next_page);
    }

    pub fn lsn(&self) -> u64 {
        u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_LSN, OFF_LSN + 8))
    }

    pub fn can_fit(&self, entry: &AppendEntry) -> bool
        requires
            self.wf(),
    {
        self.used_bytes() + (entry.encoded_len() as u32) <= PAGE_SIZE as u32
    }

    pub fn entry_count(&self) -> (result: u32)
        requires
            self.wf(),
        ensures
            result == self.entry_count_spec(),
    {
        let result = u32_from_le_bytes(
            slice_subrange(self.bytes.as_slice(), OFF_ENTRY_COUNT, OFF_ENTRY_COUNT + 4),
        );
        result
    }

    fn magic(&self) -> [u8; 4] {
        [
            self.bytes[OFF_MAGIC],
            self.bytes[OFF_MAGIC + 1],
            self.bytes[OFF_MAGIC + 2],
            self.bytes[OFF_MAGIC + 3],
        ]
    }

    pub fn used_bytes(&self) -> (result: u32)
        requires
            self.wf(),
        ensures
            result == self.used_bytes_spec(),
            WAL_PAGE_HEADER_SIZE as u32 <= result,
            result <= PAGE_SIZE as u32,
    {
        let result = u32_from_le_bytes(
            slice_subrange(self.bytes.as_slice(), OFF_USED_BYTES, OFF_USED_BYTES + 4),
        );
        result
    }

    fn version(&self) -> u16 {
        u16_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_VERSION, OFF_VERSION + 2))
    }

    pub fn append(&mut self, entry: &AppendEntry, lsn: u64) -> (result: Result<(), WalError>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            result.is_ok() ==> self.used_bytes_spec() > old(self).used_bytes_spec(),
            result.is_ok() ==> self.entry_count_spec() == (old(self).entry_count_spec() + 1),
            result.is_err() ==> self.entry_count_spec() == old(self).entry_count_spec(),
            result.is_err() ==> self.used_bytes_spec() == old(self).used_bytes_spec(),
            result.is_err() ==> self.entry_count_spec() == old(self).entry_count_spec(),
            result.is_err() ==> self.used_bytes_spec() == old(self).used_bytes_spec(),
            result.is_err() ==> result.unwrap_err() == WalError::InsufficientSpace,
    {
        let old_used = self.used_bytes();
        let old_entry_count = self.entry_count();
        let start = old_used as usize;
        let key = entry.key_bytes();
        let key_len = key.len() as u16;

        if start > PAGE_SIZE - ENTRY_HEADER_SIZE {
            return Err(WalError::InsufficientSpace);
        }
        let header_end = start + ENTRY_HEADER_SIZE;
        if key.len() > PAGE_SIZE - header_end {
            return Err(WalError::InsufficientSpace);
        }
        let end = header_end + key.len();

        let entry_count = old_entry_count;

        let mut next_bytes = self.bytes.clone();
        write_u16_le(&mut next_bytes, start, key_len);
        next_bytes[start + 2] = entry.flags();
        next_bytes[start + 3] = 0;
        write_u64_le(&mut next_bytes, start + 4, entry.offset());
        write_u32_le(&mut next_bytes, start + 12, entry.length());
        write_u64_le(&mut next_bytes, start + 16, lsn);
        copy_into_page(&mut next_bytes, start + 24, key);

        let next_entry_count = entry_count + 1;
        write_u32_le(&mut next_bytes, OFF_ENTRY_COUNT, next_entry_count);

        let end_u32 = end as u32;
        write_u32_le(&mut next_bytes, OFF_USED_BYTES, end_u32);

        let checked = Self::from_bytes(next_bytes);
        if checked.is_err() {
            return Err(WalError::InsufficientSpace);
        }
        *self = checked.unwrap();

        Ok(())
    }

    pub fn iter<'a>(&'a self) -> WalIter<'a>
        requires
            self.wf(),
    {
        WalIter::new(self)
    }
}

pub struct WalIter<'a> {
    page: &'a WalPage,
    cursor: usize,
    remaining: u32,
}

impl<'a> WalIter<'a> {
    #[verifier::type_invariant]
    spec fn type_inv(&self) -> bool {
        self.page.wf() && self.page.iter_suffix_wf(self.cursor as int, self.remaining as nat)
    }

    pub(crate) fn new(page: &'a WalPage) -> (result: Self)
        requires
            page.wf(),
    {
        let remaining = page.entry_count();
        Self { page, cursor: WAL_PAGE_HEADER_SIZE, remaining }
    }

    pub closed spec fn remaining(&self) -> u32 {
        self.remaining
    }
}

impl<'a> Iterator for WalIter<'a> {
    type Item = WalEntryRef<'a>;

    fn next(&mut self) -> (result: Option<Self::Item>)
        ensures
            result.is_some() ==> self.remaining() < old(self).remaining(),
            result.is_some() ==> result.unwrap().wf(),
            result.is_none() ==> self.remaining() == 0,
    {
        proof {
            use_type_invariant(&*self);
        }
        if self.remaining == 0 {
            return None;
        }
        let used = self.page.used_bytes() as usize;
        let tail = slice_subrange(self.page.bytes.as_slice(), self.cursor, used);

        let (entry, consumed) = WalEntryRef::decode_from(tail);

        let next_cursor = self.cursor + consumed;
        let next_remaining = self.remaining - 1;
        *self = Self { page: self.page, cursor: next_cursor, remaining: next_remaining };
        Some(entry)
    }
}

} // verus!
