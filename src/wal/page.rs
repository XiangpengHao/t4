#![allow(clippy::assign_op_pattern)] // verus doesn't support assign op pattern
#![allow(clippy::while_let_loop)] // verus doesn't support while let loop
use vstd::prelude::*;
use vstd::slice::slice_subrange;

use super::le_bytes::{
    u16_from_le_bytes, u32_from_le_bytes, u64_from_le_bytes, write_u16_le, write_u32_le,
    write_u64_le,
};
use crate::PAGE_SIZE;
use crate::input_kv::{T4Key, T4KeyRef};

const _: [(); WAL_PAGE_HEADER_SIZE] = [(); std::mem::size_of::<WalPageHeader>()];

verus! {

pub const FLAG_LIVE: u8 = 0;

pub const FLAG_TOMBSTONE: u8 = 1;

pub const ENTRY_HEADER_SIZE: usize = 24;

/// The CRC32C after each entry's key, over the entry's header and key.
pub const ENTRY_CHECKSUM_SIZE: usize = 4;

pub const WAL_PAGE_HEADER_SIZE: usize = 16;

pub const MAGIC: [u8; 4] = [0x42, 0x54, 0x46, 0x34];

pub const VERSION: u16 = 4;

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
        ) as int + ENTRY_CHECKSUM_SIZE as int <= used && Self::flags_wf(bytes[cursor + 2])
    }

    pub open spec fn encoded_entry_len(bytes: Seq<u8>, cursor: int, used: int) -> int
        recommends
            Self::encoded_entry_wf(bytes, cursor, used),
    {
        ENTRY_HEADER_SIZE as int + Self::key_len_of(bytes.subrange(cursor, used)) as int
            + ENTRY_CHECKSUM_SIZE as int
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

    /// The length of the entry at `cursor`, if one decodes there and ends
    /// by `end`. Its checksum is left to [`Self::checksum_matches`].
    fn encoded_len_at(bytes: &[u8], cursor: usize, end: usize) -> (result: Option<usize>)
        requires
            cursor <= end <= bytes.len(),
        ensures
            result.is_some() ==> Self::encoded_entry_wf(bytes@, cursor as int, end as int),
            result.is_some() ==> result.unwrap() as int == Self::encoded_entry_len(
                bytes@,
                cursor as int,
                end as int,
            ),
    {
        if end - cursor < ENTRY_HEADER_SIZE {
            return None;
        }
        let tail = slice_subrange(bytes, cursor, end);
        let key_len = u16_from_le_bytes(slice_subrange(tail, 0, 2)) as usize;
        if key_len > u8::MAX as usize {
            return None;
        }
        if tail[2] != FLAG_LIVE && tail[2] != FLAG_TOMBSTONE {
            return None;
        }
        let len = ENTRY_HEADER_SIZE + key_len + ENTRY_CHECKSUM_SIZE;
        if len > tail.len() {
            return None;
        }
        Some(len)
    }

    /// The checksum an entry stores after its key: CRC32C over the entry's
    /// header and key.
    #[verifier::external_body]
    fn checksum(header_and_key: &[u8]) -> u32 {
        crc32c::crc32c(header_and_key)
    }

    /// Whether the `len`-byte entry at `cursor` ends with the checksum of
    /// its header and key.
    fn checksum_matches(bytes: &[u8], cursor: usize, len: usize) -> bool
        requires
            ENTRY_CHECKSUM_SIZE <= len,
            cursor + len <= bytes.len(),
    {
        let key_end = cursor + len - ENTRY_CHECKSUM_SIZE;
        let stored = u32_from_le_bytes(slice_subrange(bytes, key_end, cursor + len));
        Self::checksum(slice_subrange(bytes, cursor, key_end)) == stored
    }

    pub(crate) fn decode_from(src: &'a [u8]) -> (result: (Self, usize))
        requires
            src@.len() >= ENTRY_HEADER_SIZE as int,
            Self::key_len_of(src@) as int <= u8::MAX as int,
            ENTRY_HEADER_SIZE as int + Self::key_len_of(src@) as int + ENTRY_CHECKSUM_SIZE as int
                <= src@.len(),
            Self::flags_wf(src@[2]),
        ensures
            result.0.wf(),
            result.1 as int == ENTRY_HEADER_SIZE as int + Self::key_len_of(src@) as int
                + ENTRY_CHECKSUM_SIZE as int,
            result.1 >= ENTRY_HEADER_SIZE,
            result.1 <= src.len(),
    {
        let key_len = u16_from_le_bytes(slice_subrange(src, 0, 2)) as usize;
        let key_end = ENTRY_HEADER_SIZE + key_len;
        let flags = src[2];
        let offset = u64_from_le_bytes(slice_subrange(src, 4, 12));
        let value_length = u32_from_le_bytes(slice_subrange(src, 12, 16));
        let lsn = u64_from_le_bytes(slice_subrange(src, 16, 24));
        let key = T4KeyRef::from_slice(slice_subrange(src, ENTRY_HEADER_SIZE, key_end));
        (Self { flags, offset, value_length, lsn, key }, key_end + ENTRY_CHECKSUM_SIZE)
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
            ENTRY_HEADER_SIZE + ENTRY_CHECKSUM_SIZE <= result && result <= PAGE_SIZE,
    {
        ENTRY_HEADER_SIZE + self.key_bytes().len() + ENTRY_CHECKSUM_SIZE
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
}

impl WalPageHeader {
    fn new() -> Self {
        Self { magic: MAGIC, version: VERSION, next_page: 0 }
    }
}

const OFF_MAGIC: usize = 0;

const OFF_VERSION: usize = 4;

const OFF_NEXT_PAGE: usize = 8;

#[derive(Debug)]
pub struct WalPage {
    bytes: Box<[u8; PAGE_SIZE]>,
    /// Where the entries end, header included. Zeros follow.
    used_bytes: u32,
    entry_count: u32,
}

impl WalPage {
    pub closed spec fn entry_count_spec(self) -> u32 {
        self.entry_count
    }

    pub closed spec fn used_bytes_spec(self) -> u32 {
        self.used_bytes
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
        match WalEntryRef::encoded_len_at(bytes, cursor, used) {
            Some(len) => Self::entries_wf_exec(bytes, cursor + len, remaining - 1, used),
            None => false,
        }
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
        Self { bytes, used_bytes: WAL_PAGE_HEADER_SIZE as u32, entry_count: 0 }
    }

    /// The page as a crash may have left it on disk. Its header must be
    /// intact; its entries are those before the first that doesn't decode
    /// or fails its checksum, which is where a torn write ends the page.
    /// The rest of the page comes back as zeros, so writing the page again
    /// clears what a tear left there.
    pub fn recover(src: Box<[u8; PAGE_SIZE]>) -> (result: Result<Self, WalError>)
        ensures
            result.is_ok() ==> result.unwrap().wf(),
    {
        if !Self::header_matches(&src) {
            return Err(WalError::InvalidPageLayout);
        }
        let (used_bytes, entry_count) = Self::intact_entries(&src);
        let mut bytes = src;
        zero_from(&mut bytes, used_bytes as usize);
        Self::checked(bytes, used_bytes, entry_count)
    }

    /// Whether `bytes` starts with the header of a WAL page in this version.
    fn header_matches(bytes: &[u8; PAGE_SIZE]) -> bool {
        let version = u16_from_le_bytes(
            slice_subrange(bytes.as_slice(), OFF_VERSION, OFF_VERSION + 2),
        );
        bytes[OFF_MAGIC] == MAGIC[0] && bytes[OFF_MAGIC + 1] == MAGIC[1] && bytes[OFF_MAGIC + 2]
            == MAGIC[2] && bytes[OFF_MAGIC + 3] == MAGIC[3] && version == VERSION
    }

    /// Where the entries that decode and match their checksums end, from
    /// the header on, and how many there are.
    fn intact_entries(bytes: &[u8; PAGE_SIZE]) -> (result: (u32, u32)) {
        let mut cursor = WAL_PAGE_HEADER_SIZE;
        let mut count = 0_u32;
        loop
            invariant
                cursor <= PAGE_SIZE,
                count as int <= cursor as int,
            decreases PAGE_SIZE - cursor,
        {
            let len = match WalEntryRef::encoded_len_at(bytes.as_slice(), cursor, PAGE_SIZE) {
                Some(len) => len,
                None => {
                    break;
                },
            };
            if !WalEntryRef::checksum_matches(bytes.as_slice(), cursor, len) {
                break;
            }
            cursor = cursor + len;
            count = count + 1;
        }
        (cursor as u32, count)
    }

    /// The page holding `bytes`, if `entry_count` entries decode back to
    /// back from the header and end at `used_bytes`.
    fn checked(bytes: Box<[u8; PAGE_SIZE]>, used_bytes: u32, entry_count: u32) -> (result: Result<
        Self,
        WalError,
    >)
        ensures
            result.is_ok() ==> result.unwrap().wf(),
            result.is_ok() ==> result.unwrap().used_bytes_spec() == used_bytes,
            result.is_ok() ==> result.unwrap().entry_count_spec() == entry_count,
    {
        if used_bytes < WAL_PAGE_HEADER_SIZE as u32 || used_bytes > PAGE_SIZE as u32 {
            return Err(WalError::InvalidPageLayout);
        }
        if entry_count as usize > used_bytes as usize / ENTRY_HEADER_SIZE {
            return Err(WalError::InvalidPageLayout);
        }
        let ok = Self::entries_wf_exec(
            bytes.as_slice(),
            WAL_PAGE_HEADER_SIZE,
            entry_count,
            used_bytes as usize,
        );
        if !ok {
            return Err(WalError::InvalidPageLayout);
        }
        Ok(Self { bytes, used_bytes, entry_count })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub(crate) fn next_page(&self) -> u64 {
        u64_from_le_bytes(slice_subrange(self.bytes.as_slice(), OFF_NEXT_PAGE, OFF_NEXT_PAGE + 8))
    }

    pub fn set_next_page(&mut self, next_page: u64) {
        write_u64_le(&mut self.bytes, OFF_NEXT_PAGE, next_page);
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
        self.entry_count
    }

    pub fn used_bytes(&self) -> (result: u32)
        requires
            self.wf(),
        ensures
            result == self.used_bytes_spec(),
            WAL_PAGE_HEADER_SIZE as u32 <= result,
            result <= PAGE_SIZE as u32,
    {
        self.used_bytes
    }

    pub fn append(&mut self, entry: &AppendEntry, lsn: u64) -> (result: Result<(), WalError>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            result.is_ok() ==> final(self).used_bytes_spec() > old(self).used_bytes_spec(),
            result.is_ok() ==> final(self).entry_count_spec() == (old(self).entry_count_spec() + 1),
            result.is_err() ==> final(self).entry_count_spec() == old(self).entry_count_spec(),
            result.is_err() ==> final(self).used_bytes_spec() == old(self).used_bytes_spec(),
            result.is_err() ==> result.unwrap_err() == WalError::InsufficientSpace,
    {
        let start = self.used_bytes() as usize;
        let key = entry.key_bytes();
        let key_len = key.len() as u16;

        if start > PAGE_SIZE - ENTRY_HEADER_SIZE - ENTRY_CHECKSUM_SIZE {
            return Err(WalError::InsufficientSpace);
        }
        let key_start = start + ENTRY_HEADER_SIZE;
        if key.len() > PAGE_SIZE - ENTRY_CHECKSUM_SIZE - key_start {
            return Err(WalError::InsufficientSpace);
        }
        let key_end = key_start + key.len();
        let end = key_end + ENTRY_CHECKSUM_SIZE;

        let mut next_bytes = self.bytes.clone();
        write_u16_le(&mut next_bytes, start, key_len);
        next_bytes[start + 2] = entry.flags();
        next_bytes[start + 3] = 0;
        write_u64_le(&mut next_bytes, start + 4, entry.offset());
        write_u32_le(&mut next_bytes, start + 12, entry.length());
        write_u64_le(&mut next_bytes, start + 16, lsn);
        copy_into_page(&mut next_bytes, key_start, key);
        let checksum = WalEntryRef::checksum(slice_subrange(next_bytes.as_slice(), start, key_end));
        write_u32_le(&mut next_bytes, key_end, checksum);

        let checked = Self::checked(next_bytes, end as u32, self.entry_count() + 1);
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

impl<'a> WalIter<'a> {
    pub fn next_impl(&mut self) -> (result: Option<WalEntryRef<'a>>)
        ensures
            result.is_some() ==> final(self).remaining() < old(self).remaining(),
            result.is_some() ==> result.unwrap().wf(),
            result.is_none() ==> final(self).remaining() == 0,
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

#[cfg(not(verus_only))]
impl<'a> Iterator for WalIter<'a> {
    type Item = WalEntryRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        WalIter::next_impl(self)
    }
}

fn copy_into_page(bytes: &mut [u8; PAGE_SIZE], off: usize, src: &[u8])
    requires
        off + src.len() <= PAGE_SIZE,
{
    let mut i = 0;

    while i < src.len()
        invariant
            off + src.len() <= PAGE_SIZE,
            i <= src.len(),
        decreases src.len() - i,
    {
        bytes[off + i] = src[i];
        i = i + 1;
    }
}

fn zero_from(bytes: &mut [u8; PAGE_SIZE], from: usize) {
    let mut i = from;

    while i < PAGE_SIZE
        decreases PAGE_SIZE - i,
    {
        bytes[i] = 0;
        i = i + 1;
    }
}

} // verus!
