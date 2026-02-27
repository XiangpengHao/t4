use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::format::{MAGIC, PAGE_SIZE, PAGE_SIZE_U64, VERSION};
use crate::io::AlignedBuf;
use crate::uring_worker::UringWorker;

const WAL_PAGE_HEADER_SIZE: usize = 32;
const ENTRY_HEADER_SIZE: usize = 20;

const FLAG_LIVE: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub offset: u64,
    pub length: u64,
}

// ---------------------------------------------------------------------------
// WalEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalEntry {
    key: Vec<u8>,
    offset: u64,
    length: u64,
    flags: u8,
}

impl WalEntry {
    fn live(key: Vec<u8>, offset: u64, length: u64) -> Self {
        Self {
            key,
            offset,
            length,
            flags: FLAG_LIVE,
        }
    }

    fn tombstone(key: Vec<u8>) -> Self {
        Self {
            key,
            offset: 0,
            length: 0,
            flags: FLAG_TOMBSTONE,
        }
    }

    fn serialized_len(&self) -> usize {
        ENTRY_HEADER_SIZE + self.key.len()
    }

    fn encode_into(&self, dst: &mut [u8]) -> Result<usize> {
        let key_len: u16 = self
            .key
            .len()
            .try_into()
            .map_err(|_| Error::KeyTooLarge(self.key.len()))?;
        let total = self.serialized_len();
        if dst.len() < total {
            return Err(Error::Format("entry buffer too small".into()));
        }
        dst[0..2].copy_from_slice(&key_len.to_le_bytes());
        dst[2] = self.flags;
        dst[3] = 0;
        dst[4..12].copy_from_slice(&self.offset.to_le_bytes());
        dst[12..20].copy_from_slice(&self.length.to_le_bytes());
        dst[20..20 + self.key.len()].copy_from_slice(&self.key);
        Ok(total)
    }

    fn decode_from(src: &[u8]) -> Result<(Self, usize)> {
        if src.len() < ENTRY_HEADER_SIZE {
            return Err(Error::Format("entry truncated".into()));
        }
        let key_len = u16::from_le_bytes([src[0], src[1]]) as usize;
        let total = ENTRY_HEADER_SIZE + key_len;
        if src.len() < total {
            return Err(Error::Format("entry key truncated".into()));
        }
        let flags = src[2];
        let offset = u64::from_le_bytes(src[4..12].try_into().unwrap());
        let length = u64::from_le_bytes(src[12..20].try_into().unwrap());
        let key = src[20..20 + key_len].to_vec();
        Ok((
            Self {
                key,
                offset,
                length,
                flags,
            },
            total,
        ))
    }
}

// ---------------------------------------------------------------------------
// WalPage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalPage {
    next_page: u64,
    entries: Vec<WalEntry>,
}

impl WalPage {
    fn empty() -> Self {
        Self {
            next_page: 0,
            entries: Vec::new(),
        }
    }

    fn used_bytes(&self) -> usize {
        WAL_PAGE_HEADER_SIZE
            + self
                .entries
                .iter()
                .map(WalEntry::serialized_len)
                .sum::<usize>()
    }

    fn can_fit(&self, entry: &WalEntry) -> bool {
        self.used_bytes() + entry.serialized_len() <= PAGE_SIZE
    }

    fn push(&mut self, entry: WalEntry) -> bool {
        if !self.can_fit(&entry) {
            return false;
        }
        self.entries.push(entry);
        true
    }

    fn to_bytes(&self) -> Result<[u8; PAGE_SIZE]> {
        let mut out = [0_u8; PAGE_SIZE];
        if self.used_bytes() > PAGE_SIZE {
            return Err(Error::Format("WAL page overflow".into()));
        }

        out[0..4].copy_from_slice(&MAGIC);
        out[4..6].copy_from_slice(&VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&0_u16.to_le_bytes());
        out[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        out[16..20].copy_from_slice(&(self.entries.len() as u32).to_le_bytes());
        out[20..24].copy_from_slice(&0_u32.to_le_bytes());
        out[24..32].copy_from_slice(&0_u64.to_le_bytes());

        let mut cursor = WAL_PAGE_HEADER_SIZE;
        for entry in &self.entries {
            let written = entry.encode_into(&mut out[cursor..])?;
            cursor += written;
        }
        Ok(out)
    }

    fn from_bytes(src: &[u8]) -> Result<Self> {
        if src.len() != PAGE_SIZE {
            return Err(Error::Format("WAL page must be 4096 bytes".into()));
        }
        if src[0..4] != MAGIC {
            return Err(Error::Format("bad magic".into()));
        }
        let version = u16::from_le_bytes([src[4], src[5]]);
        if version != VERSION {
            return Err(Error::Format(format!("unsupported version {version}")));
        }
        let next_page = u64::from_le_bytes(src[8..16].try_into().unwrap());
        let entry_count = u32::from_le_bytes(src[16..20].try_into().unwrap()) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = WAL_PAGE_HEADER_SIZE;
        for _ in 0..entry_count {
            let (entry, consumed) = WalEntry::decode_from(&src[cursor..])?;
            cursor += consumed;
            if cursor > PAGE_SIZE {
                return Err(Error::Format("entry overran WAL page".into()));
            }
            entries.push(entry);
        }

        Ok(Self { next_page, entries })
    }
}

// ---------------------------------------------------------------------------
// Wal
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Wal {
    uring: UringWorker,
    bump_pointer: u64,
    tail: WalPage,
    tail_offset: u64,
}

impl Wal {
    /// Initialize the WAL for a newly created (empty) file.
    pub async fn create(uring: UringWorker) -> Result<Self> {
        let page = WalPage::empty();
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        uring.write_all_at(buf, 0).await?;
        Ok(Self {
            uring,
            bump_pointer: PAGE_SIZE_U64,
            tail: page,
            tail_offset: 0,
        })
    }

    /// Replay an existing WAL, rebuilding the in-memory index.
    pub async fn replay(
        uring: UringWorker,
        file_len: u64,
    ) -> Result<(Self, HashMap<Vec<u8>, ValueRef>)> {
        if file_len < PAGE_SIZE_U64 {
            return Err(Error::Format(
                "store file shorter than first WAL page".into(),
            ));
        }

        let mut index = HashMap::new();
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = Self::read_page(&uring, offset).await?;
            for entry in &page.entries {
                if entry.flags == FLAG_TOMBSTONE {
                    index.remove(entry.key.as_slice());
                } else {
                    index.insert(
                        entry.key.clone(),
                        ValueRef {
                            offset: entry.offset,
                            length: entry.length,
                        },
                    );
                }
            }
            if page.next_page == 0 {
                break (offset, page);
            }
            offset = page.next_page;
        };

        let bump_pointer = align_up(file_len, PAGE_SIZE_U64);
        let wal = Self {
            uring,
            bump_pointer,
            tail: last_page,
            tail_offset: last_offset,
        };
        Ok((wal, index))
    }

    /// Current bump pointer (next free offset in the file).
    pub fn bump_pointer(&self) -> u64 {
        self.bump_pointer
    }

    /// Write value bytes to the data region, returning the offset written to.
    /// Advances the bump pointer by the padded size.
    pub async fn write_value(&mut self, value: &[u8]) -> Result<u64> {
        let offset = self.bump_pointer;
        let buf = AlignedBuf::from_padded_slice(value)?;
        let padded_len = buf.len() as u64;
        self.uring.write_all_at(buf, offset).await?;
        self.bump_pointer += padded_len;
        Ok(offset)
    }

    /// Append a live entry to the WAL for a put operation.
    pub async fn append_put(&mut self, key: Vec<u8>, offset: u64, length: u64) -> Result<()> {
        self.append_entry(WalEntry::live(key, offset, length)).await
    }

    /// Append a tombstone entry to the WAL for a remove operation.
    pub async fn append_tombstone(&mut self, key: Vec<u8>) -> Result<()> {
        self.append_entry(WalEntry::tombstone(key)).await
    }

    /// Read value bytes from the data region.
    pub async fn read_exact(&self, buf: AlignedBuf, offset: u64) -> Result<AlignedBuf> {
        self.uring.read_exact_at(buf, offset).await
    }

    /// fsync the underlying file.
    pub async fn fsync(&self) -> Result<()> {
        self.uring.fsync().await
    }

    // -- private -------------------------------------------------------------

    async fn append_entry(&mut self, entry: WalEntry) -> Result<()> {
        if self.tail.can_fit(&entry) {
            self.tail.push(entry);
            self.write_page(self.tail_offset, &self.tail.clone())
                .await?;
            return Ok(());
        }

        // Current page is full — allocate a new one.
        let new_page_offset = self.bump_pointer;
        self.bump_pointer = self
            .bump_pointer
            .checked_add(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Format("bump pointer overflow".into()))?;

        // Link the old tail to the new page and rewrite it.
        self.tail.next_page = new_page_offset;
        self.write_page(self.tail_offset, &self.tail.clone())
            .await?;

        // Write the new page with the entry.
        let mut new_page = WalPage::empty();
        if !new_page.push(entry) {
            return Err(Error::Format(
                "entry does not fit in empty WAL page".into(),
            ));
        }
        self.write_page(new_page_offset, &new_page).await?;
        self.tail_offset = new_page_offset;
        self.tail = new_page;
        Ok(())
    }

    async fn read_page(uring: &UringWorker, offset: u64) -> Result<WalPage> {
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        let buf = uring.read_exact_at(buf, offset).await?;
        WalPage::from_bytes(buf.as_slice())
    }

    async fn write_page(&self, offset: u64, page: &WalPage) -> Result<()> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        self.uring.write_all_at(buf, offset).await
    }
}

fn align_up(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    (value + (alignment - 1)) & !(alignment - 1)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_round_trip() {
        let mut page = WalPage::empty();
        page.next_page = 8192;
        assert!(page.push(WalEntry::live(b"alpha".to_vec(), 4096, 123)));
        assert!(page.push(WalEntry::tombstone(b"beta".to_vec())));

        let bytes = page.to_bytes().unwrap();
        let decoded = WalPage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = WalPage::empty();
        let mut i = 0_u64;
        while page.push(WalEntry::live(vec![b'k'; 64], i * 4096, 64)) {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&WalEntry::live(vec![1; 128], 0, 1)));
    }
}
