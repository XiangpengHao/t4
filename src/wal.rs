use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::format::{MAGIC, PAGE_SIZE, PAGE_SIZE_U64, VERSION};
use crate::io::AlignedBuf;
use crate::io_task::WalWriteOp;
use crate::io_worker::IoWorker;

const WAL_PAGE_HEADER_SIZE: usize = 32;
const ENTRY_HEADER_SIZE: usize = 28;

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
    lsn: u64,
    flags: u8,
}

impl WalEntry {
    fn live(key: Vec<u8>, offset: u64, length: u64, lsn: u64) -> Self {
        Self {
            key,
            offset,
            length,
            lsn,
            flags: FLAG_LIVE,
        }
    }

    fn tombstone(key: Vec<u8>, lsn: u64) -> Self {
        Self {
            key,
            offset: 0,
            length: 0,
            lsn,
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
        dst[20..28].copy_from_slice(&self.lsn.to_le_bytes());
        dst[28..28 + self.key.len()].copy_from_slice(&self.key);
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
        let lsn = u64::from_le_bytes(src[20..28].try_into().unwrap());
        let key = src[28..28 + key_len].to_vec();
        Ok((
            Self {
                key,
                offset,
                length,
                lsn,
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
struct AppendState {
    tail: WalPage,
    tail_offset: u64,
}

pub struct Wal {
    io: IoWorker,
    bump_pointer: AtomicU64,
    next_lsn: AtomicU64,
    append_state: Mutex<AppendState>,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("bump_pointer", &self.bump_pointer())
            .finish_non_exhaustive()
    }
}

impl Wal {
    /// Initialize the WAL for a newly created (empty) file.
    pub async fn create(io: IoWorker) -> Result<Self> {
        let page = WalPage::empty();
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        io.write_all_at(buf, 0).await?;
        Ok(Self {
            io,
            bump_pointer: AtomicU64::new(PAGE_SIZE_U64),
            next_lsn: AtomicU64::new(0),
            append_state: Mutex::new(AppendState {
                tail: page,
                tail_offset: 0,
            }),
        })
    }

    /// Replay an existing WAL, rebuilding the in-memory index.
    pub async fn replay(io: IoWorker, file_len: u64) -> Result<(Self, HashMap<Vec<u8>, ValueRef>)> {
        if file_len < PAGE_SIZE_U64 {
            return Err(Error::Format(
                "store file shorter than first WAL page".into(),
            ));
        }

        let mut index = HashMap::new();
        let mut expected_lsn = 0_u64;
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = Self::read_page(&io, offset).await?;
            for entry in &page.entries {
                if entry.lsn != expected_lsn {
                    return Err(Error::Format(format!(
                        "non-monotonic wal lsn: expected {expected_lsn}, got {}",
                        entry.lsn
                    )));
                }
                expected_lsn = expected_lsn
                    .checked_add(1)
                    .ok_or_else(|| Error::Format("wal lsn overflow".into()))?;
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

        let bump_pointer = align_up(file_len, PAGE_SIZE_U64)?;
        let wal = Self {
            io,
            bump_pointer: AtomicU64::new(bump_pointer),
            next_lsn: AtomicU64::new(expected_lsn),
            append_state: Mutex::new(AppendState {
                tail: last_page,
                tail_offset: last_offset,
            }),
        };
        Ok((wal, index))
    }

    /// Current bump pointer (next free offset in the file).
    pub fn bump_pointer(&self) -> u64 {
        self.bump_pointer.load(Ordering::Acquire)
    }

    /// Reserve aligned value space in the data region.
    pub fn reserve_value_space(&self, value_len: u64) -> Result<u64> {
        let padded_len = align_up(value_len, PAGE_SIZE_U64)?;
        self.reserve_space(padded_len)
    }

    /// Write value bytes to an already-reserved location.
    pub async fn write_value_at(&self, offset: u64, value: &[u8]) -> Result<()> {
        let buf = AlignedBuf::from_padded_slice(value)?;
        self.io.write_all_at(buf, offset).await?;
        Ok(())
    }

    /// Append a live entry to the WAL for a put operation.
    pub async fn append_put(&self, key: Vec<u8>, offset: u64, length: u64) -> Result<()> {
        let lsn = self.allocate_lsn()?;
        self.append_entry(WalEntry::live(key, offset, length, lsn), lsn)
            .await
    }

    /// Append a tombstone entry to the WAL for a remove operation.
    pub async fn append_tombstone(&self, key: Vec<u8>) -> Result<()> {
        let lsn = self.allocate_lsn()?;
        self.append_entry(WalEntry::tombstone(key, lsn), lsn).await
    }

    /// Read value bytes from the data region.
    pub async fn read_exact(&self, buf: AlignedBuf, offset: u64) -> Result<AlignedBuf> {
        self.io.read_exact_at(buf, offset).await
    }

    /// fsync the underlying file.
    pub async fn fsync(&self) -> Result<()> {
        self.io.fsync().await
    }

    // -- private -------------------------------------------------------------

    fn reserve_space(&self, len: u64) -> Result<u64> {
        self.bump_pointer
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(len)
            })
            .map_err(|_| Error::Format("bump pointer overflow".into()))
    }

    fn allocate_lsn(&self) -> Result<u64> {
        self.next_lsn
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Format("wal lsn overflow".into()))
    }

    fn lock_append_state(&self) -> Result<MutexGuard<'_, AppendState>> {
        self.append_state.lock().map_err(|_| Error::LockPoisoned)
    }

    async fn append_entry(&self, entry: WalEntry, lsn: u64) -> Result<()> {
        let writes = {
            let mut state = self.lock_append_state()?;

            if state.tail.can_fit(&entry) {
                state.tail.push(entry);
                vec![self.encode_page_write(state.tail_offset, &state.tail)?]
            } else {
                // Current page is full — allocate a new one.
                let new_page_offset = self.reserve_space(PAGE_SIZE_U64)?;

                // Link the old tail to the new page and rewrite it.
                let old_tail_offset = state.tail_offset;
                state.tail.next_page = new_page_offset;
                let linked_tail = state.tail.clone();

                // Write the new page with the entry.
                let mut new_page = WalPage::empty();
                if !new_page.push(entry) {
                    return Err(Error::Format("entry does not fit in empty WAL page".into()));
                }

                state.tail_offset = new_page_offset;
                state.tail = new_page.clone();

                vec![
                    self.encode_page_write(old_tail_offset, &linked_tail)?,
                    self.encode_page_write(new_page_offset, &new_page)?,
                ]
            }
        };

        self.io.wal_append(lsn, writes).await?;
        Ok(())
    }

    fn encode_page_write(&self, offset: u64, page: &WalPage) -> Result<WalWriteOp> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        Ok(WalWriteOp { buf, offset })
    }

    async fn read_page(io: &IoWorker, offset: u64) -> Result<WalPage> {
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        let buf = io.read_exact_at(buf, offset).await?;
        WalPage::from_bytes(buf.as_slice())
    }
}

fn align_up(value: u64, alignment: u64) -> Result<u64> {
    debug_assert!(alignment.is_power_of_two());
    let sum = value
        .checked_add(alignment - 1)
        .ok_or_else(|| Error::Format("value overflow while aligning".into()))?;
    Ok(sum & !(alignment - 1))
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
        assert!(page.push(WalEntry::live(b"alpha".to_vec(), 4096, 123, 0)));
        assert!(page.push(WalEntry::tombstone(b"beta".to_vec(), 1)));

        let bytes = page.to_bytes().unwrap();
        let decoded = WalPage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = WalPage::empty();
        let mut i = 0_u64;
        while page.push(WalEntry::live(vec![b'k'; 64], i * 4096, 64, i)) {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&WalEntry::live(vec![1; 128], 0, 1, i + 1)));
    }
}
