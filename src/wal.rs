use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::format::{MAGIC, PAGE_SIZE, PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64, VERSION};
use crate::io::AlignedBuf;
use crate::io_task::WalWriteOp;
use crate::io_worker::IoWorker;
use crate::types::{T4Key, T4Value};

const WAL_PAGE_HEADER_SIZE: usize = 32;
const ENTRY_HEADER_SIZE: usize = 28;

const FLAG_LIVE: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub offset: u64,
    pub length: u32,
}

// ---------------------------------------------------------------------------
// WalEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalEntry {
    key: T4Key,
    offset: u64,
    length: u32,
    lsn: u64,
    flags: u8,
}

impl WalEntry {
    fn live(key: T4Key, offset: u64, length: u32, lsn: u64) -> Self {
        Self {
            key,
            offset,
            length,
            lsn,
            flags: FLAG_LIVE,
        }
    }

    fn tombstone(key: T4Key, lsn: u64) -> Self {
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
        let key_len = self.key.len() as u16;
        let total = self.serialized_len();
        if dst.len() < total {
            return Err(Error::Format("entry buffer too small".into()));
        }
        dst[0..2].copy_from_slice(&key_len.to_le_bytes());
        dst[2] = self.flags;
        dst[3] = 0;
        dst[4..12].copy_from_slice(&self.offset.to_le_bytes());
        dst[12..20].copy_from_slice(&u64::from(self.length).to_le_bytes());
        dst[20..28].copy_from_slice(&self.lsn.to_le_bytes());
        dst[28..28 + self.key.len()].copy_from_slice(self.key.as_bytes());
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
        let length_u64 = u64::from_le_bytes(src[12..20].try_into().unwrap());
        let length: u32 = length_u64
            .try_into()
            .map_err(|_| Error::Format("value length exceeds u32".into()))?;
        let lsn = u64::from_le_bytes(src[20..28].try_into().unwrap());
        let key = T4Key::from_wal_bytes(src[28..28 + key_len].to_vec())?;
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

enum AppendEntry {
    Live {
        key: T4Key,
        offset: u64,
        length: u32,
    },
    Tombstone {
        key: T4Key,
    },
}

impl AppendEntry {
    fn into_wal_entry(self, lsn: u64) -> WalEntry {
        match self {
            Self::Live {
                key,
                offset,
                length,
            } => WalEntry::live(key, offset, length, lsn),
            Self::Tombstone { key } => WalEntry::tombstone(key, lsn),
        }
    }
}

#[derive(Debug)]
struct WalState {
    file_tail: u64,
    tail: WalPage,
    tail_offset: u64,
    last_lsn: Option<u64>,
}

pub struct Wal {
    io: IoWorker,
    state: Mutex<WalState>,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal").finish_non_exhaustive()
    }
}

impl Wal {
    /// Initialize the WAL for a newly created (empty) file.
    pub async fn create(io: IoWorker) -> Result<Self> {
        let page = WalPage::empty();
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        io.write_all_at(buf, 0).await?;
        Ok(Self {
            io,
            state: Mutex::new(WalState {
                file_tail: PAGE_SIZE_U64,
                tail: page,
                tail_offset: 0,
                last_lsn: None,
            }),
        })
    }

    /// Replay an existing WAL, rebuilding the in-memory index.
    pub async fn replay(io: IoWorker, file_len: u64) -> Result<(Self, HashMap<T4Key, ValueRef>)> {
        if file_len < PAGE_SIZE_U64 {
            return Err(Error::Format(
                "store file shorter than first WAL page".into(),
            ));
        }

        let mut index = HashMap::new();
        let mut last_lsn = None;
        let mut max_data_end = PAGE_SIZE_U64;
        let mut max_wal_end = PAGE_SIZE_U64;
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = Self::read_page(&io, offset).await?;
            let wal_end = offset
                .checked_add(PAGE_SIZE_U64)
                .ok_or_else(|| Error::Format("wal page offset overflow".into()))?;
            max_wal_end = max_wal_end.max(wal_end);

            for entry in &page.entries {
                if let Some(prev_lsn) = last_lsn
                    && entry.lsn <= prev_lsn
                {
                    return Err(Error::Format(format!(
                        "non-monotonic wal lsn: previous {prev_lsn}, got {}",
                        entry.lsn
                    )));
                }

                match entry.flags {
                    FLAG_TOMBSTONE => {
                        index.remove(&entry.key);
                    }
                    FLAG_LIVE => {
                        index.insert(
                            entry.key.clone(),
                            ValueRef {
                                offset: entry.offset,
                                length: entry.length,
                            },
                        );
                        let padded_len = align_up(u64::from(entry.length), PAGE_SIZE_U64)?;
                        let data_end = entry
                            .offset
                            .checked_add(padded_len)
                            .ok_or_else(|| Error::Format("value offset overflow".into()))?;
                        max_data_end = max_data_end.max(data_end);
                    }
                    other => {
                        return Err(Error::Format(format!("unknown wal entry flag: {other}")));
                    }
                }

                last_lsn = Some(entry.lsn);
            }

            if page.next_page == 0 {
                break (offset, page);
            }
            offset = page.next_page;
        };

        let highest_used = file_len.max(max_data_end).max(max_wal_end);
        let file_tail = align_up(highest_used, PAGE_SIZE_U64)?;
        let wal = Self {
            io,
            state: Mutex::new(WalState {
                file_tail,
                tail: last_page,
                tail_offset: last_offset,
                last_lsn,
            }),
        };
        Ok((wal, index))
    }

    /// Write value bytes into data space and append a live WAL entry.
    pub async fn put(&self, key: T4Key, value: &T4Value) -> Result<ValueRef> {
        let value_len = value.len_u32();
        let value_offset = if value_len == 0 {
            0
        } else {
            let buf = AlignedBuf::from_padded_slice(value.as_bytes())?;
            let value_offset = self.reserve_value_space(buf.len_u32())?;
            self.io.write_all_at(buf, value_offset).await?;
            value_offset
        };

        self.append_entry(AppendEntry::Live {
            key,
            offset: value_offset,
            length: value_len,
        })
        .await?;

        Ok(ValueRef {
            offset: value_offset,
            length: value_len,
        })
    }

    /// Append a tombstone entry to the WAL.
    pub async fn tombstone(&self, key: T4Key) -> Result<()> {
        self.append_entry(AppendEntry::Tombstone { key }).await
    }

    // -- private -------------------------------------------------------------

    fn lock_state(&self) -> Result<MutexGuard<'_, WalState>> {
        self.state.lock().map_err(|_| Error::LockPoisoned)
    }

    fn reserve_value_space(&self, padded_len: u32) -> Result<u64> {
        let mut state = self.lock_state()?;
        Self::reserve_space_locked(&mut state, padded_len)
    }

    fn reserve_space_locked(state: &mut WalState, len: u32) -> Result<u64> {
        let offset = state.file_tail;
        state.file_tail = state
            .file_tail
            .checked_add(u64::from(len))
            .ok_or_else(|| Error::Format("file tail overflow".into()))?;
        Ok(offset)
    }

    fn allocate_next_lsn(last_lsn: Option<u64>) -> Result<u64> {
        match last_lsn {
            Some(prev) => prev
                .checked_add(1)
                .ok_or_else(|| Error::Format("wal lsn overflow".into())),
            None => Ok(0),
        }
    }

    async fn append_entry(&self, pending: AppendEntry) -> Result<()> {
        let wal_append = {
            let mut state = self.lock_state()?;
            let lsn = Self::allocate_next_lsn(state.last_lsn)?;
            let entry = pending.into_wal_entry(lsn);

            let writes = if state.tail.can_fit(&entry) {
                state.tail.push(entry);
                vec![self.encode_page_write(state.tail_offset, &state.tail)?]
            } else {
                // Current page is full — allocate a new WAL page.
                let new_page_offset = Self::reserve_space_locked(&mut state, PAGE_SIZE_U32)?;

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
            };

            state.last_lsn = Some(lsn);
            self.io.wal_append(writes)?
        };

        wal_append.await?;
        Ok(())
    }

    fn encode_page_write(&self, offset: u64, page: &WalPage) -> Result<WalWriteOp> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        Ok(WalWriteOp { buf, offset })
    }

    async fn read_page(io: &IoWorker, offset: u64) -> Result<WalPage> {
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
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
        assert!(page.push(WalEntry::live(
            T4Key::try_from(b"alpha".to_vec()).unwrap(),
            4096,
            123,
            0,
        )));
        assert!(page.push(WalEntry::tombstone(
            T4Key::try_from(b"beta".to_vec()).unwrap(),
            1,
        )));

        let bytes = page.to_bytes().unwrap();
        let decoded = WalPage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = WalPage::empty();
        let mut i = 0_u64;
        while page.push(WalEntry::live(
            T4Key::try_from(vec![b'k'; 64]).unwrap(),
            i * 4096,
            64,
            i,
        )) {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&WalEntry::live(
            T4Key::try_from(vec![1; 128]).unwrap(),
            0,
            1,
            i + 1,
        )));
    }
}
