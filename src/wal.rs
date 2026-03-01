use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::format::{MAGIC, PAGE_SIZE, PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64, VERSION};
use crate::io::AlignedBuf;
use crate::io_task::WalWriteOp;
use crate::io_worker::IoWorker;
use crate::sync::{Mutex, MutexGuard};

use proof_core::input_kv::{T4Key, T4Value};
use proof_core::wal::WalEntryRef;
use proof_core::{align_up_u64, allocate_next_lsn, reserve_space};

const WAL_PAGE_HEADER_SIZE: usize = 32;
const ENTRY_HEADER_SIZE: usize = 24;

const FLAG_LIVE: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub offset: u64,
    pub length: u32,
}

// ---------------------------------------------------------------------------
// WalPage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalPage {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl WalPage {
    fn empty() -> Self {
        let mut page = Self {
            bytes: Box::new([0_u8; PAGE_SIZE]),
        };
        page.bytes[0..4].copy_from_slice(&MAGIC);
        page.bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
        page.bytes[6..8].copy_from_slice(&0_u16.to_le_bytes());
        page.set_next_page(0);
        page.set_entry_count(0)
            .expect("initial WAL entry count must fit u32");
        page.set_used_bytes(WAL_PAGE_HEADER_SIZE)
            .expect("initial WAL used bytes must fit u32");
        page.bytes[24..32].copy_from_slice(&0_u64.to_le_bytes());
        page
    }

    fn from_bytes(src: &[u8]) -> Result<Self> {
        if src.len() != PAGE_SIZE {
            return Err(Error::Format("WAL page must be 4096 bytes".into()));
        }

        let mut bytes = [0_u8; PAGE_SIZE];
        bytes.copy_from_slice(src);
        let page = Self {
            bytes: Box::new(bytes),
        };
        page.validate_layout()?;
        Ok(page)
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..]
    }

    fn next_page(&self) -> u64 {
        u64::from_le_bytes(
            self.bytes[8..16]
                .try_into()
                .expect("next_page bytes must be present"),
        )
    }

    fn set_next_page(&mut self, next_page: u64) {
        self.bytes[8..16].copy_from_slice(&next_page.to_le_bytes());
    }

    fn entry_count(&self) -> usize {
        u32::from_le_bytes(
            self.bytes[16..20]
                .try_into()
                .expect("entry_count bytes must be present"),
        ) as usize
    }

    fn set_entry_count(&mut self, count: usize) -> Result<()> {
        let count_u32: u32 = count
            .try_into()
            .map_err(|_| Error::Format("wal entry count overflow".into()))?;
        self.bytes[16..20].copy_from_slice(&count_u32.to_le_bytes());
        Ok(())
    }

    fn used_bytes(&self) -> usize {
        u32::from_le_bytes(
            self.bytes[20..24]
                .try_into()
                .expect("used_bytes bytes must be present"),
        ) as usize
    }

    fn set_used_bytes(&mut self, used: usize) -> Result<()> {
        let used_u32: u32 = used
            .try_into()
            .map_err(|_| Error::Format("wal used bytes overflow".into()))?;
        self.bytes[20..24].copy_from_slice(&used_u32.to_le_bytes());
        Ok(())
    }

    fn can_fit(&self, entry: &AppendEntry) -> bool {
        self.used_bytes()
            .checked_add(entry.encoded_len())
            .is_some_and(|size| size <= PAGE_SIZE)
    }

    fn append(&mut self, entry: &AppendEntry, lsn: u64) -> Result<bool> {
        let start = self.used_bytes();
        let end = match start.checked_add(entry.encoded_len()) {
            Some(end) => end,
            None => return Err(Error::Format("WAL page offset overflow".into())),
        };
        if end > PAGE_SIZE {
            return Ok(false);
        }

        let key = entry.key_bytes();
        let key_len: u16 = key
            .len()
            .try_into()
            .map_err(|_| Error::Format("key length exceeds u16".into()))?;

        let dst = &mut self.bytes[start..end];
        dst[0..2].copy_from_slice(&key_len.to_le_bytes());
        dst[2] = entry.flags();
        dst[3] = 0;
        dst[4..12].copy_from_slice(&entry.offset().to_le_bytes());
        dst[12..16].copy_from_slice(&entry.length().to_le_bytes());
        dst[16..24].copy_from_slice(&lsn.to_le_bytes());
        dst[24..].copy_from_slice(key);

        let next_entry_count = self
            .entry_count()
            .checked_add(1)
            .ok_or_else(|| Error::Format("wal entry count overflow".into()))?;
        self.set_entry_count(next_entry_count)?;
        self.set_used_bytes(end)?;
        Ok(true)
    }

    fn validate_layout(&self) -> Result<()> {
        if self.bytes[0..4] != MAGIC {
            return Err(Error::Format("bad magic".into()));
        }

        let version = u16::from_le_bytes([self.bytes[4], self.bytes[5]]);
        if version != VERSION {
            return Err(Error::Format(format!("unsupported version {version}")));
        }

        let used_bytes = self.used_bytes();
        if !(WAL_PAGE_HEADER_SIZE..=PAGE_SIZE).contains(&used_bytes) {
            return Err(Error::Format("invalid WAL used-bytes field".into()));
        }

        let mut cursor = WAL_PAGE_HEADER_SIZE;
        for _ in 0..self.entry_count() {
            let (entry, consumed) = WalEntryRef::decode_from(&self.bytes[cursor..used_bytes])?;
            cursor = cursor
                .checked_add(consumed)
                .ok_or_else(|| Error::Format("entry cursor overflow".into()))?;
            if cursor > used_bytes {
                return Err(Error::Format("entry overran WAL used bytes".into()));
            }
            let _ = entry;
        }

        if cursor != used_bytes {
            return Err(Error::Format("WAL page has trailing garbage bytes".into()));
        }

        Ok(())
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
    fn encoded_len(&self) -> usize {
        ENTRY_HEADER_SIZE + self.key_bytes().len()
    }

    fn key_bytes(&self) -> &[u8] {
        match self {
            Self::Live { key, .. } | Self::Tombstone { key } => key.as_bytes(),
        }
    }

    fn flags(&self) -> u8 {
        match self {
            Self::Live { .. } => FLAG_LIVE,
            Self::Tombstone { .. } => FLAG_TOMBSTONE,
        }
    }

    fn offset(&self) -> u64 {
        match self {
            Self::Live { offset, .. } => *offset,
            Self::Tombstone { .. } => 0,
        }
    }

    fn length(&self) -> u32 {
        match self {
            Self::Live { length, .. } => *length,
            Self::Tombstone { .. } => 0,
        }
    }
}

#[derive(Debug)]
struct WalState {
    file_tail: u64,
    tail: WalPage,
    tail_offset: u64,
    next_lsn: u64,
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
        buf.as_mut_slice().copy_from_slice(page.as_slice());
        io.write_all_at(buf, 0).await?;
        Ok(Self {
            io,
            state: Mutex::new(WalState {
                file_tail: PAGE_SIZE_U64,
                tail: page,
                tail_offset: 0,
                next_lsn: 0,
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
        let mut previous_lsn = None;
        let mut max_data_end = PAGE_SIZE_U64;
        let mut max_wal_end = PAGE_SIZE_U64;
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = Self::read_page(&io, offset).await?;
            let wal_end = offset
                .checked_add(PAGE_SIZE_U64)
                .ok_or_else(|| Error::Format("wal page offset overflow".into()))?;
            max_wal_end = max_wal_end.max(wal_end);

            let used = page.used_bytes();
            let mut cursor = WAL_PAGE_HEADER_SIZE;
            for _ in 0..page.entry_count() {
                let (entry, consumed) = WalEntryRef::decode_from(&page.as_slice()[cursor..used])?;
                cursor = cursor
                    .checked_add(consumed)
                    .ok_or_else(|| Error::Format("entry cursor overflow".into()))?;

                if let Some(prev_lsn) = previous_lsn
                    && entry.lsn() <= prev_lsn
                {
                    return Err(Error::Format(format!(
                        "non-monotonic wal lsn: previous {prev_lsn}, got {}",
                        entry.lsn()
                    )));
                }

                match entry.flags() {
                    FLAG_TOMBSTONE => {
                        index.remove(entry.key_bytes());
                    }
                    FLAG_LIVE => {
                        index.insert(
                            T4Key::try_from_vec(entry.key_bytes().to_vec())?,
                            ValueRef {
                                offset: entry.offset(),
                                length: entry.length(),
                            },
                        );
                        let padded_len = align_up_u64(u64::from(entry.length()), PAGE_SIZE_U64)
                            .ok_or_else(|| Error::Format("value overflow while aligning".into()))?;
                        let data_end = entry
                            .offset()
                            .checked_add(padded_len)
                            .ok_or_else(|| Error::Format("value offset overflow".into()))?;
                        max_data_end = max_data_end.max(data_end);
                    }
                    other => {
                        return Err(Error::Format(format!("unknown wal entry flag: {other}")));
                    }
                }

                previous_lsn = Some(entry.lsn());
            }

            if cursor != used {
                return Err(Error::Format("WAL page has trailing garbage bytes".into()));
            }

            if page.next_page() == 0 {
                break (offset, page);
            }
            offset = page.next_page();
        };

        let highest_used = file_len.max(max_data_end).max(max_wal_end);
        let file_tail = align_up_u64(highest_used, PAGE_SIZE_U64)
            .ok_or_else(|| Error::Format("value overflow while aligning".into()))?;
        let next_lsn = if let Some(last_lsn) = previous_lsn {
            allocate_next_lsn(last_lsn).ok_or_else(|| Error::Format("wal lsn overflow".into()))?
        } else {
            0
        };
        let wal = Self {
            io,
            state: Mutex::new(WalState {
                file_tail,
                tail: last_page,
                tail_offset: last_offset,
                next_lsn,
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
        let reservation = reserve_space(state.file_tail, len)
            .ok_or_else(|| Error::Format("file tail overflow".into()))?;
        state.file_tail = reservation.next_tail;
        Ok(reservation.offset)
    }

    async fn append_entry(&self, pending: AppendEntry) -> Result<()> {
        let wal_append = {
            let mut state = self.lock_state()?;
            let lsn = state.next_lsn;
            let next_lsn =
                allocate_next_lsn(lsn).ok_or_else(|| Error::Format("wal lsn overflow".into()))?;

            let writes = if state.tail.can_fit(&pending) {
                let appended = state.tail.append(&pending, lsn)?;
                debug_assert!(appended, "tail fit check and append diverged");
                vec![self.encode_page_write(state.tail_offset, &state.tail)?]
            } else {
                let new_page_offset = Self::reserve_space_locked(&mut state, PAGE_SIZE_U32)?;

                let old_tail_offset = state.tail_offset;
                state.tail.set_next_page(new_page_offset);
                let old_tail_write = self.encode_page_write(old_tail_offset, &state.tail)?;

                let mut new_page = WalPage::empty();
                if !new_page.append(&pending, lsn)? {
                    return Err(Error::Format("entry does not fit in empty WAL page".into()));
                }
                let new_page_write = self.encode_page_write(new_page_offset, &new_page)?;

                state.tail_offset = new_page_offset;
                state.tail = new_page;

                vec![old_tail_write, new_page_write]
            };

            state.next_lsn = next_lsn;
            self.io.wal_append(writes)?
        };

        wal_append.await?;
        Ok(())
    }

    fn encode_page_write(&self, offset: u64, page: &WalPage) -> Result<WalWriteOp> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        buf.as_mut_slice().copy_from_slice(page.as_slice());
        Ok(WalWriteOp { buf, offset })
    }

    async fn read_page(io: &IoWorker, offset: u64) -> Result<WalPage> {
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        let buf = io.read_exact_at(buf, offset).await?;
        WalPage::from_bytes(buf.as_slice())
    }
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
        page.set_next_page(8192);
        assert!(
            page.append(
                &AppendEntry::Live {
                    key: T4Key::try_from_vec(b"alpha".to_vec()).unwrap(),
                    offset: 4096,
                    length: 123,
                },
                0,
            )
            .unwrap()
        );
        assert!(
            page.append(
                &AppendEntry::Tombstone {
                    key: T4Key::try_from_vec(b"beta".to_vec()).unwrap(),
                },
                1,
            )
            .unwrap()
        );

        let decoded = WalPage::from_bytes(page.as_slice()).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = WalPage::empty();
        let mut i = 0_u64;
        while page
            .append(
                &AppendEntry::Live {
                    key: T4Key::try_from_vec(vec![b'k'; 64]).unwrap(),
                    offset: i * 4096,
                    length: 64,
                },
                i,
            )
            .unwrap()
        {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&AppendEntry::Live {
            key: T4Key::try_from_vec(vec![1; 128]).unwrap(),
            offset: 0,
            length: 1,
        }));
    }

    #[test]
    fn entry_ref_is_zero_copy_view() {
        let mut page = WalPage::empty();
        page.append(
            &AppendEntry::Live {
                key: T4Key::try_from_vec(b"k".to_vec()).unwrap(),
                offset: 128,
                length: 7,
            },
            42,
        )
        .unwrap();

        let used = page.used_bytes();
        let (entry, _) =
            WalEntryRef::decode_from(&page.as_slice()[WAL_PAGE_HEADER_SIZE..used]).unwrap();

        assert_eq!(entry.flags(), FLAG_LIVE);
        assert_eq!(entry.offset(), 128);
        assert_eq!(entry.length(), 7);
        assert_eq!(entry.lsn(), 42);
        assert_eq!(entry.key_bytes(), b"k");
    }
}
