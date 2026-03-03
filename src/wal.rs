use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::format::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64};
use crate::io::AlignedBuf;
use crate::io_task::WalWriteOp;
use crate::io_worker::IoWorker;
use crate::sync::{Mutex, MutexGuard};

use verified::input_kv::{T4Key, T4Value};
use verified::wal::{AppendEntry, WalPage};
use verified::{align_up_u64, allocate_next_lsn, reserve_space};

const FLAG_LIVE: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub offset: u64,
    pub length: u32,
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

            for entry in page.iter() {
                if let Some(prev_lsn) = previous_lsn
                    && entry.lsn <= prev_lsn
                {
                    return Err(Error::Format(format!(
                        "non-monotonic wal lsn: previous {prev_lsn}, got {}",
                        entry.lsn
                    )));
                }

                match entry.flags {
                    FLAG_TOMBSTONE => {
                        index.remove(entry.key.as_bytes());
                    }
                    FLAG_LIVE => {
                        index.insert(
                            T4Key::try_from_vec(entry.key.as_bytes().to_vec())?,
                            ValueRef {
                                offset: entry.offset,
                                length: entry.value_length,
                            },
                        );
                        let padded_len = align_up_u64(u64::from(entry.value_length), PAGE_SIZE_U64)
                            .ok_or_else(|| Error::Format("value overflow while aligning".into()))?;
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

                previous_lsn = Some(entry.lsn);
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
                state.tail.append(&pending, lsn)?;
                vec![self.encode_page_write(state.tail_offset, &state.tail)?]
            } else {
                let new_page_offset = Self::reserve_space_locked(&mut state, PAGE_SIZE_U32)?;

                let old_tail_offset = state.tail_offset;
                state.tail.set_next_page(new_page_offset);
                let old_tail_write = self.encode_page_write(old_tail_offset, &state.tail)?;

                let mut new_page = WalPage::empty();
                new_page.append(&pending, lsn)?;
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
        let boxed = buf
            .try_into_boxed_array()
            .expect("invalid aligned buffer layout");
        Ok(WalPage::from_bytes(boxed)?)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use verified::PAGE_SIZE;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct DecodedEntry {
        flags: u8,
        offset: u64,
        length: u32,
        lsn: u64,
        key: Vec<u8>,
    }

    fn decode_page_entries(page: &WalPage) -> Vec<DecodedEntry> {
        page.iter()
            .map(|entry| DecodedEntry {
                flags: entry.flags,
                offset: entry.offset,
                length: entry.value_length,
                lsn: entry.lsn,
                key: entry.key.as_bytes().to_vec(),
            })
            .collect()
    }

    #[test]
    fn page_round_trip() {
        let mut page = WalPage::empty();
        page.set_next_page(8192);
        page.append(
            &AppendEntry::Live {
                key: T4Key::try_from_vec(b"alpha".to_vec()).unwrap(),
                offset: 4096,
                length: 123,
            },
            0,
        )
        .unwrap();
        page.append(
            &AppendEntry::Tombstone {
                key: T4Key::try_from_vec(b"beta".to_vec()).unwrap(),
            },
            1,
        )
        .unwrap();

        let boxed: Box<[u8; PAGE_SIZE]> = Box::new(page.as_slice().try_into().unwrap());
        let decoded = WalPage::from_bytes(boxed).unwrap();
        assert_eq!(decoded.as_slice(), page.as_slice());
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
            .is_ok()
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

        let entry = page.iter().next().expect("page should have one entry");

        assert_eq!(entry.flags, FLAG_LIVE);
        assert_eq!(entry.offset, 128);
        assert_eq!(entry.value_length, 7);
        assert_eq!(entry.lsn, 42);
        assert_eq!(entry.key.as_bytes(), b"k");
    }

    #[test]
    fn append_success_makes_entry_observable_via_page_iterator() {
        let mut page = WalPage::empty();
        page.append(
            &AppendEntry::Live {
                key: T4Key::try_from_vec(b"alpha".to_vec()).unwrap(),
                offset: 256,
                length: 3,
            },
            1,
        )
        .unwrap();

        let appended = AppendEntry::Tombstone {
            key: T4Key::try_from_vec(b"beta".to_vec()).unwrap(),
        };
        let appended_lsn = 2_u64;
        let before_count = page.entry_count();
        let before_used = page.used_bytes();
        let appended_len = appended.encoded_len() as u32;

        page.append(&appended, appended_lsn).unwrap();
        assert_eq!(
            page.entry_count(),
            before_count + 1,
            "successful append must increase entry count"
        );
        assert_eq!(
            page.used_bytes(),
            before_used + appended_len,
            "successful append must advance used bytes by encoded entry length"
        );

        let entries = decode_page_entries(&page);
        assert_eq!(entries.len() as u32, page.entry_count());
        let last = entries.last().expect("appended entry must be present");
        assert_eq!(
            last,
            &DecodedEntry {
                flags: FLAG_TOMBSTONE,
                offset: 0,
                length: 0,
                lsn: appended_lsn,
                key: b"beta".to_vec(),
            },
            "iterator view should expose the appended entry as the last record"
        );
    }
}
