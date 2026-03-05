use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::format::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64};
use crate::io::AlignedBuf;
use crate::io_task::{WalCommitPlan, WalPageWrite};
use crate::io_worker::IoWorker;
use crate::sync::{Mutex, MutexGuard};

use verified::input_kv::{T4Key, T4Value, ValueRef};
use verified::wal::{AppendEntry, WalPage};
use verified::wal_replay::ReplayState;
use verified::{allocate_next_lsn, reserve_space};

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

        let mut replay_state = ReplayState::init();
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = Self::read_page(&io, offset).await?;
            let (new_replay_state, next_page) = replay_state.process_page(&page)?;
            replay_state = new_replay_state;
            if let Some(next_page) = next_page {
                offset = next_page;
            } else {
                break (offset, page);
            }
        };

        let (file_tail, next_lsn, replay_index) = replay_state
            .finalize(file_len)
            .map_err(|_| Error::Format("replay finalize overflow".into()))?;
        let wal = Self {
            io,
            state: Mutex::new(WalState {
                file_tail,
                tail: last_page,
                tail_offset: last_offset,
                next_lsn,
            }),
        };
        Ok((wal, replay_index))
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
                WalCommitPlan::RewriteTail {
                    tail: self.encode_page_write(state.tail_offset, &state.tail)?,
                }
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

                WalCommitPlan::RotateTail {
                    old_tail: old_tail_write,
                    new_tail: new_page_write,
                }
            };

            state.next_lsn = next_lsn;
            self.io.wal_commit(writes)?
        };

        wal_append.await?;
        Ok(())
    }

    fn encode_page_write(&self, offset: u64, page: &WalPage) -> Result<WalPageWrite> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        buf.as_mut_slice().copy_from_slice(page.as_slice());
        Ok(WalPageWrite { buf, offset })
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
}
