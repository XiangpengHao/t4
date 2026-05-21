use std::collections::HashMap;

use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::{Mutex, MutexGuard};
use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64};

use verified::input_kv::{FileHoles, T4Key, T4Value, ValueRef};
use verified::wal::{AppendEntry, WalPage};
use verified::wal_replay::ReplayState;
use verified::{allocate_next_lsn, reserve_space};

/// Durable WAL receipt for a put. Carries the LSN that decides ordering
/// when the put is applied to the index and the on-disk location of the
/// value bytes. Construct only via [`Wal::put`]; consume via
/// [`crate::store::Index::apply_put`].
///
/// If the receipt is dropped without being applied, the reserved value
/// space is released back to the WAL so it doesn't leak. (The on-disk
/// WAL record stays — replay will see it dominated by any later record
/// for the same key, exactly as it would for any in-flight crash.)
#[must_use = "WalCommit must be applied to the index via Index::apply_put \
              (Rejected outcomes hand back the vref to release explicitly)"]
pub(crate) struct WalCommit<'a> {
    lsn: u64,
    vref: ValueRef,
    wal: &'a Wal,
}

impl WalCommit<'_> {
    /// Extract the (lsn, vref) and *suppress* the Drop-time release.
    /// Used by the index after a successful apply to take ownership of
    /// the vref without freeing its space.
    pub(crate) fn into_parts(self) -> (u64, ValueRef) {
        let parts = (self.lsn, self.vref);
        std::mem::forget(self);
        parts
    }
}

impl Drop for WalCommit<'_> {
    fn drop(&mut self) {
        let _ = self.wal.release_value_space(self.vref);
    }
}

/// Durable WAL receipt for a tombstone. Carries the LSN that decides
/// ordering. Tombstones don't allocate value space, so dropping the
/// receipt without applying it is harmless (the on-disk tombstone just
/// won't be reflected in the live index until the next remount).
#[must_use = "WalTombstoneCommit must be applied via Index::apply_remove"]
pub(crate) struct WalTombstoneCommit {
    lsn: u64,
}

impl WalTombstoneCommit {
    pub(crate) fn lsn(&self) -> u64 {
        self.lsn
    }
}

#[derive(Debug)]
struct WalState {
    file_tail: u64,
    tail: WalPage,
    tail_offset: u64,
    next_lsn: u64,
    holes: FileHoles,
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
        io.write(vec![PageWrite { buf, offset: 0 }])?.await?;
        Ok(Self {
            io,
            state: Mutex::new(WalState {
                file_tail: PAGE_SIZE_U64,
                tail: page,
                tail_offset: 0,
                next_lsn: 0,
                holes: FileHoles::empty(),
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

        let (file_tail, next_lsn, replay_index, holes) = replay_state
            .finalize(file_len)
            .map_err(|_| Error::Format("replay finalize overflow".into()))?;
        let wal = Self {
            io,
            state: Mutex::new(WalState {
                file_tail,
                tail: last_page,
                tail_offset: last_offset,
                next_lsn,
                holes,
            }),
        };
        Ok((wal, replay_index))
    }

    /// Write value bytes into data space and append a live WAL entry.
    /// Returns a [`WalCommit`] carrying the assigned LSN — pass it to
    /// `Index::apply_put` so the index's ordering matches the WAL's.
    pub async fn put<'a>(&'a self, key: T4Key, value: &T4Value) -> Result<WalCommit<'a>> {
        let value_len = value.len_u32();
        let value_offset = if value_len == 0 {
            0
        } else {
            let buf = AlignedBuf::from_padded_slice(value.as_bytes())?;
            let value_offset = self.reserve_value_space(buf.len_u32())?;
            self.io
                .write(vec![PageWrite {
                    buf,
                    offset: value_offset,
                }])?
                .await?;
            value_offset
        };

        let lsn = self
            .append_entry(AppendEntry::Live {
                key,
                offset: value_offset,
                length: value_len,
            })
            .await?;

        let vref = ValueRef::try_new(value_offset, value_len)
            .ok_or_else(|| Error::Format("invalid value reference allocated".into()))?;
        Ok(WalCommit {
            lsn,
            vref,
            wal: self,
        })
    }

    /// Append a tombstone entry to the WAL.
    pub async fn tombstone(&self, key: T4Key) -> Result<WalTombstoneCommit> {
        let lsn = self.append_entry(AppendEntry::Tombstone { key }).await?;
        Ok(WalTombstoneCommit { lsn })
    }

    pub fn release_value_space(&self, value: ValueRef) -> Result<()> {
        self.lock_state()?.holes.release_value(value);
        Ok(())
    }

    // -- private -------------------------------------------------------------

    fn lock_state(&self) -> Result<MutexGuard<'_, WalState>> {
        self.state.lock().map_err(|_| Error::LockPoisoned)
    }

    fn reserve_value_space(&self, padded_len: u32) -> Result<u64> {
        let mut state = self.lock_state()?;
        if let Some(offset) = state.holes.reserve(padded_len) {
            return Ok(offset);
        }
        Self::reserve_tail_space_locked(&mut state, padded_len)
    }

    fn reserve_tail_space_locked(state: &mut WalState, len: u32) -> Result<u64> {
        let reservation = reserve_space(state.file_tail, len)
            .ok_or_else(|| Error::Format("file tail overflow".into()))?;
        state.file_tail = reservation.next_tail;
        Ok(reservation.offset)
    }

    async fn append_entry(&self, pending: AppendEntry) -> Result<u64> {
        let (write, lsn) = {
            let mut state = self.lock_state()?;
            let lsn = state.next_lsn;
            let next_lsn =
                allocate_next_lsn(lsn).ok_or_else(|| Error::Format("wal lsn overflow".into()))?;

            let writes = if state.tail.can_fit(&pending) {
                state.tail.append(&pending, lsn)?;
                vec![self.encode_page_write(state.tail_offset, &state.tail)?]
            } else {
                let new_page_offset = Self::reserve_tail_space_locked(&mut state, PAGE_SIZE_U32)?;

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
            (self.io.write(writes)?, lsn)
        };

        write.await?;
        Ok(lsn)
    }

    fn encode_page_write(&self, offset: u64, page: &WalPage) -> Result<PageWrite> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
        buf.as_mut_slice().copy_from_slice(page.as_slice());
        Ok(PageWrite { buf, offset })
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
            i = i + 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&AppendEntry::Live {
            key: T4Key::try_from_vec(vec![1; 128]).unwrap(),
            offset: 0,
            length: 1,
        }));
    }
}
