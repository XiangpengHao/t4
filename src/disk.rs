use std::num::NonZeroU32;
use std::sync::PoisonError;

use vstd::prelude::*;

use crate::buffer::{AlignedBuf, align_down_u64, align_up_u32, align_up_u64};
use crate::input_kv::{FileHoles, T4Key, T4Value, ValueRef};
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::{Arc, Mutex, MutexGuard};
use crate::range::{CheckedRangeU32, RangeRequestU32};
use crate::wal::page::{AppendEntry, WalPage};
use crate::wal::replay::{ReplayIndex, ReplayState};
use crate::wal::{INITIAL_FILE_TAIL, WalState, allocate_next_lsn, encode_page_write, read_page};
use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U32, PAGE_SIZE_U64};

// ---------------------------------------------------------------------------
// DiskData
//
// All on-disk concerns: the I/O worker, the file-space allocator (bump
// pointer + freed-value holes), and the WAL log state. One mutex covers
// the allocator and the WAL state because every WAL append may need to
// allocate a fresh page from the same file tail that value records draw
// from — splitting the locks would mean every append takes both, with
// nothing gained.
//
// The critical section is CPU-only: I/O `await`s always happen *after*
// the lock is released.
// ---------------------------------------------------------------------------

verus! {

#[derive(Clone, Copy)]
pub struct SpaceReservation {
    pub offset: u64,
    pub next_tail: u64,
}

/// Bump `file_tail` by `len`: the arithmetic of [`FileAllocator::reserve_tail`].
pub fn reserve_space(file_tail: u64, len: u32) -> (result: Option<SpaceReservation>)
    ensures
        result.is_some() ==> result.unwrap().offset == file_tail,
        result.is_some() ==> result.unwrap().next_tail >= file_tail,
        result.is_some() ==> result.unwrap().next_tail - file_tail == len as u64,
{
    let next_tail = file_tail.checked_add(len as u64)?;
    Some(SpaceReservation { offset: file_tail, next_tail })
}

} // verus!

/// File-space allocator. `file_tail` is a bump pointer for the end of the
/// file; `holes` is a free list of value-record slots that can be reused,
/// fed by `freed`. Both WAL pages and value records allocate from
/// `file_tail` when no hole satisfies the request.
#[derive(Debug)]
pub(crate) struct FileAllocator {
    file_tail: u64,
    holes: FileHoles,
    freed: FreedExtents,
}

impl FileAllocator {
    /// Allocate `padded_len` bytes for a value record. Tries holes first,
    /// then bumps `file_tail`.
    fn reserve_value(&mut self, padded_len: u32) -> Result<u64> {
        for value in self.freed.take() {
            self.holes.release_value(value);
        }
        if let Some(offset) = self.holes.reserve(padded_len) {
            return Ok(offset);
        }
        self.reserve_tail(padded_len)
    }

    /// Bump-allocate `len` bytes at the file tail.
    fn reserve_tail(&mut self, len: u32) -> Result<u64> {
        let reservation = reserve_space(self.file_tail, len)
            .ok_or_else(|| Error::Format("file tail overflow".into()))?;
        self.file_tail = reservation.next_tail;
        Ok(reservation.offset)
    }
}

/// Extents whose last handle dropped, waiting for the allocator to take
/// them back on its next reservation. A queue, because an [`Extent`] may
/// drop where the allocator's lock is held.
#[derive(Clone, Debug)]
pub(crate) struct FreedExtents(Arc<Mutex<Vec<ValueRef>>>);

impl FreedExtents {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn push(&self, value: ValueRef) {
        // A push can't leave the list half-changed: a poisoned lock is safe.
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value);
    }

    fn take(&self) -> Vec<ValueRef> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// Where a value's bytes are, and the right to read them there: the space
/// goes back to the allocator only when the last handle drops. The index
/// holds one; a read holds another until it is done, so no write can
/// reuse the space under it.
#[derive(Debug)]
pub(crate) struct Extent {
    value: ValueRef,
    /// Where the space goes on drop; `None` once replay has freed it.
    freed: Option<FreedExtents>,
}

impl Extent {
    pub(crate) fn new(value: ValueRef, freed: &FreedExtents) -> Arc<Self> {
        Arc::new(Self {
            value,
            freed: Some(freed.clone()),
        })
    }

    /// The location of an extent replay replaced, which replay frees itself:
    /// its space doesn't go back to the allocator a second time.
    pub(crate) fn into_replaced(this: Arc<Self>) -> ValueRef {
        let mut extent = Arc::into_inner(this).expect("replay holds the only handle");
        extent.freed = None;
        extent.value
    }
}

impl Drop for Extent {
    fn drop(&mut self) {
        if let Some(freed) = &self.freed {
            freed.push(self.value);
        }
    }
}

#[derive(Debug)]
struct DiskState {
    alloc: FileAllocator,
    wal: WalState,
}

impl DiskState {
    /// Allocate an LSN and append `entry` to the WAL. Returns the LSN
    /// plus the page writes that must be issued (in order) to make the
    /// append durable.
    ///
    /// On failure the LSN counter is left untouched (the caller's LSN
    /// is dropped, which is fine — LSN space is u64).
    fn prepare_append(&mut self, entry: AppendEntry) -> Result<(u64, Vec<PageWrite>)> {
        let lsn = self.wal.next_lsn;
        let next_lsn =
            allocate_next_lsn(lsn).ok_or_else(|| Error::Format("wal lsn overflow".into()))?;

        let writes = if self.wal.tail.can_fit(&entry) {
            self.wal.tail.append(&entry, lsn)?;
            vec![encode_page_write(self.wal.tail_offset, &self.wal.tail)?]
        } else {
            let new_page_offset = self.alloc.reserve_tail(PAGE_SIZE_U32)?;

            let old_tail_offset = self.wal.tail_offset;
            self.wal.tail.set_next_page(new_page_offset);
            let old_tail_write = encode_page_write(old_tail_offset, &self.wal.tail)?;

            let mut new_page = WalPage::empty();
            new_page.append(&entry, lsn)?;
            let new_page_write = encode_page_write(new_page_offset, &new_page)?;

            self.wal.tail_offset = new_page_offset;
            self.wal.tail = new_page;

            vec![old_tail_write, new_page_write]
        };

        self.wal.next_lsn = next_lsn;
        Ok((lsn, writes))
    }
}

/// All on-disk state of the store.
pub(crate) struct DiskData {
    io: IoWorker,
    freed: FreedExtents,
    state: Mutex<DiskState>,
}

impl std::fmt::Debug for DiskData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskData").finish_non_exhaustive()
    }
}

impl DiskData {
    /// Mount the store. `file_len == 0` means create a fresh file;
    /// otherwise replay the existing WAL into `index`, which a fresh file
    /// leaves untouched. Extents dropped go back to the allocator through
    /// `freed`.
    pub(crate) async fn mount(
        io: IoWorker,
        file_len: u64,
        freed: FreedExtents,
        index: &mut impl ReplayIndex,
    ) -> Result<Self> {
        if file_len == 0 {
            Self::create(io, freed).await
        } else {
            Self::replay(io, file_len, freed, index).await
        }
    }

    async fn create(io: IoWorker, freed: FreedExtents) -> Result<Self> {
        let wal = WalState::fresh();
        let initial_write = encode_page_write(wal.tail_offset, &wal.tail)?;
        io.write(vec![initial_write])?.await?;
        Ok(Self {
            io,
            freed: freed.clone(),
            state: Mutex::new(DiskState {
                alloc: FileAllocator {
                    file_tail: INITIAL_FILE_TAIL,
                    holes: FileHoles::empty(),
                    freed,
                },
                wal,
            }),
        })
    }

    async fn replay(
        io: IoWorker,
        file_len: u64,
        freed: FreedExtents,
        index: &mut impl ReplayIndex,
    ) -> Result<Self> {
        if file_len < PAGE_SIZE_U64 {
            return Err(Error::Format(
                "store file shorter than first WAL page".into(),
            ));
        }

        let mut replay_state = ReplayState::init();
        let mut offset = 0_u64;
        let (last_offset, last_page) = loop {
            let page = read_page(&io, offset).await?;
            let (new_replay_state, next_page) = replay_state.process_page(&page, index)?;
            replay_state = new_replay_state;
            if let Some(next_page) = next_page {
                offset = next_page;
            } else {
                break (offset, page);
            }
        };

        let (file_tail, next_lsn, holes) = replay_state
            .finalize(file_len)
            .map_err(|_| Error::Format("replay finalize overflow".into()))?;

        Ok(Self {
            io,
            freed: freed.clone(),
            state: Mutex::new(DiskState {
                alloc: FileAllocator {
                    file_tail,
                    holes,
                    freed,
                },
                wal: WalState {
                    tail: last_page,
                    tail_offset: last_offset,
                    next_lsn,
                },
            }),
        })
    }

    /// Write value bytes into a freshly-allocated slot, then append a
    /// live WAL entry. Returns a [`WalCommit`] carrying the LSN — apply
    /// it via `Index::apply_put` so the index's ordering matches the WAL's.
    pub(crate) async fn append_put(&self, key: T4Key, value: &T4Value) -> Result<WalCommit> {
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
            extent: Extent::new(vref, &self.freed),
        })
    }

    /// Append a tombstone entry to the WAL.
    pub(crate) async fn append_tombstone(&self, key: T4Key) -> Result<WalTombstoneCommit> {
        let lsn = self.append_entry(AppendEntry::Tombstone { key }).await?;
        Ok(WalTombstoneCommit { lsn })
    }

    /// Hold off every WAL append, and every reservation of file space, until
    /// the guard drops. Reads go on.
    pub(crate) fn pause_writes(&self) -> Result<WritesPaused<'_>> {
        Ok(WritesPaused {
            _state: self.lock_state()?,
        })
    }

    /// Read the full value in `extent` from disk.
    pub(crate) async fn read_value(&self, extent: &Extent) -> Result<Vec<u8>> {
        let value = extent.value;
        let Some(value_len_u32) = NonZeroU32::new(value.length()) else {
            return Ok(Vec::new());
        };
        let padded_u32 = align_up_u32(value_len_u32, PAGE_SIZE_NZ_U32)
            .map_err(|_| Error::Format("value length exceeds io buffer limit".into()))?;
        let buf = AlignedBuf::new_zeroed(padded_u32)?;
        let buf = self.io.read_exact_at(buf, value.offset()).await?;
        let value_len = value_len_u32.get() as usize;
        Ok(buf.as_slice()[..value_len].to_vec())
    }

    /// Read a byte range within the value in `extent` from disk.
    pub(crate) async fn read_value_range(
        &self,
        extent: &Extent,
        range: RangeRequestU32,
    ) -> Result<Vec<u8>> {
        let value = extent.value;
        let range: CheckedRangeU32 = range
            .checked_against(value.length())
            .ok_or(Error::RangeOutOfBounds)?;
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let abs_start = value
            .offset()
            .checked_add(u64::from(range.start()))
            .ok_or(Error::RangeOutOfBounds)?;
        let abs_end = value
            .offset()
            .checked_add(u64::from(range.end()))
            .ok_or(Error::RangeOutOfBounds)?;

        let aligned_start = align_down_u64(abs_start, PAGE_SIZE_U64);
        let aligned_end = align_up_u64(abs_end, PAGE_SIZE_U64).ok_or(Error::RangeOutOfBounds)?;
        let read_len_u64 = aligned_end
            .checked_sub(aligned_start)
            .ok_or(Error::RangeOutOfBounds)?;
        let read_len_u32: u32 = read_len_u64
            .try_into()
            .map_err(|_| Error::RangeOutOfBounds)?;
        let read_len_u32 = NonZeroU32::new(read_len_u32).ok_or(Error::RangeOutOfBounds)?;
        let buf = AlignedBuf::new_zeroed(read_len_u32)?;
        let buf = self.io.read_exact_at(buf, aligned_start).await?;

        let slice_start_u64 = abs_start
            .checked_sub(aligned_start)
            .ok_or(Error::RangeOutOfBounds)?;
        let slice_start_u32: u32 = slice_start_u64
            .try_into()
            .map_err(|_| Error::RangeOutOfBounds)?;
        let slice_start = slice_start_u32 as usize;
        let slice_len = range.len() as usize;
        let slice_end = slice_start
            .checked_add(slice_len)
            .ok_or(Error::RangeOutOfBounds)?;
        Ok(buf.as_slice()[slice_start..slice_end].to_vec())
    }

    pub(crate) async fn sync(&self) -> Result<()> {
        self.io.fsync()?.await
    }

    // -- private -------------------------------------------------------------

    fn lock_state(&self) -> Result<MutexGuard<'_, DiskState>> {
        self.state.lock().map_err(|_| Error::LockPoisoned)
    }

    fn reserve_value_space(&self, padded_len: u32) -> Result<u64> {
        self.lock_state()?.alloc.reserve_value(padded_len)
    }

    async fn append_entry(&self, entry: AppendEntry) -> Result<u64> {
        let (write, lsn) = {
            let mut state = self.lock_state()?;
            let (lsn, writes) = state.prepare_append(entry)?;
            (self.io.write_ordered(writes)?, lsn)
        };
        write.await?;
        Ok(lsn)
    }
}

/// Writes paused by [`DiskData::pause_writes`], until dropped.
pub(crate) struct WritesPaused<'a> {
    _state: MutexGuard<'a, DiskState>,
}

// ---------------------------------------------------------------------------
// Commit receipts
// ---------------------------------------------------------------------------

/// Durable WAL receipt for a put. Carries the LSN that decides ordering
/// when the put is applied to the index and the extent holding the value
/// bytes. Construct only via [`DiskData::append_put`]; consume via
/// `Index::apply_put`. Dropped unapplied, its extent goes back to the
/// allocator.
#[must_use = "WalCommit must be applied to the index via Index::apply_put"]
pub(crate) struct WalCommit {
    lsn: u64,
    extent: Arc<Extent>,
}

impl WalCommit {
    pub(crate) fn into_parts(self) -> (u64, Arc<Extent>) {
        (self.lsn, self.extent)
    }
}

/// Durable WAL receipt for a tombstone. Carries the LSN that decides
/// ordering. Tombstones don't allocate value space, so dropping the
/// receipt without applying it is harmless.
#[must_use = "WalTombstoneCommit must be applied via Index::apply_remove"]
pub(crate) struct WalTombstoneCommit {
    lsn: u64,
}

impl WalTombstoneCommit {
    pub(crate) fn lsn(&self) -> u64 {
        self.lsn
    }
}
