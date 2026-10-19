use std::num::NonZeroU32;
use std::sync::PoisonError;

use vstd::prelude::*;

use crate::buffer::{AlignedBuf, align_down_u64, align_up_u64};
use crate::input_kv::{FileHoles, InputError, T4Key, T4Value, ValueRef};
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::{Arc, Mutex, MutexGuard};
use crate::range::{CheckedRangeU32, RangeRequestU32};
use crate::wal::page::{AppendEntry, WalPage};
use crate::wal::replay::{ReplayIndex, ReplayState};
use crate::wal::{INITIAL_FILE_TAIL, WalState, allocate_next_lsn, encode_page_write, read_page};
use crate::{PAGE_SIZE_U32, PAGE_SIZE_U64};

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

    /// The value's length in bytes.
    pub(crate) fn value_len(&self) -> u32 {
        self.value.length()
    }

    /// The window that reads the whole value.
    pub(crate) fn whole_window(&self) -> Result<ValueWindow> {
        ValueWindow::whole(self.value_len())
    }

    /// The window that reads `range` of the value; `RangeOutOfBounds` if
    /// the range runs past its end.
    pub(crate) fn range_window(&self, range: RangeRequestU32) -> Result<ValueWindow> {
        ValueWindow::of(
            range
                .checked_against(self.value_len())
                .ok_or(Error::RangeOutOfBounds)?,
        )
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

/// The pages one read of a value covers. The bytes asked for, a range of
/// the value, lie inside a page-aligned window of it,
/// read whole into a buffer of exactly the window's size. A value starts on
/// a page boundary on disk, so the window depends on the range alone: a
/// client sizes its buffer with the same arithmetic the server reads with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueWindow {
    /// Where the window starts in the value: a page boundary.
    first_byte: u32,
    /// The window's size, in whole pages; 0 when nothing is asked for.
    buffer_len: u32,
    /// Where the bytes asked for start in the window.
    start: u32,
    len: u32,
}

impl ValueWindow {
    /// The window for `range` of a value.
    pub(crate) fn of(range: CheckedRangeU32) -> Result<Self> {
        if range.is_empty() {
            return Ok(Self {
                first_byte: 0,
                buffer_len: 0,
                start: 0,
                len: 0,
            });
        }
        let first_byte = align_down_u64(u64::from(range.start()), PAGE_SIZE_U64);
        let last_byte =
            align_up_u64(u64::from(range.end()), PAGE_SIZE_U64).ok_or(Error::RangeOutOfBounds)?;
        let buffer_len =
            u32::try_from(last_byte - first_byte).map_err(|_| Error::RangeOutOfBounds)?;
        // `first_byte` is at most `range.start()`, a `u32`.
        let first_byte = first_byte as u32;
        Ok(Self {
            first_byte,
            buffer_len,
            start: range.start() - first_byte,
            len: range.len(),
        })
    }

    /// The window for `range` of a value of any length. A value starts on a
    /// page boundary, so its length doesn't move the window, only decides
    /// whether the range fits: that is the server's to check.
    pub(crate) fn of_any_value(range: RangeRequestU32) -> Result<Self> {
        Self::of(
            range
                .checked_against(u32::MAX)
                .expect("every range ends within u32"),
        )
    }

    /// The window for a whole value of `len` bytes: `len` padded to whole
    /// pages.
    pub(crate) fn whole(len: u32) -> Result<Self> {
        let range = RangeRequestU32::from_u32(0, len)
            .and_then(|range| range.checked_against(len))
            .expect("a value holds the range of all its bytes");
        Self::of(range).map_err(|_| Error::InputError(InputError::ValueTooLarge(len as usize)))
    }

    pub(crate) fn buffer_len(&self) -> u32 {
        self.buffer_len
    }

    pub(crate) fn start(&self) -> u32 {
        self.start
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }
}

/// A value as the store writes it: its bytes, zero-padded to whole pages,
/// in a buffer direct I/O writes from. An empty value has no buffer.
pub(crate) struct PaddedValue {
    buf: Option<AlignedBuf>,
    len: u32,
}

impl PaddedValue {
    /// The buffer a value of `len` bytes needs: `len` padded to whole
    /// pages; `None` for an empty value.
    pub(crate) fn buffer_len(len: u32) -> Result<Option<NonZeroU32>> {
        Ok(NonZeroU32::new(ValueWindow::whole(len)?.buffer_len()))
    }

    pub(crate) fn empty() -> Self {
        Self { buf: None, len: 0 }
    }

    /// `value`, copied into a buffer of its own.
    pub(crate) fn copy_of(value: &T4Value) -> Result<Self> {
        let len = value.len_u32();
        if Self::buffer_len(len)?.is_none() {
            return Ok(Self::empty());
        }
        Ok(Self {
            buf: Some(AlignedBuf::from_padded_slice(value.as_bytes())?),
            len,
        })
    }

    /// The value in the first `len` bytes of `buf`, which holds exactly
    /// [`Self::buffer_len`] bytes. The padding is zeroed here, whatever `buf`
    /// held there, so the file holds zeros past every value.
    pub(crate) fn in_buffer(mut buf: AlignedBuf, len: u32) -> Result<Self> {
        if Self::buffer_len(len)?.map(NonZeroU32::get) != Some(buf.len_u32()) {
            return Err(Error::InvalidArgument(
                "value buffer is not its padded length",
            ));
        }
        buf.zero_from(len as usize);
        Ok(Self {
            buf: Some(buf),
            len,
        })
    }
}

#[derive(Debug)]
struct DiskState {
    alloc: FileAllocator,
    wal: WalState,
}

impl DiskState {
    /// Allocate an LSN and append `entry` to the WAL. Returns the LSN
    /// plus the page writes that make the append durable, in the order they
    /// must land: a new page before the link to it, so that no crash
    /// leaves the WAL pointing at a page never written.
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

            vec![new_page_write, old_tail_write]
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
    pub(crate) async fn append_put(&self, key: T4Key, value: PaddedValue) -> Result<WalCommit> {
        let PaddedValue {
            buf,
            len: value_len,
        } = value;
        let value_offset = match buf {
            None => 0,
            Some(buf) => {
                let value_offset = self.reserve_value_space(buf.len_u32())?;
                self.io
                    .write(vec![PageWrite {
                        buf,
                        offset: value_offset,
                    }])?
                    .await?;
                value_offset
            }
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

    /// Reads the whole value in `extent`.
    pub(crate) async fn read_value(&self, extent: &Extent) -> Result<Vec<u8>> {
        self.read_window(extent, &extent.whole_window()?).await
    }

    /// Reads `window` of the value in `extent`: the bytes it asks for.
    pub(crate) async fn read_window(
        &self,
        extent: &Extent,
        window: &ValueWindow,
    ) -> Result<Vec<u8>> {
        let Some(buffer_len) = NonZeroU32::new(window.buffer_len) else {
            return Ok(Vec::new());
        };
        let buf = AlignedBuf::new_zeroed(buffer_len)?;
        let buf = self.read_window_into(extent, window, buf).await?;
        let start = window.start as usize;
        Ok(buf.as_slice()[start..start + window.len as usize].to_vec())
    }

    /// Reads `window` of the value in `extent` into `buf`, which holds
    /// exactly the window: the bytes asked for land at `window.start()`.
    pub(crate) async fn read_window_into(
        &self,
        extent: &Extent,
        window: &ValueWindow,
        buf: AlignedBuf,
    ) -> Result<AlignedBuf> {
        if buf.len_u32() != window.buffer_len {
            return Err(Error::InvalidArgument(
                "read buffer is not its window's size",
            ));
        }
        let offset = extent
            .value
            .offset()
            .checked_add(u64::from(window.first_byte))
            .ok_or(Error::RangeOutOfBounds)?;
        self.io.read_exact_at(buf, offset).await
    }

    // -- private -------------------------------------------------------------

    fn lock_state(&self) -> Result<MutexGuard<'_, DiskState>> {
        self.state.lock().map_err(|_| Error::LockPoisoned)
    }

    fn reserve_value_space(&self, padded_len: u32) -> Result<u64> {
        self.lock_state()?.alloc.reserve_value(padded_len)
    }

    async fn append_entry(&self, entry: AppendEntry) -> Result<u64> {
        let (writes, lsn) = {
            let mut state = self.lock_state()?;
            let (lsn, writes) = state.prepare_append(entry)?;
            // One ordered write each: the worker starts one only once the
            // one before has completed, which `O_DSYNC` makes durable. A
            // single request's pages would land in any order on all but
            // io_uring.
            let writes = writes
                .into_iter()
                .map(|write| self.io.write_ordered(vec![write]))
                .collect::<Result<Vec<_>>>()?;
            (writes, lsn)
        };
        for write in writes {
            write.await?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PAGE_SIZE;

    const PAGE: u32 = PAGE_SIZE as u32;

    fn window(start: u32, len: u32) -> ValueWindow {
        ValueWindow::of_any_value(RangeRequestU32::from_u32(start, len).unwrap()).unwrap()
    }

    #[test]
    fn windows_cover_whole_pages_around_the_range() {
        let expect = |first_byte, buffer_len, start, len| ValueWindow {
            first_byte,
            buffer_len,
            start,
            len,
        };
        assert_eq!(window(0, 0), expect(0, 0, 0, 0));
        assert_eq!(window(9000, 0), expect(0, 0, 0, 0));
        assert_eq!(window(0, 1), expect(0, PAGE, 0, 1));
        assert_eq!(window(0, PAGE), expect(0, PAGE, 0, PAGE));
        assert_eq!(window(PAGE - 1, 2), expect(0, 2 * PAGE, PAGE - 1, 2));
        assert_eq!(window(PAGE + 5, 10), expect(PAGE, PAGE, 5, 10));
        assert_eq!(
            ValueWindow::whole(PAGE + 1).unwrap(),
            expect(0, 2 * PAGE, 0, PAGE + 1)
        );
        assert!(matches!(
            ValueWindow::whole(u32::MAX),
            Err(Error::InputError(InputError::ValueTooLarge(_)))
        ));
    }

    #[test]
    fn a_value_in_a_buffer_gets_zeroed_padding() {
        let len = 10;
        let buffer_len = PaddedValue::buffer_len(len).unwrap().unwrap();
        let mut buf = AlignedBuf::new_zeroed(buffer_len).unwrap();
        buf.as_mut_slice().fill(0xee);
        let value = PaddedValue::in_buffer(buf, len).unwrap();
        let bytes = value.buf.as_ref().unwrap().as_slice();
        assert!(bytes[..len as usize].iter().all(|byte| *byte == 0xee));
        assert!(bytes[len as usize..].iter().all(|byte| *byte == 0));

        let too_large = AlignedBuf::new_zeroed(NonZeroU32::new(2 * PAGE).unwrap()).unwrap();
        assert!(PaddedValue::in_buffer(too_large, len).is_err());
        assert!(PaddedValue::buffer_len(0).unwrap().is_none());
    }
}
