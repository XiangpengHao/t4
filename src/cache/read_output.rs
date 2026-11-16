use std::alloc::{Layout, alloc, dealloc};
use std::any::Any;
use std::num::NonZeroU32;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::{PAGE_SIZE, PAGE_SIZE_U64};

/// Page-aligned to the file's pages, so a direct read from a disk chunk
/// lands in place.
#[derive(Debug)]
pub(super) struct ReadOutput {
    memory: Arc<OutputMemory>,
    start: u64,
    asked: Range<u64>,
    #[cfg(debug_assertions)]
    written: Vec<Range<u64>>,
}

impl ReadOutput {
    pub(super) fn new(asked: Range<u64>) -> Result<Self> {
        let memory = OutputMemory::new(Self::index(Self::padded_len(&asked)))?;
        Self::over_memory(asked, memory)
    }

    /// # Safety
    ///
    /// `memory` is page-aligned, valid for `len` bytes while `keeper` lives,
    /// and touched by nothing else of this process meanwhile.
    pub(super) unsafe fn over(
        asked: Range<u64>,
        memory: NonNull<u8>,
        len: usize,
        keeper: Box<dyn Any + Send + Sync>,
    ) -> Result<Self> {
        if (len as u64) < Self::padded_len(&asked) {
            return Err(Error::InvalidArgument(
                "an output's memory is shorter than its bytes",
            ));
        }
        let memory = OutputMemory {
            ptr: memory,
            len,
            place: OutputPlace::Kept { _keeper: keeper },
        };
        Self::over_memory(asked, memory)
    }

    pub(super) fn padded_len(asked: &Range<u64>) -> u64 {
        let start = asked.start / PAGE_SIZE_U64 * PAGE_SIZE_U64;
        let end = (asked.end.div_ceil(PAGE_SIZE_U64) * PAGE_SIZE_U64).max(start + PAGE_SIZE_U64);
        end - start
    }

    fn over_memory(asked: Range<u64>, memory: OutputMemory) -> Result<Self> {
        let start = asked.start / PAGE_SIZE_U64 * PAGE_SIZE_U64;
        let end = start + Self::padded_len(&asked);
        memory.fault_in();
        // Nothing reads into the rounding's bytes: zero them so the buffer ends initialized.
        // SAFETY: both runs lie within the allocation, which nothing else holds yet.
        unsafe {
            let base = memory.ptr.as_ptr();
            base.write_bytes(0, Self::index(asked.start - start));
            base.add(Self::index(asked.end - start))
                .write_bytes(0, Self::index(end - asked.end));
        }
        Ok(Self {
            memory: Arc::new(memory),
            start,
            asked,
            #[cfg(debug_assertions)]
            written: Vec::new(),
        })
    }

    pub(super) fn lend(&mut self, pages: Range<u64>) -> Result<AlignedBuf> {
        let len = u32::try_from(pages.end - pages.start)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(Error::InvalidArgument("a read fills a page or more"))?;
        assert!(
            pages.start.is_multiple_of(PAGE_SIZE_U64)
                && pages.end.is_multiple_of(PAGE_SIZE_U64)
                && self.start <= pages.start
                && pages.end <= self.end(),
            "{pages:?} are whole pages within the output"
        );
        self.note_written(pages.clone());
        // SAFETY: whole pages inside the allocation, which no other write touches (`note_written`).
        let buffer = unsafe {
            let at = self.memory.ptr.add(Self::index(pages.start - self.start));
            AlignedBuf::lent(at, len, Arc::clone(&self.memory) as _)
        };
        Ok(buffer)
    }

    pub(super) fn pages_over(&self, bytes: Range<u64>) -> Option<Range<u64>> {
        assert!(
            bytes.start.is_multiple_of(PAGE_SIZE_U64),
            "{bytes:?} start on a page"
        );
        let start = bytes.start.max(self.start);
        let end = bytes.end.next_multiple_of(PAGE_SIZE_U64).min(self.end());
        (start < end).then_some(start..end)
    }

    pub(super) fn copy_in(&mut self, at: u64, bytes: &[u8]) {
        let range = at..at + bytes.len() as u64;
        assert!(
            self.start <= range.start && range.end <= self.end(),
            "{range:?} lie within the output"
        );
        self.note_written(range);
        // SAFETY: inside the allocation, and no other write touches the range (`note_written`).
        unsafe {
            let to = self.memory.ptr.add(Self::index(at - self.start));
            to.as_ptr()
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }

    pub(super) fn filled_in_place(self) -> bool {
        #[cfg(debug_assertions)]
        self.assert_asked_written();
        Arc::try_unwrap(self.memory).is_ok()
    }

    pub(super) fn into_bytes(self) -> Option<Bytes> {
        #[cfg(debug_assertions)]
        self.assert_asked_written();
        let at = Self::index(self.asked.start - self.start);
        let len = Self::index(self.asked.end - self.asked.start);
        let memory = Arc::try_unwrap(self.memory).ok()?;
        Some(Bytes::from_owner(FilledOutput(memory)).slice(at..at + len))
    }

    pub(super) fn asked(&self) -> Range<u64> {
        self.asked.clone()
    }

    fn end(&self) -> u64 {
        self.start + Self::padded_len(&self.asked)
    }

    fn index(within: u64) -> usize {
        usize::try_from(within).expect("an output fits in memory")
    }

    #[cfg(debug_assertions)]
    fn note_written(&mut self, range: Range<u64>) {
        assert!(
            self.written
                .iter()
                .all(|other| other.end <= range.start || range.end <= other.start),
            "{range:?} overlaps a write before it"
        );
        self.written.push(range);
    }

    #[cfg(not(debug_assertions))]
    fn note_written(&mut self, _range: Range<u64>) {}

    #[cfg(debug_assertions)]
    fn assert_asked_written(&self) {
        let mut written = self.written.clone();
        written.sort_by_key(|range| range.start);
        let mut covered = self.asked.start;
        for range in written {
            if range.start <= covered {
                covered = covered.max(range.end);
            }
        }
        assert!(
            covered >= self.asked.end,
            "{:?} written up to {covered} only",
            self.asked
        );
    }
}

#[derive(Debug)]
struct OutputMemory {
    ptr: NonNull<u8>,
    len: usize,
    place: OutputPlace,
}

enum OutputPlace {
    Allocated(Layout),
    Kept { _keeper: Box<dyn Any + Send + Sync> },
}

impl std::fmt::Debug for OutputPlace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allocated(layout) => f.debug_tuple("Allocated").field(layout).finish(),
            Self::Kept { .. } => f.debug_struct("Kept").finish_non_exhaustive(),
        }
    }
}

// SAFETY: writes are to disjoint ranges, and reads come after every write (`into_bytes`).
unsafe impl Send for OutputMemory {}
unsafe impl Sync for OutputMemory {}

impl OutputMemory {
    fn new(len: usize) -> Result<Self> {
        let layout = Layout::from_size_align(len, PAGE_SIZE)
            .map_err(|_| Error::InvalidArgument("an output too large to allocate"))?;
        // SAFETY: the layout is a page or more, never empty.
        let ptr = NonNull::new(unsafe { alloc(layout) }).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "output allocation failed",
            ))
        })?;
        Ok(Self {
            ptr,
            len,
            place: OutputPlace::Allocated(layout),
        })
    }

    /// Faulted in on the reading task: on the one I/O worker, zeroing every read's fresh
    /// pages caps the disk tier (`doc/04-multi-device.md`, "Measured").
    fn fault_in(&self) {
        #[cfg(target_os = "linux")]
        // SAFETY: the output's own pages; populating them changes none of their bytes.
        unsafe {
            libc::madvise(
                self.ptr.as_ptr().cast(),
                self.len,
                libc::MADV_POPULATE_WRITE,
            );
        }
    }
}

impl Drop for OutputMemory {
    fn drop(&mut self) {
        if let OutputPlace::Allocated(layout) = self.place {
            // SAFETY: allocated with this layout, and freed once, here.
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

struct FilledOutput(OutputMemory);

impl AsRef<[u8]> for FilledOutput {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: every byte is written, and no lent buffer is left to write more (`into_bytes`).
        unsafe { std::slice::from_raw_parts(self.0.ptr.as_ptr(), self.0.len) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: u64 = PAGE_SIZE_U64;

    #[test]
    fn copies_and_lent_reads_land_where_their_bytes_go() {
        let mut output = ReadOutput::new(100..3 * PAGE + 50).unwrap();
        output.copy_in(100, &[1; PAGE as usize - 100]);
        let mut lent = output.lend(PAGE..3 * PAGE).unwrap();
        // SAFETY: the lent pages are this test's to write, as a read would.
        unsafe { lent.as_mut_ptr().write_bytes(2, 2 * PAGE as usize) };
        drop(lent);
        output.copy_in(3 * PAGE, &[3; 50]);
        let bytes = output.into_bytes().unwrap();
        let mut expected = vec![1; PAGE as usize - 100];
        expected.extend(vec![2; 2 * PAGE as usize]);
        expected.extend(vec![3; 50]);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn a_lent_buffer_still_out_keeps_the_bytes() {
        let mut output = ReadOutput::new(0..PAGE).unwrap();
        let lent = output.lend(0..PAGE).unwrap();
        assert!(output.into_bytes().is_none(), "a read may still write them");
        drop(lent);
    }

    #[test]
    fn an_output_over_kept_memory_drops_its_keeper_last() {
        struct Keeper {
            _memory: OutputMemory,
            dropped: std::sync::mpsc::Sender<()>,
        }
        impl Drop for Keeper {
            fn drop(&mut self) {
                let _ = self.dropped.send(());
            }
        }
        let memory = OutputMemory::new(2 * PAGE as usize).unwrap();
        let ptr = memory.ptr;
        let (dropped, keeper_gone) = std::sync::mpsc::channel();
        let keeper = Box::new(Keeper {
            _memory: memory,
            dropped,
        });
        // SAFETY: the keeper holds the two pages, page-aligned, and nothing else touches them.
        let output = unsafe { ReadOutput::over(0..2 * PAGE, ptr, 2 * PAGE as usize, keeper) };
        let mut output = output.unwrap();
        output.copy_in(0, &[1; PAGE as usize]);
        let lent = output.lend(PAGE..2 * PAGE).unwrap();
        drop(output);
        assert!(keeper_gone.try_recv().is_err(), "the lent buffer holds it");
        drop(lent);
        assert!(keeper_gone.try_recv().is_ok());

        let short = OutputMemory::new(PAGE as usize).unwrap();
        let ptr = short.ptr;
        // SAFETY: as above, a page.
        let refused = unsafe { ReadOutput::over(0..2 * PAGE, ptr, PAGE as usize, Box::new(short)) };
        assert!(refused.is_err(), "a page holds no two");
    }

    #[test]
    fn an_empty_read_takes_no_bytes() {
        let output = ReadOutput::new(5..5).unwrap();
        assert_eq!(output.into_bytes().unwrap(), Bytes::new());
    }

    #[test]
    #[should_panic(expected = "overlaps a write before it")]
    #[cfg(debug_assertions)]
    fn overlapping_writes_are_refused() {
        let mut output = ReadOutput::new(0..2 * PAGE).unwrap();
        let _lent = output.lend(0..PAGE).unwrap();
        output.copy_in(PAGE - 1, &[0; 2]);
    }
}
