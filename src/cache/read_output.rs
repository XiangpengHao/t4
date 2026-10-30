//! [`ReadOutput`]: the buffer a read's bytes land in, handed to the caller
//! as `Bytes` with no copy (`doc/03-cache.md`, decision 3).

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

/// The buffer a read's bytes land in: page-aligned, over the file's bytes
/// the read asked for rounded out to whole pages. A block starts on a page
/// boundary of the file, so its pages fall on pages of the buffer, and a
/// direct read from a disk chunk lands in place, through a buffer the
/// output lends ([`Self::lend`]). Hits in memory and fetched bytes are
/// copied in ([`Self::copy_in`]): once, where they go.
///
/// Nothing zeroes the buffer: the reads and copies write every byte asked
/// for, and the bytes the rounding adds, under a page at each end, are
/// zeroed when it is made, so that it is initialized throughout once
/// filled. The caller gets it as `Bytes`, sliced to the bytes asked for
/// ([`Self::into_bytes`]), and it is freed when the last of them drops.
#[derive(Debug)]
pub(super) struct ReadOutput {
    memory: Arc<OutputMemory>,
    /// The file's byte the buffer starts at: a page boundary.
    start: u64,
    /// The file's bytes the read asked for.
    asked: Range<u64>,
    /// The file's bytes written so far, to check that writes never overlap
    /// and that they cover what was asked.
    #[cfg(debug_assertions)]
    written: Vec<Range<u64>>,
}

impl ReadOutput {
    /// A buffer for `asked`, the file's bytes a read takes: a page at least.
    pub(super) fn new(asked: Range<u64>) -> Result<Self> {
        let memory = OutputMemory::new(Self::index(Self::padded_len(&asked)))?;
        Self::over_memory(asked, memory)
    }

    /// A buffer for `asked` over `len` bytes at `memory`, another's memory,
    /// which `keeper` keeps valid: a span of a client's region
    /// (`doc/11-shared-mode.md`, decision 5). It isn't freed when the output
    /// and the buffers it lent drop; `keeper` is dropped then.
    ///
    /// # Errors
    ///
    /// If `len` is shorter than an output of `asked` takes
    /// ([`Self::padded_len`]).
    ///
    /// # Safety
    ///
    /// `memory` is page-aligned, and valid for reads and writes of `len`
    /// bytes as long as `keeper` lives. Nothing of this process's but the
    /// output and the buffers it lends touches them meanwhile.
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

    /// The bytes an output of `asked` takes: the file's bytes asked rounded
    /// out to whole pages, a page at least.
    pub(super) fn padded_len(asked: &Range<u64>) -> u64 {
        let start = asked.start / PAGE_SIZE_U64 * PAGE_SIZE_U64;
        let end = (asked.end.div_ceil(PAGE_SIZE_U64) * PAGE_SIZE_U64).max(start + PAGE_SIZE_U64);
        end - start
    }

    /// The output of `asked` over `memory`, its bytes from the page that
    /// holds the first asked, its padding zeroed.
    fn over_memory(asked: Range<u64>, memory: OutputMemory) -> Result<Self> {
        let start = asked.start / PAGE_SIZE_U64 * PAGE_SIZE_U64;
        let end = start + Self::padded_len(&asked);
        memory.fault_in();
        // The rounding's bytes, which nothing reads into: zeroed, so the
        // buffer is initialized throughout once every byte asked is written.
        // SAFETY: both runs lie within the allocation, which nothing else
        // holds yet.
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

    /// A buffer over `pages`, the file's bytes a direct read of a disk chunk
    /// or a fetch fills: whole pages, within the output. It holds the
    /// output's memory until it drops, however long the read or the fetch
    /// takes, whatever becomes of the output meanwhile.
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
        // SAFETY: `pages` lie within the allocation, from a page boundary
        // of it, as it starts on one. No other write touches them
        // (`note_written`), and nothing reads them until every lent buffer
        // is back (`into_bytes`). The buffer keeps the allocation alive.
        let buffer = unsafe {
            let at = self.memory.ptr.add(Self::index(pages.start - self.start));
            AlignedBuf::lent(at, len, Arc::clone(&self.memory) as _)
        };
        Ok(buffer)
    }

    /// The output's pages over `bytes`, the file's from a page boundary, as
    /// [`Self::lend`] takes them: the pages they share with the output;
    /// `None` if none.
    pub(super) fn pages_over(&self, bytes: Range<u64>) -> Option<Range<u64>> {
        assert!(
            bytes.start.is_multiple_of(PAGE_SIZE_U64),
            "{bytes:?} start on a page"
        );
        let start = bytes.start.max(self.start);
        let end = bytes.end.next_multiple_of(PAGE_SIZE_U64).min(self.end());
        (start < end).then_some(start..end)
    }

    /// Copies `bytes`, the file's from `at`, into the output.
    pub(super) fn copy_in(&mut self, at: u64, bytes: &[u8]) {
        let range = at..at + bytes.len() as u64;
        assert!(
            self.start <= range.start && range.end <= self.end(),
            "{range:?} lie within the output"
        );
        self.note_written(range);
        // SAFETY: the bytes lie within the allocation, and no other write
        // touches them (`note_written`), so no lent buffer's read does.
        unsafe {
            let to = self.memory.ptr.add(Self::index(at - self.start));
            to.as_ptr()
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }

    /// Whether every byte asked for is written, in place, and no buffer the
    /// output lent is left: so for an output over another's memory, whose
    /// keeper goes with it.
    pub(super) fn filled_in_place(self) -> bool {
        #[cfg(debug_assertions)]
        self.assert_asked_written();
        Arc::try_unwrap(self.memory).is_ok()
    }

    /// The bytes asked for, once every one is written: `None` while a
    /// buffer the output lent isn't back, as after a read given up on.
    pub(super) fn into_bytes(self) -> Option<Bytes> {
        #[cfg(debug_assertions)]
        self.assert_asked_written();
        let at = Self::index(self.asked.start - self.start);
        let len = Self::index(self.asked.end - self.asked.start);
        let memory = Arc::try_unwrap(self.memory).ok()?;
        Some(Bytes::from_owner(FilledOutput(memory)).slice(at..at + len))
    }

    /// The file's bytes the read asked for.
    pub(super) fn asked(&self) -> Range<u64> {
        self.asked.clone()
    }

    fn end(&self) -> u64 {
        self.start + Self::padded_len(&self.asked)
    }

    /// A length or an offset within the output, as a `usize`: the output is
    /// in memory, so it fits.
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

/// The memory under a [`ReadOutput`]: an allocation, freed when the last
/// of the output and the buffers it lent drops, or another's memory.
#[derive(Debug)]
struct OutputMemory {
    ptr: NonNull<u8>,
    len: usize,
    place: OutputPlace,
}

/// Whose memory an output's is.
enum OutputPlace {
    /// The output's own, allocated with this layout.
    Allocated(Layout),
    /// Another's, which the keeper keeps valid until it drops.
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

// SAFETY: the bytes are written through disjoint ranges, each by one
// direct read or one copy (`ReadOutput::note_written`), and read only once
// every write is done (`ReadOutput::into_bytes`).
unsafe impl Send for OutputMemory {}
unsafe impl Sync for OutputMemory {}

impl OutputMemory {
    /// `len` bytes, page-aligned, as they come: not zeroed.
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

    /// Faults the pages in, here, on the reading task. A direct read pins
    /// the pages it fills on the one I/O worker, which would otherwise fault
    /// in, and the kernel zero, every read's fresh pages alone: the worker
    /// then caps the disk tier (`doc/04-multi-device.md`, "Measured").
    fn fault_in(&self) {
        #[cfg(target_os = "linux")]
        // SAFETY: the range is the output's memory, page-aligned. Populating
        // its pages changes no byte of them: fresh anonymous or shared pages
        // read as zeros either way, and a page already there keeps its bytes.
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
        // Another's memory goes back with its keeper.
        if let OutputPlace::Allocated(layout) = self.place {
            // SAFETY: allocated with this layout, and freed once, here.
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

/// An output's memory once every write to it is done: initialized
/// throughout, so it can be read as a slice.
struct FilledOutput(OutputMemory);

impl AsRef<[u8]> for FilledOutput {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: every byte is written (`ReadOutput::new` and the reads and
        // copies it took), and nothing writes it any more: no buffer it lent
        // is left, or `into_bytes` couldn't have taken it.
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

    /// An output over another's memory keeps its keeper until the output
    /// and every buffer it lent have dropped: what the owner waits for
    /// before it gives a client its span back (`doc/11-shared-mode.md`).
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
        // SAFETY: the keeper holds the two pages, page-aligned, and nothing
        // else touches them.
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
