//! An owner maps a region only if it can't shrink: shrunk, it would crash the owner with `SIGBUS`.

use crate::io::sync::{Mutex, MutexGuard};
use std::any::Any;
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
#[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::{Arc, PoisonError};

use bytes::Bytes;
use rustix::mm::{MapFlags, ProtFlags};

#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
pub(in crate::cache::cache_stores::shared) type RegionDescriptor = Arc<SharedMapping>;
#[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
pub(in crate::cache::cache_stores::shared) type RegionDescriptor = OwnedFd;

use crate::PAGE_SIZE_U64;
use crate::cache::read_output::ReadOutput;
use crate::io::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache::cache_stores::shared) struct SpanName {
    offset: u64,
    len: u64,
}

impl SpanName {
    pub(in crate::cache::cache_stores::shared) fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    pub(in crate::cache::cache_stores::shared) fn offset(self) -> u64 {
        self.offset
    }

    pub(in crate::cache::cache_stores::shared) fn len(self) -> u64 {
        self.len
    }
}

pub(in crate::cache::cache_stores::shared) struct SharedMapping {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: plain memory, valid until dropped; its holders order who writes which bytes when.
unsafe impl Send for SharedMapping {}
unsafe impl Sync for SharedMapping {}

impl SharedMapping {
    #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
    fn map(memory: BorrowedFd<'_>, len: usize) -> io::Result<Self> {
        // SAFETY: a fresh mapping, at an address the kernel picks, aliases no memory of ours.
        let ptr = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                memory,
                0,
            )?
        };
        let ptr = NonNull::new(ptr.cast()).ok_or(io::ErrorKind::OutOfMemory)?;
        Ok(Self { ptr, len })
    }

    #[cfg(all(any(test, fuzzing), feature = "shuttle"))]
    fn allocate(len: usize) -> io::Result<Self> {
        // SAFETY: a fresh private mapping owned until the final region drops.
        let ptr = unsafe {
            rustix::mm::mmap_anonymous(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )?
        };
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).ok_or(io::ErrorKind::OutOfMemory)?,
            len,
        })
    }

    fn at(&self, span: SpanName) -> Option<NonNull<u8>> {
        let end = span.offset.checked_add(span.len)?;
        if end > self.len as u64 {
            return None;
        }
        // SAFETY: the span lies within the mapping.
        Some(unsafe { self.ptr.add(span.offset as usize) })
    }
}

impl Drop for SharedMapping {
    fn drop(&mut self) {
        // SAFETY: mapped by `map` with this length; every pointer into it holds the mapping.
        unsafe {
            let _ = rustix::mm::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

pub(in crate::cache::cache_stores::shared) struct ClientRegion {
    #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
    memory: OwnedFd,
    mapping: Arc<SharedMapping>,
    free: Mutex<FreeSpans>,
}

impl ClientRegion {
    /// Pages take memory only once touched.
    pub(in crate::cache::cache_stores::shared) const BYTES: u64 = 1 << 30;

    #[cfg(all(target_os = "linux", not(all(any(test, fuzzing), feature = "shuttle"))))]
    const PUNCHED_FROM: u64 = 2 << 20;

    #[cfg(all(
        not(target_os = "linux"),
        not(all(any(test, fuzzing), feature = "shuttle"))
    ))]
    const NAME_TRIES: u32 = 16;

    pub(in crate::cache::cache_stores::shared) fn make() -> io::Result<Arc<Self>> {
        #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
        let memory = Self::shared_memory()?;
        #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
        let mapping = SharedMapping::map(memory.as_fd(), Self::BYTES as usize)?;
        #[cfg(all(any(test, fuzzing), feature = "shuttle"))]
        let mapping = SharedMapping::allocate(Self::BYTES as usize)?;
        Ok(Arc::new(Self {
            #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
            memory,
            mapping: Arc::new(mapping),
            free: Mutex::new(FreeSpans::new(Self::BYTES / PAGE_SIZE_U64)),
        }))
    }

    #[cfg(all(target_os = "linux", not(all(any(test, fuzzing), feature = "shuttle"))))]
    fn shared_memory() -> io::Result<OwnedFd> {
        use rustix::fs::{MemfdFlags, SealFlags};
        let flags = MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING;
        let memory = rustix::fs::memfd_create("t4-region", flags)?;
        rustix::fs::ftruncate(&memory, Self::BYTES)?;
        let seals = SealFlags::SHRINK | SealFlags::GROW | SealFlags::SEAL;
        rustix::fs::fcntl_add_seals(&memory, seals)?;
        Ok(memory)
    }

    /// Unlinked once open, so no other process opens it by name.
    #[cfg(all(
        not(target_os = "linux"),
        not(all(any(test, fuzzing), feature = "shuttle"))
    ))]
    fn shared_memory() -> io::Result<OwnedFd> {
        use rustix::fs::Mode;
        use rustix::shm::{self, OFlags};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.subsec_nanos());
        for attempt in 0..Self::NAME_TRIES {
            // macOS keeps a name to 31 bytes: this one is 25 at most.
            let name = format!("/t4-{}-{now:x}-{attempt}", std::process::id());
            let flags = OFlags::CREATE | OFlags::EXCL | OFlags::RDWR;
            match shm::open(name.as_str(), flags, Mode::RUSR | Mode::WUSR) {
                Ok(memory) => {
                    shm::unlink(name.as_str())?;
                    rustix::fs::ftruncate(&memory, Self::BYTES)?;
                    return Ok(memory);
                }
                // Another region of this process's took the name.
                Err(rustix::io::Errno::EXIST) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::ErrorKind::AlreadyExists.into())
    }

    #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn descriptor(&self) -> BorrowedFd<'_> {
        self.memory.as_fd()
    }

    #[cfg(all(any(test, fuzzing), feature = "shuttle"))]
    pub(in crate::cache::cache_stores::shared) fn descriptor(&self) -> RegionDescriptor {
        Arc::clone(&self.mapping)
    }

    pub(in crate::cache::cache_stores::shared) fn span(
        self: &Arc<Self>,
        len: u64,
    ) -> Option<RegionSpan> {
        let pages = len.div_ceil(PAGE_SIZE_U64).max(1);
        let first_page = self.free().take(pages)?;
        Some(RegionSpan {
            region: Arc::clone(self),
            first_page,
            pages,
        })
    }

    /// Punches the memory first, before another span may take the pages.
    fn give_back(&self, first_page: u64, pages: u64) {
        #[cfg(all(target_os = "linux", not(all(any(test, fuzzing), feature = "shuttle"))))]
        if pages * PAGE_SIZE_U64 >= Self::PUNCHED_FROM {
            use rustix::fs::FallocateFlags;
            // Unpunched, the memory stays taken; the next span overwrites the bytes.
            let _ = rustix::fs::fallocate(
                &self.memory,
                FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
                first_page * PAGE_SIZE_U64,
                pages * PAGE_SIZE_U64,
            );
        }
        self.free().give_back(first_page, pages);
    }

    #[cfg(any(test, all(fuzzing, feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn free_pages(&self) -> u64 {
        self.free().runs.values().sum()
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache::cache_stores::shared) fn holds(&self, bytes: &[u8]) -> bool {
        let start = self.mapping.ptr.as_ptr() as usize;
        let at = bytes.as_ptr() as usize;
        start <= at && at + bytes.len() <= start + self.mapping.len
    }

    fn free(&self) -> MutexGuard<'_, FreeSpans> {
        self.free.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Lent to the owner, it must be kept until the owner says it's done with it.
pub(in crate::cache::cache_stores::shared) struct RegionSpan {
    region: Arc<ClientRegion>,
    first_page: u64,
    pages: u64,
}

impl RegionSpan {
    pub(in crate::cache::cache_stores::shared) fn name(&self) -> SpanName {
        SpanName::new(self.first_page * PAGE_SIZE_U64, self.pages * PAGE_SIZE_U64)
    }

    pub(in crate::cache::cache_stores::shared) fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        let name = self.name();
        assert!(
            at + bytes.len() <= name.len() as usize,
            "{} bytes from {at} lie within the span",
            bytes.len()
        );
        let start = self
            .region
            .mapping
            .at(name)
            .expect("a span lies within its region");
        // SAFETY: within this span, this one's alone; it's lent to the owner only once written.
        unsafe {
            start
                .add(at)
                .as_ptr()
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }
}

impl RegionSpan {
    pub(in crate::cache::cache_stores::shared) fn into_bytes(self, within: Range<usize>) -> Bytes {
        assert!(
            within.end <= self.name().len() as usize,
            "{within:?} lie within the span"
        );
        Bytes::from_owner(ServedSpan(self)).slice(within)
    }
}

impl Drop for RegionSpan {
    fn drop(&mut self) {
        self.region.give_back(self.first_page, self.pages);
    }
}

struct ServedSpan(RegionSpan);

impl AsRef<[u8]> for ServedSpan {
    fn as_ref(&self) -> &[u8] {
        let name = self.0.name();
        let start = self
            .0
            .region
            .mapping
            .at(name)
            .expect("a span lies within its region");
        // SAFETY: in the held mapping; no one writes it now, and a rogue owner only garbles it.
        unsafe { std::slice::from_raw_parts(start.as_ptr(), name.len() as usize) }
    }
}

pub(in crate::cache::cache_stores::shared) struct OwnerRegion {
    mapping: Arc<SharedMapping>,
}

impl OwnerRegion {
    #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
    const MOST_BYTES: u64 = 16 << 30;

    #[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn accept(memory: OwnedFd) -> Result<Self> {
        let bytes = u64::try_from(rustix::fs::fstat(&memory).map_err(io::Error::from)?.st_size)
            .map_err(|_| Self::refused("of a negative size"))?;
        if bytes == 0 || !bytes.is_multiple_of(PAGE_SIZE_U64) || bytes > Self::MOST_BYTES {
            return Err(Self::refused("of a size no region has"));
        }
        Self::cannot_shrink(&memory, bytes)?;
        let mapping = SharedMapping::map(memory.as_fd(), bytes as usize)?;
        Ok(Self {
            mapping: Arc::new(mapping),
        })
    }

    #[cfg(all(any(test, fuzzing), feature = "shuttle"))]
    pub(in crate::cache::cache_stores::shared) fn accept(
        mapping: RegionDescriptor,
    ) -> Result<Self> {
        Ok(Self { mapping })
    }

    #[cfg(all(target_os = "linux", not(all(any(test, fuzzing), feature = "shuttle"))))]
    fn cannot_shrink(memory: &OwnedFd, _bytes: u64) -> Result<()> {
        use rustix::fs::SealFlags;
        let seals = rustix::fs::fcntl_get_seals(memory)
            .map_err(|_| Self::refused("that takes no seals: not a memfd"))?;
        if !seals.contains(SealFlags::SHRINK) {
            return Err(Self::refused("not sealed against shrinking"));
        }
        Ok(())
    }

    /// A sized POSIX shared memory object refuses any `ftruncate`, even to its own size,
    /// which a regular file takes; to its own size it changes nothing either way.
    #[cfg(all(
        not(target_os = "linux"),
        not(all(any(test, fuzzing), feature = "shuttle"))
    ))]
    fn cannot_shrink(memory: &OwnedFd, bytes: u64) -> Result<()> {
        match rustix::fs::ftruncate(memory, bytes) {
            Err(rustix::io::Errno::INVAL) => Ok(()),
            Ok(()) => Err(Self::refused("that takes an ftruncate: it could shrink")),
            Err(error) => Err(io::Error::from(error).into()),
        }
    }

    pub(in crate::cache::cache_stores::shared) fn staged(&self, span: SpanName) -> Result<&[u8]> {
        let start = self
            .mapping
            .at(span)
            .ok_or_else(|| Self::refused("span: past its end"))?;
        // SAFETY: the borrow keeps the mapping; a client rewriting it garbles only its own read.
        Ok(unsafe { std::slice::from_raw_parts(start.as_ptr(), span.len as usize) })
    }

    /// `keeper` is held till the output and every buffer it lends drop: the span is done then.
    /// Page-aligned only: the output lends its pages to reads from disk, past the page cache.
    pub(in crate::cache::cache_stores::shared) fn output(
        self: &Arc<Self>,
        span: SpanName,
        asked: Range<u64>,
        keeper: impl Any + Send + Sync,
    ) -> Result<ReadOutput> {
        if !span.offset.is_multiple_of(PAGE_SIZE_U64) {
            return Err(Self::refused("span: off a page boundary"));
        }
        let memory = self
            .mapping
            .at(span)
            .ok_or_else(|| Self::refused("span: past its end"))?;
        let kept = Box::new((Arc::clone(self), keeper));
        // SAFETY: page-aligned, in the mapping `kept` holds, lent for this read alone (decision 8).
        unsafe { ReadOutput::over(asked, memory, span.len() as usize, kept) }
    }

    fn refused(why: &str) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a client's region {why}"),
        ))
    }
}

struct FreeSpans {
    runs: BTreeMap<u64, u64>,
}

impl FreeSpans {
    fn new(pages: u64) -> Self {
        Self {
            runs: BTreeMap::from([(0, pages)]),
        }
    }

    fn take(&mut self, pages: u64) -> Option<u64> {
        let (&first, &run) = self.runs.iter().find(|&(_, &run)| run >= pages)?;
        self.runs.remove(&first);
        if run > pages {
            self.runs.insert(first + pages, run - pages);
        }
        Some(first)
    }

    fn give_back(&mut self, mut first: u64, mut pages: u64) {
        if let Some((&before, &run)) = self.runs.range(..first).next_back() {
            debug_assert!(before + run <= first, "pages given back twice");
            if before + run == first {
                self.runs.remove(&before);
                first = before;
                pages += run;
            }
        }
        if let Some(after) = self.runs.remove(&(first + pages)) {
            pages += after;
        }
        debug_assert!(
            self.runs.range(first..first + pages).next().is_none(),
            "pages given back twice"
        );
        self.runs.insert(first, pages);
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
