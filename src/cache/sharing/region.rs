//! The region (`doc/11-shared-mode.md`, decision 5): shared memory a client
//! makes and sends its owner in the handshake, which both map. The client
//! hands it out in spans ([`RegionSpan`]): a read's output, a fetch's
//! staged bytes, each lent to the owner by naming it in a frame
//! ([`SpanName`]). The owner maps it ([`OwnerRegion`]) only if it can't
//! shrink: a client that shrank it would crash the owner with `SIGBUS`.
//!
//! - **Linux:** a memfd, sealed against shrinking and growing.
//! - **macOS:** a POSIX shared memory object, unlinked once made: XNU sizes
//!   one once, and refuses every `ftruncate` after, from any descriptor.

use std::any::Any;
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use rustix::mm::{MapFlags, ProtFlags};

use crate::PAGE_SIZE_U64;
use crate::cache::read_output::ReadOutput;
use crate::io::error::{Error, Result};

/// A span of a region as a frame names it: `len` bytes from byte `offset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SpanName {
    offset: u64,
    len: u64,
}

impl SpanName {
    pub(super) fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    pub(super) fn offset(self) -> u64 {
        self.offset
    }

    pub(super) fn len(self) -> u64 {
        self.len
    }
}

/// A shared mapping of a region, read and written by both processes:
/// unmapped once dropped.
struct SharedMapping {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is plain memory, valid until it drops; who writes
// which bytes of it when is its holders' to order, through raw pointers.
unsafe impl Send for SharedMapping {}
unsafe impl Sync for SharedMapping {}

impl SharedMapping {
    /// The first `len` bytes of `memory`, mapped shared, to read and write.
    fn map(memory: BorrowedFd<'_>, len: usize) -> io::Result<Self> {
        // SAFETY: a fresh mapping, at an address the kernel picks, aliases
        // no memory of this process's.
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

    /// The address of byte `offset` of `span`, if the mapping holds the
    /// whole span.
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
        // SAFETY: mapped by `map`, with this length, and unmapped once,
        // here; every holder of a pointer into it holds the mapping.
        unsafe {
            let _ = rustix::mm::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

/// A client's region: its shared memory, its mapping of it, and its spans
/// not taken. Spans hold it, so it lives as long as the last of them, the
/// caller's bytes of a read included.
pub(super) struct ClientRegion {
    memory: OwnedFd,
    mapping: SharedMapping,
    free: Mutex<FreeSpans>,
}

impl ClientRegion {
    /// A region's bytes: pages take memory only once touched.
    pub(super) const BYTES: u64 = 1 << 30;

    /// A span this long or longer, given back, gives its memory back too.
    #[cfg(target_os = "linux")]
    const PUNCHED_FROM: u64 = 2 << 20;

    /// How many names a POSIX shared memory object is tried under before
    /// a region is given up.
    #[cfg(not(target_os = "linux"))]
    const NAME_TRIES: u32 = 16;

    /// A region, its memory made and mapped, every page free.
    ///
    /// # Errors
    ///
    /// If the memory can't be made, sized, sealed or mapped.
    pub(super) fn make() -> io::Result<Arc<Self>> {
        let memory = Self::shared_memory()?;
        let mapping = SharedMapping::map(memory.as_fd(), Self::BYTES as usize)?;
        Ok(Arc::new(Self {
            memory,
            mapping,
            free: Mutex::new(FreeSpans::new(Self::BYTES / PAGE_SIZE_U64)),
        }))
    }

    /// A memfd of [`Self::BYTES`], sealed so that it can neither shrink nor
    /// grow, nor take a seal more.
    #[cfg(target_os = "linux")]
    fn shared_memory() -> io::Result<OwnedFd> {
        use rustix::fs::{MemfdFlags, SealFlags};
        let flags = MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING;
        let memory = rustix::fs::memfd_create("t4-region", flags)?;
        rustix::fs::ftruncate(&memory, Self::BYTES)?;
        let seals = SealFlags::SHRINK | SealFlags::GROW | SealFlags::SEAL;
        rustix::fs::fcntl_add_seals(&memory, seals)?;
        Ok(memory)
    }

    /// A POSIX shared memory object of [`Self::BYTES`], under a name of
    /// this process's, unlinked once open: no other process opens it by
    /// name. Sized once, it takes no `ftruncate` after.
    #[cfg(not(target_os = "linux"))]
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

    /// The region's memory, to send its owner.
    pub(super) fn descriptor(&self) -> BorrowedFd<'_> {
        self.memory.as_fd()
    }

    /// A span of `len` bytes, whole pages, a page at least; `None` if no
    /// run of free pages is that long.
    pub(super) fn span(self: &Arc<Self>, len: u64) -> Option<RegionSpan> {
        let pages = len.div_ceil(PAGE_SIZE_U64).max(1);
        let first_page = self.free().take(pages)?;
        Some(RegionSpan {
            region: Arc::clone(self),
            first_page,
            pages,
        })
    }

    /// Gives `pages` from `first_page` back, a span done with: its memory
    /// first, if it is long, on Linux, before another span may take them.
    fn give_back(&self, first_page: u64, pages: u64) {
        #[cfg(target_os = "linux")]
        if pages * PAGE_SIZE_U64 >= Self::PUNCHED_FROM {
            use rustix::fs::FallocateFlags;
            // A hole that can't be punched leaves the memory taken, and
            // the bytes as they are: the next span overwrites them.
            let _ = rustix::fs::fallocate(
                &self.memory,
                FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
                first_page * PAGE_SIZE_U64,
                pages * PAGE_SIZE_U64,
            );
        }
        self.free().give_back(first_page, pages);
    }

    /// How many of the region's pages are free: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn free_pages(&self) -> u64 {
        self.free().runs.values().sum()
    }

    /// Whether `bytes` lie in the region: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn holds(&self, bytes: &[u8]) -> bool {
        let start = self.mapping.ptr.as_ptr() as usize;
        let at = bytes.as_ptr() as usize;
        start <= at && at + bytes.len() <= start + self.mapping.len
    }

    fn free(&self) -> std::sync::MutexGuard<'_, FreeSpans> {
        self.free.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A span of a client's region, its pages its own until it drops, which
/// gives them back. Lent to the owner, it is kept until the owner says it
/// is done with it (decision 5).
pub(super) struct RegionSpan {
    region: Arc<ClientRegion>,
    first_page: u64,
    pages: u64,
}

impl RegionSpan {
    /// The span, as a frame names it.
    pub(super) fn name(&self) -> SpanName {
        SpanName::new(self.first_page * PAGE_SIZE_U64, self.pages * PAGE_SIZE_U64)
    }

    /// Copies `bytes` into the span from its byte `at`.
    ///
    /// # Panics
    ///
    /// If they run past the span.
    #[allow(dead_code)] // A fetch's bytes are staged in a span: the next commit.
    pub(super) fn copy_in(&mut self, at: usize, bytes: &[u8]) {
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
        // SAFETY: the bytes lie within the span, the span's alone, and the
        // client lends it to the owner only once it is written.
        unsafe {
            start
                .add(at)
                .as_ptr()
                .copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }
}

impl RegionSpan {
    /// The span's bytes `within` it, as `Bytes` that hold the span until
    /// the last of them drops: a read's, served in it.
    ///
    /// # Panics
    ///
    /// If `within` runs past the span.
    pub(super) fn into_bytes(self, within: Range<usize>) -> Bytes {
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

/// A span its owner has served a read in, done with it: its client's
/// caller's bytes.
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
        // SAFETY: the span lies within the mapping, which it holds. Its
        // owner served the read, and writes it no more; its client never
        // does: an owner that broke the protocol would garble the bytes,
        // not more (decision 8).
        unsafe { std::slice::from_raw_parts(start.as_ptr(), name.len() as usize) }
    }
}

/// A client's region, as its owner maps it: memory another process writes
/// too. The owner reads a span of it only where a frame names it, and
/// only to copy its bytes: a client that changes them meanwhile garbles
/// its own bytes, nothing of the owner's (decision 8).
pub(in crate::cache) struct OwnerRegion {
    mapping: SharedMapping,
}

impl OwnerRegion {
    /// The most bytes of a region an owner maps: a client's is 1 GiB.
    const MOST_BYTES: u64 = 16 << 30;

    /// The region of `memory`, mapped, if it can't shrink under the owner
    /// and is no larger than an owner maps.
    ///
    /// # Errors
    ///
    /// If it could shrink, is empty, not whole pages or too large, or can't
    /// be mapped.
    pub(super) fn accept(memory: OwnedFd) -> Result<Self> {
        let bytes = u64::try_from(rustix::fs::fstat(&memory).map_err(io::Error::from)?.st_size)
            .map_err(|_| Self::refused("of a negative size"))?;
        if bytes == 0 || !bytes.is_multiple_of(PAGE_SIZE_U64) || bytes > Self::MOST_BYTES {
            return Err(Self::refused("of a size no region has"));
        }
        Self::cannot_shrink(&memory, bytes)?;
        let mapping = SharedMapping::map(memory.as_fd(), bytes as usize)?;
        Ok(Self { mapping })
    }

    /// Whether `memory` is a memfd sealed against shrinking.
    #[cfg(target_os = "linux")]
    fn cannot_shrink(memory: &OwnedFd, _bytes: u64) -> Result<()> {
        use rustix::fs::SealFlags;
        let seals = rustix::fs::fcntl_get_seals(memory)
            .map_err(|_| Self::refused("that takes no seals: not a memfd"))?;
        if !seals.contains(SealFlags::SHRINK) {
            return Err(Self::refused("not sealed against shrinking"));
        }
        Ok(())
    }

    /// Whether `memory`, of `bytes`, is a POSIX shared memory object sized
    /// already: it refuses an `ftruncate`, to its own size too, which a
    /// regular file takes, and which changes nothing either way.
    #[cfg(not(target_os = "linux"))]
    fn cannot_shrink(memory: &OwnedFd, bytes: u64) -> Result<()> {
        match rustix::fs::ftruncate(memory, bytes) {
            Err(rustix::io::Errno::INVAL) => Ok(()),
            Ok(()) => Err(Self::refused("that takes an ftruncate: it could shrink")),
            Err(error) => Err(io::Error::from(error).into()),
        }
    }

    /// What `read` makes of the bytes of `span`, read where the region
    /// holds them, if the region holds the whole span. The slice lives for
    /// the call alone.
    ///
    /// # Errors
    ///
    /// If the span runs past the region.
    #[allow(dead_code)] // A fetch's bytes are staged in a span: the next commit.
    pub(super) fn with_bytes<R>(&self, span: SpanName, read: impl FnOnce(&[u8]) -> R) -> Result<R> {
        let start = self
            .mapping
            .at(span)
            .ok_or_else(|| Self::refused("span: past its end"))?;
        // SAFETY: the span lies within the mapping, which outlives the call.
        // The client may write the bytes meanwhile, breaking its own
        // protocol: they are bytes either way, and what `read` makes of them
        // is the client's own read's or block's (decision 8).
        let bytes = unsafe { std::slice::from_raw_parts(start.as_ptr(), span.len as usize) };
        Ok(read(bytes))
    }

    /// A read's output for the file's bytes `asked`, over `span` of the
    /// region, which the region's mapping and `keeper` are kept for until
    /// the output and every buffer it lends have dropped: the owner tells
    /// its client it is done with the span then.
    ///
    /// # Errors
    ///
    /// If the span runs past the region, or is too short for the output.
    pub(super) fn output(
        self: &Arc<Self>,
        span: SpanName,
        asked: Range<u64>,
        keeper: impl Any + Send + Sync,
    ) -> Result<ReadOutput> {
        let memory = self
            .mapping
            .at(span)
            .ok_or_else(|| Self::refused("span: past its end"))?;
        let kept = Box::new((Arc::clone(self), keeper));
        // SAFETY: the span lies within the mapping, page-aligned as the
        // mapping and every span are, and the keeper holds the mapping.
        // Nothing of the owner's but the output touches the span: its client
        // lent it for this read alone. The client may write it meanwhile,
        // breaking its own protocol: the owner only writes the output, so
        // that garbles the client's bytes alone (decision 8).
        unsafe { ReadOutput::over(asked, memory, span.len() as usize, kept) }
    }

    /// The error of a region refused, or a span of one, `why`.
    fn refused(why: &str) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a client's region {why}"),
        ))
    }
}

/// A region's free pages, as runs by their first page: spans are taken
/// first fit, and given back merged with the runs beside them.
struct FreeSpans {
    runs: BTreeMap<u64, u64>,
}

impl FreeSpans {
    /// A region of `pages`, all free.
    fn new(pages: u64) -> Self {
        Self {
            runs: BTreeMap::from([(0, pages)]),
        }
    }

    /// The first page of a run of `pages` taken from the first free run
    /// that long; `None` if none is.
    fn take(&mut self, pages: u64) -> Option<u64> {
        let (&first, &run) = self.runs.iter().find(|&(_, &run)| run >= pages)?;
        self.runs.remove(&first);
        if run > pages {
            self.runs.insert(first + pages, run - pages);
        }
        Some(first)
    }

    /// Gives `pages` from `first` back, merged with the free runs that end
    /// where they start, and start where they end.
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
