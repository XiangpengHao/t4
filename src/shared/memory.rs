//! Memory a client shares with its server for value bytes, on Linux: one
//! memfd per connection, carved into a buffer per request, and a memfd of
//! its own for a value too large for that. The server does direct I/O
//! straight between the disk and these buffers. Elsewhere there are no
//! memfds, and values travel in the frames.

// Outside Linux a client never has shared memory, so the client's half of
// this module goes unused there.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::any::Any;
use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::PAGE_SIZE;
use crate::buffer::AlignedBuf;
use crate::shared::wire::WireBuffer;

/// A memfd mapped read-write into this process.
pub(super) struct SharedMapping {
    fd: OwnedFd,
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is plain memory, valid until the mapping drops, and
// this type only reaches it through raw pointers: any thread may hold it.
unsafe impl Send for SharedMapping {}
unsafe impl Sync for SharedMapping {}

impl SharedMapping {
    /// A new memfd of `len` bytes, whole pages, sealed at that size.
    #[cfg(target_os = "linux")]
    pub(super) fn create(len: NonZeroU32) -> io::Result<Self> {
        use rustix::fs::{MemfdFlags, SealFlags};
        let fd =
            rustix::fs::memfd_create("t4-values", MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)?;
        rustix::fs::ftruncate(&fd, u64::from(len.get()))?;
        rustix::fs::fcntl_add_seals(&fd, SealFlags::SHRINK | SealFlags::GROW | SealFlags::SEAL)?;
        Self::map(fd)
    }

    /// Maps a memfd a client sent. It must be sealed against shrinking: a
    /// client that shrank it would crash the server with `SIGBUS` on its
    /// next access.
    #[cfg(target_os = "linux")]
    pub(super) fn open(fd: OwnedFd) -> io::Result<Self> {
        use rustix::fs::SealFlags;
        if !rustix::fs::fcntl_get_seals(&fd)?.contains(SealFlags::SHRINK) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared memory is not sealed against shrinking",
            ));
        }
        Self::map(fd)
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn open(_fd: OwnedFd) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "shared memory for values needs Linux",
        ))
    }

    #[cfg(target_os = "linux")]
    fn map(fd: OwnedFd) -> io::Result<Self> {
        let len = rustix::fs::fstat(&fd)?.st_size;
        let len = usize::try_from(len)
            .ok()
            .filter(|len| *len > 0 && len.is_multiple_of(PAGE_SIZE))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shared memory is not whole pages",
                )
            })?;
        // SAFETY: a new mapping of a file this process holds, at an address
        // the kernel picks: it aliases nothing.
        let ptr = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                len,
                rustix::mm::ProtFlags::READ | rustix::mm::ProtFlags::WRITE,
                rustix::mm::MapFlags::SHARED,
                &fd,
                0,
            )?
        };
        let ptr = NonNull::new(ptr.cast()).expect("a mapping is never at address 0");
        Ok(Self { fd, ptr, len })
    }

    pub(super) fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// The `len` bytes at `offset`, for direct I/O: a buffer that keeps the
    /// mapping alive until it drops, and with it any I/O still using it.
    pub(super) fn lend(self: &Arc<Self>, offset: u64, len: NonZeroU32) -> io::Result<AlignedBuf> {
        let offset = self.checked_offset(offset, len.get() as usize)?;
        if !offset.is_multiple_of(PAGE_SIZE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared buffer is not page-aligned",
            ));
        }
        // SAFETY: the bytes lie within the mapping, which the guard keeps
        // mapped, and start on a page boundary: the mapping does, and so does
        // `offset`. Lent buffers are only read and written through raw
        // pointers, by direct I/O and `AlignedBuf::zero_from`.
        Ok(unsafe {
            AlignedBuf::lent(
                self.ptr.add(offset),
                len,
                Arc::clone(self) as Arc<dyn Any + Send + Sync>,
            )
        })
    }

    /// Copies `bytes` in at `offset`.
    fn write_at(&self, offset: usize, bytes: &[u8]) -> io::Result<()> {
        let offset = self.checked_offset(offset as u64, bytes.len())?;
        // SAFETY: the bytes lie within the mapping (`checked_offset`), and
        // a raw copy forms no reference to memory the server shares.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.ptr.as_ptr().add(offset),
                bytes.len(),
            );
        }
        Ok(())
    }

    /// Copies the `len` bytes at `offset` out.
    fn read_at(&self, offset: usize, len: usize) -> io::Result<Vec<u8>> {
        let offset = self.checked_offset(offset as u64, len)?;
        let mut bytes = Vec::with_capacity(len);
        // SAFETY: the bytes lie within the mapping (`checked_offset`), and
        // `bytes` has room for `len` of them, all written before `set_len`.
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr().add(offset), bytes.as_mut_ptr(), len);
            bytes.set_len(len);
        }
        Ok(bytes)
    }

    /// `offset`, if `len` bytes from it lie within the mapping.
    fn checked_offset(&self, offset: u64, len: usize) -> io::Result<usize> {
        usize::try_from(offset)
            .ok()
            .filter(|offset| offset.checked_add(len).is_some_and(|end| end <= self.len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer runs past its shared memory",
                )
            })
    }
}

impl Drop for SharedMapping {
    fn drop(&mut self) {
        // SAFETY: the mapping is this struct's, and nothing uses it any more:
        // a lent buffer holds an `Arc` to it.
        let _ = unsafe { rustix::mm::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// One connection's shared memory, on the client: a region carved into a
/// buffer per request, sent to the server in the handshake.
pub(super) struct ValueMemory {
    region: SharedMapping,
    free: Mutex<FreePages>,
}

impl ValueMemory {
    /// Memory only takes room once touched, so a large region costs little
    /// until values fill it.
    const REGION_LEN: u32 = 64 << 20;
    /// The largest buffer taken from the region. A larger one gets a memfd
    /// of its own, so one huge value doesn't hold most of the region.
    const MAX_IN_REGION: u32 = Self::REGION_LEN / 4;

    /// Shared memory for a new connection; `None` where there is none and
    /// values travel in the frames (macOS).
    pub(super) fn for_connection() -> io::Result<Option<Arc<Self>>> {
        #[cfg(target_os = "linux")]
        {
            let len = NonZeroU32::new(Self::REGION_LEN).expect("the region is not empty");
            Ok(Some(Arc::new(Self {
                region: SharedMapping::create(len)?,
                free: Mutex::new(FreePages::new(u64::from(len.get()))),
            })))
        }
        #[cfg(not(target_os = "linux"))]
        Ok(None)
    }

    pub(super) fn region_fd(&self) -> BorrowedFd<'_> {
        self.region.fd()
    }

    /// A buffer of `capacity` bytes, whole pages: from the region if it has
    /// room, else a memfd of its own.
    pub(super) fn allocate(self: &Arc<Self>, capacity: NonZeroU32) -> io::Result<ValueBuffer> {
        let in_region = (capacity.get() <= Self::MAX_IN_REGION)
            .then(|| self.lock_free().take(u64::from(capacity.get())))
            .flatten();
        let place = match in_region {
            Some(offset) => BufferPlace::Region {
                memory: Arc::clone(self),
                offset,
            },
            None => BufferPlace::Own(Self::own_memfd(capacity)?),
        };
        Ok(ValueBuffer { place, capacity })
    }

    #[cfg(target_os = "linux")]
    fn own_memfd(capacity: NonZeroU32) -> io::Result<SharedMapping> {
        SharedMapping::create(capacity)
    }

    #[cfg(not(target_os = "linux"))]
    fn own_memfd(_capacity: NonZeroU32) -> io::Result<SharedMapping> {
        unreachable!("no shared memory outside Linux: `for_connection` returns none")
    }

    fn lock_free(&self) -> MutexGuard<'_, FreePages> {
        // Each change to the free runs completes before the lock drops.
        self.free.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One request's value bytes, in memory the server shares. Region pages go
/// back when the buffer drops, which is only once the server has replied:
/// the pending call holds the buffer until then, even if its caller left.
pub(super) struct ValueBuffer {
    place: BufferPlace,
    capacity: NonZeroU32,
}

enum BufferPlace {
    Region {
        memory: Arc<ValueMemory>,
        offset: u64,
    },
    Own(SharedMapping),
}

impl ValueBuffer {
    /// How a request names this buffer to the server.
    pub(super) fn to_wire(&self) -> io::Result<WireBuffer> {
        let capacity = self.capacity.get();
        Ok(match &self.place {
            BufferPlace::Region { offset, .. } => WireBuffer::Region {
                offset: *offset,
                capacity,
            },
            BufferPlace::Own(mapping) => WireBuffer::Own {
                memfd: mapping.fd().try_clone_to_owned()?,
                capacity,
            },
        })
    }

    /// Copies a value in, at the buffer's start.
    pub(super) fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > self.capacity.get() as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "value larger than its buffer",
            ));
        }
        let (mapping, offset) = self.mapping();
        mapping.write_at(offset, bytes)
    }

    /// Copies out the `len` bytes the server put at `start`.
    pub(super) fn read(&self, start: u32, len: u32) -> io::Result<Vec<u8>> {
        let (start, len) = (start as usize, len as usize);
        if start
            .checked_add(len)
            .is_none_or(|end| end > self.capacity.get() as usize)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reply runs past its buffer",
            ));
        }
        let (mapping, offset) = self.mapping();
        mapping.read_at(offset + start, len)
    }

    fn mapping(&self) -> (&SharedMapping, usize) {
        match &self.place {
            // A region offset is below the region's `u32` length.
            BufferPlace::Region { memory, offset } => (&memory.region, *offset as usize),
            BufferPlace::Own(mapping) => (mapping, 0),
        }
    }
}

impl Drop for ValueBuffer {
    fn drop(&mut self) {
        if let BufferPlace::Region { memory, offset } = &self.place {
            memory
                .lock_free()
                .give_back(*offset, u64::from(self.capacity.get()));
        }
    }
}

/// A region's free bytes, as runs by offset. Taking finds the first run
/// long enough; giving back merges a run with its neighbours.
#[derive(Debug, PartialEq, Eq)]
struct FreePages {
    runs: BTreeMap<u64, u64>,
}

impl FreePages {
    fn new(len: u64) -> Self {
        Self {
            runs: BTreeMap::from([(0, len)]),
        }
    }

    fn take(&mut self, len: u64) -> Option<u64> {
        let (&offset, &run) = self.runs.iter().find(|(_, run)| **run >= len)?;
        self.runs.remove(&offset);
        if run > len {
            self.runs.insert(offset + len, run - len);
        }
        Some(offset)
    }

    fn give_back(&mut self, mut offset: u64, mut len: u64) {
        if let Some(next) = self.runs.remove(&(offset + len)) {
            len += next;
        }
        if let Some((&before, &before_len)) = self.runs.range(..offset).next_back()
            && before + before_len == offset
        {
            self.runs.remove(&before);
            offset = before;
            len += before_len;
        }
        self.runs.insert(offset, len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_pages_take_first_fit_and_merge_back() {
        let page = PAGE_SIZE as u64;
        let mut free = FreePages::new(8 * page);
        let a = free.take(2 * page).unwrap();
        let b = free.take(3 * page).unwrap();
        let c = free.take(3 * page).unwrap();
        assert_eq!((a, b, c), (0, 2 * page, 5 * page));
        assert_eq!(free.take(page), None);

        free.give_back(a, 2 * page);
        free.give_back(c, 3 * page);
        // Two runs, neither long enough for four pages.
        assert_eq!(free.take(4 * page), None);
        free.give_back(b, 3 * page);
        assert_eq!(free, FreePages::new(8 * page));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn buffers_round_trip_through_a_second_mapping() {
        let memory = ValueMemory::for_connection().unwrap().unwrap();
        let mut buffer = memory
            .allocate(NonZeroU32::new(2 * PAGE_SIZE as u32).unwrap())
            .unwrap();
        buffer.write(b"hello, server").unwrap();

        // The server's view: the region the handshake carries, mapped again.
        let WireBuffer::Region { offset, .. } = buffer.to_wire().unwrap() else {
            panic!("a small buffer comes from the region");
        };
        let server = Arc::new(
            SharedMapping::open(memory.region_fd().try_clone_to_owned().unwrap()).unwrap(),
        );
        let mut lent = server
            .lend(offset, NonZeroU32::new(PAGE_SIZE as u32).unwrap())
            .unwrap();
        lent.zero_from(5);
        assert_eq!(buffer.read(0, 13).unwrap(), b"hello\0\0\0\0\0\0\0\0");
        assert!(buffer.read(PAGE_SIZE as u32, 2 * PAGE_SIZE as u32).is_err());
        assert!(
            server
                .lend(offset + 1, NonZeroU32::new(PAGE_SIZE as u32).unwrap())
                .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn large_buffers_get_a_memfd_of_their_own() {
        let memory = ValueMemory::for_connection().unwrap().unwrap();
        let large = NonZeroU32::new(ValueMemory::MAX_IN_REGION + PAGE_SIZE as u32).unwrap();
        let mut buffer = memory.allocate(large).unwrap();
        buffer.write(&[7; 100]).unwrap();
        let WireBuffer::Own { memfd, capacity } = buffer.to_wire().unwrap() else {
            panic!("a large buffer gets a memfd of its own");
        };
        assert_eq!(capacity, large.get());
        let server = Arc::new(SharedMapping::open(memfd).unwrap());
        assert!(server.lend(0, large).is_ok());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unsealed_memfds_are_refused() {
        let fd = rustix::fs::memfd_create("unsealed", rustix::fs::MemfdFlags::CLOEXEC).unwrap();
        rustix::fs::ftruncate(&fd, PAGE_SIZE as u64).unwrap();
        assert!(SharedMapping::open(fd).is_err());
    }
}
