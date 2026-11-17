//! The simulated machine's disk, in memory. A test owns one and builds each process on it, so
//! that a process built again finds its files' bytes, the lock of the process that went released,
//! and its log. Behind a `std` mutex, not shuttle's: each operation takes the disk whole, as a
//! system call does, and adds no point where shuttle may switch threads.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use super::FileIdentity;
use crate::io::error::{Error, Result};
use crate::{PAGE_SIZE, PAGE_SIZE_U64};

/// A clone is the same disk.
#[derive(Clone, Default)]
pub(crate) struct FileSystem {
    disk: Arc<Mutex<Disk>>,
}

impl FileSystem {
    pub(crate) fn open_cache_file(&self, path: &Path, direct_io: bool) -> Result<OpenFile> {
        let mut disk = self.disk();
        let inode = disk.named_or_made(path);
        Ok(disk.open(self, inode, true, direct_io))
    }

    pub(crate) fn open_existing(&self, path: &Path, writable: bool) -> io::Result<OpenFile> {
        let mut disk = self.disk();
        let inode = disk.named(path)?;
        Ok(disk.open(self, inode, writable, false))
    }

    pub(crate) fn open_to_append(&self, path: &Path) -> io::Result<OpenFile> {
        self.open_existing(path, true)
    }

    pub(crate) fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let disk = self.disk();
        let inode = disk.inode(disk.named(path)?);
        let mut bytes = vec![0; inode.size as usize];
        inode.read_at(&mut bytes, 0);
        Ok(bytes)
    }

    pub(crate) fn remove(&self, path: &Path) -> io::Result<()> {
        let mut disk = self.disk();
        let inode = disk.names.remove(path).ok_or(io::ErrorKind::NotFound)?;
        disk.unname(inode);
        Ok(())
    }

    pub(crate) fn write_durably(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut disk = self.disk();
        let inode = disk.named_or_made(path);
        let inode = disk.inode_mut(inode);
        inode.set_size(0);
        inode.write_at(bytes, 0);
        Ok(())
    }

    pub(crate) fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut disk = self.disk();
        let inode = disk.names.remove(from).ok_or(io::ErrorKind::NotFound)?;
        if let Some(replaced) = disk.names.insert(to.to_owned(), inode) {
            disk.unname(replaced);
        }
        Ok(())
    }

    /// Every name is durable once made.
    pub(crate) fn sync_directory_of(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    /// One boot for the disk's life: no machine crash comes between its writes.
    pub(crate) fn boot_id(&self) -> io::Result<String> {
        Ok("00000000-0000-0000-0000-000000000000".to_owned())
    }

    /// Another name for the file `existing` names: a lock is the file's, by any name.
    #[cfg(test)]
    pub(crate) fn link(&self, existing: &Path, link: &Path) -> io::Result<()> {
        let mut disk = self.disk();
        let inode = disk.named(existing)?;
        if disk.names.insert(link.to_owned(), inode).is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        disk.inode_mut(inode).names += 1;
        Ok(())
    }

    /// The bytes the file at `path` takes on the drive.
    #[cfg(test)]
    pub(crate) fn allocated(&self, path: &Path) -> io::Result<u64> {
        let disk = self.disk();
        Ok(disk.inode(disk.named(path)?).allocated())
    }

    fn disk(&self) -> MutexGuard<'_, Disk> {
        self.disk.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Default)]
struct Disk {
    names: BTreeMap<PathBuf, u64>,
    inodes: BTreeMap<u64, Inode>,
    inodes_made: u64,
    opens: u64,
}

impl Disk {
    /// Far past what any test writes, so it fills only for a test that keeps more free.
    const CAPACITY: u64 = 1 << 40;
    const DEVICE: u64 = 0;

    fn named(&self, path: &Path) -> io::Result<u64> {
        self.names
            .get(path)
            .copied()
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    /// An inode number is never given twice, so a file made again is another file.
    fn named_or_made(&mut self, path: &Path) -> u64 {
        if let Some(&inode) = self.names.get(path) {
            return inode;
        }
        self.inodes_made += 1;
        let inode = self.inodes_made;
        self.names.insert(path.to_owned(), inode);
        let made = Inode {
            names: 1,
            ..Inode::default()
        };
        self.inodes.insert(inode, made);
        inode
    }

    fn inode(&self, inode: u64) -> &Inode {
        &self.inodes[&inode]
    }

    fn inode_mut(&mut self, inode: u64) -> &mut Inode {
        self.inodes.get_mut(&inode).expect("a named or open inode")
    }

    fn unname(&mut self, inode: u64) {
        self.inode_mut(inode).names -= 1;
        self.forget_if_unreachable(inode);
    }

    fn forget_if_unreachable(&mut self, inode: u64) {
        let gone = self.inode(inode);
        if gone.names == 0 && gone.opens == 0 {
            self.inodes.remove(&inode);
        }
    }

    fn open(
        &mut self,
        file_system: &FileSystem,
        inode: u64,
        writable: bool,
        direct_io: bool,
    ) -> OpenFile {
        self.opens += 1;
        self.inode_mut(inode).opens += 1;
        OpenFile(Arc::new(OpenDescription {
            file_system: file_system.clone(),
            inode,
            open: self.opens,
            writable,
            direct_io,
            #[cfg(test)]
            paused_write: Mutex::default(),
        }))
    }

    fn free_bytes(&self) -> u64 {
        let allocated = self.inodes.values().map(Inode::allocated).sum::<u64>();
        Self::CAPACITY.saturating_sub(allocated)
    }
}

#[derive(Default)]
struct Inode {
    size: u64,
    /// Only the pages written: a file is sized, never allocated.
    pages: BTreeMap<u64, Box<[u8]>>,
    /// The open whose `flock` it is.
    locked_by: Option<u64>,
    opens: usize,
    names: usize,
}

impl Inode {
    /// As `pread`: short past the end; a page never written reads as zeros.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> usize {
        let len = self.size.saturating_sub(offset).min(buf.len() as u64) as usize;
        let mut done = 0;
        while done < len {
            let (page, within, n) = Self::span(offset + done as u64, len - done);
            let into = &mut buf[done..done + n];
            match self.pages.get(&page) {
                Some(bytes) => into.copy_from_slice(&bytes[within..within + n]),
                None => into.fill(0),
            }
            done += n;
        }
        len
    }

    fn write_at(&mut self, bytes: &[u8], offset: u64) {
        let mut done = 0;
        while done < bytes.len() {
            let (page, within, n) = Self::span(offset + done as u64, bytes.len() - done);
            let stored = (self.pages.entry(page)).or_insert_with(|| vec![0; PAGE_SIZE].into());
            stored[within..within + n].copy_from_slice(&bytes[done..done + n]);
            done += n;
        }
        self.size = self.size.max(offset + bytes.len() as u64);
    }

    /// Bytes cut off are gone: grown again, the file reads zeros there.
    fn set_size(&mut self, size: u64) {
        self.pages.split_off(&size.div_ceil(PAGE_SIZE_U64));
        if let Some(last) = self.pages.get_mut(&(size / PAGE_SIZE_U64)) {
            last[(size % PAGE_SIZE_U64) as usize..].fill(0);
        }
        self.size = size;
    }

    fn allocated(&self) -> u64 {
        self.pages.len() as u64 * PAGE_SIZE_U64
    }

    /// The page `at` falls in, where in it, and how many of `left` bytes fit before its end.
    fn span(at: u64, left: usize) -> (u64, usize, usize) {
        let within = (at % PAGE_SIZE_U64) as usize;
        (at / PAGE_SIZE_U64, within, left.min(PAGE_SIZE - within))
    }
}

/// A `try_clone` shares the open, and with it its lock, held till every clone drops.
pub struct OpenFile(Arc<OpenDescription>);

struct OpenDescription {
    file_system: FileSystem,
    inode: u64,
    /// Which open holds a lock: two opens of a file conflict, in one process too.
    open: u64,
    writable: bool,
    direct_io: bool,
    #[cfg(test)]
    paused_write: Mutex<Option<PausedWrite>>,
}

impl OpenFile {
    pub(crate) fn try_lock(&self) -> Result<()> {
        let mut disk = self.0.file_system.disk();
        let inode = disk.inode_mut(self.0.inode);
        match inode.locked_by {
            Some(holder) if holder != self.0.open => Err(Error::Busy),
            _ => {
                inode.locked_by = Some(self.0.open);
                Ok(())
            }
        }
    }

    pub(crate) fn size(&self) -> io::Result<u64> {
        Ok(self.0.file_system.disk().inode(self.0.inode).size)
    }

    pub(crate) fn set_size(&self, size: u64) -> io::Result<()> {
        let mut disk = self.0.file_system.disk();
        disk.inode_mut(self.0.inode).set_size(size);
        Ok(())
    }

    pub(crate) fn identity(&self) -> io::Result<FileIdentity> {
        Ok(FileIdentity::new(Disk::DEVICE, self.0.inode))
    }

    /// Every file is made at time zero: its inode, never reused, tells it apart.
    pub(crate) fn created(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    pub(crate) fn free_bytes(&self) -> io::Result<u64> {
        Ok(self.0.file_system.disk().free_bytes())
    }

    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self(Arc::clone(&self.0)))
    }

    pub(crate) fn is_writable(&self) -> io::Result<bool> {
        Ok(self.0.writable)
    }

    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.0.check_direct(offset, buf)?;
        let disk = self.0.file_system.disk();
        Ok(disk.inode(self.0.inode).read_at(buf, offset))
    }

    pub(crate) fn write_at(&self, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.0.check_writable()?;
        self.0.check_direct(offset, bytes)?;
        #[cfg(test)]
        self.0.wait_if_paused();
        let mut disk = self.0.file_system.disk();
        disk.inode_mut(self.0.inode).write_at(bytes, offset);
        Ok(bytes.len())
    }

    pub(crate) fn append(&self, bytes: &[u8]) -> io::Result<()> {
        self.0.check_writable()?;
        let mut disk = self.0.file_system.disk();
        let inode = disk.inode_mut(self.0.inode);
        let end = inode.size;
        inode.write_at(bytes, end);
        Ok(())
    }

    /// The next `write_at` waits, before it lands, until the test resumes it.
    #[cfg(test)]
    pub(crate) fn pausing_next_write(&self) -> WritePause {
        let (announce, started) = crate::io::sync::mpsc::channel();
        let (release, resume) = crate::io::sync::mpsc::channel();
        let paused = PausedWrite {
            started: announce,
            release: resume,
        };
        *self
            .0
            .paused_write
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(paused);
        WritePause { started, release }
    }
}

impl fmt::Debug for OpenFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenFile")
            .field("inode", &self.0.inode)
            .finish_non_exhaustive()
    }
}

impl OpenDescription {
    fn check_writable(&self) -> io::Result<()> {
        match self.writable {
            true => Ok(()),
            false => Err(io::Error::from_raw_os_error(libc::EBADF)),
        }
    }

    /// Linux's `O_DIRECT` refuses an I/O off the drive's sectors, of 4 KiB at most.
    fn check_direct(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let on_pages = [offset, bytes.len() as u64, bytes.as_ptr().addr() as u64]
            .iter()
            .all(|value| value.is_multiple_of(PAGE_SIZE_U64));
        match !self.direct_io || on_pages {
            true => Ok(()),
            false => Err(io::Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    /// Outside the disk's lock, which other threads take while this one waits.
    #[cfg(test)]
    fn wait_if_paused(&self) {
        let paused = self
            .paused_write
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(paused) = paused {
            paused.started.send(()).unwrap();
            paused.release.recv().unwrap();
        }
    }
}

impl Drop for OpenDescription {
    fn drop(&mut self) {
        let mut disk = self.file_system.disk();
        let inode = disk.inode_mut(self.inode);
        if inode.locked_by == Some(self.open) {
            inode.locked_by = None;
        }
        inode.opens -= 1;
        disk.forget_if_unreachable(self.inode);
    }
}

#[cfg(test)]
struct PausedWrite {
    started: crate::io::sync::mpsc::Sender<()>,
    release: crate::io::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct WritePause {
    started: crate::io::sync::mpsc::Receiver<()>,
    release: crate::io::sync::mpsc::Sender<()>,
}

#[cfg(test)]
impl WritePause {
    pub(crate) fn wait_until_started(&self) {
        self.started.recv().unwrap();
    }

    pub(crate) fn resume(self) {
        self.release.send(()).unwrap();
    }
}
