use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use rustix::fs::OFlags;

use super::FileIdentity;
use crate::io::error::{Error, Result};

/// Every process shares the one, so it holds nothing; built by `default`, as the simulated one.
#[derive(Clone, Debug, Default)]
pub(crate) struct FileSystem(());

impl FileSystem {
    /// Made if absent, never truncated; locked only by `OpenFile::try_lock`.
    pub(crate) fn open_cache_file(&self, path: &Path, direct_io: bool) -> Result<OpenFile> {
        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true).truncate(false);
        #[cfg(target_os = "linux")]
        if direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            open.custom_flags(libc::O_DIRECT);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if direct_io {
            return Err(Error::InvalidArgument(
                "direct_io not supported on target_os",
            ));
        }
        let file = open.open(path)?;
        // macOS has no O_DIRECT: it bypasses the page cache per descriptor.
        #[cfg(target_os = "macos")]
        if direct_io {
            use std::os::fd::AsRawFd;
            // SAFETY: `file` was just opened, so its descriptor is valid.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
                return Err(io::Error::last_os_error().into());
            }
        }
        Ok(OpenFile(file))
    }

    pub(crate) fn open_existing(&self, path: &Path, writable: bool) -> io::Result<OpenFile> {
        let file = File::options().read(true).write(writable).open(path)?;
        Ok(OpenFile(file))
    }

    /// Every write through it lands at the file's end.
    pub(crate) fn open_to_append(&self, path: &Path) -> io::Result<OpenFile> {
        let file = File::options().append(true).open(path)?;
        Ok(OpenFile(file))
    }

    pub(crate) fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    pub(crate) fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    /// Made, or truncated, and its bytes on the drive once this returns.
    pub(crate) fn write_durably(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(bytes)?;
        file.sync_data()
    }

    pub(crate) fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    /// Makes a rename or remove in `path`'s directory durable.
    pub(crate) fn sync_directory_of(&self, path: &Path) -> io::Result<()> {
        let directory = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        File::open(directory)?.sync_all()
    }

    /// The machine's boot, as a UUID's text: a log written in another may have lost what no
    /// sync made durable.
    #[cfg(target_os = "linux")]
    pub(crate) fn boot_id(&self) -> io::Result<String> {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn boot_id(&self) -> io::Result<String> {
        let mut bytes = [0u8; 64];
        let mut len = bytes.len();
        // SAFETY: the name is terminated and both output pointers describe live storage.
        if unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                bytes.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        let text = std::str::from_utf8(&bytes[..len]).map_err(|_| io::ErrorKind::InvalidData)?;
        Ok(text.trim_end_matches('\0').to_owned())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn boot_id(&self) -> io::Result<String> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[derive(Debug)]
pub struct OpenFile(File);

impl OpenFile {
    /// `flock`, not `fcntl`: record locks don't conflict within a process and drop on any close.
    /// A `try_clone` shares it: it is held until every clone closes.
    pub(crate) fn try_lock(&self) -> Result<()> {
        self.0.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Error(error) => Error::Io(error),
        })
    }

    pub(crate) fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    /// Sized, never allocated: the drive gives the file room as blocks are written.
    pub(crate) fn set_size(&self, size: u64) -> io::Result<()> {
        self.0.set_len(size)
    }

    pub(crate) fn identity(&self) -> io::Result<FileIdentity> {
        let meta = self.0.metadata()?;
        Ok(FileIdentity::new(meta.dev(), meta.ino()))
    }

    /// `None` on file systems that keep no creation time.
    pub(crate) fn created(&self) -> Option<Duration> {
        let created = self.0.metadata().ok()?.created().ok()?;
        created.duration_since(UNIX_EPOCH).ok()
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    /// On a file opened to append.
    pub(crate) fn append(&self, bytes: &[u8]) -> io::Result<()> {
        (&self.0).write_all(bytes)
    }

    /// What the file's drive has free for a writer that isn't root.
    pub(crate) fn free_bytes(&self) -> io::Result<u64> {
        let room = rustix::fs::fstatvfs(&self.0)?;
        Ok(room.f_bavail.saturating_mul(room.f_frsize))
    }

    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self(self.0.try_clone()?))
    }

    pub(crate) fn is_writable(&self) -> io::Result<bool> {
        let access = rustix::fs::fcntl_getfl(&self.0)?;
        Ok(access & OFlags::RWMODE == OFlags::RDWR)
    }
}

impl From<File> for OpenFile {
    fn from(file: File) -> Self {
        Self(file)
    }
}

impl From<OpenFile> for File {
    fn from(file: OpenFile) -> Self {
        file.0
    }
}

impl AsFd for OpenFile {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}
