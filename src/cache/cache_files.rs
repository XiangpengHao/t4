//! [`CacheFiles`]: the disk tier, its chunks striped over its cache files,
//! one a drive, read and written by direct I/O through one I/O worker
//! (`doc/03-cache.md`, decisions 2, 3 and 9; `doc/04-multi-device.md`).

use std::fs::{File, OpenOptions, TryLockError};
use std::future::Future;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use super::BLOCK_SIZE_U64;
use super::chunk_pool::disk_chunks::{DiskChunkReader, DiskChunkWriter, DiskChunks};
use super::sharing::FileIdentity;
use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

/// A cache file the user gives: where it is, and the bytes it may take.
#[derive(Clone, Debug)]
pub(super) struct DiskFile {
    path: PathBuf,
    capacity: u64,
}

impl DiskFile {
    pub(super) fn new(path: PathBuf, capacity: u64) -> Self {
        Self { path, capacity }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

/// The disk tier: its chunks, striped over its files by number, and the
/// one I/O worker that reads and writes them all.
pub(super) struct CacheFiles {
    io: IoWorker,
    chunks: Arc<DiskChunks>,
    /// The first file's identity, which names its owner's address
    /// (`doc/11-shared-mode.md`, decision 6); `None` for simulated files.
    first_file: Option<FileIdentity>,
}

impl CacheFiles {
    /// Opens `files`, each made if missing, with `queue_depth` I/Os in
    /// flight across them. Each holds as many chunks as the smallest has
    /// room for (`doc/04-multi-device.md`, decision 3), one at least, and
    /// takes only those. Whatever a file held is nothing to the cache: the
    /// index isn't kept across restarts (decision 9), so every chunk starts
    /// free. Direct I/O unless `direct_io` is off, and no `O_DSYNC`: nothing
    /// on disk has to outlast a crash.
    pub(super) fn open(
        files: &[DiskFile],
        queue_depth: NonZeroU32,
        direct_io: bool,
    ) -> Result<Self> {
        let count = u32::try_from(files.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(Error::InvalidArgument(
                "the disk tier needs from 1 to 2^32 - 1 files",
            ))?;
        let smallest = files.iter().map(|file| file.capacity).min().unwrap_or(0);
        let chunks_per_file =
            (smallest / BLOCK_SIZE_U64).clamp(1, u64::from(u32::MAX / count)) as u32;
        let opened = files
            .iter()
            .map(|file| {
                let opened = Self::open_file(&file.path, direct_io)?;
                // Room for every chunk now, so that no write finds the drive
                // full.
                let len = u64::from(chunks_per_file) * BLOCK_SIZE_U64;
                rustix::fs::fallocate(&opened, rustix::fs::FallocateFlags::empty(), 0, len)
                    .map_err(std::io::Error::from)?;
                Ok(opened)
            })
            .collect::<Result<Vec<_>>>()?;
        let first_file = FileIdentity::of(&opened[0])?;
        let mut files = Self::on(IoWorker::new(queue_depth, opened)?, count, chunks_per_file)?;
        files.first_file = Some(first_file);
        Ok(files)
    }

    /// The disk tier on the `files` files `io` reads and writes, of
    /// `chunks_per_file` chunks each.
    pub(super) fn on(io: IoWorker, files: NonZeroU32, chunks_per_file: u32) -> Result<Self> {
        Ok(Self {
            io,
            chunks: Arc::new(DiskChunks::new(files, chunks_per_file)?),
            first_file: None,
        })
    }

    pub(super) fn chunks(&self) -> &Arc<DiskChunks> {
        &self.chunks
    }

    /// The identity of the first cache file; `None` if the files are
    /// simulated.
    pub(super) fn first_file(&self) -> Option<FileIdentity> {
        self.first_file
    }

    /// Writes `pages` at the start of the chunk `writer` holds.
    pub(super) async fn write(
        &self,
        writer: &DiskChunkWriter<'_>,
        pages: AlignedBuf,
    ) -> Result<()> {
        let write = PageWrite {
            buf: pages,
            at: writer.position(),
        };
        self.io.write(vec![write])?.await
    }

    /// Reads the first pages of the chunk `reader` holds into `into`, as
    /// many as it has room for, and gives it back once read.
    pub(super) async fn read_chunk_start(
        &self,
        reader: &DiskChunkReader<'_>,
        into: AlignedBuf,
    ) -> Result<AlignedBuf> {
        self.io.read_exact_at(into, reader.position()).await
    }

    /// Reads the pages of the chunk `reader` holds from `from`, an offset in
    /// it, into `into`, as many as it has room for. The read is submitted
    /// now, so reads started together run at once, and the future is done
    /// when it lands.
    pub(super) fn read_into(
        &self,
        reader: &DiskChunkReader<'_>,
        from: u32,
        into: AlignedBuf,
    ) -> Result<impl Future<Output = Result<()>> + use<>> {
        let at = reader.position().advanced(u64::from(from));
        let read = self.io.read_at(into, at)?;
        Ok(async move {
            let (buf, read_len) = read.await?;
            if read_len != buf.len() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("short read of a disk chunk: {read_len} of {}", buf.len()),
                )));
            }
            Ok(())
        })
    }

    /// Opens the cache file at `path` and takes it for this cache: an
    /// exclusive `flock`, held as long as the file is open, so until the
    /// I/O worker that owns it is dropped. A file another cache holds, in
    /// this process or another, by whatever path, is [`Error::Busy`]. The
    /// kernel drops the lock when its holder exits, however it exits. A
    /// `flock`, not an `fcntl` record lock: record locks don't conflict
    /// within a process, and closing any descriptor of the file drops them.
    fn open_file(path: &Path, direct_io: bool) -> Result<File> {
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
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Error(error) => Error::Io(error),
        })?;
        // macOS has no O_DIRECT: it bypasses the page cache per descriptor.
        #[cfg(target_os = "macos")]
        if direct_io {
            use std::os::fd::AsRawFd;
            // SAFETY: `file` was just opened, so its descriptor is valid.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(file)
    }
}
