//! [`CacheFiles`]: the disk tier, its chunks striped over its cache files,
//! one a drive, read and written by direct I/O through one I/O worker
//! (`doc/03-cache.md`, decisions 2, 3 and 9; `doc/04-multi-device.md`).

use std::fs::{File, OpenOptions, TryLockError};
use std::future::Future;
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use super::allocation::{DriveHeadroom, FileAllocation};
use super::disk_chunks::{DiskChunkReader, DiskChunkWriter, DiskChunks};
use crate::buffer::AlignedBuf;
use crate::cache::BLOCK_SIZE_U64;
use crate::cache::storage::snapshot::SnapshotFile;
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

/// A file, by whatever path it is reached: its device and inode. The first
/// cache file's names its owner's address (`doc/11-shared-mode.md`,
/// decision 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    /// The file `inode` on `device`.
    pub(in crate::cache) fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    /// The identity of the file `file` is open on.
    pub(in crate::cache) fn of(file: &File) -> std::io::Result<Self> {
        let meta = file.metadata()?;
        Ok(Self {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }

    /// The device the file is on.
    pub(in crate::cache) fn device(self) -> u64 {
        self.device
    }

    /// The file's inode on its device.
    pub(in crate::cache) fn inode(self) -> u64 {
        self.inode
    }
}

/// A cache file the user gives: where it is, and the bytes it may take.
#[derive(Clone, Debug)]
pub(in crate::cache) struct DiskFile {
    path: PathBuf,
    capacity: u64,
}

impl DiskFile {
    pub(in crate::cache) fn new(path: PathBuf, capacity: u64) -> Self {
        Self { path, capacity }
    }

    pub(in crate::cache) fn path(&self) -> &Path {
        &self.path
    }
}

/// The disk tier: its chunks, striped over its files by number, and the
/// one I/O worker that reads and writes them all.
pub(in crate::cache) struct CacheFiles {
    io: IoWorker,
    chunks: Arc<DiskChunks>,
    /// The first file's identity, which names its owner's address
    /// (`doc/11-shared-mode.md`, decision 6); `None` for simulated files.
    first_file: Option<FileIdentity>,
    /// Where the disk tier is kept across a restart; `None` for simulated
    /// files, and files that don't say when they were made.
    snapshot: Option<SnapshotFile>,
    /// The room files that grow leave their drives; `None` for files
    /// allocated whole, and simulated ones.
    headroom: Option<DriveHeadroom>,
}

impl CacheFiles {
    /// Opens `files`, each made if missing, with `queue_depth` I/Os in
    /// flight across them. Each holds as many chunks as the smallest has
    /// room for (`doc/04-multi-device.md`, decision 3), one at least, and
    /// takes only those, allocated on the drive as `allocation` says. Every
    /// chunk starts free, but those a snapshot the cache takes back names
    /// (`SnapshotFile`). Direct I/O unless `direct_io` is off, and no
    /// `O_DSYNC`: a snapshot flushes the files before it is kept, and
    /// nothing else on disk has to outlast a crash.
    pub(in crate::cache) fn open(
        files: &[DiskFile],
        queue_depth: NonZeroU32,
        direct_io: bool,
        allocation: FileAllocation,
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
                allocation.size(&opened, u64::from(chunks_per_file) * BLOCK_SIZE_U64)?;
                Ok(opened)
            })
            .collect::<Result<Vec<_>>>()?;
        let first_file = FileIdentity::of(&opened[0])?;
        let snapshot = SnapshotFile::beside(&files[0].path, &opened, chunks_per_file);
        let headroom = allocation.headroom(&opened)?;
        let mut files = Self::on(IoWorker::new(queue_depth, opened)?, count, chunks_per_file)?;
        files.first_file = Some(first_file);
        files.snapshot = snapshot;
        files.headroom = headroom;
        Ok(files)
    }

    /// The disk tier on the `files` files `io` reads and writes, of
    /// `chunks_per_file` chunks each.
    pub(in crate::cache) fn on(
        io: IoWorker,
        files: NonZeroU32,
        chunks_per_file: u32,
    ) -> Result<Self> {
        Ok(Self {
            io,
            chunks: Arc::new(DiskChunks::new(files, chunks_per_file)?),
            first_file: None,
            snapshot: None,
            headroom: None,
        })
    }

    pub(in crate::cache) fn chunks(&self) -> &Arc<DiskChunks> {
        &self.chunks
    }

    /// The identity of the first cache file; `None` if the files are
    /// simulated.
    pub(in crate::cache) fn first_file(&self) -> Option<FileIdentity> {
        self.first_file
    }

    /// Where the disk tier is kept across a restart; `None` if it isn't.
    pub(in crate::cache) fn snapshot(&self) -> Option<&SnapshotFile> {
        self.snapshot.as_ref()
    }

    /// Whether a block may be written now: always to files allocated
    /// whole; to files that grow, while every drive keeps its room.
    pub(in crate::cache) fn has_room_for_a_block(&self) -> bool {
        self.headroom
            .as_ref()
            .is_none_or(DriveHeadroom::allows_a_block)
    }

    /// Writes `pages` at the start of the chunk `writer` holds.
    pub(in crate::cache) async fn write(
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
    pub(in crate::cache) async fn read_chunk_start(
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
    pub(in crate::cache) fn read_into(
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
    /// I/O worker that owns it is dropped, and the snapshot's copy of it. A
    /// file another cache holds, in this process or another, by whatever
    /// path, is [`Error::Busy`]. The kernel drops the lock when its holder
    /// exits, however it exits. A `flock`, not an `fcntl` record lock:
    /// record locks don't conflict within a process, and closing any
    /// descriptor of the file drops them.
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
