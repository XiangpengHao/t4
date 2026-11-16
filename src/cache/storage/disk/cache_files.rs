use std::fs::{File, OpenOptions, TryLockError};
use std::future::Future;
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use super::allocation::{DriveHeadroom, FileAllocation};
use super::disk_chunks::{DiskChunkReader, DiskChunkWriter, DiskChunks};
use crate::cache::BLOCK_SIZE_U64;
use crate::cache::storage::snapshot::SnapshotFile;
use crate::io::aligned_buf::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    pub(in crate::cache) fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    pub(in crate::cache) fn of(file: &File) -> std::io::Result<Self> {
        let meta = file.metadata()?;
        Ok(Self {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }

    pub(in crate::cache) fn device(self) -> u64 {
        self.device
    }

    pub(in crate::cache) fn inode(self) -> u64 {
        self.inode
    }
}

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

pub(in crate::cache) struct CacheFiles {
    io: IoWorker,
    chunks: Arc<DiskChunks>,
    /// Names its owner's address (`doc/11-shared-mode.md`, decision 6).
    first_file: Option<FileIdentity>,
    snapshot: Option<SnapshotFile>,
    headroom: Option<DriveHeadroom>,
}

impl CacheFiles {
    /// No `O_DSYNC`: a snapshot flushes the files first, and nothing else must survive a crash.
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

    pub(in crate::cache) fn first_file(&self) -> Option<FileIdentity> {
        self.first_file
    }

    pub(in crate::cache) fn snapshot(&self) -> Option<&SnapshotFile> {
        self.snapshot.as_ref()
    }

    pub(in crate::cache) fn takes_a_block(&self) -> bool {
        !self.chunks.is_closed()
            && self
                .headroom
                .as_ref()
                .is_none_or(DriveHeadroom::allows_a_block)
    }

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

    pub(in crate::cache) async fn read_chunk_start(
        &self,
        reader: &DiskChunkReader<'_>,
        into: AlignedBuf,
    ) -> Result<AlignedBuf> {
        self.io.read_exact_at(into, reader.position()).await
    }

    /// Submits now, not when polled, so reads started together run at once.
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

    /// `flock`, not `fcntl`: record locks don't conflict within a process and drop on any close.
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
