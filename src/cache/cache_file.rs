//! [`CacheFile`]: the disk tier, its chunks in one file, read and written
//! by direct I/O through the I/O worker (`doc/03-cache.md`, decisions 2, 3
//! and 9).

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::num::NonZeroU32;
use std::ops::Range;
use std::path::Path;

use super::BLOCK_SIZE_U64;
use super::chunk_pool::disk_chunks::{DiskChunkReader, DiskChunkWriter, DiskChunks};
use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

/// The disk tier: the file's chunks, all of it from offset 0, and the I/O
/// worker that reads and writes them.
pub(super) struct CacheFile {
    io: IoWorker,
    chunks: Arc<DiskChunks>,
}

impl CacheFile {
    /// Opens the cache file at `path`, made if missing, with room for
    /// `chunk_count` chunks, and `queue_depth` I/Os in flight. Whatever it
    /// held is nothing to the cache: the index isn't kept across restarts
    /// (decision 9), so every chunk starts free. Direct I/O unless
    /// `direct_io` is off, and no `O_DSYNC`: nothing on disk has to outlast
    /// a crash.
    pub(super) fn open(
        path: &Path,
        chunk_count: NonZeroU32,
        queue_depth: NonZeroU32,
        direct_io: bool,
    ) -> Result<Self> {
        let file = Self::open_file(path, direct_io)?;
        // Room for every chunk now, so that no write finds the drive full.
        let len = u64::from(chunk_count.get()) * BLOCK_SIZE_U64;
        rustix::fs::fallocate(&file, rustix::fs::FallocateFlags::empty(), 0, len)
            .map_err(std::io::Error::from)?;
        Self::on(IoWorker::new(queue_depth, file)?, chunk_count)
    }

    /// The disk tier on the file `io` reads and writes, of `chunk_count`
    /// chunks.
    pub(super) fn on(io: IoWorker, chunk_count: NonZeroU32) -> Result<Self> {
        Ok(Self {
            io,
            chunks: Arc::new(DiskChunks::new(0, chunk_count.get())?),
        })
    }

    pub(super) fn chunks(&self) -> &Arc<DiskChunks> {
        &self.chunks
    }

    /// Writes `pages` at the start of the chunk `writer` holds.
    pub(super) async fn write(
        &self,
        writer: &DiskChunkWriter<'_>,
        pages: AlignedBuf,
    ) -> Result<()> {
        let write = PageWrite {
            buf: pages,
            offset: writer.offset(),
        };
        self.io.write(vec![write])?.await
    }

    /// Reads `pages`, offsets in the chunk `reader` holds, into a buffer of
    /// their own. The read is submitted now, so reads started together run
    /// at once, and the future gives its buffer when it lands.
    pub(super) fn read_pages(
        &self,
        reader: &DiskChunkReader<'_>,
        pages: Range<u32>,
    ) -> Result<impl Future<Output = Result<AlignedBuf>> + use<>> {
        let len = NonZeroU32::new(pages.end - pages.start)
            .ok_or(Error::InvalidArgument("a disk read takes a page or more"))?;
        let offset = reader.offset() + u64::from(pages.start);
        let read = self.io.read_at(AlignedBuf::new_zeroed(len)?, offset)?;
        Ok(async move {
            let (buf, read_len) = read.await?;
            if read_len != buf.len() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("short read of a disk chunk: {read_len} of {}", buf.len()),
                )));
            }
            Ok(buf)
        })
    }

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
