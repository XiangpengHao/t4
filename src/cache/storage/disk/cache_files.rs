use std::future::Future;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use super::CacheFileOwner;
use super::disk_chunks::{DiskChunkReader, DiskChunks};
use super::headroom::DriveHeadroom;
use crate::cache::BLOCK_SIZE_U64;
use crate::cache::storage::persistence::{LogBase, PersistenceLog};
use crate::io::aligned_buf::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::file_system::{FileIdentity, FileSystem, OpenFile};
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

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
    log: Option<PersistenceLog>,
    /// The log's blocks, till the cache takes them.
    kept: Option<LogBase>,
    headroom: Option<DriveHeadroom>,
}

impl CacheFiles {
    /// No `O_DSYNC`: a kill loses no write already done, and a log of another boot is never
    /// taken back (`doc/13-persistence.md`). Files whose log it doesn't take back it claims, so
    /// that no other log can name their chunks once it writes them.
    pub(in crate::cache) fn open(
        file_system: &FileSystem,
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
        let len = CacheFileOwner::PAGE + u64::from(chunks_per_file) * BLOCK_SIZE_U64;
        let opened = files
            .iter()
            .map(|file| Self::open_file(file_system, &file.path, len, direct_io))
            .collect::<Result<Vec<_>>>()?;
        let first = &files[0].path;
        let log = PersistenceLog::beside(file_system, first, &opened, chunks_per_file)?;
        let kept = log.as_ref().and_then(PersistenceLog::replay);
        let log = match kept {
            Some(_) => log,
            None => {
                CacheFileOwner::fresh(file_system)?.claim(&opened)?;
                PersistenceLog::beside(file_system, first, &opened, chunks_per_file)?
            }
        };
        if let Some(log) = &log {
            log.rebase(kept.as_ref())?;
        }
        let first_file = opened[0].identity()?;
        let headroom = DriveHeadroom::of(&opened)?;
        let mut files = Self::on(IoWorker::new(queue_depth, opened)?, count, chunks_per_file)?;
        files.first_file = Some(first_file);
        files.log = log;
        files.kept = kept;
        files.headroom = Some(headroom);
        Ok(files)
    }

    /// The blocks the log kept, which the cache takes back once.
    pub(in crate::cache) fn take_kept(&mut self) -> Option<LogBase> {
        self.kept.take()
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn keeping_free(mut self, keep_free: u64) -> Self {
        if let Some(headroom) = &mut self.headroom {
            headroom.set_keep_free(keep_free);
        }
        self
    }

    /// On a simulated disk of its own, with no log or headroom.
    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn in_memory(
        queue_depth: NonZeroU32,
        files: NonZeroU32,
        chunks_per_file: u32,
    ) -> Result<Self> {
        let file_system = FileSystem::default();
        let opened = (0..files.get())
            .map(|file| file_system.open_cache_file(Path::new(&file.to_string()), false))
            .collect::<Result<Vec<_>>>()?;
        Self::on(IoWorker::new(queue_depth, opened)?, files, chunks_per_file)
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
            log: None,
            kept: None,
            headroom: None,
        })
    }

    pub(in crate::cache) fn chunks(&self) -> &Arc<DiskChunks> {
        &self.chunks
    }

    pub(in crate::cache) fn first_file(&self) -> Option<FileIdentity> {
        self.first_file
    }

    pub(in crate::cache) fn log(&self) -> Option<&PersistenceLog> {
        self.log.as_ref()
    }

    pub(in crate::cache) fn takes_a_block(&self) -> bool {
        !self.chunks.is_closed()
            && self
                .headroom
                .as_ref()
                .is_none_or(DriveHeadroom::allows_a_block)
    }

    pub(in crate::cache) async fn write_batch(&self, writes: Vec<PageWrite>) -> Result<()> {
        self.io.write(writes)?.await
    }

    pub(in crate::cache) async fn read_chunk_start(
        &self,
        reader: &DiskChunkReader<'_>,
        into: AlignedBuf,
    ) -> Result<AlignedBuf> {
        self.io.read_exact_at(into, reader.position()).await
    }

    /// The bytes `chunk` holds, whether or not the index names it.
    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) async fn read_whole_chunk(
        &self,
        chunk: crate::cache::ChunkId,
    ) -> Result<AlignedBuf> {
        let block = NonZeroU32::new(BLOCK_SIZE_U64 as u32).expect("a block holds bytes");
        let into = AlignedBuf::new_zeroed(block)?;
        self.io
            .read_exact_at(into, self.chunks.position(chunk))
            .await
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

    /// Locked, so that whoever holds the lock owns the files.
    fn open_file(
        file_system: &FileSystem,
        path: &Path,
        size: u64,
        direct_io: bool,
    ) -> Result<OpenFile> {
        let file = file_system.open_cache_file(path, direct_io)?;
        file.try_lock()?;
        if file.size()? < size {
            file.set_size(size)?;
        }
        Ok(file)
    }
}
