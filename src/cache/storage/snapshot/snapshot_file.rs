use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use super::{DiskSnapshot, SnapshotReader, SnapshotWriter};
use crate::cache::storage::FileIdentity;

pub(in crate::cache) struct SnapshotFile {
    path: PathBuf,
    layout: CacheLayout,
    /// Clones share the files' `flock`: it is held until these close too.
    cache_files: Vec<File>,
}

impl SnapshotFile {
    /// `None` on file systems with no creation time: such a cache starts cold.
    pub(in crate::cache) fn beside(
        first: &Path,
        opened: &[File],
        chunks_per_file: u32,
    ) -> Option<Self> {
        let files = opened.iter().map(FileStamp::of).collect::<Option<_>>()?;
        let cache_files = opened
            .iter()
            .map(|file| file.try_clone().ok())
            .collect::<Option<_>>()?;
        let mut path = first.as_os_str().to_owned();
        path.push(".snapshot");
        Some(Self {
            path: path.into(),
            layout: CacheLayout {
                chunks_per_file,
                files,
            },
            cache_files,
        })
    }

    /// Removed durably first: once chunks are reused, a snapshot left would name stale bytes.
    pub(in crate::cache) fn take(&self) -> io::Result<Option<DiskSnapshot>> {
        let read = std::fs::read(&self.path);
        if let Err(error) = &read
            && error.kind() == io::ErrorKind::NotFound
        {
            return Ok(None);
        }
        std::fs::remove_file(&self.path)?;
        self.sync_directory()?;
        Ok(read
            .ok()
            .and_then(|bytes| DiskSnapshot::decode(&bytes, &self.layout)))
    }

    /// Flushes the cache files first, so every chunk the snapshot names is on the drive.
    pub(in crate::cache) fn keep(&self, snapshot: &DiskSnapshot) -> io::Result<()> {
        for file in &self.cache_files {
            file.sync_data()?;
        }
        let mut temporary = self.path.as_os_str().to_owned();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        let written = Self::write_whole(&temporary, &snapshot.encode(&self.layout))
            .and_then(|()| std::fs::rename(&temporary, &self.path));
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        written?;
        self.sync_directory()
    }

    fn write_whole(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(bytes)?;
        file.sync_data()
    }

    fn sync_directory(&self) -> io::Result<()> {
        let directory = match self.path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        File::open(directory)?.sync_all()
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn layout(&self) -> &CacheLayout {
        &self.layout
    }
}

/// Creation times too: a file made again may reuse the inode.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CacheLayout {
    chunks_per_file: u32,
    files: Vec<FileStamp>,
}

impl CacheLayout {
    pub(super) fn chunk_count(&self) -> Option<u32> {
        u32::try_from(self.files.len())
            .ok()?
            .checked_mul(self.chunks_per_file)
    }

    pub(super) fn encode(&self, out: &mut SnapshotWriter) {
        out.u32(self.chunks_per_file);
        out.u32(self.files.len() as u32);
        for file in &self.files {
            out.u64(file.identity.device());
            out.u64(file.identity.inode());
            out.u64(file.created.as_secs());
            out.u32(file.created.subsec_nanos());
        }
    }

    pub(super) fn decode(input: &mut SnapshotReader<'_>) -> Option<Self> {
        let chunks_per_file = input.u32()?;
        let files = (0..input.u32()?)
            .map(|_| {
                let identity = FileIdentity::new(input.u64()?, input.u64()?);
                let (seconds, nanos) = (input.u64()?, input.u32()?);
                // A nanosecond count past a second would carry, and could overflow.
                let created = (nanos < 1_000_000_000).then(|| Duration::new(seconds, nanos))?;
                Some(FileStamp { identity, created })
            })
            .collect::<Option<_>>()?;
        Some(Self {
            chunks_per_file,
            files,
        })
    }
}

#[cfg(feature = "__fuzz")]
impl CacheLayout {
    pub(super) fn fuzzed(chunks_per_file: u32) -> Self {
        let file = FileStamp {
            identity: FileIdentity::new(0, 0),
            created: Duration::ZERO,
        };
        Self {
            chunks_per_file,
            files: vec![file],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    identity: FileIdentity,
    created: Duration,
}

impl FileStamp {
    fn of(file: &File) -> Option<Self> {
        let meta = file.metadata().ok()?;
        let created = meta.created().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(Self {
            identity: FileIdentity::new(meta.dev(), meta.ino()),
            created,
        })
    }
}
