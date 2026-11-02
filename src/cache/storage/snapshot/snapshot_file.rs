//! [`SnapshotFile`]: where a cache's snapshot is kept, beside its first
//! cache file; what a snapshot must match to be taken back; and the order
//! in which it is written, so that one taken back never names a chunk
//! whose bytes aren't on the drive.

use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use super::{DiskSnapshot, SnapshotReader, SnapshotWriter};
use crate::cache::storage::FileIdentity;

/// Where a cache's snapshot is kept: `<first cache file>.snapshot`, written
/// as `<first cache file>.snapshot.tmp` and renamed over it. And what a
/// snapshot must match to be taken back: the cache files' layout.
pub(in crate::cache) struct SnapshotFile {
    path: PathBuf,
    layout: CacheLayout,
    /// The cache files, opened again, flushed before a snapshot that names
    /// their chunks is written. They share the files' lock, held until
    /// these close too.
    cache_files: Vec<File>,
}

impl SnapshotFile {
    /// The snapshot of the cache whose files, the first at `first`, are
    /// `opened`, each of `chunks_per_file` chunks. `None` if a file doesn't
    /// say when it was made, as some file systems don't, or can't be opened
    /// again: such a cache starts cold at each build, as before snapshots.
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

    /// The snapshot the last cache on these files kept, if it matches
    /// them; `None` if there is none, or it doesn't. Whatever is there is
    /// removed first, and durably, before the cache writes a chunk: once it
    /// runs, the cache reuses a chunk as soon as it frees it, and a snapshot
    /// left would name bytes no longer there after a crash.
    ///
    /// # Errors
    ///
    /// If a snapshot is there and can't be removed.
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

    /// Keeps `snapshot`, whole or not at all: the cache files flushed first,
    /// so that every chunk it names is on the drive, then the snapshot
    /// written beside them and renamed into place, and the rename flushed.
    ///
    /// # Errors
    ///
    /// If a flush, the write or the rename fails: the next cache on these
    /// files starts cold.
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

    /// Writes `bytes` to a file at `path`, made or emptied, and flushes it.
    fn write_whole(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(bytes)?;
        file.sync_data()
    }

    /// Flushes the directory the snapshot is in: its removal, or its
    /// rename.
    fn sync_directory(&self) -> io::Result<()> {
        let directory = match self.path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        File::open(directory)?.sync_all()
    }

    /// The path the snapshot is kept at: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn path(&self) -> &Path {
        &self.path
    }

    /// What a snapshot must match: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn layout(&self) -> &CacheLayout {
        &self.layout
    }
}

/// What a snapshot must match to be taken back: the cache files, in order,
/// each by its identity and when it was made, so that a snapshot of a file
/// since made again, or of the files listed in another order, is never
/// taken for theirs; and the chunks each holds.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CacheLayout {
    chunks_per_file: u32,
    files: Vec<FileStamp>,
}

impl CacheLayout {
    /// The disk tier's chunks; `None` past what a tier numbers.
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
                // A nanosecond count past a second would carry, and could
                // overflow.
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

/// A cache file as a snapshot names it: its identity, and when it was made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    identity: FileIdentity,
    /// Since the epoch.
    created: Duration,
}

impl FileStamp {
    /// The stamp of the file `file` is open on; `None` if it doesn't say
    /// when it was made.
    fn of(file: &File) -> Option<Self> {
        let meta = file.metadata().ok()?;
        let created = meta.created().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(Self {
            identity: FileIdentity::new(meta.dev(), meta.ino()),
            created,
        })
    }
}
