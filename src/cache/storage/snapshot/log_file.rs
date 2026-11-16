use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use super::log::{BootId, BootIdReader, DiskLog, LogEntry};
use super::{DiskSnapshot, SnapshotReader, SnapshotWriter, VersionOnDisk};
use crate::cache::storage::FileIdentity;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::{Mutex, MutexGuard};

pub(in crate::cache) struct PersistenceLog {
    path: PathBuf,
    layout: CacheLayout,
    boot: BootId,
    cache_files: Vec<File>,
    append: Mutex<Option<LogAppend>>,
}

impl PersistenceLog {
    pub(in crate::cache) fn beside(
        first: &Path,
        opened: &[File],
        chunks_per_file: u32,
        boot_id_reader: BootIdReader,
    ) -> io::Result<Option<Self>> {
        let mut path = first.as_os_str().to_owned();
        path.push(".log");
        let path = PathBuf::from(path);
        let boot = boot_id_reader.read().ok();
        let files = opened.iter().map(FileStamp::of).collect::<Option<_>>();
        let (Some(boot), Some(files)) = (boot, files) else {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            Self::sync_parent(&path)?;
            return Ok(None);
        };
        let cache_files = opened
            .iter()
            .map(File::try_clone)
            .collect::<io::Result<_>>()?;
        Ok(Some(Self {
            path,
            layout: CacheLayout {
                chunks_per_file,
                files,
            },
            cache_files,
            boot,
            append: Mutex::new(None),
        }))
    }

    /// The old clean mark must be durably gone before a chunk can be overwritten.
    pub(in crate::cache) fn take(&self) -> io::Result<Option<DiskSnapshot>> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let snapshot = bytes.and_then(|bytes| DiskLog::replay(&bytes, &self.layout, self.boot));
        let empty = DiskSnapshot::new(0, Vec::new());
        self.replace_base(snapshot.as_ref().unwrap_or(&empty))?;
        Ok(snapshot)
    }

    fn replace_base(&self, snapshot: &DiskSnapshot) -> io::Result<()> {
        let mut temporary = self.path.as_os_str().to_owned();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        let written = Self::write_whole(
            &temporary,
            &DiskLog::base(snapshot, &self.layout, self.boot),
        )
        .and_then(|()| std::fs::rename(&temporary, &self.path));
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        written?;
        self.sync_directory()?;
        let file = File::options().append(true).open(&self.path)?;
        *self.append.lock().unwrap() = Some(LogAppend::new(
            file,
            snapshot,
            self.layout.chunk_count().unwrap(),
        ));
        Ok(())
    }

    pub(in crate::cache) fn changes(&self) -> LogChanges<'_> {
        LogChanges(self.append.lock().unwrap())
    }

    pub(in crate::cache) fn finish(&self, next_id: u64) -> io::Result<()> {
        let mut changes = self.changes();
        let Some(append) = changes.0.as_mut() else {
            return Ok(());
        };
        if append.abandoned || append.failed {
            return Ok(());
        }
        for file in &self.cache_files {
            file.sync_data()?;
        }
        append.pending.push(LogEntry::Clean(next_id));
        append.write_pending()?;
        append.file.sync_data()
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn abandon(&self) {
        if let Some(append) = self.append.lock().unwrap().as_mut() {
            append.abandoned = true;
        }
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn inspect(&self) -> Option<DiskSnapshot> {
        DiskLog::replay(&std::fs::read(&self.path).ok()?, &self.layout, self.boot)
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn seed(&self, snapshot: &DiskSnapshot) -> io::Result<()> {
        self.replace_base(snapshot)?;
        self.finish(snapshot.next_id())
    }

    fn write_whole(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = File::create(path)?;
        file.write_all(bytes)?;
        file.sync_data()
    }

    fn sync_directory(&self) -> io::Result<()> {
        Self::sync_parent(&self.path)
    }

    fn sync_parent(path: &Path) -> io::Result<()> {
        let directory = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
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

pub(in crate::cache) struct LogChanges<'a>(MutexGuard<'a, Option<LogAppend>>);

impl LogChanges<'_> {
    pub(in crate::cache) fn prepare(&mut self, versions: Vec<VersionOnDisk>) -> io::Result<()> {
        let append = self.0.as_mut().expect("the log was rebased before writes");
        for version in versions {
            if append.versions.insert(version.version().id().get()) {
                append.pending.push(LogEntry::Path(version));
            }
        }
        append.write_pending()
    }

    pub(in crate::cache) fn add(&mut self, block: BlockKey, chunk: ChunkId) {
        let append = self.0.as_mut().unwrap();
        let held = &mut append.chunks[chunk.index() as usize];
        assert!(held.is_none(), "a log add finds its chunk free");
        assert!(append.versions.contains(&block.file_version().get()));
        *held = Some(block);
        append.pending.push(LogEntry::Add {
            version: block.file_version().get(),
            index: block.index(),
            chunk: chunk.index(),
        });
    }

    pub(in crate::cache) fn free(&mut self, chunk: ChunkId) {
        if let Some(append) = self.0.as_mut()
            && append.chunks[chunk.index() as usize].take().is_some()
        {
            append.pending.push(LogEntry::Free(chunk.index()));
        }
    }
}

struct LogAppend {
    file: File,
    pending: Vec<LogEntry>,
    versions: HashSet<u64>,
    chunks: Vec<Option<BlockKey>>,
    failed: bool,
    abandoned: bool,
}

impl LogAppend {
    fn new(file: File, snapshot: &DiskSnapshot, chunks: u32) -> Self {
        let mut append = Self {
            file,
            pending: Vec::new(),
            versions: HashSet::new(),
            chunks: vec![None; chunks as usize],
            failed: false,
            abandoned: false,
        };
        for version in snapshot.versions() {
            append.versions.insert(version.version().id().get());
            for &(index, chunk) in version.blocks() {
                append.chunks[chunk.index() as usize] =
                    Some(BlockKey::new(version.version().id(), index));
            }
        }
        append
    }

    fn write_pending(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "the persistence log failed; disk writes are closed",
            ));
        }
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.file.write_all(&DiskLog::record(&self.pending)) {
            self.failed = true;
            return Err(error);
        }
        self.pending.clear();
        Ok(())
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

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;

    #[test]
    fn a_failed_append_never_appends_past_the_doubtful_record() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log");
        File::create(&path).unwrap();
        let mut append = LogAppend::new(
            File::open(&path).unwrap(),
            &DiskSnapshot::new(0, Vec::new()),
            1,
        );
        append.pending.push(LogEntry::Clean(0));
        assert!(append.write_pending().is_err());
        append.file = File::options().append(true).open(&path).unwrap();
        assert!(append.write_pending().is_err());
        assert!(std::fs::read(path).unwrap().is_empty());
    }
}
