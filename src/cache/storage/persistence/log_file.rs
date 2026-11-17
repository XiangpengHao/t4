use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::log::{BootId, DiskLog, LogEntry};
use super::{LogBase, LogReader, LogWriter, VersionOnDisk};
use crate::cache::storage::FileVersion;
use crate::cache::{BlockKey, ChunkId};
use crate::io::file_system::{FileIdentity, FileSystem, OpenFile};
use crate::io::sync::{Mutex, MutexGuard};

pub(in crate::cache) struct PersistenceLog {
    file_system: FileSystem,
    path: PathBuf,
    layout: CacheLayout,
    boot: BootId,
    append: Mutex<Option<LogAppend>>,
}

impl PersistenceLog {
    pub(in crate::cache) fn beside(
        file_system: &FileSystem,
        first: &Path,
        opened: &[OpenFile],
        chunks_per_file: u32,
    ) -> io::Result<Option<Self>> {
        let boot = BootId::current(file_system).ok();
        Self::beside_in_boot(file_system, first, opened, chunks_per_file, boot)
    }

    /// With no boot id, nothing could tell a log of another boot: no log, and none left behind.
    pub(super) fn beside_in_boot(
        file_system: &FileSystem,
        first: &Path,
        opened: &[OpenFile],
        chunks_per_file: u32,
        boot: Option<BootId>,
    ) -> io::Result<Option<Self>> {
        let mut path = first.as_os_str().to_owned();
        path.push(".log");
        let path = PathBuf::from(path);
        let files = opened.iter().map(FileStamp::of).collect::<Option<_>>();
        let (Some(boot), Some(files)) = (boot, files) else {
            match file_system.remove(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            return Ok(None);
        };
        Ok(Some(Self {
            file_system: file_system.clone(),
            path,
            layout: CacheLayout {
                chunks_per_file,
                files,
            },
            boot,
            append: Mutex::new(None),
        }))
    }

    /// Rebased before a chunk is written, so each record appended after applies to the base.
    pub(in crate::cache) fn take(&self) -> io::Result<Option<LogBase>> {
        let bytes = match self.file_system.read(&self.path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let base = bytes.and_then(|bytes| DiskLog::replay(&bytes, &self.layout, self.boot));
        let empty = LogBase::new(0, Vec::new());
        self.replace_base(base.as_ref().unwrap_or(&empty))?;
        Ok(base)
    }

    fn replace_base(&self, base: &LogBase) -> io::Result<()> {
        let mut temporary = self.path.as_os_str().to_owned();
        temporary.push(".tmp");
        let temporary = PathBuf::from(temporary);
        let base = DiskLog::base(base, &self.layout, self.boot);
        let written = (self.file_system)
            .write(&temporary, &base)
            .and_then(|()| self.file_system.rename(&temporary, &self.path));
        if written.is_err() {
            let _ = self.file_system.remove(&temporary);
        }
        written?;
        let file = self.file_system.open_to_append(&self.path)?;
        *self.append.lock().unwrap() = Some(LogAppend::new(file));
        Ok(())
    }

    pub(in crate::cache) fn changes(&self) -> LogChanges<'_> {
        LogChanges(self.append.lock().unwrap())
    }

    /// Appends what the last batch left pending, which a kill loses.
    pub(in crate::cache) fn finish(&self) -> io::Result<()> {
        match self.changes().0.as_mut() {
            Some(append) if !append.abandoned && !append.failed => append.write_pending(),
            _ => Ok(()),
        }
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn abandon(&self) {
        if let Some(append) = self.append.lock().unwrap().as_mut() {
            append.abandoned = true;
        }
    }

    #[cfg(any(all(test, not(feature = "shuttle")), feature = "__fuzz"))]
    pub(in crate::cache) fn inspect(&self) -> Option<LogBase> {
        DiskLog::replay(
            &self.file_system.read(&self.path).ok()?,
            &self.layout,
            self.boot,
        )
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn seed(&self, base: &LogBase) -> io::Result<()> {
        self.replace_base(base)
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
    /// Before any chunk the pending frees name is written again.
    pub(in crate::cache) fn flush(&mut self) -> io::Result<()> {
        let append = self.0.as_mut().expect("the log was rebased before writes");
        append.write_pending()
    }

    pub(in crate::cache) fn path(&mut self, path: &[u8], e_tag: &str, version: FileVersion) {
        if let Some(append) = self.0.as_mut() {
            let named = VersionOnDisk::new(path.into(), e_tag.into(), version, Vec::new());
            append.pending.push(LogEntry::Path(named));
        }
    }

    /// The chunk table handed `chunk` out free, and the index names `block` there.
    pub(in crate::cache) fn add(&mut self, block: BlockKey, chunk: ChunkId) {
        if let Some(append) = self.0.as_mut() {
            append.pending.push(LogEntry::Add {
                version: block.file_version().get(),
                index: block.index(),
                chunk: chunk.index(),
            });
        }
    }

    /// The index stopped naming `chunk`, under this lock, so each chunk is freed once.
    pub(in crate::cache) fn free(&mut self, chunk: ChunkId) {
        if let Some(append) = self.0.as_mut() {
            append.pending.push(LogEntry::Free(chunk.index()));
        }
    }
}

struct LogAppend {
    file: OpenFile,
    pending: Vec<LogEntry>,
    failed: bool,
    abandoned: bool,
}

impl LogAppend {
    fn new(file: OpenFile) -> Self {
        Self {
            file,
            pending: Vec::new(),
            failed: false,
            abandoned: false,
        }
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
        if let Err(error) = self.file.append(&DiskLog::record(&self.pending)) {
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

    pub(super) fn encode(&self, out: &mut LogWriter) {
        out.u32(self.chunks_per_file);
        out.u32(self.files.len() as u32);
        for file in &self.files {
            out.u64(file.identity.device());
            out.u64(file.identity.inode());
            out.u64(file.created.as_secs());
            out.u32(file.created.subsec_nanos());
        }
    }

    pub(super) fn decode(input: &mut LogReader<'_>) -> Option<Self> {
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
    fn of(file: &OpenFile) -> Option<Self> {
        Some(Self {
            identity: file.identity().ok()?,
            created: file.created()?,
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
        let file_system = FileSystem::default();
        file_system.write(&path, &[]).unwrap();
        let mut append = LogAppend::new(file_system.open_existing(&path, false).unwrap());
        append.pending.push(LogEntry::Free(0));
        assert!(append.write_pending().is_err());
        append.file = file_system.open_to_append(&path).unwrap();
        assert!(append.write_pending().is_err());
        assert!(file_system.read(&path).unwrap().is_empty());
    }
}
