//! A cache file's first page names the build that last claimed it. A log trusts the file's chunks
//! only while the file still names the build the log descends from: a build of other files that
//! takes this one claims it before it writes a chunk (`doc/13-persistence.md`).

use std::io;

use crate::io::file_system::{FileSystem, OpenFile};
use crate::{PAGE_SIZE, PAGE_SIZE_U64};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct CacheFileOwner([u8; 16]);

/// Aligned for `O_DIRECT`.
#[repr(C, align(4096))]
struct OwnerPage([u8; PAGE_SIZE]);

impl CacheFileOwner {
    /// The bytes before a cache file's first chunk.
    pub(in crate::cache) const PAGE: u64 = PAGE_SIZE_U64;
    const MAGIC: [u8; 8] = *b"t4 owner";

    pub(in crate::cache) fn new(id: [u8; 16]) -> Self {
        Self(id)
    }

    /// One that no file names.
    pub(in crate::cache) fn fresh(file_system: &FileSystem) -> io::Result<Self> {
        file_system.unique_id().map(Self)
    }

    /// `None` for a file no build has claimed: a new one reads zeros.
    pub(in crate::cache) fn of(file: &OpenFile) -> io::Result<Option<Self>> {
        let mut page = Box::new(OwnerPage([0; PAGE_SIZE]));
        let read = file.read_at(&mut page.0, 0)?;
        let (magic, id) = (&page.0[..8], &page.0[8..24]);
        Ok((read == PAGE_SIZE && magic == Self::MAGIC).then(|| Self(id.try_into().unwrap())))
    }

    /// Before any of their chunks is written: every log that named the files no longer matches.
    pub(in crate::cache) fn claim(self, files: &[OpenFile]) -> io::Result<()> {
        let mut page = Box::new(OwnerPage([0; PAGE_SIZE]));
        page.0[..8].copy_from_slice(&Self::MAGIC);
        page.0[8..24].copy_from_slice(&self.0);
        for file in files {
            if file.write_at(&page.0, 0)? != PAGE_SIZE {
                return Err(io::ErrorKind::WriteZero.into());
            }
        }
        Ok(())
    }

    pub(in crate::cache) fn id(&self) -> &[u8; 16] {
        &self.0
    }
}
