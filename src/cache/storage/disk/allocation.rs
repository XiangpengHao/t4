//! How the cache files take room on their drives
//! (`doc/10-spiral-integration.md`, decision 9): all of it as the cache is
//! built, or as blocks are written, each write leaving its drive some room.

use std::fs::File;
use std::io;

use crate::cache::BLOCK_SIZE_U64;

/// How the cache files take room on their drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) enum FileAllocation {
    /// All of it as the cache is built: no write finds its drive full. A
    /// server's, whose drives are the cache's.
    Whole,
    /// As blocks are written: each file sized, not allocated, so that it
    /// takes the room of the chunks written to it; and a block is cached on
    /// disk only while every drive keeps `keep_free` bytes free. A user's
    /// machine's, whose drive the cache shares.
    Growing { keep_free: u64 },
}

impl FileAllocation {
    /// Kismet's rule: a write leaves its drive 64 MiB free, and twice the
    /// block it may take.
    const KEEP_FREE: u64 = (64 << 20) + 2 * BLOCK_SIZE_U64;

    /// Files that grow, each write leaving its drive [`Self::KEEP_FREE`].
    pub(in crate::cache) fn growing() -> Self {
        Self::Growing {
            keep_free: Self::KEEP_FREE,
        }
    }

    /// Sizes `file` to hold `len` bytes of chunks: allocated whole, or, for
    /// a file that grows, its length set, never shortened.
    pub(super) fn size(self, file: &File, len: u64) -> io::Result<()> {
        match self {
            Self::Whole => rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
                .map_err(io::Error::from),
            Self::Growing { .. } if file.metadata()?.len() < len => file.set_len(len),
            Self::Growing { .. } => Ok(()),
        }
    }

    /// What checks the drives of the files `opened` before each write;
    /// `None` for files allocated whole, whose room is theirs already.
    ///
    /// # Errors
    ///
    /// If a file can't be opened again.
    pub(super) fn headroom(self, opened: &[File]) -> io::Result<Option<DriveHeadroom>> {
        match self {
            Self::Whole => Ok(None),
            Self::Growing { keep_free } => Ok(Some(DriveHeadroom {
                drives: opened
                    .iter()
                    .map(File::try_clone)
                    .collect::<io::Result<_>>()?,
                keep_free,
            })),
        }
    }
}

/// The room a cache whose files grow leaves their drives: a block is
/// written only while each keeps `keep_free` bytes free. A block that finds
/// too little isn't cached, as one that finds no free chunk isn't.
pub(super) struct DriveHeadroom {
    /// The cache files, opened again, to ask their drives the room left.
    drives: Vec<File>,
    keep_free: u64,
}

impl DriveHeadroom {
    /// Whether every drive has its room now, to take a block. A drive that
    /// can't say how much it has takes none.
    pub(super) fn allows_a_block(&self) -> bool {
        self.drives.iter().all(|drive| {
            rustix::fs::fstatvfs(drive)
                .is_ok_and(|room| room.f_bavail.saturating_mul(room.f_frsize) >= self.keep_free)
        })
    }
}
