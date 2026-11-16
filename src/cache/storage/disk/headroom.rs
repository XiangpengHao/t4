use std::fs::File;
use std::io;

use crate::cache::BLOCK_SIZE_U64;

/// Cache files are sized, never allocated, so a block goes to disk only while
/// every drive keeps room for it.
pub(super) struct DriveHeadroom {
    drives: Vec<File>,
    keep_free: u64,
}

impl DriveHeadroom {
    /// Kismet's rule: 64 MiB free, and twice the block a write may take.
    const KEEP_FREE: u64 = (64 << 20) + 2 * BLOCK_SIZE_U64;

    pub(super) fn of(opened: &[File]) -> io::Result<Self> {
        Ok(Self {
            drives: opened
                .iter()
                .map(File::try_clone)
                .collect::<io::Result<_>>()?,
            keep_free: Self::KEEP_FREE,
        })
    }

    #[cfg(test)]
    pub(super) fn set_keep_free(&mut self, keep_free: u64) {
        self.keep_free = keep_free;
    }

    pub(super) fn allows_a_block(&self) -> bool {
        self.drives.iter().all(|drive| {
            rustix::fs::fstatvfs(drive)
                .is_ok_and(|room| room.f_bavail.saturating_mul(room.f_frsize) >= self.keep_free)
        })
    }
}
