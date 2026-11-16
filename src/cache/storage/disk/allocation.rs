use std::fs::File;
use std::io;

use crate::cache::BLOCK_SIZE_U64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) enum FileAllocation {
    Eager,
    OnDemand { keep_free: u64 },
}

impl FileAllocation {
    /// Kismet's rule: 64 MiB free, and twice the block a write may take.
    const KEEP_FREE: u64 = (64 << 20) + 2 * BLOCK_SIZE_U64;

    pub(in crate::cache) fn on_demand() -> Self {
        Self::OnDemand {
            keep_free: Self::KEEP_FREE,
        }
    }

    pub(super) fn size(self, file: &File, len: u64) -> io::Result<()> {
        match self {
            Self::Eager => rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)
                .map_err(io::Error::from),
            Self::OnDemand { .. } if file.metadata()?.len() < len => file.set_len(len),
            Self::OnDemand { .. } => Ok(()),
        }
    }

    pub(super) fn headroom(self, opened: &[File]) -> io::Result<Option<DriveHeadroom>> {
        match self {
            Self::Eager => Ok(None),
            Self::OnDemand { keep_free } => Ok(Some(DriveHeadroom {
                drives: opened
                    .iter()
                    .map(File::try_clone)
                    .collect::<io::Result<_>>()?,
                keep_free,
            })),
        }
    }
}

pub(super) struct DriveHeadroom {
    drives: Vec<File>,
    keep_free: u64,
}

impl DriveHeadroom {
    pub(super) fn allows_a_block(&self) -> bool {
        self.drives.iter().all(|drive| {
            rustix::fs::fstatvfs(drive)
                .is_ok_and(|room| room.f_bavail.saturating_mul(room.f_frsize) >= self.keep_free)
        })
    }
}
