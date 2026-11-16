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
            Self::Eager => EagerFile(file).take_room(len),
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

/// Takes every block below a length and keeps the bytes already there, so
/// taking it again takes nothing more.
struct EagerFile<'a>(&'a File);

impl EagerFile<'_> {
    #[cfg(not(target_os = "macos"))]
    fn take_room(&self, len: u64) -> io::Result<()> {
        rustix::fs::fallocate(self.0, rustix::fs::FallocateFlags::empty(), 0, len)
            .map_err(io::Error::from)
    }
}

/// `F_PREALLOCATE` only adds room past the file's physical end: asking for
/// the whole length again takes it twice, and that room never fills a hole.
#[cfg(target_os = "macos")]
impl EagerFile<'_> {
    fn take_room(&self, len: u64) -> io::Result<()> {
        let size = self.0.metadata()?.len();
        self.zero_holes(size.min(len))?;
        let shortfall = len.saturating_sub(self.allocated()?);
        if shortfall > 0 {
            self.preallocate_past_end(shortfall)?;
        }
        if size < len {
            self.0.set_len(len)?;
        }
        Ok(())
    }

    fn zero_holes(&self, below: u64) -> io::Result<()> {
        use rustix::fs::{SeekFrom, seek};
        use std::os::unix::fs::FileExt;

        let zeros = vec![0; crate::cache::BLOCK_SIZE];
        let mut at = 0;
        while at < below {
            let hole = seek(self.0, SeekFrom::Hole(at))?;
            if hole >= below {
                break;
            }
            let data = match seek(self.0, SeekFrom::Data(hole)) {
                Ok(data) => data.min(below),
                Err(rustix::io::Errno::NXIO) => below,
                Err(error) => return Err(error.into()),
            };
            for start in (hole..data).step_by(zeros.len()) {
                let run = (data - start).min(BLOCK_SIZE_U64) as usize;
                self.0.write_all_at(&zeros[..run], start)?;
            }
            at = data;
        }
        Ok(())
    }

    fn preallocate_past_end(&self, len: u64) -> io::Result<()> {
        use std::os::fd::AsRawFd;

        let mut store = libc::fstore_t {
            // Never `F_ALLOCATECONTIG`: APFS grants it in part and reports success.
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: libc::off_t::try_from(len).map_err(|_| io::ErrorKind::FileTooLarge)?,
            fst_bytesalloc: 0,
        };
        // SAFETY: `store` lives through the call, and `self.0` is an open file.
        if unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn allocated(&self) -> io::Result<u64> {
        use std::os::unix::fs::MetadataExt;

        // `st_blocks` counts 512-byte units.
        Ok(self.0.metadata()?.blocks() * 512)
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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{FileExt, MetadataExt};

    use super::*;
    use crate::cache::BLOCK_SIZE;

    /// Long enough for holes APFS keeps: it fills those under about 16 MiB
    /// when it flushes a file.
    const LEN: u64 = 64 * BLOCK_SIZE_U64;

    fn allocated(file: &File) -> u64 {
        file.sync_all().unwrap();
        // `st_blocks` counts 512-byte units, whatever the file system's blocks.
        file.metadata().unwrap().blocks() * 512
    }

    /// As every reopen of an eager cache file does.
    #[test]
    fn sizing_a_file_eagerly_again_takes_no_more_room() {
        let file = tempfile::tempfile().unwrap();
        FileAllocation::Eager.size(&file, LEN).unwrap();
        let taken = allocated(&file);
        assert!(taken >= LEN, "{taken} bytes allocated");

        FileAllocation::Eager.size(&file, LEN).unwrap();
        let again = allocated(&file);
        assert!(again <= taken, "{again} bytes allocated, {taken} before");
    }

    /// A file used on demand, then opened eagerly.
    #[test]
    fn sizing_a_sparse_file_eagerly_takes_its_holes_and_keeps_its_bytes() {
        let file = tempfile::tempfile().unwrap();
        FileAllocation::on_demand().size(&file, LEN).unwrap();
        let block = vec![7; BLOCK_SIZE];
        let written = [0, LEN / 2];
        for at in written {
            file.write_all_at(&block, at).unwrap();
        }
        let sparse = allocated(&file);
        assert!(sparse < LEN, "{sparse} bytes allocated");

        FileAllocation::Eager.size(&file, LEN).unwrap();
        let taken = allocated(&file);
        assert!(taken >= LEN, "{taken} bytes allocated");
        for at in written {
            let mut read = vec![0; BLOCK_SIZE];
            file.read_exact_at(&mut read, at).unwrap();
            assert_eq!(read, block, "the block at {at}");
        }

        let holes = (0..LEN)
            .step_by(BLOCK_SIZE)
            .filter(|at| !written.contains(at));
        for at in holes {
            file.write_all_at(&block, at).unwrap();
        }
        let filled = allocated(&file);
        assert!(filled <= taken, "{filled} bytes allocated, {taken} before");
        FileAllocation::Eager.size(&file, LEN).unwrap();
        let again = allocated(&file);
        assert!(again <= taken, "{again} bytes allocated, {taken} before");
    }
}
