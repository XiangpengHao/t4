use std::fs::File;
use std::os::fd::AsRawFd;

use io_uring::{IoUring, opcode, types};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::{FilePosition, WorkerRequest};
use crate::io::sync::mpsc;

pub(crate) struct UringDriver {
    ring: IoUring,
    files: Box<[File]>,
    /// Pushed but not reaped, capped by the SQ size so completions fit the CQ: an overflow would
    /// wait for an `io_uring_enter`, which the loop makes only to submit.
    in_flight: usize,
}

impl UringDriver {
    fn new(files: Vec<File>, queue_depth: u32) -> Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
            files: files.into_boxed_slice(),
            in_flight: 0,
        })
    }

    fn fd(&self, at: FilePosition) -> Result<types::Fd> {
        let file = self
            .files
            .get(at.file() as usize)
            .ok_or(Error::InvalidArgument(
                "an I/O names a file the worker hasn't",
            ))?;
        Ok(types::Fd(file.as_raw_fd()))
    }

    fn build_sqe(&self, entry: SubmissionEntry) -> Result<io_uring::squeue::Entry> {
        Ok(match entry {
            SubmissionEntry::Read {
                buf_ptr,
                buf_len,
                at,
                user_data,
            } => opcode::Read::new(self.fd(at)?, buf_ptr, buf_len)
                .offset(at.offset())
                .build()
                .user_data(user_data),
            SubmissionEntry::Write {
                buf_ptr,
                buf_len,
                at,
                user_data,
                is_last_in_batch,
            } => {
                let flags = if is_last_in_batch {
                    io_uring::squeue::Flags::empty()
                } else {
                    io_uring::squeue::Flags::IO_LINK
                };
                opcode::Write::new(self.fd(at)?, buf_ptr, buf_len)
                    .offset(at.offset())
                    .build()
                    .flags(flags)
                    .user_data(user_data)
            }
        })
    }
}

impl IoDriver for UringDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.ring.submission().capacity() - self.in_flight
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let sqe = self.build_sqe(entry)?;
        let mut sq = self.ring.submission();
        unsafe {
            sq.push(&sqe).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "submission queue is full",
                ))
            })?;
        }
        self.in_flight += 1;
        Ok(())
    }

    fn submit(&mut self) -> Result<()> {
        let _ = self.ring.submit()?;
        Ok(())
    }

    /// A failed `io_uring_enter` takes none of the entries, so all queued ones are unsubmitted.
    fn unsubmitted(&mut self) -> usize {
        self.ring.submission().len()
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        let cqe = self.ring.completion().next()?;
        self.in_flight -= 1;
        Some(CompletionEvent {
            user_data: cqe.user_data(),
            result: cqe.result(),
        })
    }

    /// Busy-waits: the loop polls the ring again at once.
    fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {}
}

pub(crate) fn new_backend(
    files: Vec<File>,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<UringDriver>> {
    let driver = UringDriver::new(files, queue_depth)?;
    Ok(BackendLoop::new(queue_depth as usize, rx, driver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PAGE_SIZE_NZ_U32;
    use crate::buffer::AlignedBuf;

    #[test]
    fn a_slot_frees_once_its_completion_is_reaped() {
        let mut driver = UringDriver::new(vec![tempfile::tempfile().unwrap()], 1).unwrap();
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        let read = SubmissionEntry::Read {
            buf_ptr: buf.as_mut_ptr(),
            buf_len: buf.len_u32(),
            at: FilePosition::new(0, 0),
            user_data: 7,
        };
        driver.push(read).unwrap();
        driver.submit().unwrap();
        assert_eq!(driver.available_submission_slots(), 0, "taken, not reaped");
        let completion = std::iter::repeat_with(|| driver.pop_completion())
            .find_map(|completion| completion)
            .unwrap();
        assert_eq!(completion.user_data, 7);
        assert_eq!(driver.available_submission_slots(), 1);
    }
}
