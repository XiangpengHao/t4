use std::fs::File;
use std::os::fd::AsRawFd;

use io_uring::{IoUring, opcode, types};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

pub(crate) struct UringDriver {
    ring: IoUring,
    file: File,
}

impl UringDriver {
    fn new(file: File, queue_depth: u32) -> Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
            file,
        })
    }

    fn build_sqe(&self, entry: SubmissionEntry) -> io_uring::squeue::Entry {
        let fd = types::Fd(self.file.as_raw_fd());
        match entry {
            SubmissionEntry::Read {
                buf_ptr,
                buf_len,
                offset,
                user_data,
            } => opcode::Read::new(fd, buf_ptr, buf_len)
                .offset(offset)
                .build()
                .user_data(user_data),
            SubmissionEntry::Write {
                buf_ptr,
                buf_len,
                offset,
                user_data,
                is_last_in_batch,
            } => {
                let flags = if is_last_in_batch {
                    io_uring::squeue::Flags::empty()
                } else {
                    io_uring::squeue::Flags::IO_LINK
                };
                opcode::Write::new(fd, buf_ptr, buf_len)
                    .offset(offset)
                    .build()
                    .flags(flags)
                    .user_data(user_data)
            }
        }
    }
}

impl IoDriver for UringDriver {
    fn available_submission_slots(&mut self) -> usize {
        let sq = self.ring.submission();
        sq.capacity() - sq.len()
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let sqe = self.build_sqe(entry);
        let mut sq = self.ring.submission();
        unsafe {
            sq.push(&sqe).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "submission queue is full",
                ))
            })?;
        }
        Ok(())
    }

    fn submit(&mut self) -> Result<()> {
        let _ = self.ring.submit()?;
        Ok(())
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        let mut cq = self.ring.completion();
        cq.next().map(|cqe| CompletionEvent {
            user_data: cqe.user_data(),
            result: cqe.result(),
        })
    }

    /// Returns at once: the loop polls the ring again, busy-waiting while
    /// I/O is in flight.
    fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {}
}

pub(crate) fn new_backend(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<UringDriver>> {
    let driver = UringDriver::new(file, queue_depth)?;
    Ok(BackendLoop::new(queue_depth as usize, rx, driver))
}
