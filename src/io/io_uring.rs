use std::fs::File;

use io_uring::{IoUring, opcode, types};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

pub(crate) struct UringDriver {
    ring: IoUring,
}

impl UringDriver {
    fn new(queue_depth: u32) -> Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
        })
    }
}

fn build_sqe(entry: SubmissionEntry) -> io_uring::squeue::Entry {
    match entry {
        SubmissionEntry::Read {
            file,
            buf_ptr,
            buf_len,
            offset,
            user_data,
        } => match file {
            super::common::FileType::RawFd(fd) => {
                opcode::Read::new(types::Fd(fd), buf_ptr, buf_len)
                    .offset(offset)
                    .build()
                    .user_data(user_data)
            }
            super::common::FileType::File(_) => {
                unimplemented!("io_uring uses raw file descriptor.")
            }
        },
        SubmissionEntry::Write {
            file,
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
            match file {
                super::common::FileType::RawFd(fd) => {
                    opcode::Write::new(types::Fd(fd), buf_ptr, buf_len)
                        .offset(offset)
                        .build()
                        .flags(flags)
                        .user_data(user_data)
                }
                super::common::FileType::File(_) => {
                    unimplemented!("io_uring uses raw file descriptor.")
                }
            }
        }
        SubmissionEntry::Fsync { file, user_data } => match file {
            super::common::FileType::RawFd(fd) => opcode::Fsync::new(types::Fd(fd))
                .build()
                .user_data(user_data),
            super::common::FileType::File(_) => {
                unimplemented!("io_uring uses raw file descriptor.")
            }
        },
    }
}

impl IoDriver for UringDriver {
    fn available_submission_slots(&mut self) -> usize {
        let sq = self.ring.submission();
        sq.capacity() - sq.len()
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let sqe = build_sqe(entry);
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

    fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {
        crate::io::sync::cooperative_yield();
    }

    fn use_raw_fd(&mut self) -> bool {
        true
    }
}

pub(crate) fn new_backend(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<UringDriver>> {
    let driver = UringDriver::new(queue_depth)?;
    Ok(BackendLoop::new(file, queue_depth as usize, rx, driver))
}
