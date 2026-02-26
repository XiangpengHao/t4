use std::fmt;
use std::os::fd::RawFd;

use io_uring::{IoUring, opcode, types};

use crate::error::{Error, Result};
use crate::io::AlignedBuf;

pub struct UringDriver {
    ring: IoUring,
}

impl fmt::Debug for UringDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringDriver").finish_non_exhaustive()
    }
}

impl UringDriver {
    pub fn new(queue_depth: u32) -> Result<Self> {
        if queue_depth == 0 {
            return Err(Error::InvalidArgument("queue_depth must be > 0"));
        }
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
        })
    }

    pub fn read_at(&mut self, fd: RawFd, buf: &mut AlignedBuf, offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len_u32: u32 = buf
            .len()
            .try_into()
            .map_err(|_| Error::InvalidArgument("read buffer exceeds u32"))?;
        let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len_u32)
            .offset(offset)
            .build()
            .user_data(1);
        self.submit_one(entry)
    }

    pub fn write_at(&mut self, fd: RawFd, buf: &AlignedBuf, offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len_u32: u32 = buf
            .len()
            .try_into()
            .map_err(|_| Error::InvalidArgument("write buffer exceeds u32"))?;
        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), len_u32)
            .offset(offset)
            .build()
            .user_data(2);
        self.submit_one(entry)
    }

    pub fn fsync(&mut self, fd: RawFd) -> Result<()> {
        let entry = opcode::Fsync::new(types::Fd(fd)).build().user_data(3);
        let _ = self.submit_one(entry)?;
        Ok(())
    }

    pub fn read_exact_at(&mut self, fd: RawFd, buf: &mut AlignedBuf, offset: u64) -> Result<()> {
        let n = self.read_at(fd, buf, offset)?;
        if n != buf.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("short read: expected {}, got {n}", buf.len()),
            )));
        }
        Ok(())
    }

    pub fn write_all_at(&mut self, fd: RawFd, buf: &AlignedBuf, offset: u64) -> Result<()> {
        let n = self.write_at(fd, buf, offset)?;
        if n != buf.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("short write: expected {}, got {n}", buf.len()),
            )));
        }
        Ok(())
    }

    fn submit_one(&mut self, entry: io_uring::squeue::Entry) -> Result<usize> {
        {
            let mut sq = self.ring.submission();
            unsafe {
                sq.push(&entry).map_err(|_| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "submission queue is full",
                    ))
                })?;
            }
        }

        self.ring.submit_and_wait(1)?;

        let cqe = {
            let mut cq = self.ring.completion();
            cq.next().ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "no completion entry returned",
                ))
            })?
        };

        let res = cqe.result();
        if res < 0 {
            return Err(Error::Io(std::io::Error::from_raw_os_error(-res)));
        }
        Ok(res as usize)
    }
}
