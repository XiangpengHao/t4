use std::io::{self, IoSlice, Read};
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::BytesMut;
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::net::{SendAncillaryBuffer, SendFlags};

use crate::cache::cache_stores::shared::spin_wait::SpinWait;

/// A send to a gone peer fails rather than raise `SIGPIPE`, which the process, a user's app,
/// may not ignore.
pub(in crate::cache::cache_stores::shared) struct FrameSocket {
    stream: UnixStream,
}

impl FrameSocket {
    pub(super) fn read_into(&self, buffer: &mut BytesMut) -> io::Result<usize> {
        let (read, _) = rustix::io::read(self, buffer.spare_capacity_mut())?;
        let read = read.len();
        // SAFETY: read initialized these bytes in the buffer's spare capacity.
        unsafe { buffer.set_len(buffer.len() + read) };
        Ok(read)
    }

    /// More than a read or a piece of a GET's body holds.
    pub(in crate::cache::cache_stores::shared) const MOST_BYTES: u64 = 1 << 31;

    /// macOS asks for no `SIGPIPE` per socket instead (`Self::new`).
    #[cfg(not(target_vendor = "apple"))]
    pub(in crate::cache::cache_stores::shared) const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
    #[cfg(target_vendor = "apple")]
    pub(in crate::cache::cache_stores::shared) const SEND_FLAGS: SendFlags = SendFlags::empty();

    /// macOS ignores `MSG_DONTWAIT` on a Unix socket: there the write timeout bounds a send.
    #[cfg(not(target_vendor = "apple"))]
    const SEND_SOME_FLAGS: SendFlags = Self::SEND_FLAGS.union(SendFlags::DONTWAIT);
    #[cfg(target_vendor = "apple")]
    const SEND_SOME_FLAGS: SendFlags = Self::SEND_FLAGS;

    /// macOS can lose a shutdown's wakeup racing a read, leaving the thread asleep for good
    /// (about 1 in 300, `doc/11-shared-mode.md`, Cost): every long wait looks again this often.
    pub(in crate::cache::cache_stores::shared) const RECHECK: Duration = Duration::from_secs(1);

    /// `IOV_MAX` on Linux and macOS.
    const MOST_SLICES: usize = 1024;

    pub(in crate::cache::cache_stores::shared) fn new(stream: UnixStream) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        rustix::net::sockopt::set_socket_nosigpipe(&stream, true)?;
        Ok(Self { stream })
    }

    pub(in crate::cache::cache_stores::shared) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            stream: self.stream.try_clone()?,
        })
    }

    pub(in crate::cache::cache_stores::shared) fn send(&self, frame: &[u8]) -> io::Result<()> {
        self.send_all(&[frame])
    }

    pub(in crate::cache::cache_stores::shared) fn send_all(
        &self,
        frames: &[impl AsRef<[u8]>],
    ) -> io::Result<()> {
        let mut slices = Self::slices(frames);
        let mut unsent = &mut slices[..];
        while !unsent.is_empty() {
            match self.send_slices(unsent, Self::SEND_FLAGS) {
                Ok(sent) => IoSlice::advance_slices(&mut unsent, sent),
                // The write timeout ran out: wait for room.
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait(PollFlags::OUT)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Not by `O_NONBLOCK`: a descriptor's flag would be every clone's, the reader's too.
    pub(super) fn send_some(&self, frames: &[impl AsRef<[u8]>]) -> io::Result<usize> {
        self.send_slices(&Self::slices(frames), Self::SEND_SOME_FLAGS)
    }

    fn slices(frames: &[impl AsRef<[u8]>]) -> Vec<IoSlice<'_>> {
        frames
            .iter()
            .map(|frame| IoSlice::new(frame.as_ref()))
            .filter(|slice| !slice.is_empty())
            .collect()
    }

    fn send_slices(&self, slices: &[IoSlice<'_>], flags: SendFlags) -> io::Result<usize> {
        let some = &slices[..slices.len().min(Self::MOST_SLICES)];
        loop {
            let mut control = SendAncillaryBuffer::default();
            match rustix::net::sendmsg(&self.stream, some, &mut control, flags) {
                Ok(0) if !some.is_empty() => return Err(io::ErrorKind::WriteZero.into()),
                Err(rustix::io::Errno::INTR) => {}
                sent => return Ok(sent?),
            }
        }
    }

    /// Every clone's sends wait as long.
    #[cfg(target_vendor = "apple")]
    pub(super) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    pub(super) fn spin_until_readable(&self) -> io::Result<()> {
        SpinWait::BEFORE_SLEEP
            .until(|| self.poll(PollFlags::IN, &Timespec::default()))
            .map(drop)
    }

    fn wait(&self, ready: PollFlags) -> io::Result<()> {
        self.poll(ready, &Self::recheck()).map(drop)
    }

    fn poll(&self, ready: PollFlags, timeout: &Timespec) -> io::Result<bool> {
        let mut socket = [PollFd::new(&self.stream, ready)];
        loop {
            match rustix::event::poll(&mut socket, Some(timeout)) {
                Err(rustix::io::Errno::INTR) => {}
                polled => return Ok(polled? > 0),
            }
        }
    }

    pub(in crate::cache::cache_stores::shared) fn recheck() -> Timespec {
        Timespec {
            tv_sec: Self::RECHECK.as_secs().try_into().expect("a second fits"),
            tv_nsec: Self::RECHECK.subsec_nanos().into(),
        }
    }

    pub(in crate::cache::cache_stores::shared) fn set_read_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    pub(in crate::cache::cache_stores::shared) fn shut_down(&self) {
        // Fails only if the peer shut it first: shut either way.
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

impl AsFd for FrameSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.stream.as_fd()
    }
}

impl Read for &FrameSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&self.stream).read(buf)
    }
}
