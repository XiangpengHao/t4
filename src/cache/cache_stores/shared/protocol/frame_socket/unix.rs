//! [`FrameSocket`]: a connected socket between an owner and its client,
//! which frames cross whole (`doc/11-shared-mode.md`, decision 5);
//! [`FrameReceiver`], which takes them from it; and [`FrameSender`], which
//! sends them for many callers.

use std::io::{self, IoSlice, Read};
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::BytesMut;
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::net::{SendAncillaryBuffer, SendFlags};

/// A connected Unix socket, frames sent and received whole. A write to a
/// socket whose peer has gone fails, as any other, rather than raise
/// `SIGPIPE` in the process, which the process may not ignore: it is a
/// user's app.
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

    /// The most bytes a frame's body holds once the handshake is done: 2 GiB,
    /// more than a read or a piece of a GET's body holds.
    pub(in crate::cache::cache_stores::shared) const MOST_BYTES: u64 = 1 << 31;

    /// How a send asks for no `SIGPIPE`: per send on Linux, per socket on
    /// macOS (`Self::new`).
    #[cfg(not(target_vendor = "apple"))]
    pub(in crate::cache::cache_stores::shared) const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
    #[cfg(target_vendor = "apple")]
    pub(in crate::cache::cache_stores::shared) const SEND_FLAGS: SendFlags = SendFlags::empty();

    /// How a send of what fits asks to take no more than the socket has
    /// room for: per send on Linux; on macOS, which ignores `MSG_DONTWAIT`
    /// on a Unix socket, by the socket's write timeout
    /// (`Self::set_write_timeout`).
    #[cfg(not(target_vendor = "apple"))]
    const SEND_SOME_FLAGS: SendFlags = Self::SEND_FLAGS.union(SendFlags::DONTWAIT);
    #[cfg(target_vendor = "apple")]
    const SEND_SOME_FLAGS: SendFlags = Self::SEND_FLAGS;

    /// How often a thread waiting on a socket looks again, though nothing
    /// woke it: macOS can lose the wakeup of a shutdown that comes just as
    /// a thread starts to read the socket, which, its peer open, would
    /// leave the thread asleep for good (checked: about 1 in 300 when they
    /// race, `doc/11-shared-mode.md`, Cost). Every wait on a socket that
    /// could last ends this often, to look again.
    pub(in crate::cache::cache_stores::shared) const RECHECK: Duration = Duration::from_secs(1);

    /// The most slices a `sendmsg` takes: `IOV_MAX`, on Linux and macOS.
    const MOST_SLICES: usize = 1024;

    pub(in crate::cache::cache_stores::shared) fn new(stream: UnixStream) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        rustix::net::sockopt::set_socket_nosigpipe(&stream, true)?;
        Ok(Self { stream })
    }

    /// The same socket, for another thread to read or write.
    pub(in crate::cache::cache_stores::shared) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            stream: self.stream.try_clone()?,
        })
    }

    /// Sends `frame`, whole.
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
    pub(in crate::cache::cache_stores::shared) fn send(&self, frame: &[u8]) -> io::Result<()> {
        self.send_all(&[frame])
    }

    /// Sends `frames`, each whole, in order, in as few writes as the socket
    /// takes them in: a batch costs its sender one syscall, and its receiver
    /// one read (`FrameReceiver`).
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
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

    /// Sends what of `frames`, in order, the socket has room for in one
    /// write: at once on Linux, within its write timeout on macOS
    /// (`Self::SEND_SOME_FLAGS`); the bytes it took. A descriptor's
    /// `O_NONBLOCK` would be every clone's, the reader's too.
    ///
    /// # Errors
    ///
    /// Of kind `WouldBlock` if it took none; if the socket fails, or its
    /// peer has gone.
    pub(super) fn send_some(&self, frames: &[impl AsRef<[u8]>]) -> io::Result<usize> {
        self.send_slices(&Self::slices(frames), Self::SEND_SOME_FLAGS)
    }

    /// Each frame of `frames` an `IoSlice`, but those empty.
    fn slices(frames: &[impl AsRef<[u8]>]) -> Vec<IoSlice<'_>> {
        frames
            .iter()
            .map(|frame| IoSlice::new(frame.as_ref()))
            .filter(|slice| !slice.is_empty())
            .collect()
    }

    /// One write of `slices`, as many as a write takes, sent with `flags`:
    /// the bytes the socket took.
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

    /// How long a send waits for room before it sends what fits, or fails;
    /// forever if `None`. Every clone's sends wait as long.
    #[cfg(target_vendor = "apple")]
    pub(super) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    /// Waits until the socket is ready for `ready`, or its peer has gone,
    /// or [`Self::RECHECK`] is up.
    fn wait(&self, ready: PollFlags) -> io::Result<()> {
        let mut socket = [PollFd::new(&self.stream, ready)];
        let recheck = Self::recheck();
        loop {
            match rustix::event::poll(&mut socket, Some(&recheck)) {
                Err(rustix::io::Errno::INTR) => {}
                waited => return waited.map(drop).map_err(io::Error::from),
            }
        }
    }

    /// [`Self::RECHECK`], as `poll` takes it.
    pub(in crate::cache::cache_stores::shared) fn recheck() -> Timespec {
        Timespec {
            tv_sec: Self::RECHECK.as_secs().try_into().expect("a second fits"),
            tv_nsec: Self::RECHECK.subsec_nanos().into(),
        }
    }

    /// How long a receive waits for bytes before it fails; forever if
    /// `None`.
    pub(in crate::cache::cache_stores::shared) fn set_read_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Shuts the socket both ways: its peer sees it closed, and so does
    /// every thread of this side's, a receive waiting at once.
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
