//! [`FrameSocket`]: a connected socket between an owner and its client,
//! which frames cross whole (`doc/11-shared-mode.md`, decision 5).

use std::io::{self, Read};
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::Bytes;
use rustix::net::SendFlags;

/// A connected Unix socket, frames sent and received whole. A write to a
/// socket whose peer has gone fails, as any other, rather than raise
/// `SIGPIPE` in the process, which the process may not ignore: it is a
/// user's app.
pub(super) struct FrameSocket {
    stream: UnixStream,
}

impl FrameSocket {
    /// The most bytes a frame's body holds once the handshake is done: 2 GiB,
    /// more than a read or a piece of a GET's body holds.
    pub(super) const MOST_BYTES: u64 = 1 << 31;

    /// How a send asks for no `SIGPIPE`: per send on Linux, per socket on
    /// macOS (`Self::new`).
    #[cfg(not(target_vendor = "apple"))]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
    #[cfg(target_vendor = "apple")]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::empty();

    pub(super) fn new(stream: UnixStream) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        rustix::net::sockopt::set_socket_nosigpipe(&stream, true)?;
        Ok(Self { stream })
    }

    /// The same socket, for another thread to read or write.
    pub(super) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            stream: self.stream.try_clone()?,
        })
    }

    /// Sends `frame`, whole.
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
    pub(super) fn send(&self, mut frame: &[u8]) -> io::Result<()> {
        while !frame.is_empty() {
            match rustix::net::send(&self.stream, frame, Self::SEND_FLAGS) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(sent) => frame = &frame[sent..],
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// The body of the next frame, `most` bytes at most; `None` if the peer
    /// closed the socket between frames.
    ///
    /// # Errors
    ///
    /// If the socket fails, its peer closes it within a frame, or the frame
    /// is longer than `most`.
    pub(super) fn receive(&self, most: u64) -> io::Result<Option<Bytes>> {
        let mut length = [0; size_of::<u64>()];
        let mut read = 0;
        while read < length.len() {
            match (&self.stream).read(&mut length[read..]) {
                Ok(0) if read == 0 => return Ok(None),
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(more) => read += more,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        let length = u64::from_le_bytes(length);
        if length > most {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a frame of {length} bytes, past the {most} taken"),
            ));
        }
        let mut body = vec![0; length as usize];
        (&self.stream).read_exact(&mut body)?;
        Ok(Some(Bytes::from(body)))
    }

    /// How long a receive waits for bytes before it fails; forever if
    /// `None`.
    pub(super) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Shuts the socket both ways: its peer sees it closed, and so does
    /// every thread of this side's, a receive waiting at once.
    pub(super) fn shut_down(&self) {
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
