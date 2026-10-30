//! [`FrameSocket`]: a connected socket between an owner and its client,
//! which frames cross whole (`doc/11-shared-mode.md`, decision 5); and
//! [`FrameReceiver`], which takes them from it.

use std::io::{self, IoSlice, Read};
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::Bytes;
use rustix::net::{SendAncillaryBuffer, SendFlags};

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

    /// The most slices a `sendmsg` takes: `IOV_MAX`, on Linux and macOS.
    const MOST_SLICES: usize = 1024;

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
    pub(super) fn send(&self, frame: &[u8]) -> io::Result<()> {
        self.send_all(&[frame])
    }

    /// Sends `frames`, each whole, in order, in as few writes as the socket
    /// takes them in: a batch costs its sender one syscall, and its receiver
    /// one read (`FrameReceiver`).
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
    pub(super) fn send_all(&self, frames: &[impl AsRef<[u8]>]) -> io::Result<()> {
        let mut slices: Vec<IoSlice<'_>> = frames
            .iter()
            .map(|frame| IoSlice::new(frame.as_ref()))
            .filter(|slice| !slice.is_empty())
            .collect();
        let mut unsent = &mut slices[..];
        while !unsent.is_empty() {
            let mut control = SendAncillaryBuffer::default();
            let some = &unsent[..unsent.len().min(Self::MOST_SLICES)];
            match rustix::net::sendmsg(&self.stream, some, &mut control, Self::SEND_FLAGS) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(sent) => IoSlice::advance_slices(&mut unsent, sent),
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
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

/// Takes the frames a [`FrameSocket`]'s peer sends: each read takes all
/// that has come, up to [`Self::READ_AHEAD`] bytes, and the frames it holds
/// go out one at a time, with no read more, so that a batch of frames costs
/// one syscall (`FrameSocket::send_all`).
pub(super) struct FrameReceiver {
    socket: FrameSocket,
    /// The bytes read, from `taken` on not yet handed out.
    read: Vec<u8>,
    taken: usize,
}

impl FrameReceiver {
    /// The most bytes a read takes ahead of the frame it needs: many
    /// control frames, few enough to copy a frame out of.
    const READ_AHEAD: usize = 64 << 10;

    /// The bytes of a frame's length, which comes first.
    const LENGTH: usize = size_of::<u64>();

    pub(super) fn new(socket: FrameSocket) -> Self {
        Self {
            socket,
            read: Vec::with_capacity(Self::READ_AHEAD),
            taken: 0,
        }
    }

    /// The body of the next frame, `most` bytes at most; `None` if the peer
    /// closed the socket between frames.
    ///
    /// # Errors
    ///
    /// If the socket fails, its peer closes it within a frame, or the frame
    /// is longer than `most`.
    pub(super) fn receive(&mut self, most: u64) -> io::Result<Option<Bytes>> {
        if !self.hold(Self::LENGTH)? {
            return Ok(None);
        }
        let (length, _) = self
            .unread()
            .split_first_chunk::<{ Self::LENGTH }>()
            .expect("the length held");
        let length = u64::from_le_bytes(*length);
        if length > most {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a frame of {length} bytes, past the {most} taken"),
            ));
        }
        self.taken += Self::LENGTH;
        let length = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidData)?;
        if let Some(body) = self.unread().get(..length) {
            let body = Bytes::copy_from_slice(body);
            self.taken += length;
            return Ok(Some(body));
        }
        // Longer than what came: the rest read straight into its own
        // memory, which the allocator hands over zeroed.
        let mut body = vec![0; length];
        let held = self.unread().len();
        body[..held].copy_from_slice(self.unread());
        self.taken += held;
        (&self.socket).read_exact(&mut body[held..])?;
        Ok(Some(Bytes::from(body)))
    }

    /// Whether a whole frame came with the last read: the next receive
    /// takes it without reading the socket.
    pub(super) fn holds_frame(&self) -> bool {
        let Some((length, body)) = self.unread().split_first_chunk::<{ Self::LENGTH }>() else {
            return false;
        };
        body.len() as u64 >= u64::from_le_bytes(*length)
    }

    /// Shuts the socket both ways, as [`FrameSocket::shut_down`] does.
    pub(super) fn shut_down(&self) {
        self.socket.shut_down();
    }

    fn unread(&self) -> &[u8] {
        &self.read[self.taken..]
    }

    /// Holds `bytes` unread at least, reading all that has come, up to
    /// [`Self::READ_AHEAD`]; `false` if the peer closed the socket first,
    /// with none unread.
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer closes it with fewer unread.
    fn hold(&mut self, bytes: usize) -> io::Result<bool> {
        if self.unread().len() >= bytes {
            return Ok(true);
        }
        self.read.drain(..self.taken);
        self.taken = 0;
        while self.read.len() < bytes {
            match rustix::io::read(&self.socket, rustix::buffer::spare_capacity(&mut self.read)) {
                Ok(0) if self.read.is_empty() => return Ok(false),
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(_) => {}
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }
}
