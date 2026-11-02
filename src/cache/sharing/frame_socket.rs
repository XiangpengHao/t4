//! [`FrameSocket`]: a connected socket between an owner and its client,
//! which frames cross whole (`doc/11-shared-mode.md`, decision 5);
//! [`FrameReceiver`], which takes them from it; and [`FrameSender`], which
//! sends them for many callers.

use std::collections::VecDeque;
use std::io::{self, IoSlice, Read};
use std::net::Shutdown;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;
use rustix::event::{PollFd, PollFlags};
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
        let mut slices = Self::slices(frames);
        let mut unsent = &mut slices[..];
        while !unsent.is_empty() {
            match self.send_slices(unsent) {
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

    /// Sends what of `frames`, in order, the socket takes in one write
    /// within its write timeout (`Self::set_write_timeout`): the bytes it
    /// took. macOS ignores `MSG_DONTWAIT` on a Unix socket, and a
    /// descriptor's `O_NONBLOCK` is every clone's, the reader's too.
    ///
    /// # Errors
    ///
    /// Of kind `WouldBlock` if it took none in that time; if the socket
    /// fails, or its peer has gone.
    pub(super) fn send_some(&self, frames: &[impl AsRef<[u8]>]) -> io::Result<usize> {
        self.send_slices(&Self::slices(frames))
    }

    /// Each frame of `frames` an `IoSlice`, but those empty.
    fn slices(frames: &[impl AsRef<[u8]>]) -> Vec<IoSlice<'_>> {
        frames
            .iter()
            .map(|frame| IoSlice::new(frame.as_ref()))
            .filter(|slice| !slice.is_empty())
            .collect()
    }

    /// One write of `slices`, as many as a write takes: the bytes the
    /// socket took within its write timeout.
    fn send_slices(&self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        let some = &slices[..slices.len().min(Self::MOST_SLICES)];
        loop {
            let mut control = SendAncillaryBuffer::default();
            match rustix::net::sendmsg(&self.stream, some, &mut control, Self::SEND_FLAGS) {
                Ok(0) if !some.is_empty() => return Err(io::ErrorKind::WriteZero.into()),
                Err(rustix::io::Errno::INTR) => {}
                sent => return Ok(sent?),
            }
        }
    }

    /// How long a send waits for room before it sends what fits, or fails;
    /// forever if `None`. Every clone's sends wait as long.
    pub(super) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    /// Waits until the socket is ready for `ready`, or its peer has gone.
    fn wait(&self, ready: PollFlags) -> io::Result<()> {
        let mut socket = [PollFd::new(&self.stream, ready)];
        loop {
            match rustix::event::poll(&mut socket, None) {
                Err(rustix::io::Errno::INTR) => {}
                waited => return waited.map(drop).map_err(io::Error::from),
            }
        }
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

/// Sends frames on a [`FrameSocket`] for many callers, none of which waits
/// on it longer than [`Self::SEND_WAIT`]: a caller writes its frame itself
/// while the socket has room for it, which saves waking a thread; a frame
/// the socket has no room for, and every frame sent after it till they
/// have gone, waits for the sender's writer thread, so that frames go in
/// the order sent. Dropped, it shuts the socket down and stops its writer:
/// frames still waiting go nowhere.
pub(super) struct FrameSender {
    queue: std::sync::Arc<SenderQueue>,
    writer: Option<JoinHandle<()>>,
}

impl FrameSender {
    /// The most a caller's send waits for room on the socket before it
    /// leaves its frame to the writer: a frame takes a few µs to send; an
    /// owner that doesn't read for this long is busy, or stuck.
    const SEND_WAIT: Duration = Duration::from_micros(200);

    /// A sender on `socket`, its writer thread started. Every clone of the
    /// socket's sends wait [`Self::SEND_WAIT`] at most after.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the thread can't be started.
    pub(super) fn start(socket: FrameSocket) -> io::Result<Self> {
        socket.set_write_timeout(Some(Self::SEND_WAIT))?;
        let queue = std::sync::Arc::new(SenderQueue {
            socket,
            state: Mutex::default(),
            waiting: Condvar::new(),
        });
        let writing = std::sync::Arc::clone(&queue);
        let writer = thread::Builder::new()
            .name("t4-client-writer".into())
            .spawn(move || writing.write_waiting())?;
        Ok(Self {
            queue,
            writer: Some(writer),
        })
    }

    /// Sends `frames`, in order, after every frame sent before them, in one
    /// write if the socket has room, waiting [`Self::SEND_WAIT`] at most;
    /// drops them once the peer has gone.
    pub(super) fn send(&self, frames: Vec<Vec<u8>>) {
        let mut state = self.queue.state();
        if state.closed {
            return;
        }
        let mut frames = VecDeque::from(frames);
        if state.frames.is_empty() && !state.writing {
            match self.queue.socket.send_some(frames.make_contiguous()) {
                Ok(mut sent) => {
                    while let Some(first) = frames.front_mut() {
                        if sent < first.len() {
                            first.drain(..sent);
                            break;
                        }
                        sent -= first.len();
                        frames.pop_front();
                    }
                    if frames.is_empty() {
                        return;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => {
                    // The peer has gone: the reader sees it too.
                    state.closed = true;
                    self.queue.socket.shut_down();
                    return;
                }
            }
        }
        state.frames.extend(frames);
        self.queue.waiting.notify_one();
    }
}

impl Drop for FrameSender {
    fn drop(&mut self) {
        self.queue.state().closed = true;
        self.queue.waiting.notify_one();
        // A writer blocked on a socket its peer doesn't read fails.
        self.queue.socket.shut_down();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

/// What a [`FrameSender`]'s callers and writer share.
struct SenderQueue {
    socket: FrameSocket,
    state: Mutex<SenderState>,
    /// Wakes the writer: a frame waits, or the sender stops.
    waiting: Condvar,
}

#[derive(Default)]
struct SenderState {
    /// The frames the writer sends next, in order.
    frames: VecDeque<Vec<u8>>,
    /// Whether the writer is sending frames it took: a caller's frame
    /// waits behind them.
    writing: bool,
    /// Whether the peer has gone, or the sender stopped: frames are
    /// dropped.
    closed: bool,
}

impl SenderQueue {
    /// The writer's work: sends the frames that wait, all that wait in one
    /// write, until the peer goes or the sender stops.
    fn write_waiting(&self) {
        loop {
            let frames: Vec<Vec<u8>> = {
                let mut state = self.state();
                state.writing = false;
                while state.frames.is_empty() && !state.closed {
                    state = self
                        .waiting
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                if state.closed {
                    return;
                }
                state.writing = true;
                state.frames.drain(..).collect()
            };
            if self.socket.send_all(&frames).is_err() {
                // The peer has gone: the reader sees it too.
                self.state().closed = true;
                self.socket.shut_down();
                return;
            }
        }
    }

    fn state(&self) -> MutexGuard<'_, SenderState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
