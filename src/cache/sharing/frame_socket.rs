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

use bytes::{Buf, Bytes, BytesMut};
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::net::{SendAncillaryBuffer, SendFlags};

/// A connected Unix socket, frames sent and received whole. A write to a
/// socket whose peer has gone fails, as any other, rather than raise
/// `SIGPIPE` in the process, which the process may not ignore: it is a
/// user's app.
pub(in crate::cache) struct FrameSocket {
    stream: UnixStream,
}

impl FrameSocket {
    /// The most bytes a frame's body holds once the handshake is done: 2 GiB,
    /// more than a read or a piece of a GET's body holds.
    pub(in crate::cache) const MOST_BYTES: u64 = 1 << 31;

    /// How a send asks for no `SIGPIPE`: per send on Linux, per socket on
    /// macOS (`Self::new`).
    #[cfg(not(target_vendor = "apple"))]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
    #[cfg(target_vendor = "apple")]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::empty();

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
    pub(in crate::cache) const RECHECK: Duration = Duration::from_secs(1);

    /// The most slices a `sendmsg` takes: `IOV_MAX`, on Linux and macOS.
    const MOST_SLICES: usize = 1024;

    pub(in crate::cache) fn new(stream: UnixStream) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        rustix::net::sockopt::set_socket_nosigpipe(&stream, true)?;
        Ok(Self { stream })
    }

    /// The same socket, for another thread to read or write.
    pub(in crate::cache) fn try_clone(&self) -> io::Result<Self> {
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
    pub(super) fn recheck() -> Timespec {
        Timespec {
            tv_sec: Self::RECHECK.as_secs().try_into().expect("a second fits"),
            tv_nsec: Self::RECHECK.subsec_nanos().into(),
        }
    }

    /// How long a receive waits for bytes before it fails; forever if
    /// `None`.
    pub(in crate::cache) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Shuts the socket both ways: its peer sees it closed, and so does
    /// every thread of this side's, a receive waiting at once.
    pub(in crate::cache) fn shut_down(&self) {
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
/// that has come, up to [`Self::READ_AHEAD`] bytes, or the rest of a frame
/// longer, and the frames it holds go out one at a time, with no read
/// more, so that a batch of frames costs one syscall
/// (`FrameSocket::send_all`). A frame goes out as the read's own bytes, not
/// a copy of them.
pub(in crate::cache) struct FrameReceiver {
    socket: FrameSocket,
    /// The bytes read and not yet handed out.
    read: BytesMut,
}

impl FrameReceiver {
    /// The most bytes a read takes ahead of the frame it needs: many
    /// control frames.
    const READ_AHEAD: usize = 64 << 10;

    /// The bytes of a frame's length, which comes first.
    const LENGTH: usize = size_of::<u64>();

    pub(in crate::cache) fn new(socket: FrameSocket) -> Self {
        Self {
            socket,
            read: BytesMut::with_capacity(Self::READ_AHEAD),
        }
    }

    /// The body of the next frame, `most` bytes at most, once it comes;
    /// `None` if the peer closed the socket between frames.
    ///
    /// # Errors
    ///
    /// If the socket fails, its peer closes it within a frame, or the frame
    /// is longer than `most`.
    pub(in crate::cache) fn receive(&mut self, most: u64) -> io::Result<Option<Bytes>> {
        loop {
            if let Some(body) = self.next_frame(most)? {
                return Ok(Some(body));
            }
            if !self.fill()? {
                return match self.read.is_empty() {
                    true => Ok(None),
                    false => Err(io::ErrorKind::UnexpectedEof.into()),
                };
            }
        }
    }

    /// The body of the next frame, `most` bytes at most, if it has come
    /// whole; no read of the socket. One that has come in part makes room
    /// for the rest.
    ///
    /// # Errors
    ///
    /// If the frame is longer than `most`.
    pub(super) fn next_frame(&mut self, most: u64) -> io::Result<Option<Bytes>> {
        let Some((length, body)) = self.read.split_first_chunk::<{ Self::LENGTH }>() else {
            return Ok(None);
        };
        let length = u64::from_le_bytes(*length);
        if length > most {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a frame of {length} bytes, past the {most} taken"),
            ));
        }
        let length = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidData)?;
        if body.len() < length {
            self.read.reserve(Self::LENGTH + length - self.read.len());
            return Ok(None);
        }
        self.read.advance(Self::LENGTH);
        Ok(Some(self.read.split_to(length).freeze()))
    }

    /// Reads what has come, waiting only if nothing has: one read of the
    /// socket, which never waits for the rest of a frame. `false` if the
    /// peer has closed the socket.
    ///
    /// # Errors
    ///
    /// If the socket fails.
    pub(super) fn fill(&mut self) -> io::Result<bool> {
        // Room for a read ahead, past the rest of a frame half come; the
        // buffer is taken back once the frames handed out from it drop.
        self.read.reserve(Self::READ_AHEAD);
        loop {
            match rustix::io::read(&self.socket, self.read.spare_capacity_mut()) {
                Ok(([], _)) => return Ok(false),
                Ok((read, _)) => {
                    let read = read.len();
                    // SAFETY: the read initialized the first `read` bytes of
                    // the spare capacity.
                    unsafe { self.read.set_len(self.read.len() + read) };
                    return Ok(true);
                }
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Shuts the socket both ways, as [`FrameSocket::shut_down`] does.
    pub(in crate::cache) fn shut_down(&self) {
        self.socket.shut_down();
    }
}

/// Sends frames on a [`FrameSocket`] for many callers, none of which waits
/// on it, or on another: a caller writes its frames itself while the
/// socket has room for them and no other is writing, which saves waking a
/// thread; what doesn't fit, and every frame sent meanwhile or after, till
/// they have gone, waits for the sender's writer thread, so that frames go
/// in the order sent. Dropped, it shuts the socket down and stops its
/// writer: frames still waiting go nowhere.
pub(in crate::cache) struct FrameSender {
    queue: std::sync::Arc<SenderQueue>,
    writer: Option<JoinHandle<()>>,
}

impl FrameSender {
    /// On macOS, the most a caller's send waits for room on the socket
    /// before it leaves its frames to the writer: a frame takes a few µs to
    /// send; an owner that doesn't read for this long is busy, or stuck.
    pub(in crate::cache) const SEND_WAIT: Duration = Duration::from_micros(200);

    /// A sender on `socket`, its writer thread started. On macOS, a send on
    /// any clone of the socket waits `wait` at most for room after; on
    /// Linux, a caller's send takes what fits at once, and the writer's
    /// waits as long as it takes.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the thread can't be started.
    pub(in crate::cache) fn start(socket: FrameSocket, wait: Duration) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        socket.set_write_timeout(Some(wait))?;
        #[cfg(not(target_vendor = "apple"))]
        let _ = wait;
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
    /// write if the socket has room and no other caller or the writer is
    /// writing; drops them once the peer has gone.
    pub(in crate::cache) fn send(&self, frames: Vec<Vec<u8>>) {
        let mut frames = VecDeque::from(frames);
        {
            let mut state = self.queue.state();
            if state.closed {
                return;
            }
            if !state.frames.is_empty() || state.writing {
                state.frames.extend(frames);
                self.queue.waiting.notify_one();
                return;
            }
            // The socket is this caller's till it has sent: the others
            // queue behind it meanwhile, unheld.
            state.writing = true;
        }
        let sent = self.queue.socket.send_some(frames.make_contiguous());
        let mut state = self.queue.state();
        state.writing = false;
        match sent {
            Ok(mut sent) => {
                while let Some(first) = frames.front_mut() {
                    if sent < first.len() {
                        first.drain(..sent);
                        break;
                    }
                    sent -= first.len();
                    frames.pop_front();
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
        // What didn't fit goes first, before what others queued meanwhile.
        for frame in frames.into_iter().rev() {
            state.frames.push_front(frame);
        }
        if !state.frames.is_empty() {
            self.queue.waiting.notify_one();
        }
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
    /// Whether a caller, or the writer, is sending: every other's frames
    /// wait behind its.
    writing: bool,
    /// Whether the peer has gone, or the sender stopped: frames are
    /// dropped.
    closed: bool,
}

impl SenderQueue {
    /// The writer's work: sends the frames that wait, all that wait in one
    /// write, until the peer goes or the sender stops.
    fn write_waiting(&self) {
        let mut state = self.state();
        loop {
            while !state.closed && (state.frames.is_empty() || state.writing) {
                state = self
                    .waiting
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            if state.closed {
                return;
            }
            state.writing = true;
            let frames: Vec<Vec<u8>> = state.frames.drain(..).collect();
            drop(state);
            let sent = self.socket.send_all(&frames);
            state = self.state();
            state.writing = false;
            if sent.is_err() {
                // The peer has gone: the reader sees it too.
                state.closed = true;
                self.socket.shut_down();
                return;
            }
        }
    }

    fn state(&self) -> MutexGuard<'_, SenderState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
