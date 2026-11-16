//! Frame parsing and ordered sending, shared by Unix and modeled sockets.
use crate::io::sync::{
    Condvar, Mutex, MutexGuard,
    thread::{self, JoinHandle},
};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

#[cfg(all(test, feature = "shuttle"))]
pub(super) mod simulated;
#[cfg(not(all(test, feature = "shuttle")))]
mod unix;
#[cfg(all(test, feature = "shuttle"))]
pub(in crate::cache::cache_stores::shared) use simulated::FrameSocket;
#[cfg(all(test, feature = "shuttle"))]
pub(crate) use simulated::Transport;
#[cfg(not(all(test, feature = "shuttle")))]
pub(in crate::cache::cache_stores::shared) use unix::FrameSocket;

/// Takes the frames a [`FrameSocket`]'s peer sends: each read takes all
/// that has come, up to [`Self::READ_AHEAD`] bytes, or the rest of a frame
/// longer, and the frames it holds go out one at a time, with no read
/// more, so that a batch of frames costs one syscall
/// (`FrameSocket::send_all`). A frame goes out as the read's own bytes, not
/// a copy of them.
pub(in crate::cache::cache_stores::shared) struct FrameReceiver {
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

    pub(in crate::cache::cache_stores::shared) fn new(socket: FrameSocket) -> Self {
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
    pub(in crate::cache::cache_stores::shared) fn receive(
        &mut self,
        most: u64,
    ) -> io::Result<Option<Bytes>> {
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
    pub(in crate::cache::cache_stores::shared) fn next_frame(
        &mut self,
        most: u64,
    ) -> io::Result<Option<Bytes>> {
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
    pub(in crate::cache::cache_stores::shared) fn fill(&mut self) -> io::Result<bool> {
        // Room for a read ahead, past the rest of a frame half come; the
        // buffer is taken back once the frames handed out from it drop.
        self.read.reserve(Self::READ_AHEAD);
        loop {
            match self.socket.read_into(&mut self.read) {
                Ok(read) => return Ok(read != 0),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Shuts the socket both ways, as [`FrameSocket::shut_down`] does.
    pub(in crate::cache::cache_stores::shared) fn shut_down(&self) {
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
pub(in crate::cache::cache_stores::shared) struct FrameSender {
    queue: Arc<SenderQueue>,
    writer: Option<JoinHandle<()>>,
}

impl FrameSender {
    /// On macOS, the most a caller's send waits for room on the socket
    /// before it leaves its frames to the writer: a frame takes a few µs to
    /// send; an owner that doesn't read for this long is busy, or stuck.
    pub(in crate::cache::cache_stores::shared) const SEND_WAIT: Duration =
        Duration::from_micros(200);

    /// A sender on `socket`, its writer thread started. On macOS, a send on
    /// any clone of the socket waits `wait` at most for room after; on
    /// Linux, a caller's send takes what fits at once, and the writer's
    /// waits as long as it takes.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the thread can't be started.
    pub(in crate::cache::cache_stores::shared) fn start(
        socket: FrameSocket,
        wait: Duration,
    ) -> io::Result<Self> {
        #[cfg(target_vendor = "apple")]
        socket.set_write_timeout(Some(wait))?;
        #[cfg(not(target_vendor = "apple"))]
        let _ = wait;
        let queue = Arc::new(SenderQueue {
            socket,
            state: Mutex::default(),
            waiting: Condvar::new(),
        });
        let writing = Arc::clone(&queue);
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
    pub(in crate::cache::cache_stores::shared) fn send(&self, frames: Vec<Vec<u8>>) {
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
