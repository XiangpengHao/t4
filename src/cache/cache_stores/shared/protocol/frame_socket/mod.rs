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

#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(super) mod simulated;
#[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
mod unix;
#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(in crate::cache::cache_stores::shared) use simulated::FrameSocket;
#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(crate) use simulated::Transport;
#[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
pub(in crate::cache::cache_stores::shared) use unix::FrameSocket;

pub(in crate::cache::cache_stores::shared) struct FrameReceiver {
    socket: FrameSocket,
    read: BytesMut,
}

impl FrameReceiver {
    const READ_AHEAD: usize = 64 << 10;

    const LENGTH: usize = size_of::<u64>();

    pub(in crate::cache::cache_stores::shared) fn new(socket: FrameSocket) -> Self {
        Self {
            socket,
            read: BytesMut::with_capacity(Self::READ_AHEAD),
        }
    }

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

    pub(in crate::cache::cache_stores::shared) fn fill(&mut self) -> io::Result<bool> {
        // The buffer is taken back once the frames handed out from it drop.
        self.read.reserve(Self::READ_AHEAD);
        loop {
            match self.socket.read_into(&mut self.read) {
                Ok(read) => return Ok(read != 0),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    pub(in crate::cache::cache_stores::shared) fn shut_down(&self) {
        self.socket.shut_down();
    }
}

/// A caller writes its frames itself while the socket has room and no other is writing, which
/// saves waking the writer thread. Dropped, it drops the frames still waiting.
pub(in crate::cache::cache_stores::shared) struct FrameSender {
    queue: Arc<SenderQueue>,
    writer: Option<JoinHandle<()>>,
}

impl FrameSender {
    /// A frame takes a few µs to send: an owner not reading for this long is busy, or stuck.
    pub(in crate::cache::cache_stores::shared) const SEND_WAIT: Duration =
        Duration::from_micros(200);

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

    #[cfg(all(test, feature = "shuttle", target_os = "linux"))]
    pub(in crate::cache::cache_stores::shared) fn has_pending_frames(&self) -> bool {
        let state = self.queue.state();
        state.writing || !state.frames.is_empty()
    }

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
            // The socket is this caller's till it has sent: others queue meanwhile, unheld.
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

struct SenderQueue {
    socket: FrameSocket,
    state: Mutex<SenderState>,
    waiting: Condvar,
}

#[derive(Default)]
struct SenderState {
    frames: VecDeque<Vec<u8>>,
    writing: bool,
    closed: bool,
}

impl SenderQueue {
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
