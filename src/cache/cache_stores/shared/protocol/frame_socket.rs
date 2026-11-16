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

/// A send to a gone peer fails rather than raise `SIGPIPE`, which the process, a user's app,
/// may not ignore.
pub(in crate::cache::cache_stores::shared) struct FrameSocket {
    stream: UnixStream,
}

impl FrameSocket {
    /// More than a read or a piece of a GET's body holds.
    pub(in crate::cache::cache_stores::shared) const MOST_BYTES: u64 = 1 << 31;

    /// macOS asks for no `SIGPIPE` per socket instead (`Self::new`).
    #[cfg(not(target_vendor = "apple"))]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
    #[cfg(target_vendor = "apple")]
    pub(super) const SEND_FLAGS: SendFlags = SendFlags::empty();

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
            match rustix::io::read(&self.socket, self.read.spare_capacity_mut()) {
                Ok(([], _)) => return Ok(false),
                Ok((read, _)) => {
                    let read = read.len();
                    // SAFETY: the read initialized the first `read` bytes of the spare capacity.
                    unsafe { self.read.set_len(self.read.len() + read) };
                    return Ok(true);
                }
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
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
    queue: std::sync::Arc<SenderQueue>,
    writer: Option<JoinHandle<()>>,
}

impl FrameSender {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn pause_next_caller(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, observed) = std::sync::mpsc::channel();
        let (release, resume) = std::sync::mpsc::channel();
        *self.queue.caller_pause.lock().unwrap() = Some(CallerPause { entered, resume });
        (observed, release)
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn has_pending_frames(&self) -> bool {
        let state = self.queue.state();
        state.writing || !state.frames.is_empty()
    }

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
        let queue = std::sync::Arc::new(SenderQueue {
            socket,
            state: Mutex::default(),
            waiting: Condvar::new(),
            #[cfg(all(test, not(feature = "shuttle")))]
            caller_pause: Mutex::default(),
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
        #[cfg(all(test, not(feature = "shuttle")))]
        {
            let pause = self.queue.caller_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.entered.send(()).unwrap();
                pause.resume.recv().unwrap();
            }
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

#[cfg(all(test, not(feature = "shuttle")))]
struct CallerPause {
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

struct SenderQueue {
    socket: FrameSocket,
    state: Mutex<SenderState>,
    waiting: Condvar,
    #[cfg(all(test, not(feature = "shuttle")))]
    caller_pause: Mutex<Option<CallerPause>>,
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
