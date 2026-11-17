//! Bounded in-process byte streams. Only the transport changes: frames, sender,
//! client connection and owner sessions are the production implementations.
use std::collections::VecDeque;
use std::io::{self, Read};
use std::sync::Arc;
use std::time::Duration;

use super::super::region::RegionDescriptor;
use crate::io::file_system::{FileIdentity, OpenFile};
use crate::io::sync::{Condvar, Mutex, mpsc, thread};

struct ListenerRegistration {
    file: FileIdentity,
    connections: mpsc::Sender<FrameSocket>,
}

#[derive(Clone, Default)]
pub(crate) struct Transport {
    clock: crate::io::time::Clock,
    listeners: Arc<Mutex<Vec<ListenerRegistration>>>,
    loses_wakeups: bool,
}
impl Transport {
    pub(crate) fn clock(&self) -> &crate::io::time::Clock {
        &self.clock
    }

    /// A shutdown wakes no thread already waiting to read its own end, as macOS may lose that
    /// wakeup (`doc/11-shared-mode.md`, decision 7): such a thread wakes as its peer closes, or
    /// as it looks again once `FrameSocket::RECHECK` is up.
    pub(crate) fn losing_shutdown_wakeups(mut self) -> Self {
        self.loses_wakeups = true;
        self
    }

    pub(in crate::cache::cache_stores::shared) fn listen(
        &self,
        file: FileIdentity,
    ) -> io::Result<Listener> {
        let mut listeners = self.listeners.lock().unwrap();
        if listeners.iter().any(|listener| listener.file == file) {
            return Err(io::ErrorKind::AddrInUse.into());
        }
        let (send, receive) = mpsc::channel();
        listeners.push(ListenerRegistration {
            file,
            connections: send,
        });
        Ok(Listener(receive))
    }
    pub(in crate::cache::cache_stores::shared) fn connect(
        &self,
        file: FileIdentity,
    ) -> io::Result<FrameSocket> {
        let listeners = self.listeners.lock().unwrap();
        let listener = listeners
            .iter()
            .find(|listener| listener.file == file)
            .ok_or(io::ErrorKind::ConnectionRefused)?;
        let (client, owner) = FrameSocket::pair_on(self.clock.clone(), self.loses_wakeups)?;
        listener
            .connections
            .send(owner)
            .map_err(|_| io::ErrorKind::ConnectionRefused)?;
        Ok(client)
    }
    pub(in crate::cache::cache_stores::shared) fn remove(&self, file: FileIdentity) {
        self.listeners
            .lock()
            .unwrap()
            .retain(|listener| listener.file != file);
    }
}
pub(in crate::cache::cache_stores::shared) struct Listener(mpsc::Receiver<FrameSocket>);
impl Listener {
    #[cfg(test)]
    pub(in crate::cache::cache_stores::shared) fn accept(&self) -> io::Result<(FrameSocket, ())> {
        self.0
            .recv()
            .map(|socket| (socket, ()))
            .map_err(|_| io::ErrorKind::ConnectionAborted.into())
    }

    pub(in crate::cache::cache_stores::shared) fn incoming(
        &self,
    ) -> impl Iterator<Item = io::Result<FrameSocket>> + '_ {
        std::iter::from_fn(|| self.0.recv().ok().map(Ok))
    }
}

#[derive(Default)]
struct Pipe {
    state: Mutex<PipeState>,
    changed: Condvar,
}
#[derive(Default)]
struct PipeState {
    bytes: VecDeque<u8>,
    closed: bool,
    /// Threads asleep on the pipe whose wakeup a shutdown lost, and when: one sees the shutdown
    /// once a notify reaches it, or once its wait times out.
    unnoticed: Vec<(thread::ThreadId, Duration)>,
    session: Option<thread::Thread>,
    readers: Vec<thread::Thread>,
    descriptors: Option<(OpenFile, Option<RegionDescriptor>)>,
}
impl Pipe {
    fn notify(&self, state: &mut PipeState) {
        state.unnoticed.clear();
        self.changed.notify_all();
        for reader in &state.readers {
            reader.unpark();
        }
        if let Some(session) = &state.session {
            session.unpark();
        }
    }

    /// Wakes the peer's writers waiting for room, not this end's readers.
    fn notify_writers(&self) {
        self.changed.notify_all();
    }
}

#[derive(Clone)]
pub(in crate::cache::cache_stores::shared) struct FrameSocket {
    incoming: Arc<Pipe>,
    outgoing: Arc<Pipe>,
    life: Arc<()>,
    clock: crate::io::time::Clock,
    timeout: Arc<Mutex<Option<Duration>>>,
    loses_wakeups: bool,
}
impl FrameSocket {
    pub(super) fn read_into(&self, buffer: &mut bytes::BytesMut) -> io::Result<usize> {
        let mut bytes = vec![0; (buffer.capacity() - buffer.len()).min(Self::CAPACITY)];
        let read = (&*self).read(&mut bytes)?;
        buffer.extend_from_slice(&bytes[..read]);
        Ok(read)
    }

    /// Simulated time stands still while a thread spins: a read here sleeps at once.
    pub(super) fn spin_until_readable(&self) -> io::Result<()> {
        Ok(())
    }

    pub(in crate::cache::cache_stores::shared) const MOST_BYTES: u64 = 1 << 31;
    pub(in crate::cache::cache_stores::shared) const RECHECK: Duration = Duration::from_secs(1);
    const CAPACITY: usize = 64 << 10;

    pub(in crate::cache::cache_stores::shared) fn new(socket: Self) -> io::Result<Self> {
        Ok(socket)
    }
    #[cfg(test)]
    pub(in crate::cache::cache_stores::shared) fn pair() -> io::Result<(Self, Self)> {
        Self::pair_on(Default::default(), false)
    }
    fn pair_on(clock: crate::io::time::Clock, loses_wakeups: bool) -> io::Result<(Self, Self)> {
        let (a, b) = (Arc::new(Pipe::default()), Arc::new(Pipe::default()));
        Ok((
            Self {
                incoming: a.clone(),
                outgoing: b.clone(),
                life: Arc::new(()),
                clock: clock.clone(),
                timeout: Arc::new(Mutex::new(None)),
                loses_wakeups,
            },
            Self {
                incoming: b,
                outgoing: a,
                life: Arc::new(()),
                clock: clock.clone(),
                timeout: Arc::new(Mutex::new(None)),
                loses_wakeups,
            },
        ))
    }
    pub(in crate::cache::cache_stores::shared) fn try_clone(&self) -> io::Result<Self> {
        Ok(self.clone())
    }
    pub(in crate::cache::cache_stores::shared) fn send(&self, frame: &[u8]) -> io::Result<()> {
        self.send_all(&[frame])
    }
    pub(in crate::cache::cache_stores::shared) fn send_all(
        &self,
        frames: &[impl AsRef<[u8]>],
    ) -> io::Result<()> {
        for frame in frames {
            let mut bytes = frame.as_ref();
            while !bytes.is_empty() {
                match self.send_some(&[bytes]) {
                    Ok(sent) => bytes = &bytes[sent..],
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        let mut state = self.outgoing.state.lock().unwrap();
                        while state.bytes.len() == Self::CAPACITY && !state.closed {
                            state = self.outgoing.changed.wait(state).unwrap();
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
    pub(super) fn send_some(&self, frames: &[impl AsRef<[u8]>]) -> io::Result<usize> {
        let mut state = self.outgoing.state.lock().unwrap();
        if state.closed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let mut room = Self::CAPACITY - state.bytes.len();
        if room == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let before = room;
        for frame in frames {
            let bytes = frame.as_ref();
            let sent = room.min(bytes.len());
            state.bytes.extend(&bytes[..sent]);
            room -= sent;
            if room == 0 {
                break;
            }
        }
        self.outgoing.notify(&mut state);
        Ok(before - room)
    }
    #[cfg(target_vendor = "apple")]
    pub(super) fn set_write_timeout(&self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
    pub(in crate::cache::cache_stores::shared) fn set_read_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        *self.timeout.lock().unwrap() = timeout;
        Ok(())
    }
    pub(in crate::cache::cache_stores::shared) fn shut_down(&self) {
        for pipe in [&self.incoming, &self.outgoing] {
            let mut state = pipe.state.lock().unwrap();
            state.closed = true;
            if self.loses_wakeups && Arc::ptr_eq(pipe, &self.incoming) {
                let now = self.clock.now();
                let asleep = state.readers.iter().chain(&state.session);
                state.unnoticed = asleep.map(|thread| (thread.id(), now)).collect();
                pipe.notify_writers();
            } else {
                pipe.notify(&mut state);
            }
        }
        // On another core, the peer woken runs at once: before the shutter's next step.
        thread::yield_now();
    }
    pub(in crate::cache::cache_stores::shared) fn ready(&self) -> bool {
        let mut state = self.incoming.state.lock().unwrap();
        state.session = Some(thread::current());
        !state.bytes.is_empty() || self.closed_for_me(&state, Some(Self::RECHECK))
    }

    /// Shuttle wakes a parked thread spuriously, which would show it a shutdown whose wakeup
    /// was lost: such a thread sees it only once noticed, or once a wait of `timeout` is up.
    fn closed_for_me(&self, state: &PipeState, timeout: Option<Duration>) -> bool {
        let me = thread::current().id();
        let now = self.clock.now();
        let waiting = |since: Duration| timeout.is_none_or(|timeout| now < since + timeout);
        state.closed
            && !(state.unnoticed.iter()).any(|&(thread, since)| thread == me && waiting(since))
    }

    /// The thread wakes at the clock's next advance, to look again, as a `poll` that waits
    /// `RECHECK` at most does.
    pub(in crate::cache::cache_stores::shared) fn recheck_later(&self) {
        self.clock.register_timer();
    }
    pub(in crate::cache::cache_stores::shared) fn send_descriptors(
        &self,
        file: OpenFile,
        region: Option<RegionDescriptor>,
    ) {
        self.outgoing.state.lock().unwrap().descriptors = Some((file, region));
    }
    pub(in crate::cache::cache_stores::shared) fn take_descriptors(
        &self,
    ) -> Option<(OpenFile, Option<RegionDescriptor>)> {
        self.incoming.state.lock().unwrap().descriptors.take()
    }
}
impl Read for &FrameSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let timeout = *self.timeout.lock().unwrap();
        let deadline = timeout.map(|timeout| self.clock.now() + timeout);
        let mut state = self.incoming.state.lock().unwrap();
        while state.bytes.is_empty() && !self.closed_for_me(&state, timeout) {
            if let Some(deadline) = deadline {
                self.clock.register_timer();
                if self.clock.now() >= deadline {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
            }
            let current = thread::current();
            if !state
                .readers
                .iter()
                .any(|reader| reader.id() == current.id())
            {
                state.readers.push(current);
            }
            drop(state);
            thread::park();
            state = self.incoming.state.lock().unwrap();
        }
        let read = bytes.len().min(state.bytes.len());
        for byte in &mut bytes[..read] {
            *byte = state.bytes.pop_front().unwrap();
        }
        self.incoming.notify(&mut state);
        Ok(read)
    }
}
impl Drop for FrameSocket {
    fn drop(&mut self) {
        if !std::thread::panicking() && Arc::strong_count(&self.life) == 1 {
            self.shut_down();
        }
    }
}

impl io::Write for FrameSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.send(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
