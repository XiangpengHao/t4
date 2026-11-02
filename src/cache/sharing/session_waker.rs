//! [`SessionWaker`]: what wakes a session's thread, which waits on its
//! client's socket and on its reads at once (`doc/11-shared-mode.md`,
//! Cost): the thread takes its client's frames itself, with no thread
//! between to wake it.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;
use std::thread::{self, ThreadId};

use rustix::event::{PollFd, PollFlags};

use super::frame_socket::FrameSocket;

/// Wakes a session's thread, waiting on its client's socket, when a read
/// can go on or a frame waits to go: rings a bell, a byte on a socket of
/// its own, which the thread waits on beside its client's. Wakes between
/// two waits ring it once.
pub(super) struct SessionWaker {
    /// The session's thread, which made the waker.
    session: ThreadId,
    /// Whether the bell rang since the last wait.
    rung: AtomicBool,
    /// The bell's ends: wakes write to `ring`; the thread waits on `bell`,
    /// which never blocks a read.
    ring: UnixStream,
    bell: UnixStream,
}

impl SessionWaker {
    /// The waker of the session whose thread this is.
    ///
    /// # Errors
    ///
    /// If the bell's sockets can't be made.
    pub(super) fn new() -> io::Result<Self> {
        let (ring, bell) = UnixStream::pair()?;
        ring.set_nonblocking(true)?;
        bell.set_nonblocking(true)?;
        Ok(Self {
            session: thread::current().id(),
            rung: AtomicBool::new(false),
            ring,
            bell,
        })
    }

    /// Waits until `socket` has bytes to take, or its peer has closed it,
    /// or the session is woken, whichever comes first; returns at once if
    /// it was woken since the last wait. Whether the socket is ready.
    ///
    /// # Errors
    ///
    /// If waiting fails.
    pub(super) fn wait_for(&self, socket: &FrameSocket) -> io::Result<bool> {
        let mut ready = [
            PollFd::new(socket, PollFlags::IN),
            PollFd::new(&self.bell, PollFlags::IN),
        ];
        loop {
            match rustix::event::poll(&mut ready, None) {
                Err(rustix::io::Errno::INTR) => {}
                waited => break waited.map(drop).map_err(io::Error::from)?,
            }
        }
        let [socket, bell] = ready.map(|ready| !ready.revents().is_empty());
        if bell {
            // A bell drained whenever it rang never holds a byte when it
            // hasn't: its wait never spins.
            let mut rung = [0; 64];
            while matches!((&self.bell).read(&mut rung), Ok(read) if read > 0) {}
        }
        // Wakes from here on ring again; those before are the session's to
        // run, as it does next.
        self.rung.store(false, Ordering::Release);
        Ok(socket)
    }
}

impl SessionWaker {
    /// Wakes the session to send a frame queued for its client: from
    /// another thread; its own sends what is queued before it waits, and
    /// would only ring its own bell.
    pub(super) fn wake_to_send(self: &std::sync::Arc<Self>) {
        if thread::current().id() != self.session {
            self.wake_by_ref();
        }
    }
}

impl Wake for SessionWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        if !self.rung.swap(true, Ordering::AcqRel) {
            // A full bell has a byte to wake the thread already.
            let _ = (&self.ring).write(&[1]);
        }
    }
}
