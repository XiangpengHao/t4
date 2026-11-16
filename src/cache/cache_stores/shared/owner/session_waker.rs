use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;
use std::thread::{self, ThreadId};

use rustix::event::{PollFd, PollFlags};

use crate::cache::cache_stores::shared::protocol::FrameSocket;

pub(super) struct SessionWaker {
    session: ThreadId,
    rung: AtomicBool,
    ring: UnixStream,
    bell: UnixStream,
}

impl SessionWaker {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(super) fn has_pending_wake(&self) -> bool {
        self.rung.load(Ordering::Acquire)
    }

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

    pub(super) fn wait_for(&self, socket: &FrameSocket) -> io::Result<bool> {
        let mut ready = [
            PollFd::new(socket, PollFlags::IN),
            PollFd::new(&self.bell, PollFlags::IN),
        ];
        let recheck = FrameSocket::recheck();
        loop {
            match rustix::event::poll(&mut ready, Some(&recheck)) {
                Err(rustix::io::Errno::INTR) => {}
                waited => break waited.map(drop).map_err(io::Error::from)?,
            }
        }
        let [socket, bell] = ready.map(|ready| !ready.revents().is_empty());
        if bell {
            // A bell drained whenever it rang never holds a byte when it hasn't: no spin.
            let mut rung = [0; 64];
            while matches!((&self.bell).read(&mut rung), Ok(read) if read > 0) {}
        }
        // Wakes from here on ring again; those before are the session's to run next.
        self.rung.store(false, Ordering::Release);
        Ok(socket)
    }
}

impl SessionWaker {
    /// Its own thread sends what is queued before it waits, and would only ring its own bell.
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
