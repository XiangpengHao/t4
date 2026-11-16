//! Shuttle's park token models the socket bell without losing a wake between checks.
use super::super::protocol::FrameSocket;
use crate::io::sync::{
    atomic::{AtomicBool, Ordering},
    thread::{self, Thread},
};
use std::io;
use std::sync::Arc;
use std::task::Wake;

pub(super) struct SessionWaker {
    session: Thread,
    rung: AtomicBool,
}
impl SessionWaker {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            session: thread::current(),
            rung: AtomicBool::new(false),
        })
    }
    pub(super) fn wait_for(&self, socket: &FrameSocket) -> io::Result<bool> {
        loop {
            let ready = socket.ready();
            if self.rung.swap(false, Ordering::AcqRel) || ready {
                return Ok(ready);
            }
            thread::park();
        }
    }
    pub(super) fn wake_to_send(self: &Arc<Self>) {
        if thread::current().id() != self.session.id() {
            self.wake_by_ref();
        }
    }
}
impl Wake for SessionWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.rung.store(true, Ordering::Release);
        self.session.unpark();
    }
}
