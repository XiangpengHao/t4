use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Waker};
use std::thread::{self, JoinHandle};

use super::protocol::{
    ClientFrame, FrameReceiver, FrameSocket, Hello, OwnerAddress, OwnerFrame, OwnerRegion,
    PROTOCOL_VERSION,
};
use crate::cache::storage::{FileIdentity, ObjectCache};
use crate::io::error::Result;
use crate::io::sync::Arc;

mod client_reads;
mod session_waker;

use client_reads::ClientReads;
use session_waker::SessionWaker;

/// Dropped, it waits for its sessions, which hold the cache, so the lock goes with the owner's
/// last handle on it.
pub(super) struct OwnerListener {
    address: OwnerAddress,
    stopping: std::sync::Arc<AtomicBool>,
    sessions: std::sync::Arc<Mutex<Vec<Session>>>,
    accepting: Option<JoinHandle<()>>,
}

impl OwnerListener {
    pub(super) fn listen(cache: Arc<ObjectCache>, first_file: FileIdentity) -> Result<Self> {
        let address = OwnerAddress::of(first_file)?;
        let acceptor = Acceptor {
            listener: address.listen()?,
            stopping: std::sync::Arc::default(),
            sessions: std::sync::Arc::default(),
            cache,
            first_file,
        };
        let (stopping, sessions) = (
            std::sync::Arc::clone(&acceptor.stopping),
            std::sync::Arc::clone(&acceptor.sessions),
        );
        let accepting = thread::Builder::new()
            .name("t4-owner".into())
            .spawn(move || acceptor.run())?;
        Ok(Self {
            address,
            stopping,
            sessions,
            accepting: Some(accepting),
        })
    }
}

impl Drop for OwnerListener {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        // Wakes the accept thread, which finds itself stopping.
        self.address.wake();
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
        let sessions =
            std::mem::take(&mut *self.sessions.lock().unwrap_or_else(PoisonError::into_inner));
        for session in &sessions {
            if let Some(socket) = session.socket.upgrade() {
                socket.shut_down();
            }
        }
        for session in sessions {
            let _ = session.thread.join();
        }
        self.address.remove();
    }
}

struct Session {
    socket: std::sync::Weak<FrameSocket>,
    thread: JoinHandle<()>,
}

struct Acceptor {
    listener: UnixListener,
    stopping: std::sync::Arc<AtomicBool>,
    sessions: std::sync::Arc<Mutex<Vec<Session>>>,
    cache: Arc<ObjectCache>,
    first_file: FileIdentity,
}

impl Acceptor {
    fn run(self) {
        for stream in self.listener.incoming() {
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            // A connection that fails here is gone: its client reads from the remote, or retries.
            let Ok(socket) = stream.and_then(FrameSocket::new) else {
                continue;
            };
            let socket = std::sync::Arc::new(socket);
            let shut = std::sync::Arc::downgrade(&socket);
            let session = ClientSession {
                socket,
                stopping: std::sync::Arc::clone(&self.stopping),
                cache: Arc::clone(&self.cache),
                first_file: self.first_file,
            };
            let Ok(thread) = thread::Builder::new()
                .name("t4-session".into())
                .spawn(move || session.run())
            else {
                continue;
            };
            let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
            sessions.retain(|session| !session.thread.is_finished());
            sessions.push(Session {
                socket: shut,
                thread,
            });
        }
    }
}

struct ClientSession {
    socket: std::sync::Arc<FrameSocket>,
    stopping: std::sync::Arc<AtomicBool>,
    cache: Arc<ObjectCache>,
    first_file: FileIdentity,
}

impl ClientSession {
    fn run(self) {
        // Shut it however the session ended: the listener's handle would keep it open.
        let _ = self.serve();
        self.socket.shut_down();
    }

    fn serve(&self) -> Result<()> {
        self.socket.set_read_timeout(Some(Hello::WAIT))?;
        let hello = Hello::receive(&self.socket)?;
        if hello.version() != PROTOCOL_VERSION {
            // Told this version, a client of another gives up at once rather than retry till its
            // wait runs out (`doc/11-shared-mode.md`, decision 6).
            let (version, region) = (PROTOCOL_VERSION, false);
            self.socket
                .send(&OwnerFrame::Welcome { version, region }.encode())?;
        }
        hello.admit(self.first_file)?;
        // A region refused, the client's reads' bytes go in the frames.
        let region = hello
            .into_region()
            .and_then(|memory| OwnerRegion::accept(memory).ok())
            .map(std::sync::Arc::new);
        let welcome = OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
            region: region.is_some(),
        };
        self.socket.send(&welcome.encode())?;
        self.socket.set_read_timeout(None)?;
        let mut frames = FrameReceiver::new(self.socket.try_clone()?);
        self.serve_reads(&mut frames, region)
    }

    /// The thread takes the frames itself, as it would wait to be woken by a thread that did.
    fn serve_reads(
        &self,
        frames: &mut FrameReceiver,
        region: Option<std::sync::Arc<OwnerRegion>>,
    ) -> Result<()> {
        let waker = std::sync::Arc::new(SessionWaker::new()?);
        let mut reads = ClientReads::new(&self.cache, region, std::sync::Arc::clone(&waker));
        let task_waker = Waker::from(std::sync::Arc::clone(&waker));
        let mut context = Context::from_waker(&task_waker);
        loop {
            if waker.wait_for(&self.socket)? {
                // A frame half come waits for its rest, not the session.
                if !frames.fill()? {
                    return Ok(());
                }
                while let Some(body) = frames.next_frame(FrameSocket::MOST_BYTES)? {
                    reads.take(ClientFrame::decode(body)?)?;
                }
            }
            reads.run(&mut context);
            // Answered, a going owner's clients would fetch blocks it can't take, then refetch;
            // the flag is set before any socket shuts, so a woken session sees it (decision 7).
            if self.stopping.load(Ordering::Acquire) {
                return Ok(());
            }
            let outgoing: Vec<Vec<u8>> = reads
                .take_outgoing()?
                .iter()
                .map(OwnerFrame::encode)
                .collect();
            self.socket.send_all(&outgoing)?;
        }
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
