//! [`OwnerListener`]: an owner serving its cache to its clients
//! (`doc/11-shared-mode.md`, decisions 1 and 5): a thread that accepts
//! their connections, and a thread for each connection, its session.
//!
//! - `client_reads`: a client's reads, as its owner serves them.
//! - `session_waker`: what wakes a session's thread, waiting on its
//!   client's socket and its reads at once.

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

/// An owner listening for its clients, as long as it lives. Dropped, it
/// stops: it takes no more connections, closes every session's, and waits
/// for their threads, which hold the cache, to end; so the cache files'
/// lock goes with the owner's last handle on its cache.
pub(super) struct OwnerListener {
    address: OwnerAddress,
    /// Set once the listener is dropped: the accept thread stops at the
    /// next connection, which the drop makes.
    stopping: std::sync::Arc<AtomicBool>,
    sessions: std::sync::Arc<Mutex<Vec<Session>>>,
    accepting: Option<JoinHandle<()>>,
}

impl OwnerListener {
    /// Listens for the clients of `cache`, whose first cache file is
    /// `first_file`, at its address.
    ///
    /// # Errors
    ///
    /// If the address can't be bound, or the accept thread started.
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

/// A client's session, as its listener holds it: its socket, which the
/// listener shuts as it stops, if the session still has it; and the thread
/// serving it. The listener's hold on the socket doesn't keep it open: a
/// session that ends closes its connection, whatever its shutdown did
/// (`FrameSocket::RECHECK`).
struct Session {
    socket: std::sync::Weak<FrameSocket>,
    thread: JoinHandle<()>,
}

/// The listener's thread: it takes each connection, and starts a session
/// for it, until the listener stops.
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
            // A connection that fails before its session starts is gone:
            // its client reads from the remote, or tries again.
            let Ok(socket) = stream.and_then(FrameSocket::new) else {
                continue;
            };
            let socket = std::sync::Arc::new(socket);
            let shut = std::sync::Arc::downgrade(&socket);
            let session = ClientSession {
                socket,
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

/// One client's connection, served on a thread of its own: the handshake,
/// then the client's reads, until either side closes it. A thread of the
/// session's takes the client's frames as they come; the session's own
/// runs the reads, each a task ([`ClientReads`]), and sends their answers.
struct ClientSession {
    socket: std::sync::Arc<FrameSocket>,
    cache: Arc<ObjectCache>,
    first_file: FileIdentity,
}

impl ClientSession {
    fn run(self) {
        // Whatever ends the session closes its connection, which the
        // listener's handle on the socket would keep open: its client's
        // reads go to the remote (decision 7).
        let _ = self.serve();
        self.socket.shut_down();
    }

    /// Admits the client, maps its region if it sent one that can't
    /// shrink, then serves its reads until it closes the connection.
    ///
    /// # Errors
    ///
    /// If the socket fails, the client isn't admitted, or sends a frame
    /// the protocol doesn't allow where it comes.
    fn serve(&self) -> Result<()> {
        self.socket.set_read_timeout(Some(Hello::WAIT))?;
        let hello = Hello::receive(&self.socket)?;
        if hello.version() != PROTOCOL_VERSION {
            // Told this version, a client of another gives up at once,
            // rather than try an owner that won't admit it till its wait
            // runs out (decision 6).
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

    /// Serves the client's reads, taking their frames from `frames` as they
    /// come, their outputs in spans of `region` if the client named them:
    /// runs each read as far as it goes, sends what the reads answer, and
    /// sleeps until a frame comes or a read can go on. The thread takes the
    /// frames itself, as it would wait to be woken by a thread that did
    /// (`SessionWaker`). Every read is dropped as it ends, with all it
    /// holds.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the client sends a frame the protocol
    /// doesn't allow where it comes.
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
                // What has come, and every frame it completes: a frame half
                // come waits for its rest, not the session.
                if !frames.fill()? {
                    return Ok(());
                }
                while let Some(body) = frames.next_frame(FrameSocket::MOST_BYTES)? {
                    reads.take(ClientFrame::decode(body)?)?;
                }
            }
            reads.run(&mut context);
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
