//! [`OwnerListener`]: an owner serving its cache to its clients
//! (`doc/11-shared-mode.md`, decisions 1 and 5): a thread that accepts
//! their connections, and a thread for each connection, its session.

use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread::{self, JoinHandle};

use super::address::{FileIdentity, OwnerAddress};
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, FrameReader, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use crate::cache::object_cache::ObjectCache;
use crate::io::error::Result;
use crate::io::sync::Arc;

/// An owner listening for its clients, as long as it lives. Dropped, it
/// stops: it takes no more connections, closes every session's, and waits
/// for their threads, which hold the cache, to end; so the cache files'
/// lock goes with the owner's last handle on its cache.
pub(in crate::cache) struct OwnerListener {
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
    pub(in crate::cache) fn listen(
        cache: Arc<ObjectCache>,
        first_file: FileIdentity,
    ) -> Result<Self> {
        let address = OwnerAddress::of(first_file);
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
        let _ = self.address.connect();
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
        let sessions =
            std::mem::take(&mut *self.sessions.lock().unwrap_or_else(PoisonError::into_inner));
        for session in &sessions {
            session.socket.shut_down();
        }
        for session in sessions {
            let _ = session.thread.join();
        }
        self.address.remove();
    }
}

/// A client's session, as its listener holds it: its socket, which the
/// listener shuts as it stops, and the thread serving it.
struct Session {
    socket: FrameSocket,
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
            let Ok(shut) = socket.try_clone() else {
                continue;
            };
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
/// then the client's frames, until either side closes it.
struct ClientSession {
    socket: FrameSocket,
    #[allow(dead_code)] // Serves the client's reads: the next commit.
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

    /// Admits the client, then takes its frames until it closes the
    /// connection.
    ///
    /// # Errors
    ///
    /// If the socket fails, the client isn't admitted, or sends a frame
    /// the owner doesn't take.
    fn serve(&self) -> Result<()> {
        self.socket.set_read_timeout(Some(Hello::WAIT))?;
        Hello::receive(&self.socket)?.admit(self.first_file)?;
        let welcome = OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
        };
        self.socket.send(&welcome.encode())?;
        self.socket.set_read_timeout(None)?;
        if let Some(frame) = self.socket.receive(FrameSocket::MOST_BYTES)? {
            // No read is served yet: any frame is one the owner doesn't
            // take.
            ClientFrame::decode(frame)?;
            return Err(FrameReader::malformed("frame: no read is served yet"));
        }
        Ok(())
    }
}
