//! [`OwnerListener`]: an owner serving its cache to its clients
//! (`doc/11-shared-mode.md`, decisions 1 and 5): a thread that accepts
//! their connections, and a thread for each connection, its session.

use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Wake, Waker};
use std::thread::{self, JoinHandle, Thread};

use crossbeam_channel::{Receiver, TryRecvError};

use super::address::{FileIdentity, OwnerAddress};
use super::client_reads::ClientReads;
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use super::region::OwnerRegion;
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
/// then the client's reads, until either side closes it. A thread of the
/// session's takes the client's frames as they come; the session's own
/// runs the reads, each a task ([`ClientReads`]), and sends their answers.
struct ClientSession {
    socket: FrameSocket,
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
        hello.admit(self.first_file)?;
        // A region refused, the client's reads' bytes go in the frames.
        let region = hello
            .into_region()
            .and_then(|memory| OwnerRegion::accept(memory).ok());
        let welcome = OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
            region: region.is_some(),
        };
        self.socket.send(&welcome.encode())?;
        self.socket.set_read_timeout(None)?;
        let (frames, reader) = self.start_reader()?;
        let served = self.serve_reads(&frames);
        // The reader ends at the close, if the client hasn't closed first.
        self.socket.shut_down();
        let _ = reader.join();
        served
    }

    /// Starts the thread that takes the client's frames as they come, each
    /// decoded, and hands them to this one, which it wakes; the channel
    /// closes once the client closes the connection, or a frame breaks.
    fn start_reader(&self) -> Result<(Receiver<Result<ClientFrame>>, JoinHandle<()>)> {
        let (handing, frames) = crossbeam_channel::unbounded();
        let reading = self.socket.try_clone()?;
        let session = thread::current();
        let reader = thread::Builder::new()
            .name("t4-session-reader".into())
            .spawn(move || {
                loop {
                    let frame = match reading.receive(FrameSocket::MOST_BYTES) {
                        Ok(Some(body)) => ClientFrame::decode(body),
                        Ok(None) => break,
                        Err(error) => Err(error.into()),
                    };
                    let broken = frame.is_err();
                    if handing.send(frame).is_err() || broken {
                        break;
                    }
                    session.unpark();
                }
                drop(handing);
                session.unpark();
            })?;
        Ok((frames, reader))
    }

    /// Serves the client's reads, `frames` as they come: runs each read as
    /// far as it goes, sends what the reads answer, and sleeps until a
    /// frame comes or a read can go on. Every read is dropped as it ends,
    /// with all it holds.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the client sends a frame the protocol
    /// doesn't allow where it comes.
    fn serve_reads(&self, frames: &Receiver<Result<ClientFrame>>) -> Result<()> {
        let mut reads = ClientReads::new(&self.cache);
        let waker = Waker::from(std::sync::Arc::new(SessionWaker(thread::current())));
        let mut context = Context::from_waker(&waker);
        loop {
            loop {
                match frames.try_recv() {
                    Ok(frame) => reads.take(frame?)?,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }
            reads.run(&mut context);
            for frame in reads.take_outgoing()? {
                self.socket.send(&frame.encode())?;
            }
            // Returns at once if a frame came, or a read was woken, since
            // the last park.
            thread::park();
        }
    }
}

/// Wakes a session's thread, parked until a read can go on.
struct SessionWaker(Thread);

impl Wake for SessionWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }
}
