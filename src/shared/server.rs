//! The store's server: one process that owns the store and serves its
//! clients, until none has been connected for the idle timeout
//! (`doc/02-multi-process.md`, "Starting and stopping the server").

use std::fs::File;
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use futures_executor::ThreadPool;
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::fs::OFlags;

use crate::input_kv::T4KeyRef;
use crate::io::error::{Error, Result};
use crate::shared::address::{ServerAddress, ServerClaim, StoreIdentity};
use crate::shared::launch::{ServerArgs, ServerStatus};
use crate::shared::wire::{
    FdReceiver, FrameReader, Handshake, HandshakeReply, Reply, ReplyFrame, Request,
};
use crate::store::{FileCreation, MountOptions, T4Store};

/// The store's server process.
pub(crate) struct Server {
    // Declared before `store`, so it drops first: the address frees before
    // the mount lock, and a server started meanwhile waits on the lock.
    claim: ServerClaim,
    store: Arc<T4Store>,
    identity: StoreIdentity,
    direct_io: bool,
    idle_timeout: Duration,
    pool: ThreadPool,
}

/// Which side of the fork in [`Server::detach`] this process is.
enum Detached {
    /// The process its starter spawned, which exits at once.
    Starter,
    /// The server, in a session of its own.
    Server,
}

impl Server {
    /// How long a starting server waits for the mount lock: a server that
    /// is shutting down holds it until its store closes, a moment after it
    /// frees the address.
    const MOUNT_LOCK_WAIT: Duration = Duration::from_secs(1);

    /// `t4-server`'s `main`.
    pub(crate) fn main() -> ExitCode {
        let args = match ServerArgs::parse(std::env::args_os().skip(1)) {
            Ok(args) => args,
            Err(reason) => {
                ServerStatus::Failed(reason).report();
                return ExitCode::FAILURE;
            }
        };
        match Self::detach() {
            Ok(Detached::Starter) => return ExitCode::SUCCESS,
            Ok(Detached::Server) => {}
            Err(err) => {
                ServerStatus::Failed(format!("cannot fork: {err}")).report();
                return ExitCode::FAILURE;
            }
        }
        let server = match Self::start(&args) {
            Ok(server) => server,
            Err(status) => {
                status.report();
                return ExitCode::FAILURE;
            }
        };
        ServerStatus::Ready.report();
        // Past its status line, nothing the server prints has a reader.
        let _ = Self::silence_output();
        server.serve();
        ExitCode::SUCCESS
    }

    /// Forks, so the server outlives the client that started it without
    /// lingering as its zombie: the starter process exits at once, and its
    /// client reaps it. The server leads a new session, so the client's
    /// terminal and its Ctrl-C don't reach it.
    fn detach() -> io::Result<Detached> {
        // SAFETY: `main` calls this before starting any thread, so the child
        // is a whole copy of a single-threaded process.
        match unsafe { libc::fork() } {
            -1 => Err(io::Error::last_os_error()),
            0 => {
                rustix::process::setsid()?;
                Ok(Detached::Server)
            }
            _ => Ok(Detached::Starter),
        }
    }

    /// Claims the address, takes the mount lock, and mounts the store.
    fn start(args: &ServerArgs) -> std::result::Result<Self, ServerStatus> {
        let failed = |err: Error| ServerStatus::Failed(err.to_string());
        let options = args.mount_options();
        let file = T4Store::open_file(args.store(), options.direct_io, FileCreation::IfMissing)
            .map_err(failed)?;
        let identity = StoreIdentity::of(&file).map_err(|err| failed(err.into()))?;
        let claim = ServerAddress::new(identity)
            .and_then(|address| address.claim())
            .map_err(|err| failed(err.into()))?
            .ok_or(ServerStatus::AlreadyRunning)?;
        match Self::wait_for_mount_lock(&file) {
            Ok(()) => {}
            Err(Error::Busy) => return Err(ServerStatus::Busy),
            Err(err) => return Err(failed(err)),
        }
        let store =
            futures_executor::block_on(T4Store::mount_file(file, &options)).map_err(failed)?;
        let pool = ThreadPool::builder()
            .name_prefix("t4-server-")
            .create()
            .map_err(|err| failed(err.into()))?;
        Ok(Self {
            claim,
            store: Arc::new(store),
            identity,
            direct_io: options.direct_io,
            idle_timeout: args.idle_timeout(),
            pool,
        })
    }

    fn wait_for_mount_lock(file: &File) -> Result<()> {
        let deadline = Instant::now() + Self::MOUNT_LOCK_WAIT;
        loop {
            match T4Store::lock_file(file) {
                Err(Error::Busy) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                result => return result,
            }
        }
    }

    fn silence_output() -> io::Result<()> {
        let null = File::options().write(true).open("/dev/null")?;
        rustix::stdio::dup2_stdout(&null)?;
        rustix::stdio::dup2_stderr(&null)?;
        Ok(())
    }

    /// Accepts clients until none has been connected for the idle timeout,
    /// then returns, and the store closes as the server drops.
    fn serve(self) {
        // Each connection writes a byte here when it is over.
        let Ok((closed_tx, closed_rx)) = UnixStream::pair() else {
            return;
        };
        let listener = self.claim.listener();
        if listener.set_nonblocking(true).is_err() || closed_rx.set_nonblocking(true).is_err() {
            return;
        }
        let mut connected = 0_usize;
        let mut idle_since = Some(Instant::now());
        loop {
            let timeout = match idle_since {
                Some(since) => {
                    let left =
                        (since + self.idle_timeout).saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return;
                    }
                    Some(left)
                }
                None => None,
            };
            if Self::wait_readable(listener, &closed_rx, timeout).is_err() {
                return;
            }
            connected -= Self::closed_since_last(&closed_rx);
            loop {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let Ok(closed) = closed_tx.try_clone() else {
                            continue;
                        };
                        self.spawn_connection(socket, ConnectionClosed(closed));
                        connected += 1;
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                    // A connection that failed before it was accepted.
                    Err(_) => break,
                }
            }
            idle_since = match (connected, idle_since) {
                (0, Some(since)) => Some(since),
                (0, None) => Some(Instant::now()),
                _ => None,
            };
        }
    }

    /// Waits until a client connects or a connection is over, or until
    /// `timeout`.
    fn wait_readable(
        listener: &impl std::os::fd::AsFd,
        closed: &UnixStream,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        let timeout = timeout
            .map(Timespec::try_from)
            .transpose()
            .map_err(|_| io::Error::other("idle timeout too long"))?;
        let mut fds = [
            PollFd::new(listener, PollFlags::IN),
            PollFd::new(closed, PollFlags::IN),
        ];
        match rustix::event::poll(&mut fds, timeout.as_ref()) {
            Ok(_) | Err(rustix::io::Errno::INTR) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// How many connections ended since the last call.
    fn closed_since_last(mut closed: &UnixStream) -> usize {
        let mut count = 0;
        let mut bytes = [0; 64];
        loop {
            match closed.read(&mut bytes) {
                Ok(0) => return count,
                Ok(n) => count += n,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return count,
            }
        }
    }

    fn spawn_connection(&self, socket: UnixStream, closed: ConnectionClosed) {
        let connection = ClientConnection {
            socket,
            store: Arc::clone(&self.store),
            pool: self.pool.clone(),
            identity: self.identity,
            direct_io: self.direct_io,
        };
        // On failure the closure drops, and with it `closed`, which tells
        // the accept loop.
        let _ = thread::Builder::new()
            .name("t4-server-requests".into())
            .spawn(move || connection.serve(closed));
    }
}

/// Tells the accept loop a connection is over, when dropped: after its
/// last reply went out, or when it never got going.
struct ConnectionClosed(UnixStream);

impl Drop for ConnectionClosed {
    fn drop(&mut self) {
        let _ = (&self.0).write_all(&[0]);
    }
}

/// One client's connection, on the server. Its thread reads the requests
/// and runs each as a task on the pool; a writer thread sends the replies
/// as they finish.
struct ClientConnection {
    socket: UnixStream,
    store: Arc<T4Store>,
    pool: ThreadPool,
    identity: StoreIdentity,
    direct_io: bool,
}

impl ClientConnection {
    fn serve(self, closed: ConnectionClosed) {
        // Accepted sockets inherit the listener's non-blocking mode on macOS.
        if self.socket.set_nonblocking(false).is_err() {
            return;
        }
        let Ok(receiver) = self.socket.try_clone() else {
            return;
        };
        let mut requests = FrameReader::new(BufReader::new(FdReceiver::new(receiver)));
        if !self.accept_handshake(&mut requests) {
            return;
        }
        let (replies, outgoing) = crossbeam_channel::unbounded();
        let Ok(writer_socket) = self.socket.try_clone() else {
            return;
        };
        if thread::Builder::new()
            .name("t4-server-replies".into())
            .spawn(move || Self::write_replies(writer_socket, outgoing, closed))
            .is_err()
        {
            return;
        }
        while let Ok(Some(body)) = requests.read_frame() {
            let Ok((id, request)) = Request::decode(&body, requests.source().get_mut()) else {
                // A client that breaks the protocol is done.
                break;
            };
            self.dispatch(id, request, &replies);
        }
        // The writer ends once the requests in flight have replied.
    }

    /// Reads the client's handshake, and answers it. Whether the client may
    /// go on.
    fn accept_handshake(&self, requests: &mut FrameReader<BufReader<FdReceiver>>) -> bool {
        let Ok(Some(body)) = requests.read_frame() else {
            return false;
        };
        let reply = match Handshake::decode(&body, requests.source().get_mut()) {
            Ok(store) => self.check_store(File::from(store)),
            Err(reason) => HandshakeReply::Refused(reason),
        };
        let accepted = matches!(reply, HandshakeReply::Accepted { .. });
        (&self.socket).write_all(&reply.encode()).is_ok() && accepted
    }

    /// Accepts a client that showed this store's file, open read-write.
    fn check_store(&self, store: File) -> HandshakeReply {
        match StoreIdentity::of(&store) {
            Ok(identity) if identity == self.identity => {}
            _ => return HandshakeReply::Refused("not this server's store".into()),
        }
        match rustix::fs::fcntl_getfl(&store) {
            Ok(flags) if flags & OFlags::ACCMODE == OFlags::RDWR => {}
            _ => return HandshakeReply::Refused("store descriptor is not open read-write".into()),
        }
        HandshakeReply::Accepted {
            server_pid: std::process::id(),
            direct_io: self.direct_io,
        }
    }

    /// Runs `request`, and queues its reply.
    fn dispatch(&self, id: u64, request: Request, replies: &Sender<Vec<u8>>) {
        let store = Arc::clone(&self.store);
        match request {
            Request::Put { key, value } => self.run(id, replies, async move {
                store.put(key, value).await.map(|()| Reply::Done)
            }),
            Request::Get { key } => self.run(id, replies, async move {
                let key = T4KeyRef::try_from_slice(key.as_bytes())?;
                store.get(key).await.map(Reply::Bytes)
            }),
            Request::GetRange { key, range } => self.run(id, replies, async move {
                let key = T4KeyRef::try_from_slice(key.as_bytes())?;
                store.get_range(key, range).await.map(Reply::Bytes)
            }),
            Request::Remove { key } => self.run(id, replies, async move {
                store.remove(key).await.map(Reply::Removed)
            }),
            Request::Len => self.run(
                id,
                replies,
                async move { Ok(Reply::Count(store.len() as u64)) },
            ),
            Request::Snapshot { target } => {
                // A snapshot holds the store's write lock across its awaits,
                // which no pool task may: it gets a thread of its own.
                let snapshot_replies = replies.clone();
                let snapshot = move || {
                    let result = futures_executor::block_on(
                        store.snapshot(target, &MountOptions::default()),
                    );
                    let reply = ReplyFrame::new(id, result.map(|()| Reply::Done));
                    let _ = snapshot_replies.send(reply.encode());
                };
                if let Err(err) = thread::Builder::new()
                    .name("t4-server-snapshot".into())
                    .spawn(snapshot)
                {
                    let _ = replies.send(ReplyFrame::new(id, Err(Error::Io(err))).encode());
                }
            }
        }
    }

    fn run(
        &self,
        id: u64,
        replies: &Sender<Vec<u8>>,
        request: impl Future<Output = Result<Reply>> + Send + 'static,
    ) {
        let replies = replies.clone();
        self.pool.spawn_ok(async move {
            let _ = replies.send(ReplyFrame::new(id, request.await).encode());
        });
    }

    /// Sends the replies in the order they finish. A client that has gone
    /// still has its requests in flight run; their replies are dropped.
    fn write_replies(socket: UnixStream, outgoing: Receiver<Vec<u8>>, _closed: ConnectionClosed) {
        let mut client = Some(socket);
        for reply in outgoing {
            if let Some(socket) = &client
                && (&*socket).write_all(&reply).is_err()
            {
                client = None;
            }
        }
    }
}
