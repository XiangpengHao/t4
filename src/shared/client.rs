//! A client's side of shared mode: a store whose engine runs in its server.

use std::collections::HashMap;
use std::fs::File;
use std::future::poll_fn;
use std::io::BufReader;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use crate::input_kv::{T4Key, T4Value};
use crate::io::error::{Error, Result};
use crate::io::io_task::TaskCompletion;
use crate::range::RangeRequestU32;
use crate::shared::address::{ServerAddress, StoreIdentity};
use crate::shared::launch::ServerLauncher;
use crate::shared::wire::{FrameReader, OutgoingFrame, Reply, ReplyFrame, Request};
use crate::store::{MountOptions, ServerOptions};

/// What a client needs to reach its store's server, and to reach it again
/// whenever the server went away: the store, its server's address, and how
/// to start a server.
pub(super) struct ServerEndpoint {
    /// Absolute, so the server finds the store whatever its working directory.
    path: PathBuf,
    /// Open read-write, to show the server in each handshake.
    store_file: File,
    address: ServerAddress,
    options: MountOptions,
    server: ServerOptions,
}

impl ServerEndpoint {
    fn new(path: &Path, options: MountOptions, server: ServerOptions) -> Result<Self> {
        // The address comes from the file's inode, so the file must exist:
        // created here if missing, as an in-process mount would.
        let store_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let address = ServerAddress::new(StoreIdentity::of(&store_file)?)?;
        Ok(Self {
            path: std::fs::canonicalize(path)?,
            store_file,
            address,
            options,
            server,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn store_file(&self) -> &File {
        &self.store_file
    }

    pub(super) fn address(&self) -> &ServerAddress {
        &self.address
    }

    pub(super) fn options(&self) -> &MountOptions {
        &self.options
    }

    pub(super) fn server_options(&self) -> &ServerOptions {
        &self.server
    }
}

/// A store owned by its server process. Calls go over one connection,
/// opened again, starting a server if need be, after the server goes away.
pub(crate) struct SharedStore {
    endpoint: Arc<ServerEndpoint>,
    connection: Mutex<Arc<ServerConnection>>,
}

impl std::fmt::Debug for SharedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedStore")
            .field("path", &self.endpoint.path)
            .finish_non_exhaustive()
    }
}

impl SharedStore {
    pub(crate) async fn mount(
        path: &Path,
        options: MountOptions,
        server: ServerOptions,
    ) -> Result<Self> {
        let endpoint = Arc::new(ServerEndpoint::new(path, options, server)?);
        let connection = ServerConnection::open(Arc::clone(&endpoint)).await?;
        Ok(Self {
            endpoint,
            connection: Mutex::new(connection),
        })
    }

    pub(crate) async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        self.call(Request::Put { key, value }).await?.into_done()
    }

    pub(crate) async fn get(&self, key: T4Key) -> Result<Vec<u8>> {
        self.call(Request::Get { key }).await?.into_bytes()
    }

    pub(crate) async fn get_range(&self, key: T4Key, range: RangeRequestU32) -> Result<Vec<u8>> {
        self.call(Request::GetRange { key, range })
            .await?
            .into_bytes()
    }

    pub(crate) async fn remove(&self, key: T4Key) -> Result<bool> {
        self.call(Request::Remove { key }).await?.into_removed()
    }

    /// Snapshots into `target`, an empty store file this client created.
    pub(crate) async fn snapshot(&self, target: File) -> Result<()> {
        self.call(Request::Snapshot { target }).await?.into_done()
    }

    pub(crate) async fn len(&self) -> Result<usize> {
        let count = self.call(Request::Len).await?.into_count()?;
        usize::try_from(count).map_err(|_| Error::Server(format!("entry count {count}")))
    }

    /// The process id of the server this store is connected to.
    pub(crate) fn server_pid(&self) -> u32 {
        self.lock_connection().server_pid
    }

    async fn call(&self, request: Request) -> Result<Reply> {
        self.connection().await?.call(request).await
    }

    /// The connection to use: the current one, unless its server went away.
    async fn connection(&self) -> Result<Arc<ServerConnection>> {
        let current = Arc::clone(&self.lock_connection());
        if !current.calls.is_gone() {
            return Ok(current);
        }
        let fresh = ServerConnection::open(Arc::clone(&self.endpoint)).await?;
        let mut slot = self.lock_connection();
        // Another call may have reconnected first; its connection wins.
        if slot.calls.is_gone() {
            *slot = fresh;
        }
        Ok(Arc::clone(&slot))
    }

    fn lock_connection(&self) -> MutexGuard<'_, Arc<ServerConnection>> {
        // A swap of one `Arc` can't be left half done.
        self.connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// One connection to the store's server. Callers hand their requests to
/// its writer thread; its reader thread completes their calls by request
/// id as the replies arrive: `IoWorker`'s pattern, so the API stays
/// runtime-agnostic.
struct ServerConnection {
    requests: Sender<OutgoingFrame>,
    calls: Arc<PendingCalls>,
    /// Shut down on drop, so the reader thread sees the end, and the
    /// server sees this client leave.
    socket: UnixStream,
    server_pid: u32,
}

impl ServerConnection {
    /// Connects, starting a server if none is running. The blocking part
    /// runs on a thread of its own, which then reads the replies.
    async fn open(endpoint: Arc<ServerEndpoint>) -> Result<Arc<Self>> {
        let opened = Arc::new(TaskCompletion::new());
        let report = Arc::clone(&opened);
        thread::Builder::new()
            .name("t4-client-replies".into())
            .spawn(move || {
                let connected = match ServerLauncher::new(&endpoint).connect() {
                    Ok(connected) => connected,
                    Err(err) => return report.complete(Err(err)),
                };
                let (socket, replies, server_pid) = connected.into_parts();
                let calls = Arc::new(PendingCalls::default());
                match Self::start(socket, Arc::clone(&calls), server_pid) {
                    Ok(connection) => report.complete(Ok(Arc::new(connection))),
                    Err(err) => return report.complete(Err(err)),
                }
                // If the caller stopped waiting, this was the connection's
                // last holder: dropping it closes the socket, and ends the
                // loop below.
                drop(report);
                calls.read_replies(replies);
            })?;
        poll_fn(|cx| opened.poll_result(cx)).await
    }

    /// Starts the writer thread on `socket`.
    fn start(socket: UnixStream, calls: Arc<PendingCalls>, server_pid: u32) -> Result<Self> {
        let (requests, outgoing) = crossbeam_channel::unbounded();
        let writer_socket = socket.try_clone()?;
        thread::Builder::new()
            .name("t4-client-requests".into())
            .spawn(move || Self::write_requests(writer_socket, outgoing))?;
        Ok(Self {
            requests,
            calls,
            socket,
            server_pid,
        })
    }

    fn write_requests(socket: UnixStream, outgoing: Receiver<OutgoingFrame>) {
        for frame in outgoing {
            if frame.write_to(&socket).is_err() {
                // Make sure the reader sees the end too, and fails the calls.
                let _ = socket.shutdown(Shutdown::Both);
                return;
            }
        }
    }

    async fn call(&self, request: Request) -> Result<Reply> {
        let (id, reply) = self.calls.register()?;
        if self.requests.send(request.into_frame(id)).is_err() {
            self.calls.forget(id);
            return Err(Error::ServerGone);
        }
        poll_fn(|cx| reply.poll_result(cx)).await
    }
}

impl Drop for ServerConnection {
    fn drop(&mut self) {
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

/// The calls on one connection waiting for their replies, by request id.
#[derive(Default)]
struct PendingCalls {
    state: Mutex<PendingState>,
}

#[derive(Default)]
struct PendingState {
    next_id: u64,
    waiting: HashMap<u64, Arc<TaskCompletion<Reply>>>,
    /// The server went away: no reply will come.
    gone: bool,
}

impl PendingCalls {
    fn lock(&self) -> MutexGuard<'_, PendingState> {
        // Every change below is one map or flag update: none half done.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_gone(&self) -> bool {
        self.lock().gone
    }

    fn register(&self) -> Result<(u64, Arc<TaskCompletion<Reply>>)> {
        let mut state = self.lock();
        if state.gone {
            return Err(Error::ServerGone);
        }
        let id = state.next_id;
        state.next_id += 1;
        let reply = Arc::new(TaskCompletion::new());
        state.waiting.insert(id, Arc::clone(&reply));
        Ok((id, reply))
    }

    fn forget(&self, id: u64) {
        self.lock().waiting.remove(&id);
    }

    /// Completes calls as their replies arrive, until the connection ends;
    /// then fails the calls still waiting.
    fn read_replies(&self, mut replies: FrameReader<BufReader<UnixStream>>) {
        while let Ok(Some(body)) = replies.read_frame() {
            let Ok(frame) = ReplyFrame::decode(&body) else {
                break;
            };
            let (id, result) = frame.into_parts();
            // A call whose caller stopped waiting is still here: its
            // completion just goes unread.
            let waiting = self.lock().waiting.remove(&id);
            if let Some(reply) = waiting {
                reply.complete(result);
            }
        }
        let waiting = {
            let mut state = self.lock();
            state.gone = true;
            std::mem::take(&mut state.waiting)
        };
        for reply in waiting.into_values() {
            reply.complete(Err(Error::ServerGone));
        }
    }
}
