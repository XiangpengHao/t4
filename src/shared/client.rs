//! A client's side of shared mode: a store whose engine runs in its server.

use std::collections::HashMap;
use std::fs::File;
use std::future::poll_fn;
use std::io::BufReader;
use std::net::Shutdown;
use std::num::NonZeroU32;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use crate::disk::{PaddedValue, ValueWindow};
use crate::input_kv::{T4Key, T4Value};
use crate::io::error::{Error, Result};
use crate::io::io_task::TaskCompletion;
use crate::range::RangeRequestU32;
use crate::shared::address::{ServerAddress, StoreIdentity};
use crate::shared::launch::ServerLauncher;
use crate::shared::memory::{ValueBuffer, ValueMemory};
use crate::shared::wire::{
    FrameReader, OutgoingFrame, Reply, ReplyFrame, Request, ValueIn, WireBuffer,
};
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
        self.connection().await?.put(key, value).await
    }

    pub(crate) async fn get(&self, key: T4Key) -> Result<Vec<u8>> {
        self.connection().await?.get(key).await
    }

    pub(crate) async fn get_range(&self, key: T4Key, range: RangeRequestU32) -> Result<Vec<u8>> {
        self.connection().await?.get_range(key, range).await
    }

    pub(crate) async fn remove(&self, key: T4Key) -> Result<bool> {
        let connection = self.connection().await?;
        connection
            .call(Request::Remove { key }, None)
            .await?
            .reply
            .into_removed()
    }

    /// Snapshots into `target`, an empty store file this client created.
    pub(crate) async fn snapshot(&self, target: File) -> Result<()> {
        let connection = self.connection().await?;
        connection
            .call(Request::Snapshot { target }, None)
            .await?
            .reply
            .into_done()
    }

    pub(crate) async fn len(&self) -> Result<usize> {
        let connection = self.connection().await?;
        let count = connection
            .call(Request::Len, None)
            .await?
            .reply
            .into_count()?;
        usize::try_from(count).map_err(|_| Error::Server(format!("entry count {count}")))
    }

    /// The process id of the server this store is connected to.
    pub(crate) fn server_pid(&self) -> u32 {
        self.lock_connection().server_pid
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
/// runtime-agnostic. Where the client shares memory with the server, value
/// bytes go through buffers in it instead of the frames.
struct ServerConnection {
    requests: Sender<OutgoingFrame>,
    calls: Arc<PendingCalls>,
    /// Shut down on drop, so the reader thread sees the end, and the
    /// server sees this client leave.
    socket: UnixStream,
    server_pid: u32,
    memory: Option<Arc<ValueMemory>>,
    /// The buffer a get first lends: the size the last get needed.
    get_capacity: AtomicU32,
}

impl ServerConnection {
    /// What a get lends before it has seen a value.
    const FIRST_GET_CAPACITY: u32 = 64 << 10;

    /// Connects, starting a server if none is running. The blocking part
    /// runs on a thread of its own, which then reads the replies.
    async fn open(endpoint: Arc<ServerEndpoint>) -> Result<Arc<Self>> {
        let opened = Arc::new(TaskCompletion::new());
        let report = Arc::clone(&opened);
        thread::Builder::new()
            .name("t4-client-replies".into())
            .spawn(move || {
                let memory = match ValueMemory::for_connection() {
                    Ok(memory) => memory,
                    Err(err) => return report.complete(Err(err.into())),
                };
                let region = memory.as_ref().map(|memory| memory.region_fd());
                let connected = match ServerLauncher::new(&endpoint, region).connect() {
                    Ok(connected) => connected,
                    Err(err) => return report.complete(Err(err)),
                };
                let (socket, replies, server_pid) = connected.into_parts();
                let calls = Arc::new(PendingCalls::default());
                match Self::start(socket, Arc::clone(&calls), server_pid, memory) {
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
    fn start(
        socket: UnixStream,
        calls: Arc<PendingCalls>,
        server_pid: u32,
        memory: Option<Arc<ValueMemory>>,
    ) -> Result<Self> {
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
            memory,
            get_capacity: AtomicU32::new(Self::FIRST_GET_CAPACITY),
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

    async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        let len = value.len_u32();
        let (Some(memory), Some(buffer_len)) = (&self.memory, PaddedValue::buffer_len(len)?) else {
            let value = ValueIn::Inline(value);
            return self
                .call(Request::Put { key, value }, None)
                .await?
                .reply
                .into_done();
        };
        let mut buffer = memory.allocate(buffer_len)?;
        buffer.write(value.as_bytes())?;
        let value = ValueIn::Shared {
            buffer: buffer.to_wire()?,
            len,
        };
        self.call(Request::Put { key, value }, Some(buffer))
            .await?
            .reply
            .into_done()
    }

    async fn get(&self, key: T4Key) -> Result<Vec<u8>> {
        let capacity = self.get_capacity.load(Ordering::Relaxed);
        self.read(capacity, true, |into| Request::Get {
            key: key.clone(),
            into,
        })
        .await
    }

    async fn get_range(&self, key: T4Key, range: RangeRequestU32) -> Result<Vec<u8>> {
        // The server reads the same window, so it fits the first time.
        let capacity = ValueWindow::of_any_value(range)?.buffer_len();
        self.read(capacity, false, |into| Request::GetRange {
            key: key.clone(),
            range,
            into,
        })
        .await
    }

    /// Sends the read `request` makes, lending the server a buffer of
    /// `capacity` bytes where there is shared memory, and asking again with
    /// a larger one if the bytes don't fit. A get `learns` the size it
    /// needed, for the next get's first try.
    async fn read(
        &self,
        capacity: u32,
        learns: bool,
        request: impl Fn(Option<WireBuffer>) -> Request,
    ) -> Result<Vec<u8>> {
        let (Some(memory), Some(mut capacity)) = (&self.memory, NonZeroU32::new(capacity)) else {
            return self.call(request(None), None).await?.reply.into_bytes();
        };
        loop {
            let buffer = memory.allocate(capacity)?;
            let into = Some(buffer.to_wire()?);
            let answer = self.call(request(into), Some(buffer)).await?;
            match answer.reply {
                Reply::InBuffer { start, len } => {
                    if learns && let Some(needed) = PaddedValue::buffer_len(len)? {
                        self.get_capacity.store(needed.get(), Ordering::Relaxed);
                    }
                    let buffer = answer.buffer.expect("the call keeps its buffer");
                    return Ok(buffer.read(start, len)?);
                }
                Reply::TooSmall { needed } => {
                    capacity = NonZeroU32::new(needed)
                        .filter(|needed| *needed > capacity)
                        .ok_or_else(|| Error::Server(format!("too small for {needed} bytes")))?;
                    if learns {
                        self.get_capacity.store(needed, Ordering::Relaxed);
                    }
                }
                other => return Err(other.unexpected()),
            }
        }
    }

    /// Sends `request`, and waits for its reply. `buffer`, the one the
    /// request lends the server, stays with the call until the reply comes,
    /// even if the caller stops waiting: the server may use it until then.
    async fn call(&self, request: Request, buffer: Option<ValueBuffer>) -> Result<Answer> {
        let (id, answer) = self.calls.register(buffer)?;
        if self.requests.send(request.into_frame(id)).is_err() {
            self.calls.forget(id);
            return Err(Error::ServerGone);
        }
        poll_fn(|cx| answer.poll_result(cx)).await
    }
}

impl Drop for ServerConnection {
    fn drop(&mut self) {
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

/// A reply, and the buffer its request lent the server.
struct Answer {
    reply: Reply,
    buffer: Option<ValueBuffer>,
}

/// The calls on one connection waiting for their replies, by request id.
#[derive(Default)]
struct PendingCalls {
    state: Mutex<PendingState>,
}

#[derive(Default)]
struct PendingState {
    next_id: u64,
    waiting: HashMap<u64, PendingCall>,
    /// The server went away: no reply will come.
    gone: bool,
}

struct PendingCall {
    answer: Arc<TaskCompletion<Answer>>,
    buffer: Option<ValueBuffer>,
}

impl PendingCalls {
    fn lock(&self) -> MutexGuard<'_, PendingState> {
        // Every change below is one map or flag update: none half done.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn is_gone(&self) -> bool {
        self.lock().gone
    }

    fn register(&self, buffer: Option<ValueBuffer>) -> Result<(u64, Arc<TaskCompletion<Answer>>)> {
        let mut state = self.lock();
        if state.gone {
            return Err(Error::ServerGone);
        }
        let id = state.next_id;
        state.next_id += 1;
        let answer = Arc::new(TaskCompletion::new());
        let call = PendingCall {
            answer: Arc::clone(&answer),
            buffer,
        };
        state.waiting.insert(id, call);
        Ok((id, answer))
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
            // completion just goes unread, and frees its buffer as it drops.
            let waiting = self.lock().waiting.remove(&id);
            if let Some(PendingCall { answer, buffer }) = waiting {
                answer.complete(result.map(|reply| Answer { reply, buffer }));
            }
        }
        let waiting = {
            let mut state = self.lock();
            state.gone = true;
            std::mem::take(&mut state.waiting)
        };
        for call in waiting.into_values() {
            call.answer.complete(Err(Error::ServerGone));
        }
    }
}
