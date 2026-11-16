//! [`OwnerConnection`]: a client's connection to the owner of its cache
//! (`doc/11-shared-mode.md`, decisions 5 to 7): admitted by the handshake,
//! then a thread that sends the client's frames, in order, and one that
//! takes the owner's, each answer completing the call it names, until the
//! owner goes. The shared cache store's reads and fetches call the owner
//! through it.

use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};

use bytes::Bytes;

use crate::cache::cache_stores::VersionIdSource;
use crate::cache::cache_stores::shared::protocol::{
    ClientFrame, ClientRegion, FrameReader, FrameReceiver, FrameSender, FrameSocket, Hello,
    OwnerAddress, OwnerFrame, PROTOCOL_VERSION, RegionSpan,
};
use crate::cache::policy::{Io, IoEvent, ReadPolicy};
use crate::cache::storage::FileIdentity;
use crate::io::error::{Error, Result};
use crate::io::io_task::TaskCompletion;
use crate::io::sync::Arc;

/// A client's connection to its owner. Dropped, it closes: the owner sees
/// the client gone, and its threads end.
pub(in crate::cache) struct OwnerConnection {
    socket: FrameSocket,
    /// The client's region, if its owner mapped it: its reads' outputs and
    /// its fetches' bytes are spans of it (decision 5); without it, their
    /// bytes go in the frames.
    region: Option<std::sync::Arc<ClientRegion>>,
    /// The spans of the reads the client ended, until the owner has too.
    lent: std::sync::Arc<LentSpans>,
    /// The bytes sent and taken on the socket so far: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    traffic: std::sync::Arc<Traffic>,
    /// The calls waiting for their answers, shared with the reader.
    calls: std::sync::Arc<CallTable>,
    /// The number the next read takes.
    next_read: AtomicU64,
    /// The connection's number among this process's, each to an owner in
    /// turn: which owner the version ids the connection is given come from.
    number: u64,
    /// Sends the client's frames, in order.
    outgoing: FrameSender,
    /// Takes the owner's frames until the owner goes, or breaks the
    /// protocol.
    reader: Option<JoinHandle<()>>,
}

impl OwnerConnection {
    /// The bytes of an owner's welcome at most: all a client takes before
    /// it is admitted.
    const WELCOME_BYTES: u64 = 16;

    /// A connection to the owner of the cache whose first cache file is at
    /// `first_file`, admitted, the reads from disk the owner makes for the
    /// client's reads told to `reads`, numbered `number` among this
    /// process's connections; `None` if no owner answered, or one closed
    /// the connection first: it is starting, or leaving, or refused the
    /// client.
    ///
    /// # Errors
    ///
    /// If the cache file can't be opened for writing, or the socket fails
    /// otherwise; [`Error::Busy`] if the owner speaks another version of
    /// the protocol, so that the client gives up at once.
    pub(in crate::cache::cache_stores::shared) fn connect(
        first_file: &Path,
        reads: Arc<dyn ReadPolicy>,
        number: u64,
    ) -> Result<Option<Self>> {
        match Self::admitted(first_file) {
            Ok((socket, frames, region)) => {
                Ok(Some(Self::start(socket, frames, region, reads, number)?))
            }
            Err(Error::Io(error)) if Self::owner_absent(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// A socket to the owner of the cache whose first cache file is at
    /// `first_file`, which admitted the client: its hello sent, with its
    /// descriptor of that file and of a region of its own, and the owner's
    /// welcome taken; what takes the owner's frames after; and the region,
    /// if the owner mapped it. A client that can't make a region goes
    /// without: its reads' bytes go in the frames.
    fn admitted(
        first_file: &Path,
    ) -> Result<(
        FrameSocket,
        FrameReceiver,
        Option<std::sync::Arc<ClientRegion>>,
    )> {
        let cache_file = File::options().read(true).write(true).open(first_file)?;
        let address = OwnerAddress::of(FileIdentity::of(&cache_file)?)?;
        let socket = FrameSocket::new(address.connect(rustix::process::geteuid())?)?;
        socket.set_read_timeout(Some(Hello::WAIT))?;
        let region = ClientRegion::make().ok();
        let mut hello = Hello::new(PROTOCOL_VERSION, cache_file);
        if let Some(region) = &region {
            hello = hello.with_region(region.descriptor().try_clone_to_owned()?);
        }
        hello.send(&socket)?;
        let mut frames = FrameReceiver::new(socket.try_clone()?);
        let welcome = frames
            .receive(Self::WELCOME_BYTES)?
            .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let OwnerFrame::Welcome {
            version,
            region: mapped,
        } = OwnerFrame::decode(welcome)?
        else {
            return Err(FrameReader::malformed("welcome"));
        };
        if version != PROTOCOL_VERSION {
            // An owner of another version admits no try of this client's.
            return Err(Error::Busy);
        }
        // The reader's every read ends that often, to look again.
        socket.set_read_timeout(Some(FrameSocket::RECHECK))?;
        Ok((socket, frames, region.filter(|_| mapped)))
    }

    /// Whether `error`, met while connecting, means no owner is there to
    /// answer: none listens, the file is gone, or the owner closed the
    /// connection, or never answered.
    fn owner_absent(error: &io::Error) -> bool {
        use io::ErrorKind::{
            BrokenPipe, ConnectionRefused, ConnectionReset, NotFound, TimedOut, UnexpectedEof,
            WouldBlock,
        };
        matches!(
            error.kind(),
            ConnectionRefused
                | NotFound
                | ConnectionReset
                | BrokenPipe
                | UnexpectedEof
                | WouldBlock
                | TimedOut
        )
    }

    /// The connection numbered `number` over `socket`, the owner's frames
    /// taken by `frames`, its owner having mapped `region` if any, its
    /// threads started: the sender's writer sends the frames a caller
    /// couldn't, each batch in one write; the reader answers each call, and
    /// tells `reads` of each read from disk.
    fn start(
        socket: FrameSocket,
        frames: FrameReceiver,
        region: Option<std::sync::Arc<ClientRegion>>,
        reads: Arc<dyn ReadPolicy>,
        number: u64,
    ) -> Result<Self> {
        let outgoing = FrameSender::start(socket.try_clone()?, FrameSender::SEND_WAIT)?;
        let calls = std::sync::Arc::new(CallTable::default());
        let lent = std::sync::Arc::new(LentSpans::default());
        #[cfg(all(test, not(feature = "shuttle")))]
        let traffic = std::sync::Arc::new(Traffic::default());
        let reader = OwnerFrames {
            frames,
            calls: std::sync::Arc::clone(&calls),
            lent: std::sync::Arc::clone(&lent),
            reads,
            #[cfg(all(test, not(feature = "shuttle")))]
            traffic: std::sync::Arc::clone(&traffic),
        };
        let reader = thread::Builder::new()
            .name("t4-client-reader".into())
            .spawn(move || reader.run())?;
        Ok(Self {
            socket,
            region,
            lent,
            #[cfg(all(test, not(feature = "shuttle")))]
            traffic,
            calls,
            next_read: AtomicU64::new(0),
            number,
            outgoing,
            reader: Some(reader),
        })
    }

    /// Calls the owner: sends the frame `call` makes of the call's number,
    /// and waits for the answer.
    ///
    /// # Errors
    ///
    /// If the owner is gone, or goes before it answers.
    pub(super) async fn call(&self, call: impl FnOnce(u64) -> ClientFrame) -> Result<OwnerFrame> {
        self.send_call(call)?.await
    }

    /// Sends the frame `call` makes of a new call's number: the call, its
    /// answer awaited later, or given up if it drops first.
    ///
    /// # Errors
    ///
    /// If the owner is gone.
    pub(super) fn send_call(&self, call: impl FnOnce(u64) -> ClientFrame) -> Result<WaitingCall> {
        let waiting = self.calls.wait()?;
        self.tell(&[call(waiting.number)]);
        Ok(waiting)
    }

    /// Sends the frames `first` and `then` make of two new calls' numbers,
    /// in one write: the calls, their answers awaited later.
    ///
    /// # Errors
    ///
    /// If the owner is gone.
    pub(super) fn send_calls(
        &self,
        first: impl FnOnce(u64) -> ClientFrame,
        then: impl FnOnce(u64) -> ClientFrame,
    ) -> Result<(WaitingCall, WaitingCall)> {
        let (one, other) = (self.calls.wait()?, self.calls.wait()?);
        self.tell(&[first(one.number), then(other.number)]);
        Ok((one, other))
    }

    /// Sends `frames`, in one write if the socket has room; dropped if the
    /// owner is gone.
    pub(super) fn tell(&self, frames: &[ClientFrame]) {
        let frames: Vec<Vec<u8>> = frames.iter().map(ClientFrame::encode).collect();
        #[cfg(all(test, not(feature = "shuttle")))]
        for frame in &frames {
            (self.traffic.sent).fetch_add(frame.len() as u64, Ordering::Relaxed);
        }
        self.outgoing.send(frames);
    }

    /// Where the version ids this connection is given come from: its
    /// owner, told apart from those this process read through before, and
    /// will after.
    pub(super) fn version_ids(&self) -> VersionIdSource {
        VersionIdSource::Owner {
            connection: self.number,
        }
    }

    /// The size of the version the owner's cache holds of the file at
    /// `path`, if it holds one.
    ///
    /// # Errors
    ///
    /// If the owner is gone, or goes before it answers.
    pub(in crate::cache::cache_stores::shared) async fn known_size(
        &self,
        path: &[u8],
    ) -> Result<Option<u64>> {
        let path = Bytes::copy_from_slice(path);
        match self
            .call(|call| ClientFrame::KnownSize { call, path })
            .await?
        {
            OwnerFrame::KnownSize { size, .. } => Ok(size),
            answer => Err(Self::unexpected(&answer)),
        }
    }

    /// The number the client's next read takes.
    pub(super) fn take_read_number(&self) -> u64 {
        self.next_read.fetch_add(1, Ordering::Relaxed)
    }

    /// A span of `len` bytes of the client's region, if the owner mapped it
    /// and one is free.
    pub(super) fn span(&self, len: u64) -> Option<RegionSpan> {
        self.region.as_ref().and_then(|region| region.span(len))
    }

    /// Keeps `spans`, the read `read`'s, until the owner ends the read
    /// ([`LentSpans`]).
    pub(super) fn keep_until_ended(&self, read: u64, spans: Vec<RegionSpan>) {
        self.lent.keep_until_ended(read, spans);
    }

    /// Whether the owner has gone: the connection closed, or broke, and
    /// every call fails.
    pub(in crate::cache) fn owner_gone(&self) -> bool {
        self.calls.owner_gone()
    }

    /// Whether the owner mapped the client's region: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn has_region(&self) -> bool {
        self.region.is_some()
    }

    /// How many of the region's pages are free, if the owner mapped it:
    /// for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn region_free_pages(&self) -> Option<u64> {
        self.region.as_ref().map(|region| region.free_pages())
    }

    /// The client's region, if its owner mapped it: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn region(
        &self,
    ) -> Option<&std::sync::Arc<ClientRegion>> {
        self.region.as_ref()
    }

    /// The bytes the connection has sent and taken so far, its frames whole:
    /// for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache::cache_stores::shared) fn socket_bytes(&self) -> (u64, u64) {
        let traffic = &self.traffic;
        (
            traffic.sent.load(Ordering::Relaxed),
            traffic.taken.load(Ordering::Relaxed),
        )
    }

    /// Whether `bytes` lie in the client's region: for tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn region_holds(&self, bytes: &[u8]) -> bool {
        self.region
            .as_ref()
            .is_some_and(|region| region.holds(bytes))
    }

    /// The numbers of the calls waiting for their answers, in order: for
    /// tests.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn calls_waiting(&self) -> Vec<u64> {
        let mut waiting: Vec<u64> = self.calls.calls().waiting.keys().copied().collect();
        waiting.sort_unstable();
        waiting
    }

    /// The error of a call the owner answered with a frame no call of its
    /// kind takes.
    pub(super) fn unexpected(answer: &OwnerFrame) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the owner answered a call with {answer:?}"),
        ))
    }
}

impl Drop for OwnerConnection {
    fn drop(&mut self) {
        // The sender, a field, stops its writer as it drops after.
        self.socket.shut_down();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// The calls a client made that wait for their answers, by number; and
/// whether the owner has gone, which fails them all, and every call after.
#[derive(Default)]
struct CallTable {
    calls: Mutex<WaitingCalls>,
}

#[derive(Default)]
struct WaitingCalls {
    /// The number the next call takes.
    next: u64,
    waiting: HashMap<u64, Arc<TaskCompletion<OwnerFrame>>>,
    owner_gone: bool,
}

impl CallTable {
    /// A new call, waiting in the table for its answer.
    ///
    /// # Errors
    ///
    /// If the owner is gone.
    fn wait(self: &std::sync::Arc<Self>) -> Result<WaitingCall> {
        let mut calls = self.calls();
        if calls.owner_gone {
            return Err(Self::gone_error());
        }
        let number = calls.next;
        calls.next += 1;
        let answer = Arc::new(TaskCompletion::new());
        calls.waiting.insert(number, Arc::clone(&answer));
        Ok(WaitingCall {
            table: std::sync::Arc::clone(self),
            number,
            answer,
        })
    }

    /// Answers the call `answer` names, if it still waits: the answer to a
    /// call given up is dropped.
    ///
    /// # Errors
    ///
    /// If it names no call made, or isn't an answer.
    fn answer(&self, answer: OwnerFrame) -> Result<()> {
        let Some(call) = answer.call() else {
            return Err(FrameReader::malformed("answer: of no call"));
        };
        let waiting = {
            let mut calls = self.calls();
            if call >= calls.next {
                return Err(FrameReader::malformed("answer: to no call made"));
            }
            calls.waiting.remove(&call)
        };
        if let Some(waiting) = waiting {
            waiting.complete(Ok(answer));
        }
        Ok(())
    }

    /// Gives up the call `number`: it leaves the table, and its answer, if
    /// the owner sends it, is dropped.
    fn give_up(&self, number: u64) {
        self.calls().waiting.remove(&number);
    }

    /// Whether the owner is gone: every call fails.
    fn owner_gone(&self) -> bool {
        self.calls().owner_gone
    }

    /// Fails every call waiting, and every call after: the owner is gone.
    fn fail_all(&self) {
        let waiting = {
            let mut calls = self.calls();
            calls.owner_gone = true;
            std::mem::take(&mut calls.waiting)
        };
        for (_, call) in waiting {
            call.complete(Err(Self::gone_error()));
        }
    }

    /// The error of a call made, or waiting, once the owner is gone.
    fn gone_error() -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "the owner of the shared cache is gone",
        ))
    }

    fn calls(&self) -> std::sync::MutexGuard<'_, WaitingCalls> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A call waiting in its table: done once the owner answers it, or goes.
/// Dropped first, as with a read given up, the call is given up, and
/// leaves the table: the owner may never answer it, as it doesn't a read's
/// calls once the read ends.
pub(super) struct WaitingCall {
    table: std::sync::Arc<CallTable>,
    number: u64,
    answer: Arc<TaskCompletion<OwnerFrame>>,
}

impl Future for WaitingCall {
    type Output = Result<OwnerFrame>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.answer.poll_result(cx)
    }
}

impl Drop for WaitingCall {
    fn drop(&mut self) {
        // Answered or failed, the call left the table already.
        self.table.give_up(self.number);
    }
}

/// The spans of the client's reads that the client has ended and its owner
/// may still touch, by read: each read's freed once the owner ends it too
/// ([`OwnerFrame::Ended`]); every one once the owner is gone, and those
/// lent after at once (`None`).
struct LentSpans {
    reads: Mutex<Option<HashMap<u64, Vec<RegionSpan>>>>,
}

impl Default for LentSpans {
    fn default() -> Self {
        Self {
            reads: Mutex::new(Some(HashMap::new())),
        }
    }
}

impl LentSpans {
    /// Keeps `spans`, the read `read`'s, until the owner ends the read: the
    /// read's number hands them over as it drops, the last of the read and
    /// its fetches, before it ends the read.
    fn keep_until_ended(&self, read: u64, spans: Vec<RegionSpan>) {
        if spans.is_empty() {
            return;
        }
        let mut reads = self.reads();
        if let Some(reads) = reads.as_mut() {
            reads.entry(read).or_default().extend(spans);
        }
        // The owner gone, the spans drop here, free.
    }

    /// Frees the spans of the read `read`, which the owner ended.
    fn ended(&self, read: u64) {
        let spans = self.reads().as_mut().and_then(|reads| reads.remove(&read));
        drop(spans);
    }

    /// Frees every span, and every one lent after: the owner is gone.
    fn owner_gone(&self) {
        let reads = self.reads().take();
        drop(reads);
    }

    fn reads(&self) -> std::sync::MutexGuard<'_, Option<HashMap<u64, Vec<RegionSpan>>>> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The connection's reader: it takes the owner's frames, each answer
/// completing its call, each read from disk told to the client's read
/// policy, and each read's end freeing its spans, until the owner goes or
/// breaks the protocol; then it fails the calls waiting, and every call
/// after, and frees every span.
struct OwnerFrames {
    frames: FrameReceiver,
    calls: std::sync::Arc<CallTable>,
    lent: std::sync::Arc<LentSpans>,
    reads: Arc<dyn ReadPolicy>,
    #[cfg(all(test, not(feature = "shuttle")))]
    traffic: std::sync::Arc<Traffic>,
}

/// The bytes a connection has sent and taken, its frames whole: for tests.
#[cfg(all(test, not(feature = "shuttle")))]
#[derive(Default)]
struct Traffic {
    sent: AtomicU64,
    taken: AtomicU64,
}

impl OwnerFrames {
    fn run(mut self) {
        let _ = self.take_frames();
        self.calls.fail_all();
        self.lent.owner_gone();
        self.frames.shut_down();
    }

    /// Takes the owner's frames until it closes the connection.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the owner sends a frame the protocol doesn't
    /// allow.
    fn take_frames(&mut self) -> Result<()> {
        loop {
            let body = match self.frames.receive(FrameSocket::MOST_BYTES) {
                Ok(Some(body)) => body,
                Ok(None) => return Ok(()),
                // Nothing for a while: look again, in case a shutdown's
                // wakeup was lost (`FrameSocket::RECHECK`).
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            };
            #[cfg(all(test, not(feature = "shuttle")))]
            (self.traffic.taken).fetch_add(8 + body.len() as u64, Ordering::Relaxed);
            match OwnerFrame::decode(body)? {
                OwnerFrame::DiskRead {
                    drive,
                    bytes,
                    phase,
                } => {
                    let read = Io::DiskRead { drive, bytes };
                    self.reads.on_io(&IoEvent::new(read, phase));
                }
                OwnerFrame::Ended { read } => self.lent.ended(read),
                answer => self.calls.answer(answer)?,
            }
        }
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
