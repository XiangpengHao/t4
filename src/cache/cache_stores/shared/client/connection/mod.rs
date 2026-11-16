use crate::io::sync::Mutex;
use crate::io::sync::atomic::{AtomicU64, Ordering};
use crate::io::sync::thread::{self, JoinHandle};
use std::collections::BTreeMap;
use std::fs::File;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::PoisonError;
use std::task::{Context, Poll};

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

pub(in crate::cache) struct OwnerConnection {
    socket: FrameSocket,
    region: Option<std::sync::Arc<ClientRegion>>,
    lent: std::sync::Arc<LentSpans>,
    #[cfg(test)]
    traffic: std::sync::Arc<Traffic>,
    calls: std::sync::Arc<CallTable>,
    next_read: AtomicU64,
    number: u64,
    outgoing: FrameSender,
    reader: Option<JoinHandle<()>>,
}

impl OwnerConnection {
    const WELCOME_BYTES: u64 = 16;

    pub(in crate::cache::cache_stores::shared) fn connect(
        first_file: &Path,
        reads: Arc<dyn ReadPolicy>,
        number: u64,
        #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
        transport: &super::super::protocol::Transport,
    ) -> Result<Option<Self>> {
        match Self::admitted(
            first_file,
            #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
            transport,
        ) {
            Ok((socket, frames, region)) => {
                Ok(Some(Self::start(socket, frames, region, reads, number)?))
            }
            Err(Error::Io(error)) if Self::owner_absent(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// A client that can't make a region goes without: its reads' bytes go in the frames.
    fn admitted(
        first_file: &Path,
        #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
        transport: &super::super::protocol::Transport,
    ) -> Result<(
        FrameSocket,
        FrameReceiver,
        Option<std::sync::Arc<ClientRegion>>,
    )> {
        let cache_file = File::options().read(true).write(true).open(first_file)?;
        let address = OwnerAddress::of(
            FileIdentity::of(&cache_file)?,
            #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
            transport,
        )?;
        let socket = FrameSocket::new(address.connect(rustix::process::geteuid())?)?;
        socket.set_read_timeout(Some(Hello::WAIT))?;
        let region = ClientRegion::make().ok();
        let mut hello = Hello::new(PROTOCOL_VERSION, cache_file);
        if let Some(region) = &region {
            #[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
            let descriptor = region.descriptor().try_clone_to_owned()?;
            #[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
            let descriptor = region.descriptor();
            hello = hello.with_region(descriptor);
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
        #[cfg(test)]
        let traffic = std::sync::Arc::new(Traffic::default());
        let reader = OwnerFrames {
            frames,
            calls: std::sync::Arc::clone(&calls),
            lent: std::sync::Arc::clone(&lent),
            reads,
            #[cfg(test)]
            traffic: std::sync::Arc::clone(&traffic),
        };
        let reader = thread::Builder::new()
            .name("t4-client-reader".into())
            .spawn(move || reader.run())?;
        Ok(Self {
            socket,
            region,
            lent,
            #[cfg(test)]
            traffic,
            calls,
            next_read: AtomicU64::new(0),
            number,
            outgoing,
            reader: Some(reader),
        })
    }

    pub(super) async fn call(&self, call: impl FnOnce(u64) -> ClientFrame) -> Result<OwnerFrame> {
        self.send_call(call)?.await
    }

    pub(super) fn send_call(&self, call: impl FnOnce(u64) -> ClientFrame) -> Result<WaitingCall> {
        let waiting = self.calls.wait()?;
        self.tell(&[call(waiting.number)]);
        Ok(waiting)
    }

    pub(super) fn send_calls(
        &self,
        first: impl FnOnce(u64) -> ClientFrame,
        then: impl FnOnce(u64) -> ClientFrame,
    ) -> Result<(WaitingCall, WaitingCall)> {
        let (one, other) = (self.calls.wait()?, self.calls.wait()?);
        self.tell(&[first(one.number), then(other.number)]);
        Ok((one, other))
    }

    pub(super) fn tell(&self, frames: &[ClientFrame]) {
        let frames: Vec<Vec<u8>> = frames.iter().map(ClientFrame::encode).collect();
        #[cfg(test)]
        for frame in &frames {
            (self.traffic.sent).fetch_add(frame.len() as u64, Ordering::Relaxed);
        }
        self.outgoing.send(frames);
    }

    pub(super) fn version_ids(&self) -> VersionIdSource {
        VersionIdSource::Owner {
            connection: self.number,
        }
    }

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

    pub(super) fn take_read_number(&self) -> u64 {
        self.next_read.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn span(&self, len: u64) -> Option<RegionSpan> {
        self.region.as_ref().and_then(|region| region.span(len))
    }

    pub(super) fn keep_until_ended(&self, read: u64, spans: Vec<RegionSpan>) {
        self.lent.keep_until_ended(read, spans);
    }

    pub(in crate::cache) fn owner_gone(&self) -> bool {
        self.calls.owner_gone()
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache::cache_stores::shared) fn has_region(&self) -> bool {
        self.region.is_some()
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn region_free_pages(&self) -> Option<u64> {
        self.region.as_ref().map(|region| region.free_pages())
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache::cache_stores::shared) fn region(
        &self,
    ) -> Option<&std::sync::Arc<ClientRegion>> {
        self.region.as_ref()
    }

    #[cfg(test)]
    pub(in crate::cache::cache_stores::shared) fn socket_bytes(&self) -> (u64, u64) {
        let traffic = &self.traffic;
        (
            traffic.sent.load(Ordering::Relaxed),
            traffic.taken.load(Ordering::Relaxed),
        )
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn region_holds(&self, bytes: &[u8]) -> bool {
        self.region
            .as_ref()
            .is_some_and(|region| region.holds(bytes))
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) fn calls_waiting(&self) -> Vec<u64> {
        let mut waiting: Vec<u64> = self.calls.calls().waiting.keys().copied().collect();
        waiting.sort_unstable();
        waiting
    }

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

#[derive(Default)]
struct CallTable {
    calls: Mutex<WaitingCalls>,
}

#[derive(Default)]
struct WaitingCalls {
    next: u64,
    waiting: BTreeMap<u64, Arc<TaskCompletion<OwnerFrame>>>,
    owner_gone: bool,
}

impl CallTable {
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

    /// An answer to a call given up is dropped, not an error.
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

    fn give_up(&self, number: u64) {
        self.calls().waiting.remove(&number);
    }

    fn owner_gone(&self) -> bool {
        self.calls().owner_gone
    }

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

    fn gone_error() -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "the owner of the shared cache is gone",
        ))
    }

    fn calls(&self) -> crate::io::sync::MutexGuard<'_, WaitingCalls> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Dropped unanswered, the call is given up: the owner never answers a read's calls once it ends.
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

/// Spans of reads the client ended that the owner may still touch, until it ends them too;
/// `None` once the owner is gone, so spans lent after free at once.
struct LentSpans {
    reads: Mutex<Option<BTreeMap<u64, Vec<RegionSpan>>>>,
}

impl Default for LentSpans {
    fn default() -> Self {
        Self {
            reads: Mutex::new(Some(BTreeMap::new())),
        }
    }
}

impl LentSpans {
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

    fn ended(&self, read: u64) {
        let spans = self.reads().as_mut().and_then(|reads| reads.remove(&read));
        drop(spans);
    }

    fn owner_gone(&self) {
        let reads = self.reads().take();
        drop(reads);
    }

    fn reads(&self) -> crate::io::sync::MutexGuard<'_, Option<BTreeMap<u64, Vec<RegionSpan>>>> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct OwnerFrames {
    frames: FrameReceiver,
    calls: std::sync::Arc<CallTable>,
    lent: std::sync::Arc<LentSpans>,
    reads: Arc<dyn ReadPolicy>,
    #[cfg(test)]
    traffic: std::sync::Arc<Traffic>,
}

#[cfg(test)]
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

    fn take_frames(&mut self) -> Result<()> {
        loop {
            let body = match self.frames.receive(FrameSocket::MOST_BYTES) {
                Ok(Some(body)) => body,
                Ok(None) => return Ok(()),
                // Look again, in case a shutdown's wakeup was lost (`FrameSocket::RECHECK`).
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
            };
            #[cfg(test)]
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

#[cfg(all(test, feature = "shuttle"))]
mod tests;
