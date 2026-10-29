//! [`OwnerConnection`]: a client's connection to the owner of its cache
//! (`doc/11-shared-mode.md`, decisions 5 to 7): admitted by the handshake,
//! then a thread that sends the client's frames, in order, and one that
//! takes the owner's, each answer completing the call it names, until the
//! owner goes. A client's read through it is a [`ClientRead`], and each of
//! its fetches a [`ClientFetch`].

use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io;
use std::num::NonZeroU8;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::Sender;

use super::address::{FileIdentity, OwnerAddress};
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use crate::cache::FileVersionId;
use crate::cache::block_index::FileVersion;
use crate::cache::object_cache::{CacheableFile, LocatedRun};
use crate::cache::policy::{Io, IoEvent, ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::read_output::ReadOutput;
use crate::io::error::{Error, Result};
use crate::io::io_task::TaskCompletion;
use crate::io::sync::Arc;

/// A client's connection to its owner. Dropped, it closes: the owner sees
/// the client gone, and its threads end.
pub(in crate::cache) struct OwnerConnection {
    socket: FrameSocket,
    /// The calls waiting for their answers, shared with the reader.
    calls: std::sync::Arc<CallTable>,
    /// The number the next read takes.
    next_read: AtomicU64,
    /// The frames the writer sends, in order; taken as the connection
    /// drops, so that the writer's channel closes.
    outgoing: Option<Sender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
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
    /// client's reads told to `reads`; `None` if no owner answered, or one
    /// closed the connection first: it is starting, or leaving, or refused
    /// the client.
    ///
    /// # Errors
    ///
    /// If the cache file can't be opened for writing, or the socket fails
    /// otherwise.
    pub(in crate::cache) fn connect(
        first_file: &Path,
        reads: Arc<dyn ReadPolicy>,
    ) -> Result<Option<Self>> {
        match Self::admitted(first_file) {
            Ok(socket) => Ok(Some(Self::start(socket, reads)?)),
            Err(Error::Io(error)) if Self::owner_absent(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// A socket to the owner of the cache whose first cache file is at
    /// `first_file`, which admitted the client: its hello sent, with its
    /// descriptor of that file, and the owner's welcome taken.
    fn admitted(first_file: &Path) -> Result<FrameSocket> {
        let cache_file = File::options().read(true).write(true).open(first_file)?;
        let address = OwnerAddress::of(FileIdentity::of(&cache_file)?);
        let socket = FrameSocket::new(address.connect()?)?;
        socket.set_read_timeout(Some(Hello::WAIT))?;
        Hello::new(PROTOCOL_VERSION, cache_file).send(&socket)?;
        let welcome = socket
            .receive(Self::WELCOME_BYTES)?
            .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
        match OwnerFrame::decode(welcome)? {
            OwnerFrame::Welcome {
                version: PROTOCOL_VERSION,
            } => {}
            _ => return Err(FrameReader::malformed("welcome")),
        }
        socket.set_read_timeout(None)?;
        Ok(socket)
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

    /// The connection over `socket`, its threads started: the reader
    /// answers each call, and tells `reads` of each read from disk.
    fn start(socket: FrameSocket, reads: Arc<dyn ReadPolicy>) -> Result<Self> {
        let (outgoing, frames) = crossbeam_channel::unbounded::<Vec<u8>>();
        let writing = socket.try_clone()?;
        let writer = thread::Builder::new()
            .name("t4-client-writer".into())
            .spawn(move || {
                for frame in frames {
                    if writing.send(&frame).is_err() {
                        // The owner is gone: the reader sees it too.
                        writing.shut_down();
                        return;
                    }
                }
            })?;
        let calls = std::sync::Arc::new(CallTable::default());
        let reader = OwnerFrames {
            socket: socket.try_clone()?,
            calls: std::sync::Arc::clone(&calls),
            reads,
        };
        let reader = thread::Builder::new()
            .name("t4-client-reader".into())
            .spawn(move || reader.run())?;
        Ok(Self {
            socket,
            calls,
            next_read: AtomicU64::new(0),
            outgoing: Some(outgoing),
            writer: Some(writer),
            reader: Some(reader),
        })
    }

    /// Calls the owner: sends the frame `call` makes of the call's number,
    /// and waits for the answer.
    ///
    /// # Errors
    ///
    /// If the owner is gone, or goes before it answers.
    async fn call(&self, call: impl FnOnce(u64) -> ClientFrame) -> Result<OwnerFrame> {
        let (number, answer) = self.calls.wait()?;
        self.tell(&call(number));
        OwnerAnswer(answer).await
    }

    /// Sends `frame`, which takes no answer; dropped if the owner is gone.
    fn tell(&self, frame: &ClientFrame) {
        if let Some(outgoing) = &self.outgoing {
            // Fails only once the writer has gone with the owner.
            let _ = outgoing.send(frame.encode());
        }
    }

    /// Whether the owner has gone: the connection closed, or broke, and
    /// every call fails.
    pub(in crate::cache) fn owner_gone(&self) -> bool {
        self.calls.owner_gone()
    }

    /// The error of a call the owner answered with a frame no call of its
    /// kind takes.
    fn unexpected(answer: &OwnerFrame) -> Error {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the owner answered a call with {answer:?}"),
        ))
    }
}

impl Drop for OwnerConnection {
    fn drop(&mut self) {
        self.outgoing.take();
        self.socket.shut_down();
        for thread in [self.writer.take(), self.reader.take()]
            .into_iter()
            .flatten()
        {
            let _ = thread.join();
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
    /// A call's number, and where its answer comes.
    ///
    /// # Errors
    ///
    /// If the owner is gone.
    fn wait(&self) -> Result<(u64, Arc<TaskCompletion<OwnerFrame>>)> {
        let mut calls = self.calls();
        if calls.owner_gone {
            return Err(Self::gone_error());
        }
        let number = calls.next;
        calls.next += 1;
        let answer = Arc::new(TaskCompletion::new());
        calls.waiting.insert(number, Arc::clone(&answer));
        Ok((number, answer))
    }

    /// Answers the call `answer` names.
    ///
    /// # Errors
    ///
    /// If it names no call waiting, or isn't an answer.
    fn answer(&self, answer: OwnerFrame) -> Result<()> {
        let waiting = answer
            .call()
            .and_then(|call| self.calls().waiting.remove(&call));
        let Some(waiting) = waiting else {
            return Err(FrameReader::malformed("answer: to no call waiting"));
        };
        waiting.complete(Ok(answer));
        Ok(())
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

/// An answer to a call, once the owner sends it.
struct OwnerAnswer(Arc<TaskCompletion<OwnerFrame>>);

impl Future for OwnerAnswer {
    type Output = Result<OwnerFrame>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_result(cx)
    }
}

/// The connection's reader: it takes the owner's frames, each answer
/// completing its call and each read from disk told to the client's read
/// policy, until the owner goes or breaks the protocol; then it fails the
/// calls waiting, and every call after.
struct OwnerFrames {
    socket: FrameSocket,
    calls: std::sync::Arc<CallTable>,
    reads: Arc<dyn ReadPolicy>,
}

impl OwnerFrames {
    fn run(self) {
        let _ = self.take_frames();
        self.calls.fail_all();
        self.socket.shut_down();
    }

    /// Takes the owner's frames until it closes the connection.
    ///
    /// # Errors
    ///
    /// If the socket fails, or the owner sends a frame the protocol doesn't
    /// allow.
    fn take_frames(&self) -> Result<()> {
        while let Some(body) = self.socket.receive(FrameSocket::MOST_BYTES)? {
            match OwnerFrame::decode(body)? {
                OwnerFrame::DiskRead {
                    drive,
                    bytes,
                    phase,
                } => {
                    let read = Io::DiskRead { drive, bytes };
                    self.reads.on_io(&IoEvent::new(read, phase));
                }
                answer => self.calls.answer(answer)?,
            }
        }
        Ok(())
    }
}

/// A client's read through its owner (`doc/11-shared-mode.md`, decision
/// 5): its number on the connection, the read's next step as the owner
/// answered it, and, once served, its bytes. Its client plans its GETs and
/// makes them; the owner does the rest.
pub(in crate::cache) struct ClientRead<'c> {
    number: ReadNumber<'c>,
    path: &'c [u8],
    version: FileVersion,
    /// Where the read's bytes start in the file.
    start: u64,
    next: Option<NextStep>,
    served: Option<Bytes>,
}

impl<'c> ClientRead<'c> {
    /// The most bytes a client's read takes: its bytes come back in one
    /// frame (decision 5), which holds 2 GiB.
    const MOST_BYTES: u64 = 1 << 30;

    /// Starts a read through `owner` of `range` of the version of the file
    /// at `path` with `e_tag` and `size`, within it, for `intent`. `None`
    /// if the cache can't hold the file, or a client's read of that many
    /// bytes.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    pub(in crate::cache) async fn start(
        owner: &'c OwnerConnection,
        path: &'c [u8],
        e_tag: &str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
    ) -> Result<Option<Self>> {
        if range.end - range.start > Self::MOST_BYTES {
            return Ok(None);
        }
        let number = ReadNumber::take(owner);
        let start = range.start;
        let answer = owner
            .call(|call| ClientFrame::StartRead {
                call,
                read: number.number,
                path: Bytes::copy_from_slice(path),
                e_tag: e_tag.to_owned(),
                size,
                range,
                intent,
            })
            .await?;
        match answer {
            OwnerFrame::Started { version, next, .. } => Ok(Some(Self {
                number,
                path,
                version,
                start,
                next: Some(next),
                served: None,
            })),
            OwnerFrame::Uncacheable { .. } => Ok(None),
            answer => Err(Self::failed(&answer)),
        }
    }

    /// The read's file, as the owner named its version.
    pub(in crate::cache) fn file(&self) -> CacheableFile<'c> {
        CacheableFile::named(self.path, self.version)
    }

    /// The run of the read the owner located last, for the client to plan
    /// its GETs; `None` once its GETs are planned.
    pub(in crate::cache) fn locate_next_run(&mut self) -> Option<LocatedRun> {
        match self.next.take() {
            Some(NextStep::Plan(run)) => Some(run),
            other => {
                self.next = other;
                None
            }
        }
    }

    /// Hands the owner `gets`, planned for the run it located last: it
    /// answers with the next run, or the read's fetches.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    pub(in crate::cache) async fn take_gets(&mut self, gets: &ReadPlan) -> Result<()> {
        let read = self.number.number;
        let answer = self
            .number
            .owner
            .call(|call| ClientFrame::TakeGets {
                call,
                read,
                plan: gets.clone(),
            })
            .await?;
        match answer {
            OwnerFrame::Next { next, .. } => {
                self.next = Some(next);
                Ok(())
            }
            answer => Err(Self::failed(&answer)),
        }
    }

    /// The read's fetches, which the client makes, as the owner answered
    /// once every run was planned; none after.
    pub(in crate::cache) fn fetches(&mut self) -> Vec<ClientFetch<'c>> {
        let Some(NextStep::Fetch(specs)) = self.next.take() else {
            return Vec::new();
        };
        (0..)
            .zip(specs)
            .map(|(fetch, spec)| ClientFetch {
                owner: self.number.owner,
                read: self.number.number,
                fetch,
                spec,
                file: self.version.id(),
                settled: false,
            })
            .collect()
    }

    /// Waits for the read's work in the owner, done once the owner has
    /// served what it holds and the read's fetches are committed or
    /// abandoned: whether blocks are left to fetch, those it waited for
    /// that no fetch cached, its next step.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    pub(in crate::cache) async fn finish(&mut self) -> Result<bool> {
        let read = self.number.number;
        let answer = self
            .number
            .owner
            .call(|call| ClientFrame::Finish { call, read })
            .await?;
        match answer {
            OwnerFrame::Retry { next, .. } => {
                self.next = Some(next);
                Ok(true)
            }
            OwnerFrame::Served { bytes, .. } => {
                self.served = bytes;
                Ok(false)
            }
            answer => Err(Self::failed(&answer)),
        }
    }

    /// The read's bytes, once served: in an output of the client's own, so
    /// that their address agrees with their file offset mod 4 KiB, as an
    /// owner's read's does. `None` if the owner served none, as after a read
    /// from disk failed, or the output can't be made.
    pub(in crate::cache) fn into_bytes(self) -> Option<Bytes> {
        let served = self.served.as_ref()?;
        let bytes = self.start..self.start + served.len() as u64;
        let mut output = ReadOutput::new(bytes).ok()?;
        output.copy_in(self.start, served);
        output.into_bytes()
    }

    /// The error of a read the owner answered `answer`: failed it, or
    /// answered as no call of the kind takes.
    fn failed(answer: &OwnerFrame) -> Error {
        match answer {
            OwnerFrame::Failed { .. } => Error::Io(io::Error::other("the owner failed the read")),
            answer => OwnerConnection::unexpected(answer),
        }
    }
}

/// A read's number on its connection, from its start: dropped, it ends
/// the read in the owner, served or not, which drops all it holds of it.
struct ReadNumber<'c> {
    owner: &'c OwnerConnection,
    number: u64,
}

impl<'c> ReadNumber<'c> {
    fn take(owner: &'c OwnerConnection) -> Self {
        Self {
            owner,
            number: owner.next_read.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl Drop for ReadNumber<'_> {
    fn drop(&mut self) {
        self.owner.tell(&ClientFrame::EndRead { read: self.number });
    }
}

/// One of a client's read's fetches (`ClientRead::fetches`): the bytes of
/// its GET, sent to the owner as they come in, which lands them; then
/// committed, the blocks cached as the owner said. Dropped before, as when
/// the GET fails, it is abandoned: the owner caches none of it, and the
/// reads waiting on its blocks fetch for themselves.
pub(in crate::cache) struct ClientFetch<'c> {
    owner: &'c OwnerConnection,
    read: u64,
    /// Its place in the read's fetches.
    fetch: u32,
    spec: FetchSpec,
    file: FileVersionId,
    /// Committed or abandoned.
    settled: bool,
}

impl ClientFetch<'_> {
    /// The file's bytes the fetch holds.
    pub(in crate::cache) fn range(&self) -> Range<u64> {
        self.spec.bytes()
    }

    /// The version of the file they are of, as the owner named it.
    pub(in crate::cache) fn file(&self) -> FileVersionId {
        self.file
    }

    /// How many identical requests race for them.
    pub(in crate::cache) fn race(&self) -> NonZeroU8 {
        self.spec.race()
    }

    /// When one more request races them, if they aren't all in.
    pub(in crate::cache) fn late_race(&self) -> Option<Duration> {
        self.spec.late_race()
    }

    /// Sends `piece`, the fetch's next bytes, to the owner.
    pub(in crate::cache) fn land(&mut self, piece: &[u8]) {
        self.owner.tell(&ClientFrame::Land {
            read: self.read,
            fetch: self.fetch,
            bytes: Bytes::copy_from_slice(piece),
        });
    }

    /// Commits the fetch, every byte in: the owner caches its blocks where
    /// the read said. An owner gone is no error here: the read finds it,
    /// and goes to the remote.
    ///
    /// # Errors
    ///
    /// If the owner found the fetch brought fewer bytes than it holds, or
    /// more: nothing is cached.
    pub(in crate::cache) async fn finish(mut self) -> Result<()> {
        self.settled = true;
        let (read, fetch) = (self.read, self.fetch);
        let answer = self
            .owner
            .call(|call| ClientFrame::Commit { call, read, fetch })
            .await;
        match answer {
            Ok(OwnerFrame::Committed { .. }) | Err(_) => Ok(()),
            Ok(OwnerFrame::Failed { .. }) => Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the fetch of {:?} brought other bytes than it holds",
                    self.range()
                ),
            ))),
            Ok(answer) => Err(OwnerConnection::unexpected(&answer)),
        }
    }
}

impl Drop for ClientFetch<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.owner.tell(&ClientFrame::Abandon {
                read: self.read,
                fetch: self.fetch,
            });
        }
    }
}
