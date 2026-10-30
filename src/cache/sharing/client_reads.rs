//! [`ClientReads`]: a client's reads, as its owner serves them
//! (`doc/11-shared-mode.md`, decision 5). Each read is a task on the
//! client's session thread that takes the read's frames in the order they
//! came and runs its storage half, as `T4Caching::read_through` runs an
//! owner's own: the client plans the GETs of each run the task locates, and
//! lands their bytes, which the task lands as an owner's fetches land.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::ops::Range;
use std::rc::Rc;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread::{self, Thread};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{AbortHandle, Abortable, LocalBoxFuture, join};
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::frames::{ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame};
use super::region::{OwnerRegion, SpanName};
use crate::cache::block_index::FileVersion;
use crate::cache::object_cache::{FetchedBlocks, ObjectCache, PartialRead, TierWork};
use crate::cache::policy::{FetchRuns, Io, IoEvent, LocatedRead, ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::read_output::ReadOutput;
use crate::io::error::Result;

/// The reads of one client in flight, each a task, by the number the
/// client gave it; and the frames they send it, queued.
pub(super) struct ClientReads<'c> {
    cache: &'c ObjectCache,
    /// The client's region, if the owner mapped it: its reads' outputs are
    /// spans of it.
    region: Option<std::sync::Arc<OwnerRegion>>,
    outbox: Outbox,
    /// Each read the client started and hasn't ended. A read's task may
    /// have ended before: the frames the client sent it since are dropped.
    reads: HashMap<u64, ReadInbox>,
    tasks: FuturesUnordered<Abortable<LocalBoxFuture<'c, ()>>>,
}

impl<'c> ClientReads<'c> {
    /// No reads yet, of a client whose region, if any, is `region`; made
    /// on the session's thread, which a read's end wakes, from whatever
    /// thread it comes.
    pub(super) fn new(cache: &'c ObjectCache, region: Option<std::sync::Arc<OwnerRegion>>) -> Self {
        Self {
            cache,
            region,
            outbox: Outbox::new(thread::current()),
            reads: HashMap::new(),
            tasks: FuturesUnordered::new(),
        }
    }

    /// Takes `frame`, the client's next: starts a read, hands a frame to
    /// its read, or ends a read, its task dropped with all it holds: its
    /// claims end, and the reads waiting on them fetch for themselves.
    ///
    /// # Errors
    ///
    /// If the protocol doesn't allow the frame: a hello once admitted, a
    /// read's number started twice, or a frame of a read not started or
    /// ended already.
    pub(super) fn take(&mut self, frame: ClientFrame) -> Result<()> {
        match frame {
            ClientFrame::Hello { .. } => Err(FrameReader::malformed("hello: once admitted")),
            ClientFrame::StartRead {
                call,
                read,
                path,
                e_tag,
                size,
                range,
                intent,
                output,
            } => {
                if self.reads.contains_key(&read) {
                    return Err(FrameReader::malformed("read number: started twice"));
                }
                let output = match (output, &self.region) {
                    (Some(span), Some(region)) => Some((std::sync::Arc::clone(region), span)),
                    (Some(_), None) => {
                        return Err(FrameReader::malformed("output: a span of no region"));
                    }
                    (None, _) => None,
                };
                let (abort, registration) = AbortHandle::new_pair();
                let frames = Rc::default();
                let end = std::sync::Arc::new(ReadEnd {
                    read,
                    outbox: self.outbox.clone(),
                });
                let served = ServedRead {
                    cache: self.cache,
                    region: self.region.clone(),
                    outbox: self.outbox.clone(),
                    frames: Rc::clone(&frames),
                    end: std::sync::Arc::clone(&end),
                };
                let start = ReadStart {
                    call,
                    path,
                    e_tag,
                    size,
                    range,
                    intent,
                    output,
                };
                let task = served.serve(start).boxed_local();
                self.tasks.push(Abortable::new(task, registration));
                let inbox = ReadInbox {
                    frames,
                    abort,
                    _end: end,
                };
                self.reads.insert(read, inbox);
                Ok(())
            }
            ClientFrame::EndRead { read } => {
                // The read's end goes with the last of its inbox, its task
                // and the buffers over its spans.
                let ended = self.reads.remove(&read).ok_or_else(Self::no_read)?;
                ended.abort.abort();
                Ok(())
            }
            ClientFrame::TakeGets { read, .. }
            | ClientFrame::Land { read, .. }
            | ClientFrame::LandStaged { read, .. }
            | ClientFrame::Commit { read, .. }
            | ClientFrame::Abandon { read, .. }
            | ClientFrame::Finish { read, .. } => {
                self.reads
                    .get(&read)
                    .ok_or_else(Self::no_read)?
                    .deliver(frame);
                Ok(())
            }
        }
    }

    /// Runs the reads until none can go on: the waker in `cx` is woken once
    /// one can.
    pub(super) fn run(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(_)) = self.tasks.poll_next_unpin(cx) {}
    }

    /// The frames the reads queued for the client since last taken.
    ///
    /// # Errors
    ///
    /// If a read took a frame the protocol doesn't allow where it came.
    pub(super) fn take_outgoing(&self) -> Result<Vec<OwnerFrame>> {
        self.outbox.take()
    }

    fn no_read() -> crate::io::error::Error {
        FrameReader::malformed("read number: of no read started")
    }
}

/// Where the frames of one of the client's reads wait for its task, and
/// what ends the task; and a hold on the read's end, until the client ends
/// it.
struct ReadInbox {
    frames: Rc<RefCell<ReadFrames>>,
    abort: AbortHandle,
    _end: std::sync::Arc<ReadEnd>,
}

impl ReadInbox {
    /// Hands `frame` to the read's task; drops it if the task has ended,
    /// the read served or failed before the client knew.
    fn deliver(&self, frame: ClientFrame) {
        // The task holds the other handle on its frames as long as it runs.
        if Rc::strong_count(&self.frames) == 1 {
            return;
        }
        let mut frames = self.frames.borrow_mut();
        frames.queued.push_back(frame);
        if let Some(waker) = frames.waker.take() {
            waker.wake();
        }
    }
}

/// A read's frames not taken yet, and the waker of its task waiting for
/// the next.
#[derive(Default)]
struct ReadFrames {
    queued: VecDeque<ClientFrame>,
    waker: Option<Waker>,
}

/// The end of one of the client's reads: once the client has ended it, its
/// task has dropped, and so has every buffer over its spans, which the last
/// I/O into them may drop on another thread, the owner tells the client so
/// ([`OwnerFrame::Ended`]), which then takes the spans back.
struct ReadEnd {
    read: u64,
    outbox: Outbox,
}

impl Drop for ReadEnd {
    fn drop(&mut self) {
        self.outbox
            .send_waking(OwnerFrame::Ended { read: self.read });
    }
}

/// The frames a session sends its client, queued as its reads make them:
/// answers, and news of the reads from disk made for them, which the read
/// policy of a read hears deep in a poll; and whether a read found the
/// client breaking the protocol. The session's thread sends them; a frame
/// queued from another thread wakes it.
#[derive(Clone)]
struct Outbox {
    frames: std::sync::Arc<Mutex<OutboxFrames>>,
    session: Thread,
}

#[derive(Default)]
struct OutboxFrames {
    frames: Vec<OwnerFrame>,
    /// The frame a read was sent where the protocol doesn't allow it.
    broken: Option<&'static str>,
}

impl Outbox {
    fn new(session: Thread) -> Self {
        Self {
            frames: std::sync::Arc::default(),
            session,
        }
    }

    fn send(&self, frame: OwnerFrame) {
        self.frames().frames.push(frame);
    }

    /// Sends `frame` from any thread: the session's wakes to send it.
    fn send_waking(&self, frame: OwnerFrame) {
        self.send(frame);
        self.session.unpark();
    }

    /// Notes the client sent `frame`, a frame the protocol doesn't allow
    /// where it came: the session closes the connection.
    fn break_protocol(&self, frame: &'static str) {
        self.frames().broken.get_or_insert(frame);
    }

    fn take(&self) -> Result<Vec<OwnerFrame>> {
        let mut frames = self.frames();
        match frames.broken {
            Some(frame) => Err(FrameReader::malformed(frame)),
            None => Ok(std::mem::take(&mut frames.frames)),
        }
    }

    fn frames(&self) -> std::sync::MutexGuard<'_, OutboxFrames> {
        self.frames.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The read policy of a client's read in its owner: it tells the client of
/// each read from disk the read makes, for the client's own read policy.
/// It plans nothing: the storage half never asks it, as the client plans.
struct ForwardedDiskReads {
    outbox: Outbox,
}

impl ReadPolicy for ForwardedDiskReads {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        FetchRuns::default().plan(read)
    }

    fn on_io(&self, event: &IoEvent<'_>) {
        if let Io::DiskRead { drive, bytes } = *event.io() {
            self.outbox.send(OwnerFrame::DiskRead {
                drive,
                bytes,
                phase: event.phase(),
            });
        }
    }
}

/// What a client's read starts as: [`ClientFrame::StartRead`]'s fields,
/// its output's span with the region it is of.
struct ReadStart {
    call: u64,
    path: Bytes,
    e_tag: String,
    size: u64,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    output: Option<(std::sync::Arc<OwnerRegion>, SpanName)>,
}

/// The call a read's next step answers: its start, its GETs taken, or its
/// finish, with blocks left to fetch.
#[derive(Clone, Copy)]
enum StepAnswer {
    Started { call: u64, version: FileVersion },
    Next { call: u64 },
    Retry { call: u64 },
}

impl StepAnswer {
    fn call(self) -> u64 {
        match self {
            Self::Started { call, .. } | Self::Next { call } | Self::Retry { call } => call,
        }
    }

    fn with(self, next: NextStep) -> OwnerFrame {
        match self {
            Self::Started { call, version } => OwnerFrame::Started {
                call,
                version,
                next,
            },
            Self::Next { call } => OwnerFrame::Next { call, next },
            Self::Retry { call } => OwnerFrame::Retry { call, next },
        }
    }
}

/// One of the client's reads, served: its task, which holds the read's end
/// until it drops; and the client's region, if the owner mapped it, where
/// the read's fetches may stage their bytes.
struct ServedRead<'c> {
    cache: &'c ObjectCache,
    region: Option<std::sync::Arc<OwnerRegion>>,
    outbox: Outbox,
    frames: Rc<RefCell<ReadFrames>>,
    end: std::sync::Arc<ReadEnd>,
}

impl<'c> ServedRead<'c> {
    /// Serves the read `start` asks for, as `T4Caching::read_through`
    /// serves an owner's, its GETs the client's: each run located is
    /// planned by the client, its GETs' bytes landed as they come, and,
    /// once the read's work on the tiers is done, the read served, or its
    /// blocks no fetch cached fetched again. A read the cache fails is
    /// answered `Failed`, and ends.
    async fn serve(self, start: ReadStart) {
        let ReadStart {
            call,
            path,
            e_tag,
            size,
            range,
            intent,
            output,
        } = start;
        let Ok(file) = self.cache.resolve(&path, Some(&e_tag), size) else {
            self.outbox.send(OwnerFrame::Uncacheable { call });
            return;
        };
        let reads = ForwardedDiskReads {
            outbox: self.outbox.clone(),
        };
        let in_span = output.is_some();
        let make_output = |asked: Range<u64>| match output {
            Some((region, span)) => region.output(span, asked, std::sync::Arc::clone(&self.end)),
            None => ReadOutput::new(asked),
        };
        let started = self
            .cache
            .start_read_into(&file, range.clone(), intent, &reads, make_output);
        let (mut read, mut work) = match started {
            Ok(started) => started,
            // A span past the client's region, or too short for the read,
            // is the client's mistake: it breaks the protocol.
            Err(_) if in_span => {
                self.outbox
                    .break_protocol("output: a span that can't hold it");
                return;
            }
            Err(_) => {
                self.outbox.send(OwnerFrame::Failed { call });
                return;
            }
        };
        let mut answer = StepAnswer::Started {
            call,
            version: file.version(),
        };
        loop {
            // The round's finish: the client may send it with the round's
            // start, before its runs are planned.
            let mut finish = None;
            let Some(fetches) = self.plan(&mut read, &mut work, answer, &mut finish).await else {
                return;
            };
            let landed = self.land(fetches, finish);
            let (finish, worked) = join(landed, work.finish(&mut read)).await;
            let Some(call) = finish else {
                return;
            };
            match worked {
                Ok(left) if left.is_empty() => {
                    let served = if !in_span {
                        let bytes = read.into_bytes();
                        OwnerFrame::Served { call, bytes }
                    } else {
                        let asked = read.bytes();
                        match read.filled_in_place() {
                            true => OwnerFrame::ServedInSpan { call, asked },
                            false => OwnerFrame::Served { call, bytes: None },
                        }
                    };
                    self.outbox.send(served);
                    return;
                }
                Ok(left) => {
                    work = self.cache.retry(&file, range.clone(), intent, left, &reads);
                    answer = StepAnswer::Retry { call };
                }
                Err(_) => {
                    self.outbox.send(OwnerFrame::Failed { call });
                    return;
                }
            }
        }
    }

    /// Locates `work`'s runs one at a time, each sent to the client, the
    /// first in `answer`, and the GETs it plans for each taken; then takes
    /// the read's fetches, sent in the answer to the last call. The round's
    /// finish, if the client sends it meanwhile, is held in `finish`. `None`
    /// if the read ends: the cache failed it, which fails the finish held
    /// too, or the client sent a frame other than a run's GETs or the
    /// round's one finish.
    async fn plan(
        &self,
        read: &mut PartialRead,
        work: &mut TierWork<'_>,
        mut answer: StepAnswer,
        finish: &mut Option<u64>,
    ) -> Option<Vec<FetchedBlocks<'c>>> {
        loop {
            match work.locate_next_run(read) {
                Ok(Some(run)) => {
                    self.outbox.send(answer.with(NextStep::Plan(run)));
                    let (call, plan) = self.run_gets(finish).await?;
                    work.take_gets(read, &plan);
                    answer = StepAnswer::Next { call };
                }
                Ok(None) => break,
                Err(_) => {
                    self.fail(answer, *finish);
                    return None;
                }
            }
        }
        let mut fetches = Vec::new();
        for bytes in read.fetches() {
            let Ok(blocks) = self.cache.receive(read, bytes) else {
                self.fail(answer, *finish);
                return None;
            };
            fetches.push(blocks);
        }
        let specs = fetches
            .iter()
            .map(|blocks| FetchSpec::new(blocks.range(), blocks.race(), blocks.late_race()))
            .collect();
        self.outbox.send(answer.with(NextStep::Fetch(specs)));
        Some(fetches)
    }

    /// The GETs the client planned for the run located last: the call
    /// that sent them, and the plan. The round's finish, if it comes first,
    /// is held in `finish`. `None` if the client sent another frame, or a
    /// second finish: it broke the protocol.
    async fn run_gets(&self, finish: &mut Option<u64>) -> Option<(u64, ReadPlan)> {
        loop {
            match self.next_frame().await {
                ClientFrame::TakeGets { call, plan, .. } => return Some((call, plan)),
                ClientFrame::Finish { call, .. } if finish.is_none() => *finish = Some(call),
                _ => {
                    self.outbox.break_protocol("frame: a run's GETs were due");
                    return None;
                }
            }
        }
    }

    /// Answers `answer`'s call, and the round's `finish` if the client sent
    /// it already, `Failed`: the cache failed the read, which ends.
    fn fail(&self, answer: StepAnswer, finish: Option<u64>) {
        for call in std::iter::once(answer.call()).chain(finish) {
            self.outbox.send(OwnerFrame::Failed { call });
        }
    }

    /// Takes the read's frames as the client lands `fetches`, by their
    /// place in the list: each one's bytes landed in order, from the frame
    /// or from where the client staged them in its region, committed, or
    /// abandoned; until the client has asked to finish, in `finish` already
    /// or in a frame now, and every fetch is committed or abandoned. Returns
    /// the finish's call; `None` if the client sent a frame the protocol
    /// doesn't allow.
    async fn land(&self, fetches: Vec<FetchedBlocks<'c>>, mut finish: Option<u64>) -> Option<u64> {
        let mut fetches: Vec<_> = fetches.into_iter().map(LandingFetch::Landing).collect();
        while finish.is_none() || fetches.iter().any(LandingFetch::is_open) {
            let frame = self.next_frame().await;
            let landed = match frame {
                ClientFrame::Land { fetch, bytes, .. } => match fetches.get_mut(fetch as usize) {
                    Some(landing) => landing.land(&bytes).await,
                    None => Err("fetch number: of no fetch"),
                },
                ClientFrame::LandStaged { fetch, staged, .. } => {
                    match (fetches.get_mut(fetch as usize), &self.region) {
                        // Copied where they go as they land, and let go of.
                        (Some(landing), Some(region)) => match region.staged(staged) {
                            Ok(bytes) => landing.land(bytes).await,
                            Err(_) => Err("staged bytes: past the region"),
                        },
                        (None, _) => Err("fetch number: of no fetch"),
                        (Some(_), None) => Err("staged bytes: in no region"),
                    }
                }
                ClientFrame::Commit { call, fetch, .. } => match fetches.get_mut(fetch as usize) {
                    Some(landing) => match landing.commit().await {
                        Ok(committed) => {
                            let answer = if committed {
                                OwnerFrame::Committed { call }
                            } else {
                                OwnerFrame::Failed { call }
                            };
                            self.outbox.send(answer);
                            Ok(())
                        }
                        Err(misuse) => Err(misuse),
                    },
                    None => Err("fetch number: of no fetch"),
                },
                ClientFrame::Abandon { fetch, .. } => match fetches.get_mut(fetch as usize) {
                    Some(landing) => landing.abandon(),
                    None => Err("fetch number: of no fetch"),
                },
                ClientFrame::Finish { call, .. } if finish.is_none() => {
                    finish = Some(call);
                    Ok(())
                }
                _ => Err("frame: a fetch's or the finish was due"),
            };
            if let Err(misuse) = landed {
                self.outbox.break_protocol(misuse);
                return None;
            }
        }
        finish
    }

    /// The read's next frame from the client, once it comes. The read's
    /// task is dropped if the client ends the read first.
    async fn next_frame(&self) -> ClientFrame {
        poll_fn(|cx| {
            let mut frames = self.frames.borrow_mut();
            match frames.queued.pop_front() {
                Some(frame) => Poll::Ready(frame),
                None => {
                    frames.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }
}

/// One of a read's fetches, as the client lands it.
enum LandingFetch<'c> {
    Landing(FetchedBlocks<'c>),
    /// It brought more bytes than it holds: its blocks go uncached, and its
    /// commit fails.
    Overflowed,
    /// Committed or abandoned.
    Settled,
}

impl LandingFetch<'_> {
    /// Whether the client hasn't committed or abandoned it yet.
    fn is_open(&self) -> bool {
        !matches!(self, Self::Settled)
    }

    /// Lands `bytes`, the fetch's next.
    ///
    /// # Errors
    ///
    /// The misuse, if the fetch is settled.
    async fn land(&mut self, bytes: &[u8]) -> std::result::Result<(), &'static str> {
        match self {
            Self::Landing(blocks) => {
                if blocks.land(bytes).await.is_err() {
                    *self = Self::Overflowed;
                }
                Ok(())
            }
            Self::Overflowed => Ok(()),
            Self::Settled => Err("land: after its fetch was settled"),
        }
    }

    /// Commits the fetch: caches its blocks, if it brought all its bytes
    /// and no more. Whether it did.
    ///
    /// # Errors
    ///
    /// The misuse, if the fetch is settled.
    async fn commit(&mut self) -> std::result::Result<bool, &'static str> {
        match std::mem::replace(self, Self::Settled) {
            Self::Landing(blocks) => Ok(blocks.finish().await.is_ok()),
            Self::Overflowed => Ok(false),
            Self::Settled => Err("commit: after its fetch was settled"),
        }
    }

    /// Abandons the fetch: its blocks go uncached, and the reads waiting on
    /// them fetch for themselves.
    ///
    /// # Errors
    ///
    /// The misuse, if the fetch is settled.
    fn abandon(&mut self) -> std::result::Result<(), &'static str> {
        match std::mem::replace(self, Self::Settled) {
            Self::Landing(_) | Self::Overflowed => Ok(()),
            Self::Settled => Err("abandon: after its fetch was settled"),
        }
    }
}
