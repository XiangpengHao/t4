use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::ops::Range;
use std::rc::Rc;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{AbortHandle, Abortable, LocalBoxFuture, join};
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::session_waker::SessionWaker;
use crate::cache::cache_stores::shared::protocol::{
    ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, OwnerRegion, SpanName,
};
use crate::cache::policy::{FetchRuns, Io, IoEvent, LocatedRead, ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::FileVersion;
use crate::cache::storage::{FetchedBlocks, ObjectCache, PartialRead, TierWork};
use crate::io::error::Result;

pub(super) struct ClientReads<'c> {
    cache: &'c ObjectCache,
    region: Option<std::sync::Arc<OwnerRegion>>,
    outbox: Outbox,
    reads: HashMap<u64, ReadInbox>,
    tasks: FuturesUnordered<Abortable<LocalBoxFuture<'c, ()>>>,
}

impl<'c> ClientReads<'c> {
    pub(super) fn new(
        cache: &'c ObjectCache,
        region: Option<std::sync::Arc<OwnerRegion>>,
        session: std::sync::Arc<SessionWaker>,
    ) -> Self {
        Self {
            cache,
            region,
            outbox: Outbox::new(session),
            reads: HashMap::new(),
            tasks: FuturesUnordered::new(),
        }
    }

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
            ClientFrame::KnownSize { call, path } => {
                let size = self.cache.known_size(&path);
                self.outbox.send(OwnerFrame::KnownSize { call, size });
                Ok(())
            }
            ClientFrame::EndRead { read } => {
                // Ended goes with the last of its inbox, its task and the buffers over its spans.
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

    pub(super) fn run(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(_)) = self.tasks.poll_next_unpin(cx) {}
    }

    pub(super) fn take_outgoing(&self) -> Result<Vec<OwnerFrame>> {
        self.outbox.take()
    }

    fn no_read() -> crate::io::error::Error {
        FrameReader::malformed("read number: of no read started")
    }
}

struct ReadInbox {
    frames: Rc<RefCell<ReadFrames>>,
    abort: AbortHandle,
    _end: std::sync::Arc<ReadEnd>,
}

impl ReadInbox {
    /// Drops `frame` if the task ended, the read served or failed before the client knew.
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

#[derive(Default)]
struct ReadFrames {
    queued: VecDeque<ClientFrame>,
    waker: Option<Waker>,
}

/// Its last holder may be a buffer over the read's spans, dropped by I/O on another thread.
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

/// Queued, as a read's policy hears its reads from disk deep in a poll.
#[derive(Clone)]
struct Outbox {
    frames: std::sync::Arc<Mutex<OutboxFrames>>,
    session: std::sync::Arc<SessionWaker>,
}

#[derive(Default)]
struct OutboxFrames {
    frames: Vec<OwnerFrame>,
    broken: Option<&'static str>,
}

impl Outbox {
    fn new(session: std::sync::Arc<SessionWaker>) -> Self {
        Self {
            frames: std::sync::Arc::default(),
            session,
        }
    }

    fn send(&self, frame: OwnerFrame) {
        self.frames().frames.push(frame);
    }

    fn send_waking(&self, frame: OwnerFrame) {
        self.send(frame);
        self.session.wake_to_send();
    }

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

struct ReadStart {
    call: u64,
    path: Bytes,
    e_tag: String,
    size: u64,
    range: Range<u64>,
    intent: Option<ReadIntent>,
    output: Option<(std::sync::Arc<OwnerRegion>, SpanName)>,
}

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

struct ServedRead<'c> {
    cache: &'c ObjectCache,
    region: Option<std::sync::Arc<OwnerRegion>>,
    outbox: Outbox,
    frames: Rc<RefCell<ReadFrames>>,
    end: std::sync::Arc<ReadEnd>,
}

impl<'c> ServedRead<'c> {
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
            // A span that can't hold the read is the client's mistake: it breaks the protocol.
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
            // The client may send the round's finish with its start, before its runs are planned.
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

    fn fail(&self, answer: StepAnswer, finish: Option<u64>) {
        for call in std::iter::once(answer.call()).chain(finish) {
            self.outbox.send(OwnerFrame::Failed { call });
        }
    }

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

    /// The read's task is dropped if the client ends the read first.
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

enum LandingFetch<'c> {
    Landing(FetchedBlocks<'c>),
    Overflowed,
    Settled,
}

impl LandingFetch<'_> {
    fn is_open(&self) -> bool {
        !matches!(self, Self::Settled)
    }

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

    async fn commit(&mut self) -> std::result::Result<bool, &'static str> {
        match std::mem::replace(self, Self::Settled) {
            Self::Landing(blocks) => Ok(blocks.finish().await.is_ok()),
            Self::Overflowed => Ok(false),
            Self::Settled => Err("commit: after its fetch was settled"),
        }
    }

    fn abandon(&mut self) -> std::result::Result<(), &'static str> {
        match std::mem::replace(self, Self::Settled) {
            Self::Landing(_) | Self::Overflowed => Ok(()),
            Self::Settled => Err("abandon: after its fetch was settled"),
        }
    }
}
