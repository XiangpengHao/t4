use crate::io::sync::Mutex;
use std::io;
use std::ops::Range;
use std::sync::PoisonError;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::connection::{OwnerConnection, WaitingCall};
use super::fetch::ClientFetch;
use crate::PAGE_SIZE_U64;
use crate::cache::cache_stores::shared::protocol::{
    ClientFrame, FetchSpec, MOST_READ_BYTES, NextStep, OwnerFrame, RegionSpan, SpanName,
};
use crate::cache::cache_stores::{
    CacheStoreFetch, FileVersionKey, ReadRound, RoundEnd, RoundFetches, ServedOutput,
};
use crate::cache::policy::{ReadIntent, ReadPlan, ReadPolicy};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::{CacheableFile, FileVersion};
use crate::io::error::{Error, Result};

/// What a client's read keeps from round to round.
pub(in crate::cache::cache_stores::shared) struct ClientRead<'c> {
    number: std::sync::Arc<ReadNumber>,
    path: &'c [u8],
    version: FileVersion,
    asked: Range<u64>,
    intent: Option<ReadIntent>,
    reads: &'c dyn ReadPolicy,
}

pub(in crate::cache::cache_stores::shared) struct ClientRound<'c> {
    read: ClientRead<'c>,
    next: NextStep,
    /// Sent as the round starts: the owner holds it till the round's work is done, so that a
    /// read of hits is one round trip.
    finish: WaitingCall,
}

impl<'c> ClientRead<'c> {
    pub(in crate::cache::cache_stores::shared) async fn start(
        owner: std::sync::Arc<OwnerConnection>,
        path: &'c [u8],
        e_tag: &str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        reads: &'c dyn ReadPolicy,
    ) -> Result<Option<ClientRound<'c>>> {
        if range.end - range.start > MOST_READ_BYTES {
            return Ok(None);
        }
        let number = ReadNumber::take(std::sync::Arc::clone(&owner));
        let output = owner.span(ReadOutput::padded_len(&range));
        let output = number.lend_output(output);
        let asked = range.clone();
        let read = number.number;
        let (started, finish) = owner.send_calls(
            |call| ClientFrame::StartRead {
                call,
                read,
                path: Bytes::copy_from_slice(path),
                e_tag: e_tag.to_owned(),
                size,
                range,
                intent,
                output,
            },
            |call| ClientFrame::Finish { call, read },
        )?;
        match started.await? {
            OwnerFrame::Started { version, next, .. } => {
                let read = Self {
                    number: std::sync::Arc::new(number),
                    path,
                    version,
                    asked,
                    intent,
                    reads,
                };
                Ok(Some(ClientRound { read, next, finish }))
            }
            OwnerFrame::Uncacheable { .. } => Ok(None),
            answer => Err(Self::failed(&answer)),
        }
    }

    fn failed(answer: &OwnerFrame) -> Error {
        match answer {
            OwnerFrame::Failed { .. } => Error::Io(io::Error::other("the owner failed the read")),
            answer => OwnerConnection::unexpected(answer),
        }
    }

    async fn take_gets(&self, plan: ReadPlan) -> Result<Vec<FetchSpec>> {
        let read = self.number.number;
        let answer = (self.number.owner)
            .call(|call| ClientFrame::TakeGets { call, read, plan })
            .await?;
        match answer {
            OwnerFrame::Next {
                next: NextStep::Fetch(specs),
                ..
            } => Ok(specs),
            answer => Err(Self::failed(&answer)),
        }
    }

    fn fetches(&self, specs: Vec<FetchSpec>) -> Vec<Box<dyn CacheStoreFetch<'c> + 'c>> {
        let file = FileVersionKey::new(self.number.owner.version_ids(), self.version.id());
        (0..)
            .zip(specs)
            .map(|(fetch, spec)| {
                let read = std::sync::Arc::clone(&self.number);
                Box::new(ClientFetch::new(read, fetch, spec, file))
                    as Box<dyn CacheStoreFetch<'c> + 'c>
            })
            .collect()
    }

    async fn finish(self, finish: WaitingCall) -> Result<RoundEnd<'c>> {
        match finish.await? {
            OwnerFrame::Retry { next, .. } => {
                let (owner, read) = (&self.number.owner, self.number.number);
                let finish = owner.send_call(|call| ClientFrame::Finish { call, read })?;
                let next = ClientRound {
                    read: self,
                    next,
                    finish,
                };
                Ok(RoundEnd::Retry(Box::new(next)))
            }
            OwnerFrame::Served { bytes, .. } => Ok(self.served(Served::InFrame(bytes))),
            OwnerFrame::ServedInSpan { asked, .. } => Ok(self.served(Served::InSpan(asked))),
            answer => Err(Self::failed(&answer)),
        }
    }

    fn served(self, served: Served) -> RoundEnd<'c> {
        RoundEnd::Served(Box::new(ClientOutput { read: self, served }))
    }
}

struct ClientOutput<'c> {
    read: ClientRead<'c>,
    served: Served,
}

enum Served {
    InFrame(Option<Bytes>),
    InSpan(Range<u64>),
}

impl ServedOutput for ClientOutput<'_> {
    /// Their address agrees with their file offset mod 4 KiB, as an owner's read's does.
    fn into_bytes(self: Box<Self>) -> Option<Bytes> {
        let read = &self.read;
        match self.served {
            Served::InFrame(served) => {
                let served = served?;
                let start = read.asked.start;
                let mut output = ReadOutput::new(start..start + served.len() as u64).ok()?;
                output.copy_in(start, &served);
                output.into_bytes()
            }
            Served::InSpan(asked) => {
                let at = usize::try_from(asked.start % PAGE_SIZE_U64).ok()?;
                let len = usize::try_from(asked.end - asked.start).ok()?;
                let span = read.number.take_output()?;
                let within = read.asked.start <= asked.start && asked.end <= read.asked.end;
                let fits = (at + len) as u64 <= span.name().len();
                (within && fits).then(|| span.into_bytes(at..at + len))
            }
        }
    }
}

impl<'c> ReadRound<'c> for ClientRound<'c> {
    fn plan_gets(self: Box<Self>) -> BoxFuture<'c, Result<RoundFetches<'c>>> {
        Box::pin(async move {
            let Self { read, next, finish } = *self;
            let specs = match next {
                NextStep::Plan(run) => {
                    let file = CacheableFile::named(read.path, read.version);
                    let gets = run.planned_by(read.reads, &file, read.intent);
                    read.take_gets(gets).await?
                }
                NextStep::Fetch(specs) => specs,
            };
            let fetches = read.fetches(specs);
            let ended = Box::pin(read.finish(finish));
            Ok(RoundFetches { fetches, ended })
        })
    }
}

/// Shared by the read and its fetches, the last to drop ends the read: no frame of the read's
/// may go after its end.
pub(super) struct ReadNumber {
    /// Outlives the store's handle, which is replaced once its owner goes.
    owner: std::sync::Arc<OwnerConnection>,
    number: u64,
    output: Mutex<Option<RegionSpan>>,
    staged: Mutex<Vec<RegionSpan>>,
}

impl ReadNumber {
    fn take(owner: std::sync::Arc<OwnerConnection>) -> Self {
        Self {
            number: owner.take_read_number(),
            owner,
            output: Mutex::default(),
            staged: Mutex::default(),
        }
    }

    fn lend_output(&self, output: Option<RegionSpan>) -> Option<SpanName> {
        let name = output.as_ref().map(RegionSpan::name);
        *self.output.lock().unwrap_or_else(PoisonError::into_inner) = output;
        name
    }

    fn take_output(&self) -> Option<RegionSpan> {
        self.output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(super) fn number(&self) -> u64 {
        self.number
    }

    pub(super) fn owner(&self) -> &OwnerConnection {
        &self.owner
    }

    pub(super) fn keep_staged(&self, staged: Option<RegionSpan>) {
        (self.staged.lock().unwrap_or_else(PoisonError::into_inner)).extend(staged);
    }
}

impl Drop for ReadNumber {
    fn drop(&mut self) {
        // Kept before the read ends: the owner's end comes after.
        let output = self.take_output();
        let mut spans = std::mem::take(
            self.staged
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner),
        );
        spans.extend(output);
        self.owner.keep_until_ended(self.number, spans);
        self.owner
            .tell(&[ClientFrame::EndRead { read: self.number }]);
    }
}
