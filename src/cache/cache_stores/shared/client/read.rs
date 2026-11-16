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
    ClientFrame, NextStep, OwnerFrame, RegionSpan, SpanName,
};
use crate::cache::cache_stores::{CacheStoreFetch, CacheStoreRead, FileVersionKey};
use crate::cache::policy::{ReadIntent, ReadPlan};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::{CacheableFile, FileVersion, LocatedRun};
use crate::io::error::{Error, Result};

pub(in crate::cache::cache_stores::shared) struct ClientRead<'c> {
    number: std::sync::Arc<ReadNumber>,
    path: &'c [u8],
    version: FileVersion,
    asked: Range<u64>,
    next: Option<NextStep>,
    /// Sent as the round starts: the owner holds it till the round's work is done, so that a
    /// read of hits is one round trip.
    finish: Option<WaitingCall>,
    served: Option<Served>,
}

enum Served {
    InFrame(Option<Bytes>),
    InSpan(Range<u64>),
}

impl<'c> ClientRead<'c> {
    /// Its bytes come back in one frame (`doc/11-shared-mode.md`, decision 5), which holds 2 GiB.
    const MOST_BYTES: u64 = 1 << 30;

    pub(in crate::cache::cache_stores::shared) async fn start(
        owner: std::sync::Arc<OwnerConnection>,
        path: &'c [u8],
        e_tag: &str,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
    ) -> Result<Option<Self>> {
        if range.end - range.start > Self::MOST_BYTES {
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
            OwnerFrame::Started { version, next, .. } => Ok(Some(Self {
                number: std::sync::Arc::new(number),
                path,
                version,
                asked,
                next: Some(next),
                finish: Some(finish),
                served: None,
            })),
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
}

impl<'c> CacheStoreRead<'c> for ClientRead<'c> {
    fn file(&self) -> CacheableFile<'c> {
        CacheableFile::named(self.path, self.version)
    }

    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        match self.next.take() {
            Some(NextStep::Plan(run)) => Ok(Some(run)),
            other => {
                self.next = other;
                Ok(None)
            }
        }
    }

    fn take_gets<'a>(&'a mut self, gets: &'a ReadPlan) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
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
        })
    }

    fn fetches(&mut self) -> Result<Vec<Box<dyn CacheStoreFetch<'c> + 'c>>> {
        let Some(NextStep::Fetch(specs)) = self.next.take() else {
            return Ok(Vec::new());
        };
        let file = FileVersionKey::new(self.number.owner.version_ids(), self.version.id());
        Ok((0..)
            .zip(specs)
            .map(|(fetch, spec)| {
                let read = std::sync::Arc::clone(&self.number);
                Box::new(ClientFetch::new(read, fetch, spec, file))
                    as Box<dyn CacheStoreFetch<'c> + 'c>
            })
            .collect())
    }

    fn finish(&mut self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let finish = self
                .finish
                .take()
                .expect("a round's finish, sent as it starts");
            match finish.await? {
                OwnerFrame::Retry { next, .. } => {
                    self.next = Some(next);
                    let owner = &self.number.owner;
                    let read = self.number.number;
                    self.finish = Some(owner.send_call(|call| ClientFrame::Finish { call, read })?);
                    Ok(true)
                }
                OwnerFrame::Served { bytes, .. } => {
                    self.served = Some(Served::InFrame(bytes));
                    Ok(false)
                }
                OwnerFrame::ServedInSpan { asked, .. } => {
                    self.served = Some(Served::InSpan(asked));
                    Ok(false)
                }
                answer => Err(Self::failed(&answer)),
            }
        })
    }

    /// Their address agrees with their file offset mod 4 KiB, as an owner's read's does.
    fn into_bytes(mut self: Box<Self>) -> Option<Bytes> {
        match self.served.take()? {
            Served::InFrame(served) => {
                let served = served?;
                let start = self.asked.start;
                let mut output = ReadOutput::new(start..start + served.len() as u64).ok()?;
                output.copy_in(start, &served);
                output.into_bytes()
            }
            Served::InSpan(asked) => {
                let at = usize::try_from(asked.start % PAGE_SIZE_U64).ok()?;
                let len = usize::try_from(asked.end - asked.start).ok()?;
                let span = self.number.take_output()?;
                let within = self.asked.start <= asked.start && asked.end <= self.asked.end;
                let fits = (at + len) as u64 <= span.name().len();
                (within && fits).then(|| span.into_bytes(at..at + len))
            }
        }
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
