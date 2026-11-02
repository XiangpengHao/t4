//! [`ClientRead`]: a client's read through its owner, and [`ReadNumber`],
//! the read's number on the connection, which its fetches hold too.

use std::io;
use std::ops::Range;
use std::sync::{Mutex, PoisonError};

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

/// A client's read through its owner (`doc/11-shared-mode.md`, decision
/// 5): its number on the connection, the read's next step as the owner
/// answered it, its round's finish, and, once served, where its bytes are.
/// Its client plans its GETs and makes them; the owner does the rest.
pub(in crate::cache::cache_stores::shared) struct ClientRead<'c> {
    number: std::sync::Arc<ReadNumber>,
    path: &'c [u8],
    version: FileVersion,
    /// The file's bytes the client asked for.
    asked: Range<u64>,
    next: Option<NextStep>,
    /// The round's finish, sent as the round starts, with the read's start
    /// or once the owner answers `Retry`: the owner holds it till the
    /// round's work is done, so that a read of hits is one round trip.
    finish: Option<WaitingCall>,
    served: Option<Served>,
}

/// Where the owner served a read's bytes.
enum Served {
    /// In its answer's frame; none if a read from disk was still writing
    /// them, as after one failed.
    InFrame(Option<Bytes>),
    /// In the read's output's span: the file's bytes `asked`, where an
    /// output of them has them.
    InSpan(Range<u64>),
}

impl<'c> ClientRead<'c> {
    /// The most bytes a client's read takes: its bytes come back in one
    /// frame (decision 5), which holds 2 GiB.
    const MOST_BYTES: u64 = 1 << 30;

    /// Starts a read through `owner` of `range` of the version of the file
    /// at `path` with `e_tag` and `size`, within it, for `intent`, its first
    /// round's finish sent with it. `None` if the cache can't hold the file,
    /// or a client's read of that many bytes.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
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
        // No span free, the read's bytes come in the frame.
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

    /// The error of a read the owner answered `answer`: failed it, or
    /// answered as no call of the kind takes.
    fn failed(answer: &OwnerFrame) -> Error {
        match answer {
            OwnerFrame::Failed { .. } => Error::Io(io::Error::other("the owner failed the read")),
            answer => OwnerConnection::unexpected(answer),
        }
    }
}

impl<'c> CacheStoreRead<'c> for ClientRead<'c> {
    /// The read's file, as the owner named its version.
    fn file(&self) -> CacheableFile<'c> {
        CacheableFile::named(self.path, self.version)
    }

    /// The run of the read the owner located last, for the client to plan
    /// its GETs; `None` once its GETs are planned.
    fn locate_next_run(&mut self) -> Result<Option<LocatedRun>> {
        match self.next.take() {
            Some(NextStep::Plan(run)) => Ok(Some(run)),
            other => {
                self.next = other;
                Ok(None)
            }
        }
    }

    /// Hands the owner `gets`, planned for the run it located last: it
    /// answers with the next run, or the read's fetches.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
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

    /// The read's fetches, which the client makes, as the owner answered
    /// once every run was planned; none after.
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

    /// Waits for the read's work in the owner, done once the owner has
    /// served what it holds and the read's fetches are committed or
    /// abandoned: whether blocks are left to fetch, those it waited for
    /// that no fetch cached, its next step. The finish was sent as the
    /// round started; the next round's goes at once.
    ///
    /// # Errors
    ///
    /// If the owner failed the read, or is gone.
    ///
    /// # Panics
    ///
    /// If the read was served already: it has no round left to finish.
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

    /// The read's bytes, once served, their address agreeing with their
    /// file offset mod 4 KiB, as an owner's read's does: in the read's
    /// output's span, which they hold until the last of them drops; or, from
    /// a frame, copied into an output of the client's own. `None` if the
    /// owner served none, as after a read from disk failed, or served them
    /// where they can't be.
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
                // An output's bytes start in its first page, where the
                // file's first asked would.
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

/// A read's number on its connection, from its start, held by the read and
/// each of its fetches; and the spans they lent the owner. Dropped with the
/// last of them, it ends the read in the owner, served or not, which drops
/// all it holds of it, and keeps the spans until the owner says it has
/// (`OwnerConnection::keep_until_ended`): no frame of the read's goes after
/// its end, which would break the protocol.
pub(super) struct ReadNumber {
    /// The read's own handle on the connection: it outlives the store's,
    /// replaced once its owner goes.
    owner: std::sync::Arc<OwnerConnection>,
    number: u64,
    /// The read's output's span, until its caller takes it, served.
    output: Mutex<Option<RegionSpan>>,
    /// The spans its fetches staged their bytes in, as they drop.
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

    /// Keeps `output`, the read's output's span if one was free, and names
    /// it for the read's start.
    fn lend_output(&self, output: Option<RegionSpan>) -> Option<SpanName> {
        let name = output.as_ref().map(RegionSpan::name);
        *self.output.lock().unwrap_or_else(PoisonError::into_inner) = output;
        name
    }

    /// The read's output's span, served: its caller's now.
    fn take_output(&self) -> Option<RegionSpan> {
        self.output
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The read's number on its connection.
    pub(super) fn number(&self) -> u64 {
        self.number
    }

    /// The connection the read goes through.
    pub(super) fn owner(&self) -> &OwnerConnection {
        &self.owner
    }

    /// Keeps `staged`, a fetch's span, until the owner ends the read.
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
