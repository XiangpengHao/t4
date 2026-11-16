use std::io;
use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;

use super::connection::OwnerConnection;
use super::read::ReadNumber;
use crate::cache::cache_stores::shared::protocol::{
    ClientFrame, FetchSpec, OwnerFrame, RegionSpan, SpanName,
};
use crate::cache::cache_stores::{CacheStoreFetch, FileVersionKey};
use crate::io::error::{Error, Result};

pub(super) struct ClientFetch {
    read: std::sync::Arc<ReadNumber>,
    fetch: u32,
    spec: FetchSpec,
    file: FileVersionKey,
    staged: Option<RegionSpan>,
    landed: u64,
    settled: bool,
}

impl ClientFetch {
    pub(super) fn new(
        read: std::sync::Arc<ReadNumber>,
        fetch: u32,
        spec: FetchSpec,
        file: FileVersionKey,
    ) -> Self {
        let bytes = spec.bytes();
        let staged = read.owner().span(bytes.end - bytes.start);
        Self {
            read,
            fetch,
            spec,
            file,
            staged,
            landed: 0,
            settled: false,
        }
    }
}

impl<'c> CacheStoreFetch<'c> for ClientFetch {
    fn range(&self) -> Range<u64> {
        self.spec.bytes()
    }

    fn file(&self) -> FileVersionKey {
        self.file
    }

    fn race(&self) -> NonZeroU8 {
        self.spec.race()
    }

    fn late_race(&self) -> Option<Duration> {
        self.spec.late_race()
    }

    /// Bytes past the span go in the frame: the owner refuses the fetch at the commit.
    fn land<'a>(&'a mut self, piece: &'a [u8]) -> BoxFuture<'a, Result<()>> {
        let (read, fetch, at) = (self.read.number(), self.fetch, self.landed);
        let len = piece.len() as u64;
        let staged = (self.staged.as_mut()).filter(|span| at + len <= span.name().len());
        let frame = match staged {
            Some(span) => {
                span.copy_in(at as usize, piece);
                let staged = SpanName::new(span.name().offset() + at, len);
                ClientFrame::LandStaged {
                    read,
                    fetch,
                    staged,
                }
            }
            None => ClientFrame::Land {
                read,
                fetch,
                bytes: Bytes::copy_from_slice(piece),
            },
        };
        self.landed += len;
        self.read.owner().tell(&[frame]);
        Box::pin(std::future::ready(Ok(())))
    }

    /// An owner gone is no error here: the read finds it, and goes to the remote.
    fn finish(mut self: Box<Self>) -> BoxFuture<'c, Result<()>> {
        Box::pin(async move {
            self.settled = true;
            let (read, fetch) = (self.read.number(), self.fetch);
            let answer = (self.read.owner())
                .call(|call| ClientFrame::Commit { call, read, fetch })
                .await;
            match answer {
                Ok(OwnerFrame::Committed { .. }) | Err(_) => Ok(()),
                Ok(OwnerFrame::Failed { .. }) => Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "the fetch of {:?} brought other bytes than it holds",
                        self.spec.bytes()
                    ),
                ))),
                Ok(answer) => Err(OwnerConnection::unexpected(&answer)),
            }
        })
    }
}

impl Drop for ClientFetch {
    fn drop(&mut self) {
        if !self.settled {
            self.read.owner().tell(&[ClientFrame::Abandon {
                read: self.read.number(),
                fetch: self.fetch,
            }]);
        }
        // The owner may read the span until it ends the read.
        self.read.keep_staged(self.staged.take());
    }
}
