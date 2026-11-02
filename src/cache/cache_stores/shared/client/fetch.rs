//! [`ClientFetch`]: one of a client's read's fetches, its bytes handed to
//! the owner, which lands them, as they come in.

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

/// One of a client's read's fetches (`ClientRead`'s): the bytes of
/// its GET, staged in a span of the client's region as they come in, or
/// sent in frames, which the owner lands; then committed, the blocks cached
/// as the owner said. Dropped before, as when the GET fails, it is
/// abandoned: the owner caches none of it, and the reads waiting on its
/// blocks fetch for themselves. Dropped either way, its span is kept until
/// the owner ends the read, which it holds open until then.
pub(super) struct ClientFetch {
    read: std::sync::Arc<ReadNumber>,
    /// Its place in the read's fetches.
    fetch: u32,
    spec: FetchSpec,
    file: FileVersionKey,
    /// Where its bytes are staged, if a span was free.
    staged: Option<RegionSpan>,
    /// How many of its bytes have landed.
    landed: u64,
    /// Committed or abandoned.
    settled: bool,
}

impl ClientFetch {
    /// The fetch `fetch` of the read `read`, of `spec`'s bytes of `file`:
    /// staged in a span of the client's region, if one is free; else its
    /// bytes go in the frames.
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
    /// The file's bytes the fetch holds.
    fn range(&self) -> Range<u64> {
        self.spec.bytes()
    }

    /// The version of the file they are of, as the owner named it.
    fn file(&self) -> FileVersionKey {
        self.file
    }

    /// How many identical requests race for them.
    fn race(&self) -> NonZeroU8 {
        self.spec.race()
    }

    /// When one more request races them, if they aren't all in.
    fn late_race(&self) -> Option<Duration> {
        self.spec.late_race()
    }

    /// Hands the owner `piece`, the fetch's next bytes: staged in the
    /// fetch's span, named in the frame; or in the frame, with no span, or
    /// past the span, as a GET that brings more bytes than it holds, which
    /// the owner refuses at the commit.
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

    /// Commits the fetch, every byte in: the owner caches its blocks where
    /// the read said. An owner gone is no error here: the read finds it,
    /// and goes to the remote.
    ///
    /// # Errors
    ///
    /// If the owner found the fetch brought fewer bytes than it holds, or
    /// more: nothing is cached.
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
        // The owner may read the span until it ends the read, which the
        // read's number, as the last of the read and its fetches drops,
        // asks for after.
        self.read.keep_staged(self.staged.take());
    }
}
