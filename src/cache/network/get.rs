//! One GET of a read's: its turn in the queue, the identical requests that
//! race for its bytes, a late racer for the rest, and its body landed in
//! the cache as it comes in.

use std::num::NonZeroU8;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::{Either, FutureExt, pending, select, select_ok};
use futures_util::stream::TryStreamExt;
use object_store::path::Path;
use object_store::{Extensions, GetOptions, GetRange, GetResult};

use super::Network;
use super::get_queue::{GetPlace, GetTurn, JoinedParts};
use crate::cache::io_report::IoReport;
use crate::cache::policy::Io;
use crate::cache::storage_link::LinkedFetch;

/// Where a GET's body goes: first the bytes of the waiting GETs joined to
/// it, then the read's own blocks.
struct BodyLanding<'c> {
    joined: JoinedParts,
    blocks: LinkedFetch<'c>,
    /// The body's bytes landed so far.
    received: u64,
}

impl<'c> BodyLanding<'c> {
    fn new(joined: JoinedParts, blocks: LinkedFetch<'c>) -> Self {
        Self {
            joined,
            blocks,
            received: 0,
        }
    }

    /// Lands `piece`, the body's next bytes.
    async fn land(&mut self, mut piece: Bytes) -> object_store::Result<()> {
        self.received += piece.len() as u64;
        self.joined.take_front(&mut piece);
        if piece.is_empty() {
            return Ok(());
        }
        self.blocks.land(&piece).await.map_err(Network::cache_error)
    }
}

impl Network {
    /// Fetches the bytes `blocks` holds from the inner store, if the file is
    /// still the version with `e_tag`, and hands each piece of the answer to
    /// `blocks` as it comes in. The GET waits its turn first, if the cap on
    /// bytes in flight is reached, and keeps its place until its body is in;
    /// it may take, before its own, the bytes of waiting GETs joined to it,
    /// or join one and take its bytes from it. As many identical GETs as
    /// the plan says race for them, sent at once: the first answer takes
    /// the bytes, and the others are dropped, their requests with them. If
    /// the plan races it late too, and the cache has a timer, one more
    /// request goes once the GET has been out that long, for the bytes not
    /// in yet: the first to have them all is taken, and the other dropped.
    /// The read policy hears the GET finished once its body is in, and
    /// landed once its blocks are cached.
    pub(in crate::cache) async fn fetch(
        &self,
        location: &Path,
        e_tag: &str,
        mut blocks: LinkedFetch<'_>,
        extensions: &Extensions,
    ) -> object_store::Result<()> {
        let waiting = Instant::now();
        let Some((place, joined)) = self.turn(&mut blocks).await? else {
            // Its bytes came with another GET's.
            return blocks.finish().await.map_err(Self::cache_error);
        };
        let own = blocks.range();
        let bytes = joined.start().unwrap_or(own.start)..own.end;
        let mut body = BodyLanding::new(joined, blocks);
        let options = GetOptions {
            range: Some(GetRange::Bounded(bytes.clone())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let path = location.as_ref().as_bytes();
        let sent = Instant::now();
        let waited = sent - waiting;
        let late = body.blocks.late_race().zip(self.sleep.clone());
        let racers = body.blocks.race();
        // The time the body's pieces took to land, apart from the time
        // spent waiting for them; and how many bytes have come, for a late
        // racer to ask for the rest.
        let mut landing = Duration::ZERO;
        let landed = AtomicU64::new(0);
        let first = async {
            let (answer, mut report) = self
                .first_answer(location, &options, path, &bytes, racers)
                .await?;
            let received = async {
                let mut answer = answer.into_stream();
                while let Some(piece) = answer.try_next().await? {
                    let started = Instant::now();
                    let len = piece.len() as u64;
                    body.land(piece).await?;
                    landing += started.elapsed();
                    landed.fetch_add(len, Ordering::Relaxed);
                }
                Ok::<_, object_store::Error>(())
            };
            match received.await {
                Ok(()) => {
                    report.finished(bytes.end - bytes.start);
                    Ok(report)
                }
                Err(error) => {
                    report.failed();
                    Err(error)
                }
            }
        };
        let report = match late {
            None => first.await?,
            Some((after, sleep)) => {
                let rest = async {
                    sleep(after).await;
                    let from = bytes.start + landed.load(Ordering::Relaxed);
                    let racer = racers.get();
                    self.late_answer(location, &options, path, from..bytes.end, racer, sent)
                        .await
                };
                // The first request, or the late racer, with the rest.
                let won = match select(Box::pin(first), Box::pin(rest)).await {
                    Either::Left((done, _rest)) => Either::Left(done?),
                    Either::Right((Ok(rest), _first)) => Either::Right(rest),
                    Either::Right((Err(_), first)) => Either::Left(first.await?),
                };
                match won {
                    Either::Left(report) => report,
                    // The first request given up on: the bytes not in yet
                    // land from the late racer's.
                    Either::Right((from, rest, report)) => {
                        let skip = usize::try_from(bytes.start + body.received - from)
                            .expect("a GET's bytes fit in memory");
                        let started = Instant::now();
                        body.land(rest.slice(skip..)).await?;
                        landing += started.elapsed();
                        report
                    }
                }
            }
        };
        // Its body in, the GET gives its place back.
        drop(place);
        body.blocks.finish().await.map_err(Self::cache_error)?;
        report.landed(landing, waited);
        Ok(())
    }

    /// The turn of the GET of `blocks`, once it waited for it, if the cap on
    /// bytes in flight is reached: its place, and the waiting GETs joined to
    /// it. `None` if it joined another GET, whose bytes `blocks` took; if
    /// that GET failed or was given up on, it waits again.
    async fn turn(
        &self,
        blocks: &mut LinkedFetch<'_>,
    ) -> object_store::Result<Option<(GetPlace, JoinedParts)>> {
        loop {
            match self
                .gets
                .turn(blocks.file(), blocks.range(), blocks.race())
                .await
            {
                GetTurn::Send { place, joined } => return Ok(Some((place, joined))),
                GetTurn::Joined(get) => {
                    let Some(pieces) = get.bytes().await else {
                        continue;
                    };
                    for piece in pieces {
                        blocks.land(&piece).await.map_err(Self::cache_error)?;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// The first answer of `racers` identical GETs, sent at once, the
    /// others dropped before their bodies; and its report.
    async fn first_answer<'a>(
        &'a self,
        location: &Path,
        options: &GetOptions,
        path: &'a [u8],
        bytes: &Range<u64>,
        racers: NonZeroU8,
    ) -> object_store::Result<(GetResult, IoReport<'a>)> {
        let racers = (0..racers.get()).map(|racer| {
            let get = Io::Get {
                path,
                bytes: bytes.clone(),
                racer,
            };
            let mut report = IoReport::start(&*self.reads, get);
            let get = self.inner.get_opts(location, options.clone());
            async move {
                match get.await {
                    Ok(answer) => {
                        report.answered();
                        Ok((answer, report))
                    }
                    Err(error) => {
                        report.failed();
                        Err(error)
                    }
                }
            }
            .boxed()
        });
        let (first, _losers) = select_ok(racers).await?;
        Ok(first)
    }

    /// A late racer's GET of `rest`, the bytes of a GET not in yet, whole:
    /// where they start, and its report, timed from `sent`, when the GET's
    /// first request went. Never done if no bytes are left.
    async fn late_answer<'a>(
        &'a self,
        location: &Path,
        options: &GetOptions,
        path: &'a [u8],
        rest: Range<u64>,
        racer: u8,
        sent: Instant,
    ) -> object_store::Result<(u64, Bytes, IoReport<'a>)> {
        if rest.is_empty() {
            return pending().await;
        }
        let get = Io::Get {
            path,
            bytes: rest.clone(),
            racer,
        };
        let mut report = IoReport::start_at(&*self.reads, get, sent);
        let options = GetOptions {
            range: Some(GetRange::Bounded(rest.clone())),
            ..options.clone()
        };
        let answer = match self.inner.get_opts(location, options).await {
            Ok(answer) => answer,
            Err(error) => {
                report.failed();
                return Err(error);
            }
        };
        report.answered();
        match answer.bytes().await {
            Ok(body) => {
                report.finished(rest.end - rest.start);
                Ok((rest.start, body, report))
            }
            Err(error) => {
                report.failed();
                Err(error)
            }
        }
    }
}
