use std::future::Future;
use std::num::NonZeroU8;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::{Either, FutureExt, pending, select, select_ok};
use futures_util::stream::TryStreamExt;
use object_store::path::Path;
use object_store::{Extensions, GetOptions, GetRange, GetResult};

use super::Network;
use super::get_queue::{GetPlace, GetTurn, JoinedParts};
use crate::cache::cache_stores::CacheStoreFetch;
use crate::cache::io_report::IoReport;
use crate::cache::policy::Io;
use crate::io::time::Clock;

struct BodyLanding<'c, 'f> {
    joined: JoinedParts,
    blocks: Box<dyn CacheStoreFetch<'c> + 'c>,
    clock: &'f Clock,
    /// The GET's bytes landed so far, read by the late request as it asks for the rest.
    landed: &'f AtomicU64,
    landing: Duration,
}

impl<'c, 'f> BodyLanding<'c, 'f> {
    fn new(
        joined: JoinedParts,
        blocks: Box<dyn CacheStoreFetch<'c> + 'c>,
        clock: &'f Clock,
        landed: &'f AtomicU64,
    ) -> Self {
        Self {
            joined,
            blocks,
            clock,
            landed,
            landing: Duration::ZERO,
        }
    }

    /// Only the waits race the late request: a piece dropped half landed would leave `landed`
    /// counting bytes the blocks never took.
    async fn land_first<'a, Late>(
        &mut self,
        first: impl Future<Output = object_store::Result<(GetResult, IoReport<'a>)>>,
        mut late: Pin<&mut impl Future<Output = Late>>,
        len: u64,
    ) -> object_store::Result<Either<IoReport<'a>, Late>> {
        let (answer, mut report) = match select(pin!(first), late.as_mut()).await {
            Either::Left((first, _)) => first?,
            Either::Right((late, _)) => return Ok(Either::Right(late)),
        };
        let mut pieces = answer.into_stream();
        let received = async {
            loop {
                match select(pieces.try_next(), late.as_mut()).await {
                    Either::Left((Ok(Some(piece)), _)) => self.land(piece).await?,
                    Either::Left((Ok(None), _)) => return Ok(None),
                    Either::Left((Err(error), _)) => return Err(error),
                    Either::Right((late, _)) => return Ok(Some(late)),
                }
            }
        };
        match received.await {
            Ok(None) => {
                report.finished(len);
                Ok(Either::Left(report))
            }
            Ok(Some(late)) => Ok(Either::Right(late)),
            Err(error) => {
                report.failed();
                Err(error)
            }
        }
    }

    async fn land(&mut self, mut piece: Bytes) -> object_store::Result<()> {
        let started = self.clock.now();
        let len = piece.len() as u64;
        self.joined.take_front(&mut piece);
        if !piece.is_empty() {
            self.blocks
                .land(&piece)
                .await
                .map_err(Network::cache_error)?;
        }
        self.landing += self.clock.now() - started;
        self.landed.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }
}

impl Network {
    pub(in crate::cache) async fn fetch<'c>(
        &self,
        location: &Path,
        e_tag: &str,
        mut blocks: Box<dyn CacheStoreFetch<'c> + 'c>,
        extensions: &Extensions,
    ) -> object_store::Result<()> {
        let waiting = self.clock.now();
        let Some((place, joined)) = self.turn(&mut *blocks).await? else {
            // Its bytes came with another GET's.
            return blocks.finish().await.map_err(Self::cache_error);
        };
        let own = blocks.range();
        let bytes = joined.start().unwrap_or(own.start)..own.end;
        let options = GetOptions {
            range: Some(GetRange::Bounded(bytes.clone())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let path = location.as_ref().as_bytes();
        let sent = self.clock.now();
        let waited = sent - waiting;
        let late_race = blocks.late_race().zip(self.sleep.clone());
        let racers = blocks.race();
        let landed = AtomicU64::new(0);
        let mut body = BodyLanding::new(joined, blocks, &self.clock, &landed);
        // Never answers if there is no late request or it fails: the first goes on alone.
        let late = async {
            let Some((after, sleep)) = late_race else {
                return pending().await;
            };
            sleep(after).await;
            let from = bytes.start + landed.load(Ordering::Relaxed);
            let racer = racers.get();
            let rest = from..bytes.end;
            match self
                .late_answer(location, &options, path, rest, racer, sent)
                .await
            {
                Ok(answer) => answer,
                Err(_) => pending().await,
            }
        };
        let first = self.first_answer(location, &options, path, &bytes, racers);
        let report = match body
            .land_first(first, pin!(late), bytes.end - bytes.start)
            .await?
        {
            Either::Left(report) => report,
            // The first request may have landed more since the late one asked.
            Either::Right((from, rest, report)) => {
                let skip = usize::try_from(bytes.start + landed.load(Ordering::Relaxed) - from)
                    .expect("a GET's bytes fit in memory");
                body.land(rest.slice(skip..)).await?;
                report
            }
        };
        // The cap is on bytes in flight: give the place back before caching the blocks.
        drop(place);
        body.blocks.finish().await.map_err(Self::cache_error)?;
        report.landed(body.landing, waited);
        Ok(())
    }

    async fn turn(
        &self,
        blocks: &mut dyn CacheStoreFetch<'_>,
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
            let mut report = IoReport::start(&*self.reads, get, &self.clock);
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

    /// Never done if no bytes are left, so the first request wins.
    async fn late_answer<'a>(
        &'a self,
        location: &Path,
        options: &GetOptions,
        path: &'a [u8],
        rest: Range<u64>,
        racer: u8,
        sent: Duration,
    ) -> object_store::Result<(u64, Bytes, IoReport<'a>)> {
        if rest.is_empty() {
            return pending().await;
        }
        let get = Io::Get {
            path,
            bytes: rest.clone(),
            racer,
        };
        let mut report = IoReport::start_at(&*self.reads, get, sent, &self.clock);
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
