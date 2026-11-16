use std::num::NonZeroU8;
use std::ops::Range;
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

struct BodyLanding<'c> {
    joined: JoinedParts,
    blocks: Box<dyn CacheStoreFetch<'c> + 'c>,
    received: u64,
}

impl<'c> BodyLanding<'c> {
    fn new(joined: JoinedParts, blocks: Box<dyn CacheStoreFetch<'c> + 'c>) -> Self {
        Self {
            joined,
            blocks,
            received: 0,
        }
    }

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
        let mut body = BodyLanding::new(joined, blocks);
        let options = GetOptions {
            range: Some(GetRange::Bounded(bytes.clone())),
            if_match: Some(e_tag.to_owned()),
            extensions: extensions.clone(),
            ..GetOptions::default()
        };
        let path = location.as_ref().as_bytes();
        let sent = self.clock.now();
        let waited = sent - waiting;
        let late = body.blocks.late_race().zip(self.sleep.clone());
        let racers = body.blocks.race();
        let mut landing = Duration::ZERO;
        let landed = AtomicU64::new(0);
        let first = async {
            let (answer, mut report) = self
                .first_answer(location, &options, path, &bytes, racers)
                .await?;
            let received = async {
                let mut answer = answer.into_stream();
                while let Some(piece) = answer.try_next().await? {
                    let started = self.clock.now();
                    let len = piece.len() as u64;
                    body.land(piece).await?;
                    landing += self.clock.now() - started;
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
                let won = match select(Box::pin(first), Box::pin(rest)).await {
                    Either::Left((done, _rest)) => Either::Left(done?),
                    Either::Right((Ok(rest), _first)) => Either::Right(rest),
                    Either::Right((Err(_), first)) => Either::Left(first.await?),
                };
                match won {
                    Either::Left(report) => report,
                    // The first request may have landed more since the late racer asked.
                    Either::Right((from, rest, report)) => {
                        let skip = usize::try_from(bytes.start + body.received - from)
                            .expect("a GET's bytes fit in memory");
                        let started = self.clock.now();
                        body.land(rest.slice(skip..)).await?;
                        landing += self.clock.now() - started;
                        report
                    }
                }
            }
        };
        // The cap is on bytes in flight: give the place back before caching the blocks.
        drop(place);
        body.blocks.finish().await.map_err(Self::cache_error)?;
        report.landed(landing, waited);
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
