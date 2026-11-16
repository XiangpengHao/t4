use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;

use super::region::SpanName;
use crate::cache::policy::{BlockSource, IoPhase, PlannedGet, ReadIntent, ReadPlan};
use crate::cache::storage::FileVersion;
use crate::cache::storage::LocatedRun;
use crate::cache::{BlockKey, FileVersionId};
use crate::io::error::{Error, Result};

/// `Hello` and `Welcome` stay the same in every version, so that peers of different versions
/// learn each other's; a version that needs more handshake adds frames after the welcome.
pub(in crate::cache::cache_stores::shared) const PROTOCOL_VERSION: u32 = 4;

/// A read's bytes come back in one frame, which holds 2 GiB, or in a span of a 1 GiB region: a
/// client asks for no more, and its owner serves no more.
pub(in crate::cache::cache_stores::shared) const MOST_READ_BYTES: u64 = 1 << 30;

/// A frame with a `call` is answered by that number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache::cache_stores::shared) enum ClientFrame {
    /// Sent with descriptors beside it: the first cache file's, and the region's if `region`.
    Hello {
        version: u32,
        region: bool,
    },
    StartRead {
        call: u64,
        read: u64,
        path: Bytes,
        e_tag: String,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        output: Option<SpanName>,
    },
    TakeGets {
        call: u64,
        read: u64,
        plan: ReadPlan,
    },
    Land {
        read: u64,
        fetch: u32,
        bytes: Bytes,
    },
    LandStaged {
        read: u64,
        fetch: u32,
        staged: SpanName,
    },
    Commit {
        call: u64,
        read: u64,
        fetch: u32,
    },
    Abandon {
        read: u64,
        fetch: u32,
    },
    /// Sent once a round, any time after it starts (with `StartRead` or after `Retry`);
    /// answered once the round's fetches are committed or abandoned.
    Finish {
        call: u64,
        read: u64,
    },
    /// Answered by [`OwnerFrame::Ended`] once the owner is done with the read's spans.
    EndRead {
        read: u64,
    },
    KnownSize {
        call: u64,
        path: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache::cache_stores::shared) enum OwnerFrame {
    /// Without `region`, the client's reads' bytes go in the frames. A client of another
    /// version is told the owner's too, then refused.
    Welcome {
        version: u32,
        region: bool,
    },
    Started {
        call: u64,
        version: FileVersion,
        next: NextStep,
    },
    Uncacheable {
        call: u64,
    },
    Next {
        call: u64,
        next: NextStep,
    },
    Committed {
        call: u64,
    },
    /// The blocks the read waited for went uncached: it fetches them itself.
    Retry {
        call: u64,
        next: NextStep,
    },
    /// `None` if a read from disk still writes the bytes, as after one failed.
    Served {
        call: u64,
        bytes: Option<Bytes>,
    },
    Failed {
        call: u64,
    },
    ServedInSpan {
        call: u64,
        asked: Range<u64>,
    },
    Ended {
        read: u64,
    },
    DiskRead {
        drive: u32,
        bytes: u32,
        phase: IoPhase,
    },
    KnownSize {
        call: u64,
        size: Option<u64>,
    },
}

/// A fetch is numbered by its place in `Fetch`'s list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache::cache_stores::shared) enum NextStep {
    Plan(LocatedRun),
    Fetch(Vec<FetchSpec>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::cache::cache_stores::shared) struct FetchSpec {
    bytes: Range<u64>,
    race: NonZeroU8,
    late: Option<Duration>,
}

impl FetchSpec {
    pub(in crate::cache::cache_stores::shared) fn new(
        bytes: Range<u64>,
        race: NonZeroU8,
        late: Option<Duration>,
    ) -> Self {
        Self { bytes, race, late }
    }

    pub(in crate::cache::cache_stores::shared) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    pub(in crate::cache::cache_stores::shared) fn race(&self) -> NonZeroU8 {
        self.race
    }

    pub(in crate::cache::cache_stores::shared) fn late_race(&self) -> Option<Duration> {
        self.late
    }
}

impl ClientFrame {
    const HELLO: u8 = 1;
    const START_READ: u8 = 2;
    const TAKE_GETS: u8 = 3;
    const LAND: u8 = 4;
    const COMMIT: u8 = 5;
    const ABANDON: u8 = 6;
    const FINISH: u8 = 7;
    const END_READ: u8 = 8;
    const LAND_STAGED: u8 = 9;
    const KNOWN_SIZE: u8 = 10;

    const MAGIC: [u8; 4] = *b"t4sh";

    pub(in crate::cache::cache_stores::shared) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Hello { version, region } => {
                let mut frame = FrameWriter::new(Self::HELLO);
                frame.raw(&Self::MAGIC);
                frame.u32(*version);
                frame.flag(*region);
                frame.finish()
            }
            Self::StartRead {
                call,
                read,
                path,
                e_tag,
                size,
                range,
                intent,
                output,
            } => {
                let mut frame = FrameWriter::new(Self::START_READ);
                frame.u64(*call);
                frame.u64(*read);
                frame.bytes(path);
                frame.bytes(e_tag.as_bytes());
                frame.u64(*size);
                frame.range(range);
                frame.intent(*intent);
                frame.span(*output);
                frame.finish()
            }
            Self::TakeGets { call, read, plan } => {
                let mut frame = FrameWriter::new(Self::TAKE_GETS);
                frame.u64(*call);
                frame.u64(*read);
                frame.plan(plan);
                frame.finish()
            }
            Self::Land { read, fetch, bytes } => {
                let mut frame = FrameWriter::new(Self::LAND);
                frame.u64(*read);
                frame.u32(*fetch);
                frame.bytes(bytes);
                frame.finish()
            }
            Self::LandStaged {
                read,
                fetch,
                staged,
            } => {
                let mut frame = FrameWriter::new(Self::LAND_STAGED);
                frame.u64(*read);
                frame.u32(*fetch);
                frame.span(Some(*staged));
                frame.finish()
            }
            Self::Commit { call, read, fetch } => {
                let mut frame = FrameWriter::new(Self::COMMIT);
                frame.u64(*call);
                frame.u64(*read);
                frame.u32(*fetch);
                frame.finish()
            }
            Self::Abandon { read, fetch } => {
                let mut frame = FrameWriter::new(Self::ABANDON);
                frame.u64(*read);
                frame.u32(*fetch);
                frame.finish()
            }
            Self::Finish { call, read } => {
                let mut frame = FrameWriter::new(Self::FINISH);
                frame.u64(*call);
                frame.u64(*read);
                frame.finish()
            }
            Self::EndRead { read } => {
                let mut frame = FrameWriter::new(Self::END_READ);
                frame.u64(*read);
                frame.finish()
            }
            Self::KnownSize { call, path } => {
                let mut frame = FrameWriter::new(Self::KNOWN_SIZE);
                frame.u64(*call);
                frame.bytes(path);
                frame.finish()
            }
        }
    }

    pub(in crate::cache::cache_stores::shared) fn decode(body: Bytes) -> Result<Self> {
        let mut frame = FrameReader::new(body);
        let decoded = match frame.u8()? {
            Self::HELLO => {
                if frame.take(Self::MAGIC.len())? != Self::MAGIC[..] {
                    return Err(FrameReader::malformed("a hello's magic"));
                }
                Self::Hello {
                    version: frame.u32()?,
                    region: frame.flag()?,
                }
            }
            Self::START_READ => Self::StartRead {
                call: frame.u64()?,
                read: frame.u64()?,
                path: frame.bytes()?,
                e_tag: String::from_utf8(frame.bytes()?.to_vec())
                    .map_err(|_| FrameReader::malformed("an e_tag"))?,
                size: frame.u64()?,
                range: frame.range()?,
                intent: frame.intent()?,
                output: frame.span()?,
            },
            Self::TAKE_GETS => Self::TakeGets {
                call: frame.u64()?,
                read: frame.u64()?,
                plan: frame.plan()?,
            },
            Self::LAND => Self::Land {
                read: frame.u64()?,
                fetch: frame.u32()?,
                bytes: frame.bytes()?,
            },
            Self::LAND_STAGED => Self::LandStaged {
                read: frame.u64()?,
                fetch: frame.u32()?,
                staged: frame
                    .span()?
                    .ok_or_else(|| FrameReader::malformed("staged bytes: no span"))?,
            },
            Self::COMMIT => Self::Commit {
                call: frame.u64()?,
                read: frame.u64()?,
                fetch: frame.u32()?,
            },
            Self::ABANDON => Self::Abandon {
                read: frame.u64()?,
                fetch: frame.u32()?,
            },
            Self::FINISH => Self::Finish {
                call: frame.u64()?,
                read: frame.u64()?,
            },
            Self::END_READ => Self::EndRead { read: frame.u64()? },
            Self::KNOWN_SIZE => Self::KnownSize {
                call: frame.u64()?,
                path: frame.bytes()?,
            },
            _ => return Err(FrameReader::malformed("a client frame's tag")),
        };
        frame.finish()?;
        Ok(decoded)
    }
}

impl OwnerFrame {
    pub(in crate::cache::cache_stores::shared) fn call(&self) -> Option<u64> {
        match *self {
            Self::Started { call, .. }
            | Self::Uncacheable { call }
            | Self::Next { call, .. }
            | Self::Committed { call }
            | Self::Retry { call, .. }
            | Self::Served { call, .. }
            | Self::Failed { call }
            | Self::ServedInSpan { call, .. }
            | Self::KnownSize { call, .. } => Some(call),
            Self::Welcome { .. } | Self::DiskRead { .. } | Self::Ended { .. } => None,
        }
    }

    const WELCOME: u8 = 1;
    const STARTED: u8 = 2;
    const UNCACHEABLE: u8 = 3;
    const NEXT: u8 = 4;
    const COMMITTED: u8 = 5;
    const RETRY: u8 = 6;
    const SERVED: u8 = 7;
    const FAILED: u8 = 8;
    const DISK_READ: u8 = 9;
    const SERVED_IN_SPAN: u8 = 10;
    const ENDED: u8 = 11;
    const KNOWN_SIZE: u8 = 12;

    pub(in crate::cache::cache_stores::shared) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Welcome { version, region } => {
                let mut frame = FrameWriter::new(Self::WELCOME);
                frame.u32(*version);
                frame.flag(*region);
                frame.finish()
            }
            Self::Started {
                call,
                version,
                next,
            } => {
                let mut frame = FrameWriter::new(Self::STARTED);
                frame.u64(*call);
                frame.u64(version.id().get());
                frame.u64(version.size());
                frame.next_step(next);
                frame.finish()
            }
            Self::Uncacheable { call } => FrameWriter::numbered(Self::UNCACHEABLE, *call),
            Self::Next { call, next } => {
                let mut frame = FrameWriter::new(Self::NEXT);
                frame.u64(*call);
                frame.next_step(next);
                frame.finish()
            }
            Self::Committed { call } => FrameWriter::numbered(Self::COMMITTED, *call),
            Self::Retry { call, next } => {
                let mut frame = FrameWriter::new(Self::RETRY);
                frame.u64(*call);
                frame.next_step(next);
                frame.finish()
            }
            Self::Served { call, bytes } => {
                let mut frame = FrameWriter::new(Self::SERVED);
                frame.u64(*call);
                match bytes {
                    Some(bytes) => {
                        frame.u8(1);
                        frame.bytes(bytes);
                    }
                    None => frame.u8(0),
                }
                frame.finish()
            }
            Self::Failed { call } => FrameWriter::numbered(Self::FAILED, *call),
            Self::ServedInSpan { call, asked } => {
                let mut frame = FrameWriter::new(Self::SERVED_IN_SPAN);
                frame.u64(*call);
                frame.range(asked);
                frame.finish()
            }
            Self::Ended { read } => FrameWriter::numbered(Self::ENDED, *read),
            Self::DiskRead {
                drive,
                bytes,
                phase,
            } => {
                let mut frame = FrameWriter::new(Self::DISK_READ);
                frame.u32(*drive);
                frame.u32(*bytes);
                frame.phase(phase);
                frame.finish()
            }
            Self::KnownSize { call, size } => {
                let mut frame = FrameWriter::new(Self::KNOWN_SIZE);
                frame.u64(*call);
                frame.flag(size.is_some());
                if let Some(size) = size {
                    frame.u64(*size);
                }
                frame.finish()
            }
        }
    }

    pub(in crate::cache::cache_stores::shared) fn decode(body: Bytes) -> Result<Self> {
        let mut frame = FrameReader::new(body);
        let decoded = match frame.u8()? {
            Self::WELCOME => Self::Welcome {
                version: frame.u32()?,
                region: frame.flag()?,
            },
            Self::STARTED => Self::Started {
                call: frame.u64()?,
                version: frame.version()?,
                next: frame.next_step()?,
            },
            Self::UNCACHEABLE => Self::Uncacheable { call: frame.u64()? },
            Self::NEXT => Self::Next {
                call: frame.u64()?,
                next: frame.next_step()?,
            },
            Self::COMMITTED => Self::Committed { call: frame.u64()? },
            Self::RETRY => Self::Retry {
                call: frame.u64()?,
                next: frame.next_step()?,
            },
            Self::SERVED => Self::Served {
                call: frame.u64()?,
                bytes: match frame.u8()? {
                    0 => None,
                    1 => Some(frame.bytes()?),
                    _ => return Err(FrameReader::malformed("a served read's bytes")),
                },
            },
            Self::FAILED => Self::Failed { call: frame.u64()? },
            Self::SERVED_IN_SPAN => Self::ServedInSpan {
                call: frame.u64()?,
                asked: frame.range()?,
            },
            Self::ENDED => Self::Ended { read: frame.u64()? },
            Self::DISK_READ => Self::DiskRead {
                drive: frame.u32()?,
                bytes: frame.u32()?,
                phase: frame.phase()?,
            },
            Self::KNOWN_SIZE => Self::KnownSize {
                call: frame.u64()?,
                size: match frame.flag()? {
                    true => Some(frame.u64()?),
                    false => None,
                },
            },
            _ => return Err(FrameReader::malformed("an owner frame's tag")),
        };
        frame.finish()?;
        Ok(decoded)
    }
}

pub(in crate::cache::cache_stores::shared) struct FrameWriter {
    frame: Vec<u8>,
}

impl FrameWriter {
    const LENGTH_BYTES: usize = size_of::<u64>();

    fn new(tag: u8) -> Self {
        let mut frame = Vec::with_capacity(64);
        frame.extend_from_slice(&[0; Self::LENGTH_BYTES]);
        frame.push(tag);
        Self { frame }
    }

    fn numbered(tag: u8, number: u64) -> Vec<u8> {
        let mut frame = Self::new(tag);
        frame.u64(number);
        frame.finish()
    }

    fn finish(mut self) -> Vec<u8> {
        let body = (self.frame.len() - Self::LENGTH_BYTES) as u64;
        self.frame[..Self::LENGTH_BYTES].copy_from_slice(&body.to_le_bytes());
        self.frame
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.frame.extend_from_slice(bytes);
    }

    fn u8(&mut self, value: u8) {
        self.frame.push(value);
    }

    fn flag(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn span(&mut self, span: Option<SpanName>) {
        self.flag(span.is_some());
        if let Some(span) = span {
            self.u64(span.offset());
            self.u64(span.len());
        }
    }

    pub(in crate::cache::cache_stores::shared) fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.u64(bytes.len() as u64);
        self.raw(bytes);
    }

    pub(in crate::cache::cache_stores::shared) fn range(&mut self, range: &Range<u64>) {
        self.u64(range.start);
        self.u64(range.end);
    }

    /// Saturates: a `u64` of nanoseconds is 584 years.
    fn duration(&mut self, duration: Duration) {
        self.u64(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
    }

    fn intent(&mut self, intent: Option<ReadIntent>) {
        self.u8(match intent {
            None => 0,
            Some(ReadIntent::Latency) => 1,
            Some(ReadIntent::Throughput) => 2,
            Some(ReadIntent::Warm) => 3,
        });
    }

    pub(in crate::cache::cache_stores::shared) fn sources(&mut self, sources: &[BlockSource]) {
        self.u32(u32::try_from(sources.len()).expect("a run's blocks fit a u32"));
        for source in sources {
            match *source {
                BlockSource::Memory => self.u8(0),
                BlockSource::Disk => self.u8(1),
                BlockSource::Remote => self.u8(2),
                BlockSource::InFlight { requests } => {
                    self.u8(3);
                    self.u32(requests);
                }
            }
        }
    }

    fn plan(&mut self, plan: &ReadPlan) {
        let gets = plan.gets();
        self.u32(u32::try_from(gets.len()).expect("a plan's GETs fit a u32"));
        for get in gets {
            let blocks = get.blocks();
            self.u32(blocks.start);
            self.u32(blocks.end);
            self.u8(get.race().get());
            self.late_race(get.late_race());
        }
    }

    fn late_race(&mut self, late: Option<Duration>) {
        match late {
            Some(after) => {
                self.u8(1);
                self.duration(after);
            }
            None => self.u8(0),
        }
    }

    fn located_run(&mut self, run: &LocatedRun) {
        self.u32(run.first());
        self.range(&run.asked());
        self.sources(run.sources());
    }

    fn next_step(&mut self, next: &NextStep) {
        match next {
            NextStep::Plan(run) => {
                self.u8(0);
                self.located_run(run);
            }
            NextStep::Fetch(fetches) => {
                self.u8(1);
                self.u32(u32::try_from(fetches.len()).expect("a read's GETs fit a u32"));
                for fetch in fetches {
                    self.range(&fetch.bytes);
                    self.u8(fetch.race.get());
                    self.late_race(fetch.late);
                }
            }
        }
    }

    fn phase(&mut self, phase: &IoPhase) {
        match *phase {
            IoPhase::Started => self.u8(0),
            IoPhase::Answered { after } => {
                self.u8(1);
                self.duration(after);
            }
            IoPhase::Finished { bytes, after } => {
                self.u8(2);
                self.u64(bytes);
                self.duration(after);
            }
            IoPhase::Landed {
                answered,
                received,
                landing,
                after,
                waited,
            } => {
                self.u8(3);
                for duration in [answered, received, landing, after, waited] {
                    self.duration(duration);
                }
            }
            IoPhase::Failed { after } => {
                self.u8(4);
                self.duration(after);
            }
            IoPhase::Cancelled { after } => {
                self.u8(5);
                self.duration(after);
            }
        }
    }
}

pub(in crate::cache::cache_stores::shared) struct FrameReader {
    body: Bytes,
    at: usize,
}

impl FrameReader {
    fn new(body: Bytes) -> Self {
        Self { body, at: 0 }
    }

    pub(in crate::cache::cache_stores::shared) fn malformed(field: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a shared cache's frame holds a malformed {field}"),
        ))
    }

    fn finish(self) -> Result<()> {
        if self.at == self.body.len() {
            Ok(())
        } else {
            Err(Self::malformed("end: bytes past its last field"))
        }
    }

    fn take(&mut self, len: usize) -> Result<Bytes> {
        if self.body.len() - self.at < len {
            return Err(Self::malformed("field: it ends early"));
        }
        let taken = self.body.slice(self.at..self.at + len);
        self.at += len;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok((*self.take(N)?).try_into().expect("taken N bytes"))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn flag(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Self::malformed("flag")),
        }
    }

    /// Unchecked here: the owner checks it against the region it mapped.
    fn span(&mut self) -> Result<Option<SpanName>> {
        Ok(match self.flag()? {
            true => Some(SpanName::new(self.u64()?, self.u64()?)),
            false => None,
        })
    }

    pub(in crate::cache::cache_stores::shared) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn bytes(&mut self) -> Result<Bytes> {
        let len = usize::try_from(self.u64()?).map_err(|_| Self::malformed("length"))?;
        self.take(len)
    }

    pub(in crate::cache::cache_stores::shared) fn range(&mut self) -> Result<Range<u64>> {
        let (start, end) = (self.u64()?, self.u64()?);
        if start > end {
            return Err(Self::malformed("range: it ends before it starts"));
        }
        Ok(start..end)
    }

    fn duration(&mut self) -> Result<Duration> {
        Ok(Duration::from_nanos(self.u64()?))
    }

    fn intent(&mut self) -> Result<Option<ReadIntent>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(ReadIntent::Latency)),
            2 => Ok(Some(ReadIntent::Throughput)),
            3 => Ok(Some(ReadIntent::Warm)),
            _ => Err(Self::malformed("read intent")),
        }
    }

    fn version(&mut self) -> Result<FileVersion> {
        let id = self.u64()?;
        let size = self.u64()?;
        if id >= FileVersionId::LIMIT {
            return Err(Self::malformed("file version id"));
        }
        FileVersion::named(FileVersionId::new(id), size)
            .ok_or_else(|| Self::malformed("file size: past what a cache holds"))
    }

    pub(in crate::cache::cache_stores::shared) fn sources(&mut self) -> Result<Vec<BlockSource>> {
        (0..self.u32()?)
            .map(|_| match self.u8()? {
                0 => Ok(BlockSource::Memory),
                1 => Ok(BlockSource::Disk),
                2 => Ok(BlockSource::Remote),
                3 => Ok(BlockSource::InFlight {
                    requests: self.u32()?,
                }),
                _ => Err(Self::malformed("block source")),
            })
            .collect()
    }

    fn plan(&mut self) -> Result<ReadPlan> {
        let gets = (0..self.u32()?)
            .map(|_| {
                let blocks = self.u32()?..self.u32()?;
                let get = PlannedGet::new(blocks, self.race()?);
                Ok(match self.late_race()? {
                    Some(after) => get.with_late_race(after),
                    None => get,
                })
            })
            .collect::<Result<_>>()?;
        Ok(ReadPlan::new(gets))
    }

    fn race(&mut self) -> Result<NonZeroU8> {
        NonZeroU8::new(self.u8()?).ok_or_else(|| Self::malformed("race: none racing"))
    }

    fn late_race(&mut self) -> Result<Option<Duration>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.duration()?)),
            _ => Err(Self::malformed("late race")),
        }
    }

    fn located_run(&mut self) -> Result<LocatedRun> {
        let first = self.u32()?;
        let asked = self.range()?;
        let sources = self.sources()?;
        let blocks = u32::try_from(sources.len()).ok();
        let end = blocks.and_then(|blocks| first.checked_add(blocks));
        if sources.is_empty() || end.is_none_or(|end| end > BlockKey::INDEX_LIMIT) {
            return Err(Self::malformed("a located run's blocks"));
        }
        Ok(LocatedRun::new(first, asked, sources))
    }

    fn next_step(&mut self) -> Result<NextStep> {
        match self.u8()? {
            0 => Ok(NextStep::Plan(self.located_run()?)),
            1 => {
                let fetches = (0..self.u32()?)
                    .map(|_| {
                        Ok(FetchSpec::new(
                            self.range()?,
                            self.race()?,
                            self.late_race()?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                Ok(NextStep::Fetch(fetches))
            }
            _ => Err(Self::malformed("next step")),
        }
    }

    fn phase(&mut self) -> Result<IoPhase> {
        Ok(match self.u8()? {
            0 => IoPhase::Started,
            1 => IoPhase::Answered {
                after: self.duration()?,
            },
            2 => IoPhase::Finished {
                bytes: self.u64()?,
                after: self.duration()?,
            },
            3 => IoPhase::Landed {
                answered: self.duration()?,
                received: self.duration()?,
                landing: self.duration()?,
                after: self.duration()?,
                waited: self.duration()?,
            },
            4 => IoPhase::Failed {
                after: self.duration()?,
            },
            5 => IoPhase::Cancelled {
                after: self.duration()?,
            },
            _ => return Err(Self::malformed("I/O phase")),
        })
    }
}

#[cfg(feature = "__fuzz")]
mod fuzzing;
#[cfg(feature = "__fuzz")]
pub use fuzzing::FuzzedFrame;

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
