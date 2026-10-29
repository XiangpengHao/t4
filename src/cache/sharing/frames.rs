//! The frames a shared cache's socket carries (`doc/11-shared-mode.md`,
//! decision 5): a little-endian `u64` length, then the body, a tag and its
//! fields, little-endian too. A client's frames ask its owner
//! ([`ClientFrame`]); the owner's answer a call, by the number the client
//! gave it, or tell of a read from disk made for one ([`OwnerFrame`]). Each
//! field is decoded and checked here, as it arrives: a frame that ends
//! early, runs past its end, or holds a value its field can't take is a
//! protocol error, which closes the connection (decision 7).

use std::num::NonZeroU8;
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;

use crate::cache::FileVersionId;
use crate::cache::block_index::FileVersion;
use crate::cache::object_cache::LocatedRun;
use crate::cache::policy::{BlockSource, IoPhase, PlannedGet, ReadIntent, ReadPlan};
use crate::io::error::{Error, Result};

/// The protocol's version: a client and an owner of different versions
/// don't talk.
pub(super) const PROTOCOL_VERSION: u32 = 1;

/// A frame a client sends its owner. A call, which the owner answers,
/// carries the number the client gave it, `call`; the other frames take no
/// answer. A read is named by the number the client gave it, `read`, from
/// its start to its end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ClientFrame {
    /// The first frame: the client's protocol version, sent with its
    /// descriptor of the first cache file.
    Hello { version: u32 },
    /// Starts a read of `range` of the version of the file at `path` with
    /// `e_tag` and `size`, for `intent`.
    StartRead {
        call: u64,
        read: u64,
        path: Bytes,
        e_tag: String,
        size: u64,
        range: Range<u64>,
        intent: Option<ReadIntent>,
    },
    /// The GETs the client's read policy planned for the run of the read
    /// the owner located last.
    TakeGets {
        call: u64,
        read: u64,
        plan: ReadPlan,
    },
    /// The next bytes of the read's fetch `fetch`.
    Land { read: u64, fetch: u32, bytes: Bytes },
    /// The read's fetch `fetch` is all in: its blocks are cached.
    Commit { call: u64, read: u64, fetch: u32 },
    /// The read's fetch `fetch` failed: its blocks go uncached.
    Abandon { read: u64, fetch: u32 },
    /// The read's work on the tiers, answered once it is done and the
    /// read's fetches are committed or abandoned.
    Finish { call: u64, read: u64 },
    /// The client is done with the read, or gave it up.
    EndRead { read: u64 },
}

/// A frame an owner sends its client: the answer to one of its calls, or
/// news of a read from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum OwnerFrame {
    /// The answer to [`ClientFrame::Hello`]: the owner's protocol version.
    Welcome {
        version: u32,
    },
    /// The read started: its file's version, and its next step.
    Started {
        call: u64,
        version: FileVersion,
        next: NextStep,
    },
    /// The cache can't hold the read's file: the read goes to the remote.
    Uncacheable {
        call: u64,
    },
    /// The GETs are taken: the read's next step.
    Next {
        call: u64,
        next: NextStep,
    },
    Committed {
        call: u64,
    },
    /// The read waited for blocks that no fetch cached: it fetches them,
    /// its next step.
    Retry {
        call: u64,
        next: NextStep,
    },
    /// The read's bytes, and its end; `None` if a read from disk is still
    /// writing them, as after one failed.
    Served {
        call: u64,
        bytes: Option<Bytes>,
    },
    /// The cache failed the call: the read is over, and goes to the remote.
    Failed {
        call: u64,
    },
    /// A read from cache file `drive`, of `bytes`, made for one of the
    /// client's reads, went on: for the client's read policy to hear.
    DiskRead {
        drive: u32,
        bytes: u32,
        phase: IoPhase,
    },
}

/// What a read does next: plan the GETs of a run of its blocks, or make
/// the GETs planned, each landed as fetch number its place in the list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum NextStep {
    Plan(LocatedRun),
    Fetch(Vec<FetchSpec>),
}

/// A GET a read makes: the file's bytes it fetches, how many requests race
/// for them at once, and when one more races late.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FetchSpec {
    bytes: Range<u64>,
    race: NonZeroU8,
    late: Option<Duration>,
}

impl FetchSpec {
    pub(super) fn new(bytes: Range<u64>, race: NonZeroU8, late: Option<Duration>) -> Self {
        Self { bytes, race, late }
    }

    pub(super) fn bytes(&self) -> Range<u64> {
        self.bytes.clone()
    }

    pub(super) fn race(&self) -> NonZeroU8 {
        self.race
    }

    pub(super) fn late_race(&self) -> Option<Duration> {
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

    /// What a client's first frame starts with.
    const MAGIC: [u8; 4] = *b"t4sh";

    /// The frame, its length first.
    pub(super) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Hello { version } => {
                let mut frame = FrameWriter::new(Self::HELLO);
                frame.raw(&Self::MAGIC);
                frame.u32(*version);
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
            } => {
                let mut frame = FrameWriter::new(Self::START_READ);
                frame.u64(*call);
                frame.u64(*read);
                frame.bytes(path);
                frame.bytes(e_tag.as_bytes());
                frame.u64(*size);
                frame.range(range);
                frame.intent(*intent);
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
        }
    }

    /// The frame whose body, its length taken off, is `body`.
    ///
    /// # Errors
    ///
    /// If the body isn't a client frame's, whole.
    pub(super) fn decode(body: Bytes) -> Result<Self> {
        let mut frame = FrameReader::new(body);
        let decoded = match frame.u8()? {
            Self::HELLO => {
                if frame.take(Self::MAGIC.len())? != Self::MAGIC[..] {
                    return Err(FrameReader::malformed("a hello's magic"));
                }
                Self::Hello {
                    version: frame.u32()?,
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
            _ => return Err(FrameReader::malformed("a client frame's tag")),
        };
        frame.finish()?;
        Ok(decoded)
    }
}

impl OwnerFrame {
    /// The number of the call the frame answers; `None` if it answers none.
    pub(super) fn call(&self) -> Option<u64> {
        match *self {
            Self::Started { call, .. }
            | Self::Uncacheable { call }
            | Self::Next { call, .. }
            | Self::Committed { call }
            | Self::Retry { call, .. }
            | Self::Served { call, .. }
            | Self::Failed { call } => Some(call),
            Self::Welcome { .. } | Self::DiskRead { .. } => None,
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

    /// The frame, its length first.
    pub(super) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Welcome { version } => {
                let mut frame = FrameWriter::new(Self::WELCOME);
                frame.u32(*version);
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
            Self::Uncacheable { call } => FrameWriter::call(Self::UNCACHEABLE, *call),
            Self::Next { call, next } => {
                let mut frame = FrameWriter::new(Self::NEXT);
                frame.u64(*call);
                frame.next_step(next);
                frame.finish()
            }
            Self::Committed { call } => FrameWriter::call(Self::COMMITTED, *call),
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
            Self::Failed { call } => FrameWriter::call(Self::FAILED, *call),
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
        }
    }

    /// The frame whose body, its length taken off, is `body`.
    ///
    /// # Errors
    ///
    /// If the body isn't an owner frame's, whole.
    pub(super) fn decode(body: Bytes) -> Result<Self> {
        let mut frame = FrameReader::new(body);
        let decoded = match frame.u8()? {
            Self::WELCOME => Self::Welcome {
                version: frame.u32()?,
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
            Self::DISK_READ => Self::DiskRead {
                drive: frame.u32()?,
                bytes: frame.u32()?,
                phase: frame.phase()?,
            },
            _ => return Err(FrameReader::malformed("an owner frame's tag")),
        };
        frame.finish()?;
        Ok(decoded)
    }
}

/// A frame being written: its length, filled in once it is whole, then its
/// tag and fields, each appended in order.
pub(in crate::cache) struct FrameWriter {
    frame: Vec<u8>,
}

impl FrameWriter {
    /// The bytes of a frame's length.
    const LENGTH_BYTES: usize = size_of::<u64>();

    fn new(tag: u8) -> Self {
        let mut frame = Vec::with_capacity(64);
        frame.extend_from_slice(&[0; Self::LENGTH_BYTES]);
        frame.push(tag);
        Self { frame }
    }

    /// A frame of `tag` whose one field is `call`.
    fn call(tag: u8, call: u64) -> Vec<u8> {
        let mut frame = Self::new(tag);
        frame.u64(call);
        frame.finish()
    }

    /// The frame, its length filled in.
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

    pub(in crate::cache) fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    /// `bytes`, their length first.
    fn bytes(&mut self, bytes: &[u8]) {
        self.u64(bytes.len() as u64);
        self.raw(bytes);
    }

    pub(in crate::cache) fn range(&mut self, range: &Range<u64>) {
        self.u64(range.start);
        self.u64(range.end);
    }

    /// `duration` in nanoseconds, the most a `u64` holds at most: 584 years.
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

    /// `sources`, their count first.
    pub(in crate::cache) fn sources(&mut self, sources: &[BlockSource]) {
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

    /// `plan`'s GETs, their count first.
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

    fn next_step(&mut self, next: &NextStep) {
        match next {
            NextStep::Plan(run) => {
                self.u8(0);
                run.write_to(self);
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

/// A frame's body being read: each field taken in order, checked as it is.
pub(in crate::cache) struct FrameReader {
    body: Bytes,
    /// Where the next field starts.
    at: usize,
}

impl FrameReader {
    fn new(body: Bytes) -> Self {
        Self { body, at: 0 }
    }

    /// The error of a frame whose `field` is wrong.
    pub(in crate::cache) fn malformed(field: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a shared cache's frame holds a malformed {field}"),
        ))
    }

    /// Whether the whole body was read: no field is left over.
    fn finish(self) -> Result<()> {
        if self.at == self.body.len() {
            Ok(())
        } else {
            Err(Self::malformed("end: bytes past its last field"))
        }
    }

    /// The next `len` bytes of the body.
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

    pub(in crate::cache) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    /// Bytes, their length first; a slice of the body, not a copy.
    fn bytes(&mut self) -> Result<Bytes> {
        let len = usize::try_from(self.u64()?).map_err(|_| Self::malformed("length"))?;
        self.take(len)
    }

    pub(in crate::cache) fn range(&mut self) -> Result<Range<u64>> {
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

    pub(in crate::cache) fn sources(&mut self) -> Result<Vec<BlockSource>> {
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

    fn next_step(&mut self) -> Result<NextStep> {
        match self.u8()? {
            0 => Ok(NextStep::Plan(LocatedRun::read_from(self)?)),
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

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
