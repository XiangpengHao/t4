use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::PoisonError;

use bytes::Bytes;

use super::shared_files::SharedFiles;
use super::{FuzzedHello, FuzzedPath, FuzzedSpan, FuzzedStep};
use crate::cache::cache_stores::shared::protocol::{
    ClientFrame, ClientRegion, FrameReceiver, FrameSocket, Hello, NextStep, OwnerAddress,
    OwnerFrame, PROTOCOL_VERSION, RegionSpan, SpanName, Transport,
};
use crate::cache::policy::FetchRuns;
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::{CacheableFile, FileVersion, LocatedRun};
use crate::io::file_system::{FileSystem, OpenFile};
use crate::io::sync::thread::{self, JoinHandle};
use crate::io::sync::{Arc, Condvar, Mutex, MutexGuard};

/// A connection the fuzzer scripts frame by frame. It tracks what it asked and what the owner
/// answered only to land a shared file's true bytes; everything else it sends as told.
pub(super) struct MisbehavingClient {
    socket: FrameSocket,
    region: Option<std::sync::Arc<ClientRegion>>,
    /// Held to the end: no span the owner was lent is ever reused.
    spans: Vec<RegionSpan>,
    heard: Arc<HeardFrames>,
    listener: Option<JoinHandle<()>>,
    reads: BTreeMap<u64, ReadOfFile>,
    calls: BTreeMap<u64, CallOf>,
    stream: StreamShape,
    probes_answered: u64,
    next_call: u64,
}

impl MisbehavingClient {
    /// Never one of the script's calls, which are a `u8`'s.
    const PROBE: u64 = u64::MAX;
    /// A well-behaved step's calls start past the script's, so that they say whose read is answered.
    const FIRST_OWN_CALL: u64 = 1 << 8;
    /// Fresh spans, staging a shared file's true bytes among them, come first fit from the
    /// region's start; the script names spans and writes only past this, so it never garbles
    /// bytes it stages for a shared file.
    const NAMED_FROM: u64 = ClientRegion::BYTES / 2;
    const NAMED_WITHIN: u64 = 4 << 20;
    const MOST_SCRIBBLED: u64 = 1 << 20;
    /// The rest of a fetch of a version the script made up may be GiBs.
    const MOST_LANDED: u64 = 4 << 20;

    pub(super) fn connect(
        file_system: &FileSystem,
        transport: &Transport,
        hello: &FuzzedHello,
    ) -> Self {
        let cache_file = Self::cache_file(file_system, true);
        let file = cache_file.identity().expect("the cache file's identity");
        let address = OwnerAddress::of(file, transport).expect("the owner's address");
        let socket = address
            .connect(rustix::process::geteuid())
            .expect("the owner listens");
        let frames = FrameReceiver::new(socket.try_clone().expect("a simulated socket clones"));
        let heard = Arc::new(HeardFrames::default());
        let listener = {
            let heard = Arc::clone(&heard);
            thread::spawn(move || heard.listen(frames))
        };
        let wants_region = match *hello {
            FuzzedHello::Admitted { region }
            | FuzzedHello::Versioned { region, .. }
            | FuzzedHello::ReadOnly { region }
            | FuzzedHello::Raw { region, .. } => region,
        };
        let region = wants_region.then(|| ClientRegion::make().expect("a simulated region"));
        let mut client = Self {
            socket,
            region,
            spans: Vec::new(),
            heard,
            listener: Some(listener),
            reads: BTreeMap::new(),
            calls: BTreeMap::new(),
            stream: StreamShape::default(),
            probes_answered: 0,
            next_call: Self::FIRST_OWN_CALL,
        };
        client.say_hello(file_system, hello, cache_file);
        client
    }

    pub(super) fn take(&mut self, step: &FuzzedStep, files: &SharedFiles) {
        self.take_heard();
        match step {
            FuzzedStep::StartRead {
                call,
                read,
                path,
                e_tag,
                size,
                range,
                intent,
                output,
            } => {
                let path = match path {
                    FuzzedPath::Picked(pick) => SharedFiles::path(*pick),
                    FuzzedPath::Named(path) => Bytes::copy_from_slice(path),
                };
                let (true_e_tag, true_size) = files.version_of(&path);
                let size = size.unwrap_or(true_size);
                let output = match output {
                    None => None,
                    Some(FuzzedSpan::Fresh) => self.fresh_output(range, size),
                    Some(FuzzedSpan::Named { offset, len }) => Some(self.named(*offset, *len)),
                };
                let (call, read) = (u64::from(*call), u64::from(*read));
                self.note_call(call, read);
                let shared = files.is_read_by_both(&path);
                self.reads
                    .entry(read)
                    .or_insert_with(|| ReadOfFile::new(path.clone(), shared));
                self.send_frame(&ClientFrame::StartRead {
                    call,
                    read,
                    path,
                    e_tag: e_tag.clone().unwrap_or(true_e_tag),
                    size,
                    range: range.clone(),
                    intent: *intent,
                    output,
                });
            }
            FuzzedStep::TakeGets { call, read, plan } => {
                let (call, read) = (u64::from(*call), u64::from(*read));
                self.note_call(call, read);
                let plan = plan.clone();
                self.send_frame(&ClientFrame::TakeGets { call, read, plan });
            }
            &FuzzedStep::Land { read, fetch, len } => {
                let (read, fetch) = (u64::from(read), u32::from(fetch));
                if let Some(bytes) = self.landing(read, fetch, len.map(u64::from)) {
                    let bytes = Bytes::from(bytes);
                    self.send_frame(&ClientFrame::Land { read, fetch, bytes });
                }
            }
            FuzzedStep::LandStaged {
                read,
                fetch,
                len,
                staged,
            } => self.land_staged(
                u64::from(*read),
                u32::from(*fetch),
                len.map(u64::from),
                staged,
            ),
            &FuzzedStep::Commit { call, read, fetch } => {
                let (call, read, fetch) = (u64::from(call), u64::from(read), u32::from(fetch));
                self.note_call(call, read);
                self.settle(read, fetch);
                self.send_frame(&ClientFrame::Commit { call, read, fetch });
            }
            &FuzzedStep::Abandon { read, fetch } => {
                let (read, fetch) = (u64::from(read), u32::from(fetch));
                self.settle(read, fetch);
                self.send_frame(&ClientFrame::Abandon { read, fetch });
            }
            &FuzzedStep::Finish { call, read } => {
                let (call, read) = (u64::from(call), u64::from(read));
                self.note_call(call, read);
                self.send_frame(&ClientFrame::Finish { call, read });
            }
            &FuzzedStep::EndRead { read } => {
                let read = u64::from(read);
                self.end(read);
                self.send_frame(&ClientFrame::EndRead { read });
            }
            FuzzedStep::KnownSize { call, path } => {
                let path = match path {
                    FuzzedPath::Picked(pick) => SharedFiles::path(*pick),
                    FuzzedPath::Named(path) => Bytes::copy_from_slice(path),
                };
                let call = u64::from(*call);
                self.send_frame(&ClientFrame::KnownSize { call, path });
            }
            &FuzzedStep::Hello { version, region } => {
                self.send_frame(&ClientFrame::Hello { version, region });
            }
            FuzzedStep::Frame { body } => {
                let mut frame = (body.len() as u64).to_le_bytes().to_vec();
                frame.extend_from_slice(body);
                self.send(&frame);
            }
            FuzzedStep::CutOff { length, body } => {
                let mut frame = length.to_le_bytes().to_vec();
                frame.extend_from_slice(body);
                self.send(&frame);
            }
            &FuzzedStep::Scribble { offset, len, byte } => {
                if let Some(region) = &self.region {
                    let offset = Self::NAMED_FROM + offset % Self::NAMED_WITHIN;
                    let len = u64::from(len).min(Self::MOST_SCRIBBLED);
                    region.scribble(SpanName::new(offset, len), byte);
                }
            }
            FuzzedStep::Fence => self.fence(),
            &FuzzedStep::Proceed { read, file, staged } => {
                self.proceed(u64::from(read), file, staged, files);
            }
        }
    }

    /// Sends what a well-behaved client would next, as far as the owner's answers say, so that
    /// the script's other steps come mid-read.
    fn proceed(&mut self, read: u64, file: u8, staged: bool, files: &SharedFiles) {
        self.fence();
        let call = self.next_call;
        self.next_call += 1;
        let Some(of_file) = self.reads.get_mut(&read) else {
            let path = SharedFiles::path(file);
            let (e_tag, size) = files.version_of(&path);
            let output = self.fresh_output(&(0..size), size);
            self.note_call(call, read);
            let shared = files.is_read_by_both(&path);
            self.reads
                .insert(read, ReadOfFile::new(path.clone(), shared));
            return self.send_frame(&ClientFrame::StartRead {
                call,
                read,
                path,
                e_tag,
                size,
                range: 0..size,
                intent: None,
                output,
            });
        };
        if of_file.done {
            self.end(read);
            return self.send_frame(&ClientFrame::EndRead { read });
        }
        if let (Some(run), Some(version)) = (of_file.plan.take(), of_file.version) {
            let file = CacheableFile::named(&of_file.path, version);
            let plan = run.planned_by(&FetchRuns::default(), &file, None);
            self.note_call(call, read);
            return self.send_frame(&ClientFrame::TakeGets { call, read, plan });
        }
        let open = of_file
            .fetches
            .iter()
            .flatten()
            .position(|landing| !landing.settled);
        if let Some(fetch) = open {
            let fetch = fetch as u32;
            match staged && self.region.is_some() {
                true => self.land_staged(read, fetch, None, &FuzzedSpan::Fresh),
                false => {
                    let bytes = self.landing(read, fetch, None).map(Bytes::from);
                    if let Some(bytes) = bytes {
                        self.send_frame(&ClientFrame::Land { read, fetch, bytes });
                    }
                }
            }
            self.note_call(call, read);
            self.settle(read, fetch);
            return self.send_frame(&ClientFrame::Commit { call, read, fetch });
        }
        if !of_file.finished {
            of_file.finished = true;
            self.note_call(call, read);
            self.send_frame(&ClientFrame::Finish { call, read });
        }
    }

    /// The owner answers two probes in turn, or closes: every answer to a frame before the
    /// first has come by the second's, as the owner answers a probe as it takes it, and a read's
    /// frames in the same pass. A stream cut mid-frame would take the probe in, so it isn't
    /// probed.
    pub(super) fn fence(&mut self) {
        if !self.stream.at_frame_boundary() {
            return;
        }
        for _ in 0..2 {
            self.send_frame(&ClientFrame::KnownSize {
                call: Self::PROBE,
                path: Bytes::new(),
            });
            let probes = self.probes_answered + 1;
            loop {
                let (frames, closed) = self.heard.wait();
                for frame in frames {
                    self.take_in(frame);
                }
                if self.probes_answered >= probes {
                    break;
                }
                if closed {
                    return;
                }
            }
        }
    }

    /// Kills the connection, mid-frame or not, and hands back its region, if it has one.
    pub(super) fn close(mut self) -> Option<std::sync::Arc<ClientRegion>> {
        self.take_heard();
        self.socket.shut_down();
        if let Some(listener) = self.listener.take() {
            listener.join().expect("the listener only takes frames");
        }
        self.region.take()
    }

    /// Its descriptors go beside its first byte, as `SCM_RIGHTS` sends them.
    fn say_hello(&mut self, file_system: &FileSystem, hello: &FuzzedHello, cache_file: OpenFile) {
        let region = self.region.is_some();
        let hello_of = |version| ClientFrame::Hello { version, region }.encode();
        let (frame, cache_file) = match hello {
            FuzzedHello::Admitted { .. } => (hello_of(PROTOCOL_VERSION), cache_file),
            &FuzzedHello::Versioned { version, .. } => (hello_of(version), cache_file),
            FuzzedHello::ReadOnly { .. } => (
                hello_of(PROTOCOL_VERSION),
                Self::cache_file(file_system, false),
            ),
            FuzzedHello::Raw { bytes, .. } => (bytes.clone(), cache_file),
        };
        let descriptor = self.region.as_ref().map(|region| region.descriptor());
        self.socket.send_descriptors(cache_file, descriptor);
        self.send(&frame);
    }

    fn cache_file(file_system: &FileSystem, writable: bool) -> OpenFile {
        let path = Path::new(super::FuzzedSession::CACHE_FILE);
        (file_system.open_existing(path, writable)).expect("the owner's cache file")
    }

    fn send_frame(&mut self, frame: &ClientFrame) {
        self.send(&frame.encode());
    }

    /// The owner may have closed the connection: what follows goes nowhere.
    fn send(&mut self, bytes: &[u8]) {
        self.stream.sent(bytes);
        let _ = self.socket.send(bytes);
    }

    fn take_heard(&mut self) {
        let frames = self.heard.take();
        for frame in frames {
            self.take_in(frame);
        }
    }

    fn take_in(&mut self, frame: OwnerFrame) {
        match frame {
            OwnerFrame::KnownSize {
                call: Self::PROBE, ..
            } => self.probes_answered += 1,
            OwnerFrame::Started {
                call,
                version,
                next,
            } => {
                if let Some(read) = self.read_of(call) {
                    read.version = Some(version);
                    read.next(next);
                }
            }
            OwnerFrame::Next { call, next } => {
                if let Some(read) = self.read_of(call) {
                    read.next(next);
                }
            }
            OwnerFrame::Retry { call, next } => {
                if let Some(read) = self.read_of(call) {
                    read.finished = false;
                    read.next(next);
                }
            }
            // A failed commit's read goes on, but ending it is always allowed.
            OwnerFrame::Served { call, .. }
            | OwnerFrame::ServedInSpan { call, .. }
            | OwnerFrame::Failed { call }
            | OwnerFrame::Uncacheable { call } => {
                if let Some(read) = self.read_of(call) {
                    read.done();
                }
            }
            OwnerFrame::Welcome { .. }
            | OwnerFrame::Committed { .. }
            | OwnerFrame::Ended { .. }
            | OwnerFrame::DiskRead { .. }
            | OwnerFrame::KnownSize { .. } => {}
        }
    }

    fn read_of(&mut self, call: u64) -> Option<&mut ReadOfFile> {
        match self.calls.get(&call)? {
            CallOf::Read(read) => self.reads.get_mut(read),
            CallOf::Unknown => None,
        }
    }

    /// A call number of two reads, or of a read since ended, says nothing of whose its answer is.
    fn note_call(&mut self, call: u64, read: u64) {
        self.calls
            .entry(call)
            .and_modify(|of| {
                if *of != CallOf::Read(read) {
                    *of = CallOf::Unknown;
                }
            })
            .or_insert(CallOf::Read(read));
    }

    fn end(&mut self, read: u64) {
        self.reads.remove(&read);
        for of in self.calls.values_mut() {
            if *of == CallOf::Read(read) {
                *of = CallOf::Unknown;
            }
        }
    }

    /// Once all are settled, the owner may start the next round's, which its answer names.
    fn settle(&mut self, read: u64, fetch: u32) {
        let Some(ReadOfFile {
            fetches: Some(fetches),
            ..
        }) = self.reads.get_mut(&read)
        else {
            return;
        };
        if let Some(landing) = fetches.get_mut(fetch as usize) {
            landing.settled = true;
        }
        if fetches.iter().all(|landing| landing.settled) {
            self.reads.get_mut(&read).expect("just found").fetches = None;
        }
    }

    /// The fetch's next `len` bytes, or the rest of it, true if the owner named it; for a shared
    /// file, none if it didn't, as they might land.
    fn landing(&mut self, read: u64, fetch: u32, len: Option<u64>) -> Option<Vec<u8>> {
        let filler = |len: Option<u64>| Some(vec![0xee; len.unwrap_or(0) as usize]);
        let Some(of_file) = self.reads.get_mut(&read) else {
            return filler(len);
        };
        let open = of_file
            .fetches
            .as_mut()
            .and_then(|fetches| fetches.get_mut(fetch as usize))
            .filter(|landing| !landing.settled);
        match open {
            Some(landing) => {
                let at = landing.bytes.start.saturating_add(landing.landed);
                let rest = landing.bytes.end.saturating_sub(at).min(Self::MOST_LANDED);
                let len = len.unwrap_or(rest);
                landing.landed = landing.landed.saturating_add(len);
                Some(SharedFiles::bytes(
                    &of_file.path,
                    at..at.saturating_add(len),
                ))
            }
            None if of_file.shared => None,
            None => filler(len),
        }
    }

    fn land_staged(&mut self, read: u64, fetch: u32, len: Option<u64>, staged: &FuzzedSpan) {
        let shared = self.reads.get(&read).is_some_and(|of_file| of_file.shared);
        let staged = match *staged {
            FuzzedSpan::Named { offset, len } => {
                let named = self.named(offset, len);
                let in_region = named
                    .offset()
                    .checked_add(named.len())
                    .is_some_and(|end| end <= ClientRegion::BYTES);
                // A shared file's bytes from a span the script wrote would land garbled.
                (!(shared && in_region && self.region.is_some())).then_some(named)
            }
            FuzzedSpan::Fresh => None,
        };
        let staged = match staged {
            Some(named) => named,
            None => {
                let Some(bytes) = self.landing(read, fetch, len) else {
                    return;
                };
                let len = bytes.len() as u64;
                match self.fresh_span(len) {
                    Some(mut span) => {
                        span.copy_in(0, &bytes);
                        let name = SpanName::new(span.name().offset(), len);
                        self.spans.push(span);
                        name
                    }
                    None => SpanName::new(ClientRegion::BYTES, len),
                }
            }
        };
        self.send_frame(&ClientFrame::LandStaged {
            read,
            fetch,
            staged,
        });
    }

    fn fresh_output(&mut self, range: &std::ops::Range<u64>, size: u64) -> Option<SpanName> {
        let start = range.start.min(size);
        let asked = start..range.end.clamp(start, size);
        let span = self.fresh_span(ReadOutput::padded_len(&asked))?;
        let name = span.name();
        self.spans.push(span);
        Some(name)
    }

    fn fresh_span(&mut self, len: u64) -> Option<RegionSpan> {
        let span = self.region.as_ref()?.span(len)?;
        let name = span.name();
        (name.offset() + name.len() <= Self::NAMED_FROM).then_some(span)
    }

    /// Past the region it stays, for the owner to refuse; within, it lies past the fresh spans.
    fn named(&self, offset: u64, len: u64) -> SpanName {
        if self.region.is_none() || offset >= ClientRegion::BYTES {
            return SpanName::new(offset, len);
        }
        SpanName::new(Self::NAMED_FROM + offset % Self::NAMED_WITHIN, len)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallOf {
    Read(u64),
    Unknown,
}

struct ReadOfFile {
    path: Bytes,
    shared: bool,
    version: Option<FileVersion>,
    /// The run the owner last asked a plan of, till one is sent.
    plan: Option<LocatedRun>,
    /// The round's, as the owner last named them: none between rounds.
    fetches: Option<Vec<FetchLanding>>,
    finished: bool,
    done: bool,
}

impl ReadOfFile {
    fn new(path: Bytes, shared: bool) -> Self {
        Self {
            path,
            shared,
            version: None,
            plan: None,
            fetches: None,
            finished: false,
            done: false,
        }
    }

    fn next(&mut self, next: NextStep) {
        (self.plan, self.fetches) = match next {
            NextStep::Plan(run) => (Some(run), None),
            NextStep::Fetch(specs) => {
                let fetches = specs.iter().map(|spec| FetchLanding::new(spec.bytes()));
                (None, Some(fetches.collect()))
            }
        };
    }

    fn done(&mut self) {
        (self.plan, self.fetches, self.done) = (None, None, true);
    }
}

struct FetchLanding {
    bytes: std::ops::Range<u64>,
    landed: u64,
    settled: bool,
}

impl FetchLanding {
    fn new(bytes: std::ops::Range<u64>) -> Self {
        Self {
            bytes,
            landed: 0,
            settled: false,
        }
    }
}

/// Where the bytes sent so far leave the owner's framing: a hello of `Hello::FRAME_BYTES`, then
/// frames, each a little-endian `u64` length and its body.
struct StreamShape {
    hello_left: usize,
    length: Vec<u8>,
    body_left: u64,
}

impl Default for StreamShape {
    fn default() -> Self {
        Self {
            hello_left: Hello::FRAME_BYTES,
            length: Vec::new(),
            body_left: 0,
        }
    }
}

impl StreamShape {
    fn sent(&mut self, mut bytes: &[u8]) {
        let hello = bytes.len().min(self.hello_left);
        self.hello_left -= hello;
        bytes = &bytes[hello..];
        while !bytes.is_empty() {
            if self.body_left > 0 {
                let body = (bytes.len() as u64).min(self.body_left);
                self.body_left -= body;
                bytes = &bytes[body as usize..];
                continue;
            }
            let length = (size_of::<u64>() - self.length.len()).min(bytes.len());
            self.length.extend_from_slice(&bytes[..length]);
            bytes = &bytes[length..];
            if let Ok(length) = <[u8; 8]>::try_from(self.length.as_slice()) {
                self.body_left = u64::from_le_bytes(length);
                self.length.clear();
            }
        }
    }

    fn at_frame_boundary(&self) -> bool {
        self.hello_left == 0 && self.length.is_empty() && self.body_left == 0
    }
}

#[derive(Default)]
struct HeardFrames {
    state: Mutex<Heard>,
    changed: Condvar,
}

#[derive(Default)]
struct Heard {
    frames: VecDeque<OwnerFrame>,
    closed: bool,
}

impl HeardFrames {
    /// Takes every frame, so that the owner never waits on a full socket to send.
    fn listen(&self, mut frames: FrameReceiver) {
        loop {
            let frame = match frames.receive(FrameSocket::MOST_BYTES) {
                Ok(Some(body)) => {
                    Some(OwnerFrame::decode(body).expect("an owner sends only frames it may"))
                }
                Ok(None) | Err(_) => None,
            };
            let mut heard = self.state();
            match frame {
                Some(frame) => heard.frames.push_back(frame),
                None => heard.closed = true,
            }
            self.changed.notify_all();
            if heard.closed {
                return;
            }
        }
    }

    fn take(&self) -> VecDeque<OwnerFrame> {
        std::mem::take(&mut self.state().frames)
    }

    /// Some frames, or none once closed.
    fn wait(&self) -> (VecDeque<OwnerFrame>, bool) {
        let mut heard = self.state();
        while heard.frames.is_empty() && !heard.closed {
            heard = self
                .changed
                .wait(heard)
                .unwrap_or_else(PoisonError::into_inner);
        }
        (std::mem::take(&mut heard.frames), heard.closed)
    }

    fn state(&self) -> MutexGuard<'_, Heard> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
