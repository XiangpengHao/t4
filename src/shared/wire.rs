//! What a client and the store's server say to each other: frames over a
//! stream socket, each a little-endian `u64` body length, then the body. A
//! handshake comes first; then requests and replies, matched by request id.
//! A descriptor travels as `SCM_RIGHTS`, attached to the first byte of the
//! frame that carries it. Value bytes travel in the frames, or, where the
//! client shares memory with the server (Linux), in a buffer a frame names.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, IoSlice, IoSliceMut, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;

use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags,
};

use crate::input_kv::{InputError, T4Key, T4Value};
use crate::io::error::{Error, Result};
use crate::range::RangeRequestU32;

/// Bumped on any change to the frames below: a client and a server of
/// different versions refuse each other in the handshake.
const PROTOCOL_VERSION: u32 = 2;
/// Opens every handshake, so a server can tell a t4 client from anything
/// else that connects.
const HANDSHAKE_MAGIC: [u8; 4] = *b"t4sh";

/// A frame on its way out: its bytes, and the descriptors attached to it.
pub(super) struct OutgoingFrame {
    bytes: Vec<u8>,
    fds: Vec<OwnedFd>,
}

impl OutgoingFrame {
    /// The most descriptors a frame carries: a handshake's two.
    const MAX_FDS: usize = 2;

    pub(super) fn write_to(&self, mut socket: &UnixStream) -> io::Result<()> {
        if self.fds.is_empty() {
            return socket.write_all(&self.bytes);
        }
        let fds: Vec<_> = self.fds.iter().map(AsFd::as_fd).collect();
        let mut space =
            [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(OutgoingFrame::MAX_FDS))];
        let mut control = SendAncillaryBuffer::new(&mut space);
        let fits = control.push(SendAncillaryMessage::ScmRights(&fds));
        debug_assert!(fits, "the control buffer is sized for MAX_FDS");
        let sent = loop {
            match rustix::net::sendmsg(
                socket,
                &[IoSlice::new(&self.bytes)],
                &mut control,
                SendFlags::empty(),
            ) {
                Err(rustix::io::Errno::INTR) => continue,
                result => break result?,
            }
        };
        // The descriptors went with the first byte; the rest is plain bytes.
        socket.write_all(&self.bytes[sent..])
    }
}

/// One frame being built: room for the length, then the body's fields.
struct FrameBuilder {
    bytes: Vec<u8>,
}

impl FrameBuilder {
    fn new() -> Self {
        Self {
            bytes: vec![0; size_of::<u64>()],
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// A length-prefixed run of bytes.
    fn bytes(&mut self, bytes: &[u8]) {
        self.u64(bytes.len() as u64);
        self.bytes.extend_from_slice(bytes);
    }

    fn key(&mut self, key: &T4Key) {
        self.bytes(key.as_bytes());
    }

    fn text(&mut self, text: &str) {
        self.bytes(text.as_bytes());
    }

    fn finish(mut self) -> Vec<u8> {
        let body_len = (self.bytes.len() - size_of::<u64>()) as u64;
        self.bytes[..size_of::<u64>()].copy_from_slice(&body_len.to_le_bytes());
        self.bytes
    }
}

/// One frame's body, its fields read front to back.
struct FrameFields<'a> {
    rest: &'a [u8],
}

impl<'a> FrameFields<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { rest: body }
    }

    fn malformed(what: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("malformed frame: {what}"),
        )
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if self.rest.len() < len {
            return Err(Self::malformed("ends early"));
        }
        let (field, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(field)
    }

    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("took exactly N bytes"))
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn bool(&mut self) -> io::Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Self::malformed("bool")),
        }
    }

    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = usize::try_from(self.u64()?).map_err(|_| Self::malformed("length"))?;
        self.take(len)
    }

    fn key(&mut self) -> io::Result<T4Key> {
        T4Key::try_from_slice(self.bytes()?).map_err(|_| Self::malformed("key too long"))
    }

    fn text(&mut self) -> io::Result<String> {
        Ok(String::from_utf8_lossy(self.bytes()?).into_owned())
    }

    /// Checks nothing is left over: every frame's fields fill its body.
    fn finish(self) -> io::Result<()> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(Self::malformed("trailing bytes"))
        }
    }
}

/// Reads whole frames off a stream.
pub(super) struct FrameReader<R> {
    source: R,
}

impl<R: Read> FrameReader<R> {
    pub(super) fn new(source: R) -> Self {
        Self { source }
    }

    pub(super) fn source(&mut self) -> &mut R {
        &mut self.source
    }

    /// The next frame's body, or `None` if the stream ended between frames.
    pub(super) fn read_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut len = [0; size_of::<u64>()];
        let first = loop {
            match self.source.read(&mut len) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                result => break result?,
            }
        };
        if first == 0 {
            return Ok(None);
        }
        self.source.read_exact(&mut len[first..])?;
        let len = u64::from_le_bytes(len);
        // Grown as the bytes arrive, so a bogus length can't allocate ahead.
        let mut body = Vec::new();
        (&mut self.source).take(len).read_to_end(&mut body)?;
        if body.len() as u64 != len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(Some(body))
    }
}

/// A socket read with `recvmsg`, so the descriptors a client attaches
/// arrive with its bytes. They wait here, in order, until the frame that
/// carries one claims it.
pub(super) struct FdReceiver {
    socket: UnixStream,
    fds: VecDeque<OwnedFd>,
}

impl FdReceiver {
    /// The most descriptors one read takes: frames carry one each.
    const MAX_FDS_PER_READ: usize = 4;

    pub(super) fn new(socket: UnixStream) -> Self {
        Self {
            socket,
            fds: VecDeque::new(),
        }
    }

    fn take_fd(&mut self) -> Option<OwnedFd> {
        self.fds.pop_front()
    }
}

impl Read for FdReceiver {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut space =
            [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(FdReceiver::MAX_FDS_PER_READ))];
        let mut control = RecvAncillaryBuffer::new(&mut space);
        #[cfg(target_os = "linux")]
        let flags = RecvFlags::CMSG_CLOEXEC;
        #[cfg(not(target_os = "linux"))]
        let flags = RecvFlags::empty();
        let received = rustix::net::recvmsg(
            &self.socket,
            &mut [IoSliceMut::new(buf)],
            &mut control,
            flags,
        )?;
        if received.flags.contains(ReturnFlags::CTRUNC) {
            return Err(FrameFields::malformed("too many descriptors"));
        }
        for message in control.drain() {
            if let RecvAncillaryMessage::ScmRights(fds) = message {
                self.fds.extend(fds);
            }
        }
        Ok(received.bytes)
    }
}

/// The client's opening frame. It carries the client's own read-write
/// descriptor of the store: the kernel checked the client's permissions
/// when it opened the file, so whoever can open the store read-write may
/// use its server. Abstract socket names have no permissions of their own.
/// After the store comes the memfd of the connection's shared region, if
/// the client has one.
pub(super) struct Handshake;

impl Handshake {
    pub(super) fn frame(store: OwnedFd, region: Option<OwnedFd>) -> OutgoingFrame {
        let mut frame = FrameBuilder::new();
        HANDSHAKE_MAGIC.iter().for_each(|byte| frame.u8(*byte));
        frame.u32(PROTOCOL_VERSION);
        frame.u8(u8::from(region.is_some()));
        OutgoingFrame {
            bytes: frame.finish(),
            fds: [Some(store), region].into_iter().flatten().collect(),
        }
    }

    /// The store descriptor and the region memfd the handshake in `body`
    /// carries, or why it is refused.
    pub(super) fn decode(
        body: &[u8],
        fds: &mut FdReceiver,
    ) -> std::result::Result<(OwnedFd, Option<OwnedFd>), String> {
        let mut fields = FrameFields::new(body);
        let mut check = || -> io::Result<bool> {
            let magic: [u8; 4] = fields.array()?;
            let version = fields.u32()?;
            Ok(magic == HANDSHAKE_MAGIC && version == PROTOCOL_VERSION)
        };
        match check() {
            Ok(true) => {}
            _ => return Err(format!("not a t4 client of protocol {PROTOCOL_VERSION}")),
        }
        let has_region = fields.bool().map_err(|err| err.to_string())?;
        fields.finish().map_err(|err| err.to_string())?;
        let store = fds
            .take_fd()
            .ok_or_else(|| "handshake carries no store descriptor".to_string())?;
        let region = has_region
            .then(|| fds.take_fd())
            .map(|region| region.ok_or_else(|| "handshake carries no region".to_string()))
            .transpose()?;
        Ok((store, region))
    }
}

/// The server's answer to a handshake.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum HandshakeReply {
    /// The client may send requests. It learns the server's process id,
    /// and whether the server's store uses direct I/O, which the client
    /// checks against its own options.
    Accepted {
        server_pid: u32,
        direct_io: bool,
    },
    Refused(String),
}

impl HandshakeReply {
    const ACCEPTED: u8 = 0;
    const REFUSED: u8 = 1;

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut frame = FrameBuilder::new();
        match self {
            Self::Accepted {
                server_pid,
                direct_io,
            } => {
                frame.u8(Self::ACCEPTED);
                frame.u32(*server_pid);
                frame.u8(u8::from(*direct_io));
            }
            Self::Refused(reason) => {
                frame.u8(Self::REFUSED);
                frame.text(reason);
            }
        }
        frame.finish()
    }

    pub(super) fn decode(body: &[u8]) -> io::Result<Self> {
        let mut fields = FrameFields::new(body);
        let reply = match fields.u8()? {
            Self::ACCEPTED => Self::Accepted {
                server_pid: fields.u32()?,
                direct_io: fields.bool()?,
            },
            Self::REFUSED => Self::Refused(fields.text()?),
            _ => return Err(FrameFields::malformed("handshake reply")),
        };
        fields.finish()?;
        Ok(reply)
    }
}

/// Where a request's value bytes are, when not in its frame: in the
/// connection's shared region, or in a memfd of their own the frame
/// carries. `capacity` is the buffer's size, in whole pages.
pub(super) enum WireBuffer {
    Region { offset: u64, capacity: u32 },
    Own { memfd: OwnedFd, capacity: u32 },
}

impl WireBuffer {
    const REGION: u8 = 0;
    const OWN: u8 = 1;

    fn encode(self, frame: &mut FrameBuilder, fds: &mut Vec<OwnedFd>) {
        match self {
            Self::Region { offset, capacity } => {
                frame.u8(Self::REGION);
                frame.u64(offset);
                frame.u32(capacity);
            }
            Self::Own { memfd, capacity } => {
                frame.u8(Self::OWN);
                frame.u32(capacity);
                fds.push(memfd);
            }
        }
    }

    fn decode(fields: &mut FrameFields<'_>, fds: &mut FdReceiver) -> io::Result<Self> {
        match fields.u8()? {
            Self::REGION => Ok(Self::Region {
                offset: fields.u64()?,
                capacity: fields.u32()?,
            }),
            Self::OWN => Ok(Self::Own {
                capacity: fields.u32()?,
                memfd: fds
                    .take_fd()
                    .ok_or_else(|| FrameFields::malformed("buffer carries no memfd"))?,
            }),
            _ => Err(FrameFields::malformed("buffer")),
        }
    }

    fn encode_optional(buffer: Option<Self>, frame: &mut FrameBuilder, fds: &mut Vec<OwnedFd>) {
        frame.u8(u8::from(buffer.is_some()));
        if let Some(buffer) = buffer {
            buffer.encode(frame, fds);
        }
    }

    fn decode_optional(
        fields: &mut FrameFields<'_>,
        fds: &mut FdReceiver,
    ) -> io::Result<Option<Self>> {
        fields
            .bool()?
            .then(|| Self::decode(fields, fds))
            .transpose()
    }
}

/// A put's value: in the frame, or `len` bytes at the start of a buffer.
pub(super) enum ValueIn {
    Inline(T4Value),
    Shared { buffer: WireBuffer, len: u32 },
}

/// What a client asks of the store. A read names the buffer it wants the
/// bytes in, if the client shares memory with the server; without one the
/// bytes come back in the reply's frame.
pub(super) enum Request {
    Put {
        key: T4Key,
        value: ValueIn,
    },
    Get {
        key: T4Key,
        into: Option<WireBuffer>,
    },
    GetRange {
        key: T4Key,
        range: RangeRequestU32,
        into: Option<WireBuffer>,
    },
    Remove {
        key: T4Key,
    },
    Len,
    /// Snapshot into `target`, an empty store file the client created.
    Snapshot {
        target: File,
    },
}

impl Request {
    const PUT: u8 = 1;
    const GET: u8 = 2;
    const GET_RANGE: u8 = 3;
    const REMOVE: u8 = 4;
    const LEN: u8 = 5;
    const SNAPSHOT: u8 = 6;

    const INLINE: u8 = 0;
    const SHARED: u8 = 1;

    pub(super) fn into_frame(self, id: u64) -> OutgoingFrame {
        let mut frame = FrameBuilder::new();
        frame.u64(id);
        let mut fds = Vec::new();
        match self {
            Self::Put { key, value } => {
                frame.u8(Self::PUT);
                frame.key(&key);
                match value {
                    ValueIn::Inline(value) => {
                        frame.u8(Self::INLINE);
                        frame.bytes(value.as_bytes());
                    }
                    ValueIn::Shared { buffer, len } => {
                        frame.u8(Self::SHARED);
                        frame.u32(len);
                        buffer.encode(&mut frame, &mut fds);
                    }
                }
            }
            Self::Get { key, into } => {
                frame.u8(Self::GET);
                frame.key(&key);
                WireBuffer::encode_optional(into, &mut frame, &mut fds);
            }
            Self::GetRange { key, range, into } => {
                frame.u8(Self::GET_RANGE);
                frame.key(&key);
                frame.u32(range.start);
                frame.u32(range.len);
                WireBuffer::encode_optional(into, &mut frame, &mut fds);
            }
            Self::Remove { key } => {
                frame.u8(Self::REMOVE);
                frame.key(&key);
            }
            Self::Len => frame.u8(Self::LEN),
            Self::Snapshot { target } => {
                frame.u8(Self::SNAPSHOT);
                fds.push(target.into());
            }
        }
        OutgoingFrame {
            bytes: frame.finish(),
            fds,
        }
    }

    /// The request in `body`, and its id. A descriptor the request carries,
    /// a snapshot's target or a buffer's memfd, is the next one `fds`
    /// received.
    pub(super) fn decode(body: &[u8], fds: &mut FdReceiver) -> io::Result<(u64, Self)> {
        let mut fields = FrameFields::new(body);
        let id = fields.u64()?;
        let request = match fields.u8()? {
            Self::PUT => {
                let key = fields.key()?;
                let value = match fields.u8()? {
                    Self::INLINE => ValueIn::Inline(
                        T4Value::try_from_vec(fields.bytes()?.to_vec())
                            .map_err(|_| FrameFields::malformed("value too long"))?,
                    ),
                    Self::SHARED => {
                        let len = fields.u32()?;
                        ValueIn::Shared {
                            buffer: WireBuffer::decode(&mut fields, fds)?,
                            len,
                        }
                    }
                    _ => return Err(FrameFields::malformed("value")),
                };
                Self::Put { key, value }
            }
            Self::GET => {
                let key = fields.key()?;
                let into = WireBuffer::decode_optional(&mut fields, fds)?;
                Self::Get { key, into }
            }
            Self::GET_RANGE => {
                let key = fields.key()?;
                let range = RangeRequestU32::from_u32(fields.u32()?, fields.u32()?)
                    .ok_or_else(|| FrameFields::malformed("range"))?;
                let into = WireBuffer::decode_optional(&mut fields, fds)?;
                Self::GetRange { key, range, into }
            }
            Self::REMOVE => Self::Remove { key: fields.key()? },
            Self::LEN => Self::Len,
            Self::SNAPSHOT => {
                let target = fds
                    .take_fd()
                    .ok_or_else(|| FrameFields::malformed("snapshot carries no file"))?;
                Self::Snapshot {
                    target: target.into(),
                }
            }
            _ => return Err(FrameFields::malformed("operation")),
        };
        fields.finish()?;
        Ok((id, request))
    }
}

/// What a request comes back with, when it succeeds.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Reply {
    Done,
    /// A read's bytes, in the frame.
    Bytes(Vec<u8>),
    /// A read's bytes are in the buffer the request named: `len` of them,
    /// from `start`.
    InBuffer {
        start: u32,
        len: u32,
    },
    /// The buffer a read named is too small: it needs `needed` bytes. The
    /// value wasn't read.
    TooSmall {
        needed: u32,
    },
    Removed(bool),
    Count(u64),
}

impl Reply {
    const DONE: u8 = 0;
    const BYTES: u8 = 1;
    const REMOVED: u8 = 2;
    const COUNT: u8 = 3;
    const FAILED: u8 = 4;
    const IN_BUFFER: u8 = 5;
    const TOO_SMALL: u8 = 6;

    pub(super) fn unexpected(self) -> Error {
        Error::Server(format!("unexpected reply {self:?}"))
    }

    pub(super) fn into_done(self) -> Result<()> {
        match self {
            Self::Done => Ok(()),
            other => Err(other.unexpected()),
        }
    }

    pub(super) fn into_bytes(self) -> Result<Vec<u8>> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            other => Err(other.unexpected()),
        }
    }

    pub(super) fn into_removed(self) -> Result<bool> {
        match self {
            Self::Removed(removed) => Ok(removed),
            other => Err(other.unexpected()),
        }
    }

    pub(super) fn into_count(self) -> Result<u64> {
        match self {
            Self::Count(count) => Ok(count),
            other => Err(other.unexpected()),
        }
    }
}

/// The reply to one request: its id, and how the request went.
pub(super) struct ReplyFrame {
    id: u64,
    result: Result<Reply>,
}

impl ReplyFrame {
    pub(super) fn new(id: u64, result: Result<Reply>) -> Self {
        Self { id, result }
    }

    pub(super) fn into_parts(self) -> (u64, Result<Reply>) {
        (self.id, self.result)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut frame = FrameBuilder::new();
        frame.u64(self.id);
        match &self.result {
            Ok(Reply::Done) => frame.u8(Reply::DONE),
            Ok(Reply::Bytes(bytes)) => {
                frame.u8(Reply::BYTES);
                frame.bytes(bytes);
            }
            Ok(Reply::Removed(removed)) => {
                frame.u8(Reply::REMOVED);
                frame.u8(u8::from(*removed));
            }
            Ok(Reply::Count(count)) => {
                frame.u8(Reply::COUNT);
                frame.u64(*count);
            }
            Ok(Reply::InBuffer { start, len }) => {
                frame.u8(Reply::IN_BUFFER);
                frame.u32(*start);
                frame.u32(*len);
            }
            Ok(Reply::TooSmall { needed }) => {
                frame.u8(Reply::TOO_SMALL);
                frame.u32(*needed);
            }
            Err(err) => {
                frame.u8(Reply::FAILED);
                WireError(err).encode(&mut frame);
            }
        }
        frame.finish()
    }

    pub(super) fn decode(body: &[u8]) -> io::Result<Self> {
        let mut fields = FrameFields::new(body);
        let id = fields.u64()?;
        let result = match fields.u8()? {
            Reply::DONE => Ok(Reply::Done),
            Reply::BYTES => Ok(Reply::Bytes(fields.bytes()?.to_vec())),
            Reply::REMOVED => Ok(Reply::Removed(fields.bool()?)),
            Reply::COUNT => Ok(Reply::Count(fields.u64()?)),
            Reply::IN_BUFFER => Ok(Reply::InBuffer {
                start: fields.u32()?,
                len: fields.u32()?,
            }),
            Reply::TOO_SMALL => Ok(Reply::TooSmall {
                needed: fields.u32()?,
            }),
            Reply::FAILED => Err(WireError::decode(&mut fields)?),
            _ => return Err(FrameFields::malformed("reply")),
        };
        fields.finish()?;
        Ok(Self { id, result })
    }
}

/// An [`Error`] as a reply carries it. The variants a caller matches on
/// come through as they are; I/O errors keep their OS error code; anything
/// else arrives as [`Error::Server`] with its message.
struct WireError<'a>(&'a Error);

impl WireError<'_> {
    const NOT_FOUND: u8 = 1;
    const RANGE_OUT_OF_BOUNDS: u8 = 2;
    const BUSY: u8 = 3;
    const LOCK_POISONED: u8 = 4;
    const IO: u8 = 5;
    const FORMAT: u8 = 6;
    const KEY_TOO_LARGE: u8 = 7;
    const VALUE_TOO_LARGE: u8 = 8;
    const OTHER: u8 = 9;

    fn encode(&self, frame: &mut FrameBuilder) {
        match self.0 {
            Error::NotFound => frame.u8(Self::NOT_FOUND),
            Error::RangeOutOfBounds => frame.u8(Self::RANGE_OUT_OF_BOUNDS),
            Error::Busy => frame.u8(Self::BUSY),
            Error::LockPoisoned => frame.u8(Self::LOCK_POISONED),
            Error::Io(err) => {
                frame.u8(Self::IO);
                match err.raw_os_error() {
                    Some(code) => {
                        frame.u8(1);
                        frame.u32(code as u32);
                    }
                    None => frame.u8(0),
                }
                frame.text(&err.to_string());
            }
            Error::Format(msg) => {
                frame.u8(Self::FORMAT);
                frame.text(msg);
            }
            Error::InputError(InputError::KeyTooLarge(len)) => {
                frame.u8(Self::KEY_TOO_LARGE);
                frame.u64(*len as u64);
            }
            Error::InputError(InputError::ValueTooLarge(len)) => {
                frame.u8(Self::VALUE_TOO_LARGE);
                frame.u64(*len as u64);
            }
            other @ (Error::InvalidArgument(_) | Error::ServerGone | Error::Server(_)) => {
                frame.u8(Self::OTHER);
                frame.text(&other.to_string());
            }
        }
    }

    fn decode(fields: &mut FrameFields<'_>) -> io::Result<Error> {
        let length = |fields: &mut FrameFields<'_>| {
            usize::try_from(fields.u64()?).map_err(|_| FrameFields::malformed("length"))
        };
        Ok(match fields.u8()? {
            Self::NOT_FOUND => Error::NotFound,
            Self::RANGE_OUT_OF_BOUNDS => Error::RangeOutOfBounds,
            Self::BUSY => Error::Busy,
            Self::LOCK_POISONED => Error::LockPoisoned,
            Self::IO => {
                let code = fields.bool()?.then(|| fields.u32()).transpose()?;
                let message = fields.text()?;
                Error::Io(match code {
                    Some(code) => io::Error::from_raw_os_error(code as i32),
                    None => io::Error::other(message),
                })
            }
            Self::FORMAT => Error::Format(fields.text()?),
            Self::KEY_TOO_LARGE => Error::InputError(InputError::KeyTooLarge(length(fields)?)),
            Self::VALUE_TOO_LARGE => Error::InputError(InputError::ValueTooLarge(length(fields)?)),
            Self::OTHER => Error::Server(fields.text()?),
            _ => return Err(FrameFields::malformed("error")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> T4Key {
        T4Key::try_from_slice(bytes).unwrap()
    }

    /// Sends `frame` through a socket pair and reads it back, as a server would.
    fn through_socket(frame: OutgoingFrame) -> (Vec<u8>, FdReceiver) {
        let (client, server) = UnixStream::pair().unwrap();
        frame.write_to(&client).unwrap();
        let mut reader = FrameReader::new(FdReceiver::new(server));
        let body = reader.read_frame().unwrap().unwrap();
        (body, reader.source)
    }

    #[test]
    fn requests_round_trip() {
        let requests = [
            Request::Put {
                key: key(b"k"),
                value: ValueIn::Inline(T4Value::try_from_vec(vec![7; 5000]).unwrap()),
            },
            Request::Put {
                key: key(b"k"),
                value: ValueIn::Shared {
                    buffer: WireBuffer::Region {
                        offset: 8192,
                        capacity: 4096,
                    },
                    len: 5,
                },
            },
            Request::Get {
                key: key(b""),
                into: None,
            },
            Request::Get {
                key: key(b"k"),
                into: Some(WireBuffer::Region {
                    offset: 0,
                    capacity: 65536,
                }),
            },
            Request::GetRange {
                key: key(&[1; 255]),
                range: RangeRequestU32::from_u32(3, 9).unwrap(),
                into: None,
            },
            Request::Remove { key: key(b"gone") },
            Request::Len,
        ];
        for (id, request) in requests.into_iter().enumerate() {
            let frame = request.into_frame(id as u64);
            let sent = frame.bytes.clone();
            let (body, mut fds) = through_socket(frame);
            let (decoded_id, decoded) = Request::decode(&body, &mut fds).unwrap();
            assert_eq!(decoded_id, id as u64);
            assert_eq!(decoded.into_frame(id as u64).bytes, sent);
        }
    }

    #[test]
    fn snapshot_request_carries_its_file() {
        let mut target = tempfile::tempfile().unwrap();
        target.write_all(b"marker").unwrap();
        let (body, mut fds) = through_socket(Request::Snapshot { target }.into_frame(9));
        let (id, Request::Snapshot { target }) = Request::decode(&body, &mut fds).unwrap() else {
            panic!("decoded as another request");
        };
        assert_eq!(id, 9);
        assert_eq!(target.metadata().unwrap().len(), b"marker".len() as u64);
    }

    #[test]
    fn a_buffer_of_its_own_carries_its_memfd() {
        let mut memfd = tempfile::tempfile().unwrap();
        memfd.write_all(b"marker").unwrap();
        let request = Request::Get {
            key: key(b"k"),
            into: Some(WireBuffer::Own {
                memfd: memfd.into(),
                capacity: 4096,
            }),
        };
        let (body, mut fds) = through_socket(request.into_frame(3));
        let Ok((
            3,
            Request::Get {
                into: Some(WireBuffer::Own { memfd, capacity }),
                ..
            },
        )) = Request::decode(&body, &mut fds)
        else {
            panic!("decoded as another request");
        };
        assert_eq!(capacity, 4096);
        assert_eq!(
            File::from(memfd).metadata().unwrap().len(),
            b"marker".len() as u64
        );
    }

    #[test]
    fn handshake_carries_the_store_and_checks_the_version() {
        let store = tempfile::tempfile().unwrap();
        let region = tempfile::tempfile().unwrap();
        let (body, mut fds) = through_socket(Handshake::frame(store.into(), Some(region.into())));
        let (_, region) = Handshake::decode(&body, &mut fds).unwrap();
        assert!(region.is_some());

        let mut wrong_version = body.clone();
        wrong_version[4] ^= 1;
        assert!(Handshake::decode(&wrong_version, &mut fds).is_err());

        for reply in [
            HandshakeReply::Accepted {
                server_pid: 42,
                direct_io: true,
            },
            HandshakeReply::Refused("no".into()),
        ] {
            let frame = reply.encode();
            assert_eq!(HandshakeReply::decode(&frame[8..]).unwrap(), reply);
        }
    }

    #[test]
    fn replies_round_trip_with_their_errors() {
        let results = [
            Ok(Reply::Done),
            Ok(Reply::Bytes(vec![1, 2, 3])),
            Ok(Reply::Removed(true)),
            Ok(Reply::Count(u64::MAX)),
            Ok(Reply::InBuffer { start: 7, len: 9 }),
            Ok(Reply::TooSmall { needed: 8192 }),
            Err(Error::NotFound),
            Err(Error::RangeOutOfBounds),
            Err(Error::Busy),
            Err(Error::Io(io::Error::from_raw_os_error(libc::ENOSPC))),
            Err(Error::InputError(InputError::KeyTooLarge(300))),
            Err(Error::InvalidArgument("bad")),
        ];
        for (id, result) in results.into_iter().enumerate() {
            let expected = match &result {
                Err(Error::InvalidArgument(msg)) => {
                    format!("Err(Server(\"invalid argument: {msg}\"))")
                }
                other => format!("{other:?}"),
            };
            let frame = ReplyFrame::new(id as u64, result).encode();
            let (decoded_id, decoded) = ReplyFrame::decode(&frame[8..]).unwrap().into_parts();
            assert_eq!(decoded_id, id as u64);
            assert_eq!(format!("{decoded:?}"), expected);
        }
    }

    #[test]
    fn truncated_frames_are_errors() {
        let frame = Request::Len.into_frame(1).bytes;
        let mut reader = FrameReader::new(&frame[..frame.len() - 1]);
        assert!(reader.read_frame().is_err());
        let mut empty = FrameReader::new(&[][..]);
        assert!(empty.read_frame().unwrap().is_none());
    }
}
