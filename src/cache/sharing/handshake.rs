//! [`Hello`]: a client's first frame to its owner, and the owner's check of
//! it (`doc/11-shared-mode.md`, decision 6).

use std::fs::File;
use std::io::{IoSlice, IoSliceMut, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::time::Duration;

use bytes::Bytes;
use rustix::fs::OFlags;
use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags, SendAncillaryBuffer,
    SendAncillaryMessage,
};

use super::address::FileIdentity;
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, FrameReader, PROTOCOL_VERSION};
use crate::io::error::{Error, Result};

/// A client's hello: its protocol's version, its descriptor of the first
/// cache file, and of its region if it made one, sent beside the frame by
/// `SCM_RIGHTS`. The owner admits a client whose descriptor names that
/// file and is open for writing it: whoever may write the cache file may
/// use the cache. Its region the owner maps if it can't shrink
/// (`OwnerRegion::accept`).
pub(super) struct Hello {
    version: u32,
    cache_file: File,
    region: Option<OwnedFd>,
}

impl Hello {
    /// How long each side of a handshake waits for the other's word: a peer
    /// that says nothing that long is gone.
    pub(super) const WAIT: Duration = Duration::from_secs(5);

    /// The bytes of a hello's frame, its length with it: all an owner takes
    /// from a peer it hasn't admitted, so that a peer can't make it
    /// allocate.
    const FRAME_BYTES: usize = 8 + 1 + 4 + 4 + 1;

    /// The most descriptors a hello comes with: the cache file's and the
    /// region's.
    const MOST_DESCRIPTORS: usize = 2;

    /// How a receive takes the descriptor: closed on `exec` from the first,
    /// where the kernel can.
    #[cfg(target_os = "linux")]
    const RECEIVE_FLAGS: RecvFlags = RecvFlags::CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    const RECEIVE_FLAGS: RecvFlags = RecvFlags::empty();

    pub(super) fn new(version: u32, cache_file: File) -> Self {
        Self {
            version,
            cache_file,
            region: None,
        }
    }

    /// The hello, with `region`'s memory sent beside it.
    #[must_use]
    pub(super) fn with_region(self, region: OwnedFd) -> Self {
        Self {
            region: Some(region),
            ..self
        }
    }

    /// The client's region's memory, if it sent one: the owner's to check.
    pub(super) fn into_region(self) -> Option<OwnedFd> {
        self.region
    }

    /// Sends the hello on `socket`, the descriptors beside its frame's
    /// first bytes.
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
    pub(super) fn send(&self, socket: &FrameSocket) -> std::io::Result<()> {
        let frame = ClientFrame::Hello {
            version: self.version,
            region: self.region.is_some(),
        };
        self.send_as(socket, &frame.encode())
    }

    /// Sends `frame` on `socket`, the hello's descriptors beside its first
    /// bytes: the hello's own frame, or, in a test, one of a peer that
    /// breaks the protocol.
    pub(super) fn send_as(&self, socket: &FrameSocket, frame: &[u8]) -> std::io::Result<()> {
        debug_assert_eq!(frame.len(), Self::FRAME_BYTES);
        let mut space =
            [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(Self::MOST_DESCRIPTORS))];
        let mut control = SendAncillaryBuffer::new(&mut space);
        let cache_file = self.cache_file.as_fd();
        let descriptors = match &self.region {
            Some(region) => vec![cache_file, region.as_fd()],
            None => vec![cache_file],
        };
        let pushed = control.push(SendAncillaryMessage::ScmRights(&descriptors));
        assert!(pushed, "the buffer has room for every descriptor");
        let sent = rustix::net::sendmsg(
            socket,
            &[IoSlice::new(frame)],
            &mut control,
            FrameSocket::SEND_FLAGS,
        )?;
        socket.send(&frame[sent..])
    }

    /// The hello the peer of `socket` sent first, and its descriptors.
    ///
    /// # Errors
    ///
    /// If the socket fails or closes, or the peer sent anything but a
    /// hello's frame with the descriptors it says beside it.
    pub(super) fn receive(socket: &FrameSocket) -> Result<Self> {
        let mut frame = [0; Self::FRAME_BYTES];
        let mut space =
            [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(Self::MOST_DESCRIPTORS))];
        let mut control = RecvAncillaryBuffer::new(&mut space);
        let received = rustix::net::recvmsg(
            socket,
            &mut [IoSliceMut::new(&mut frame)],
            &mut control,
            Self::RECEIVE_FLAGS,
        )
        .map_err(std::io::Error::from)?;
        if received.bytes == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        if received.flags.contains(ReturnFlags::CTRUNC) {
            return Err(FrameReader::malformed("hello: more than two descriptors"));
        }
        let mut descriptors: Vec<OwnedFd> = Vec::new();
        for message in control.drain() {
            if let RecvAncillaryMessage::ScmRights(sent) = message {
                descriptors.extend(sent);
            }
        }
        (&*socket).read_exact(&mut frame[received.bytes..])?;
        let (length, body) = frame.split_at(size_of::<u64>());
        let length = u64::from_le_bytes(length.try_into().expect("a u64's bytes"));
        if length != body.len() as u64 {
            return Err(FrameReader::malformed("hello's length"));
        }
        let ClientFrame::Hello { version, region } =
            ClientFrame::decode(Bytes::copy_from_slice(body))?
        else {
            return Err(FrameReader::malformed("hello: another frame"));
        };
        if descriptors.len() != 1 + usize::from(region) {
            return Err(FrameReader::malformed(
                "hello: other descriptors than it says",
            ));
        }
        let mut descriptors = descriptors.into_iter();
        let cache_file = descriptors.next().expect("one descriptor at least");
        Ok(Self {
            version,
            cache_file: File::from(cache_file),
            region: descriptors.next(),
        })
    }

    /// Whether the owner of the cache whose first cache file is
    /// `first_file` admits the client.
    ///
    /// # Errors
    ///
    /// If it doesn't, saying why.
    pub(super) fn admit(&self, first_file: FileIdentity) -> Result<()> {
        if self.version != PROTOCOL_VERSION {
            return Err(Self::refused("of another protocol version"));
        }
        if FileIdentity::of(&self.cache_file)? != first_file {
            return Err(Self::refused("with a descriptor of another file"));
        }
        let access = rustix::fs::fcntl_getfl(&self.cache_file).map_err(std::io::Error::from)?;
        if access & OFlags::RWMODE != OFlags::RDWR {
            return Err(Self::refused("with a descriptor not open for writing"));
        }
        Ok(())
    }

    /// The error of a hello refused for `why`.
    fn refused(why: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("a client's hello {why}"),
        ))
    }
}
