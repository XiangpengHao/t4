//! [`Hello`]: a client's first frame to its owner, and the owner's check of
//! it (`doc/11-shared-mode.md`, decision 6).

use std::fs::File;
use std::io::{IoSlice, IoSliceMut, Read};
use std::mem::MaybeUninit;
use std::os::fd::AsFd;
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

/// A client's hello: its protocol's version, and its descriptor of the
/// first cache file, sent beside the frame by `SCM_RIGHTS`. The owner
/// admits a client whose descriptor names that file and is open for
/// writing it: whoever may write the cache file may use the cache.
pub(super) struct Hello {
    version: u32,
    cache_file: File,
}

impl Hello {
    /// How long each side of a handshake waits for the other's word: a peer
    /// that says nothing that long is gone.
    pub(super) const WAIT: Duration = Duration::from_secs(5);

    /// The bytes of a hello's frame, its length with it: all an owner takes
    /// from a peer it hasn't admitted, so that a peer can't make it
    /// allocate.
    const FRAME_BYTES: usize = 8 + 1 + 4 + 4;

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
        }
    }

    /// Sends the hello on `socket`, the descriptor beside its frame's first
    /// bytes.
    ///
    /// # Errors
    ///
    /// If the socket fails, or its peer has gone.
    pub(super) fn send(&self, socket: &FrameSocket) -> std::io::Result<()> {
        let frame = ClientFrame::Hello {
            version: self.version,
        }
        .encode();
        debug_assert_eq!(frame.len(), Self::FRAME_BYTES);
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
        let mut control = SendAncillaryBuffer::new(&mut space);
        let descriptors = [self.cache_file.as_fd()];
        let pushed = control.push(SendAncillaryMessage::ScmRights(&descriptors));
        assert!(pushed, "the buffer has room for one descriptor");
        let sent = rustix::net::sendmsg(
            socket,
            &[IoSlice::new(&frame)],
            &mut control,
            FrameSocket::SEND_FLAGS,
        )?;
        socket.send(&frame[sent..])
    }

    /// The hello the peer of `socket` sent first, and its descriptor.
    ///
    /// # Errors
    ///
    /// If the socket fails or closes, or the peer sent anything but a
    /// hello's frame with one descriptor beside it.
    pub(super) fn receive(socket: &FrameSocket) -> Result<Self> {
        let mut frame = [0; Self::FRAME_BYTES];
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
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
            return Err(FrameReader::malformed("hello: more than one descriptor"));
        }
        let cache_file = control.drain().find_map(|message| match message {
            RecvAncillaryMessage::ScmRights(mut descriptors) => descriptors.next(),
            _ => None,
        });
        let Some(cache_file) = cache_file else {
            return Err(FrameReader::malformed("hello: no descriptor"));
        };
        (&*socket).read_exact(&mut frame[received.bytes..])?;
        let (length, body) = frame.split_at(size_of::<u64>());
        let length = u64::from_le_bytes(length.try_into().expect("a u64's bytes"));
        if length != body.len() as u64 {
            return Err(FrameReader::malformed("hello's length"));
        }
        let ClientFrame::Hello { version } = ClientFrame::decode(Bytes::copy_from_slice(body))?
        else {
            return Err(FrameReader::malformed("hello: another frame"));
        };
        Ok(Self::new(version, File::from(cache_file)))
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
