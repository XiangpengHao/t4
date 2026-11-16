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

use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, FrameReader, PROTOCOL_VERSION};
use crate::cache::storage::FileIdentity;
use crate::io::error::{Error, Result};

/// Admission is by write access to the first cache file: whoever may write it may use the cache.
pub(in crate::cache::cache_stores::shared) struct Hello {
    version: u32,
    cache_file: File,
    region: Option<OwnedFd>,
}

impl Hello {
    pub(in crate::cache::cache_stores::shared) const WAIT: Duration = Duration::from_secs(5);

    /// All an owner reads from a peer it hasn't admitted, so that a peer can't make it allocate.
    const FRAME_BYTES: usize = 8 + 1 + 4 + 4 + 1;

    const MOST_DESCRIPTORS: usize = 2;

    #[cfg(target_os = "linux")]
    const RECEIVE_FLAGS: RecvFlags = RecvFlags::CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    const RECEIVE_FLAGS: RecvFlags = RecvFlags::empty();

    pub(in crate::cache::cache_stores::shared) fn new(version: u32, cache_file: File) -> Self {
        Self {
            version,
            cache_file,
            region: None,
        }
    }

    #[must_use]
    pub(in crate::cache::cache_stores::shared) fn with_region(self, region: OwnedFd) -> Self {
        Self {
            region: Some(region),
            ..self
        }
    }

    pub(in crate::cache::cache_stores::shared) fn version(&self) -> u32 {
        self.version
    }

    pub(in crate::cache::cache_stores::shared) fn into_region(self) -> Option<OwnedFd> {
        self.region
    }

    pub(in crate::cache::cache_stores::shared) fn send(
        &self,
        socket: &FrameSocket,
    ) -> std::io::Result<()> {
        let frame = ClientFrame::Hello {
            version: self.version,
            region: self.region.is_some(),
        };
        self.send_as(socket, &frame.encode())
    }

    /// `frame` is the hello's own, or in a test, a protocol-breaking peer's.
    pub(in crate::cache::cache_stores::shared) fn send_as(
        &self,
        socket: &FrameSocket,
        frame: &[u8],
    ) -> std::io::Result<()> {
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

    pub(in crate::cache::cache_stores::shared) fn receive(socket: &FrameSocket) -> Result<Self> {
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
        // Set at once after the receive, but a process started meanwhile could still inherit them.
        #[cfg(not(target_os = "linux"))]
        for descriptor in &descriptors {
            rustix::io::fcntl_setfd(descriptor, rustix::io::FdFlags::CLOEXEC)
                .map_err(std::io::Error::from)?;
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

    pub(in crate::cache::cache_stores::shared) fn admit(
        &self,
        first_file: FileIdentity,
    ) -> Result<()> {
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

    fn refused(why: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("a client's hello {why}"),
        ))
    }
}
