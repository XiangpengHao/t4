//! [`OwnerConnection`]: a client's connection to the owner of its cache
//! (`doc/11-shared-mode.md`, decisions 5 to 7): admitted by the handshake,
//! then a thread that sends the client's frames, in order, and one that
//! takes the owner's, until the owner goes.

use std::fs::File;
use std::io;
use std::path::Path;
use std::thread::{self, JoinHandle};

use crossbeam_channel::Sender;

use super::address::{FileIdentity, OwnerAddress};
use super::frame_socket::FrameSocket;
use super::frames::{FrameReader, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use crate::io::error::{Error, Result};

/// A client's connection to its owner. Dropped, it closes: the owner sees
/// the client gone, and its threads end.
pub(in crate::cache) struct OwnerConnection {
    socket: FrameSocket,
    /// The frames the writer sends, in order; taken as the connection
    /// drops, so that the writer's channel closes.
    outgoing: Option<Sender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
    /// Takes the owner's frames until the owner goes, or breaks the
    /// protocol.
    reader: Option<JoinHandle<()>>,
}

impl OwnerConnection {
    /// The bytes of an owner's welcome at most: all a client takes before
    /// it is admitted.
    const WELCOME_BYTES: u64 = 16;

    /// A connection to the owner of the cache whose first cache file is at
    /// `first_file`, admitted; `None` if no owner answered, or one closed
    /// the connection first: it is starting, or leaving, or refused the
    /// client.
    ///
    /// # Errors
    ///
    /// If the cache file can't be opened for writing, or the socket fails
    /// otherwise.
    pub(in crate::cache) fn connect(first_file: &Path) -> Result<Option<Self>> {
        match Self::admitted(first_file) {
            Ok(socket) => Ok(Some(Self::start(socket)?)),
            Err(Error::Io(error)) if Self::owner_absent(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// A socket to the owner of the cache whose first cache file is at
    /// `first_file`, which admitted the client: its hello sent, with its
    /// descriptor of that file, and the owner's welcome taken.
    fn admitted(first_file: &Path) -> Result<FrameSocket> {
        let cache_file = File::options().read(true).write(true).open(first_file)?;
        let address = OwnerAddress::of(FileIdentity::of(&cache_file)?);
        let socket = FrameSocket::new(address.connect()?)?;
        socket.set_read_timeout(Some(Hello::WAIT))?;
        Hello::new(PROTOCOL_VERSION, cache_file).send(&socket)?;
        let welcome = socket
            .receive(Self::WELCOME_BYTES)?
            .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
        match OwnerFrame::decode(welcome)? {
            OwnerFrame::Welcome {
                version: PROTOCOL_VERSION,
            } => {}
            _ => return Err(FrameReader::malformed("welcome")),
        }
        socket.set_read_timeout(None)?;
        Ok(socket)
    }

    /// Whether `error`, met while connecting, means no owner is there to
    /// answer: none listens, the file is gone, or the owner closed the
    /// connection, or never answered.
    fn owner_absent(error: &io::Error) -> bool {
        use io::ErrorKind::{
            BrokenPipe, ConnectionRefused, ConnectionReset, NotFound, TimedOut, UnexpectedEof,
            WouldBlock,
        };
        matches!(
            error.kind(),
            ConnectionRefused
                | NotFound
                | ConnectionReset
                | BrokenPipe
                | UnexpectedEof
                | WouldBlock
                | TimedOut
        )
    }

    /// The connection over `socket`, its threads started.
    fn start(socket: FrameSocket) -> Result<Self> {
        let (outgoing, frames) = crossbeam_channel::unbounded::<Vec<u8>>();
        let writing = socket.try_clone()?;
        let writer = thread::Builder::new()
            .name("t4-client-writer".into())
            .spawn(move || {
                for frame in frames {
                    if writing.send(&frame).is_err() {
                        // The owner is gone: the reader sees it too.
                        writing.shut_down();
                        return;
                    }
                }
            })?;
        let reading = socket.try_clone()?;
        let reader = thread::Builder::new()
            .name("t4-client-reader".into())
            .spawn(move || {
                // Calls are answered from the next commit: until then any
                // frame breaks the protocol, and ends the connection as the
                // owner's going does.
                let _ = reading.receive(FrameSocket::MOST_BYTES);
                reading.shut_down();
            })?;
        Ok(Self {
            socket,
            outgoing: Some(outgoing),
            writer: Some(writer),
            reader: Some(reader),
        })
    }

    /// Whether the owner has gone: the connection closed, or broke.
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn owner_gone(&self) -> bool {
        self.reader.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

impl Drop for OwnerConnection {
    fn drop(&mut self) {
        self.outgoing.take();
        self.socket.shut_down();
        for thread in [self.writer.take(), self.reader.take()]
            .into_iter()
            .flatten()
        {
            let _ = thread.join();
        }
    }
}
