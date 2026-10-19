//! How a client reaches its store's server, starting one if none is
//! running; and the command line and status line a server is started with.

use std::ffi::OsString;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use std::os::fd::BorrowedFd;
use std::os::unix::net::UnixStream;

use crate::io::error::{Error, Result};
use crate::shared::address::ServerAddress;
use crate::shared::client::ServerEndpoint;
use crate::shared::wire::{FrameReader, Handshake, HandshakeReply};
use crate::store::{MountMode, MountOptions};

/// The command line a client starts `t4-server` with, and the server
/// reads: everything it needs to mount the store.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ServerArgs {
    store: PathBuf,
    queue_depth: u32,
    direct_io: bool,
    idle_timeout: Duration,
}

impl ServerArgs {
    pub(super) fn new(store: PathBuf, options: &MountOptions, idle_timeout: Duration) -> Self {
        Self {
            store,
            queue_depth: options.queue_depth,
            direct_io: options.direct_io,
            idle_timeout,
        }
    }

    pub(super) fn store(&self) -> &Path {
        &self.store
    }

    pub(super) fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// How the server mounts the store: in its own process.
    pub(super) fn mount_options(&self) -> MountOptions {
        MountOptions {
            queue_depth: self.queue_depth,
            direct_io: self.direct_io,
            mode: MountMode::InProcess,
        }
    }

    fn to_args(&self) -> Vec<OsString> {
        vec![
            "--store".into(),
            self.store.clone().into(),
            "--queue-depth".into(),
            self.queue_depth.to_string().into(),
            "--direct-io".into(),
            self.direct_io.to_string().into(),
            "--idle-timeout-ms".into(),
            self.idle_timeout.as_millis().to_string().into(),
        ]
    }

    /// Reads the command line `to_args` wrote, program name excluded.
    pub(super) fn parse(
        args: impl IntoIterator<Item = OsString>,
    ) -> std::result::Result<Self, String> {
        let mut store = None;
        let mut queue_depth = None;
        let mut direct_io = None;
        let mut idle_timeout = None;
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("{} needs a value", flag.to_string_lossy()))?;
            let text = || {
                value
                    .to_str()
                    .ok_or_else(|| format!("{} is not UTF-8", flag.to_string_lossy()))
            };
            match flag.to_str() {
                Some("--store") => store = Some(PathBuf::from(&value)),
                Some("--queue-depth") => {
                    queue_depth = Some(
                        text()?
                            .parse()
                            .map_err(|err| format!("--queue-depth: {err}"))?,
                    )
                }
                Some("--direct-io") => {
                    direct_io = Some(
                        text()?
                            .parse()
                            .map_err(|err| format!("--direct-io: {err}"))?,
                    )
                }
                Some("--idle-timeout-ms") => {
                    let millis = text()?
                        .parse()
                        .map_err(|err| format!("--idle-timeout-ms: {err}"))?;
                    idle_timeout = Some(Duration::from_millis(millis));
                }
                _ => return Err(format!("unknown flag {}", flag.to_string_lossy())),
            }
        }
        let missing = |flag: &str| format!("{flag} is required");
        Ok(Self {
            store: store.ok_or_else(|| missing("--store"))?,
            queue_depth: queue_depth.ok_or_else(|| missing("--queue-depth"))?,
            direct_io: direct_io.ok_or_else(|| missing("--direct-io"))?,
            idle_timeout: idle_timeout.ok_or_else(|| missing("--idle-timeout-ms"))?,
        })
    }
}

/// The one line a starting server prints to the client that started it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ServerStatus {
    /// The store is mounted and the server answers handshakes.
    Ready,
    /// Another server holds the address: connect to it.
    AlreadyRunning,
    /// Another process owns the store: an in-process mount.
    Busy,
    Failed(String),
}

impl ServerStatus {
    const READY: &str = "ready";
    const ALREADY_RUNNING: &str = "already-running";
    const BUSY: &str = "busy";
    const FAILED: &str = "error: ";

    /// Prints the status line on stdout, the pipe its starter reads.
    pub(super) fn report(&self) {
        let line = match self {
            Self::Ready => Self::READY.to_string(),
            Self::AlreadyRunning => Self::ALREADY_RUNNING.to_string(),
            Self::Busy => Self::BUSY.to_string(),
            Self::Failed(reason) => format!("{}{}", Self::FAILED, reason.replace('\n', " ")),
        };
        let mut stdout = io::stdout().lock();
        // Nothing to do if the starter is gone.
        let _ = writeln!(stdout, "{line}").and_then(|()| stdout.flush());
    }

    fn parse(line: &str) -> Self {
        match line.trim_end() {
            Self::READY => Self::Ready,
            Self::ALREADY_RUNNING => Self::AlreadyRunning,
            Self::BUSY => Self::Busy,
            "" => Self::Failed("t4-server exited without a status".into()),
            other => Self::Failed(other.strip_prefix(Self::FAILED).unwrap_or(other).into()),
        }
    }
}

/// A connection to the store's server, past its handshake.
pub(super) struct Connected {
    socket: UnixStream,
    replies: FrameReader<BufReader<UnixStream>>,
    server_pid: u32,
}

impl Connected {
    /// The socket to send requests on, the reader its replies come from,
    /// and the server's process id.
    pub(super) fn into_parts(self) -> (UnixStream, FrameReader<BufReader<UnixStream>>, u32) {
        (self.socket, self.replies, self.server_pid)
    }
}

/// Connects a client to its store's server, starting the server if none is
/// running (`doc/02-multi-process.md`, "Finding the server").
pub(super) struct ServerLauncher<'a> {
    endpoint: &'a ServerEndpoint,
    /// The connection's shared region, shown to the server with the store.
    region: Option<BorrowedFd<'a>>,
}

impl<'a> ServerLauncher<'a> {
    /// How long a client keeps trying while servers start and stop. A
    /// server replaying a long WAL doesn't count against it: the client
    /// waits for its status line, or in its handshake.
    const DEADLINE: Duration = Duration::from_secs(10);
    const MAX_BACKOFF: Duration = Duration::from_millis(100);

    pub(super) fn new(endpoint: &'a ServerEndpoint, region: Option<BorrowedFd<'a>>) -> Self {
        Self { endpoint, region }
    }

    /// Blocks until connected; a client calls it on a thread of its own.
    pub(super) fn connect(&self) -> Result<Connected> {
        let deadline = Instant::now() + Self::DEADLINE;
        let mut backoff = Duration::from_millis(1);
        loop {
            match self.endpoint.address().connect() {
                Ok(socket) => {
                    if let Some(connected) = self.handshake(socket)? {
                        return Ok(connected);
                    }
                    // Reset in the handshake: a server shutting down.
                }
                Err(err) if ServerAddress::no_server(&err) => match self.start_server()? {
                    ServerStatus::Ready | ServerStatus::AlreadyRunning => {}
                    ServerStatus::Busy => return Err(Error::Busy),
                    ServerStatus::Failed(reason) => return Err(Error::Server(reason)),
                },
                Err(err) => return Err(err.into()),
            }
            if Instant::now() >= deadline {
                return Err(Error::Server(
                    "timed out connecting to the store's server".into(),
                ));
            }
            thread::sleep(backoff);
            backoff = (backoff * 2).min(Self::MAX_BACKOFF);
        }
    }

    /// Shows the server the store, and reads its answer. `None` if the
    /// server went away meanwhile.
    fn handshake(&self, socket: UnixStream) -> Result<Option<Connected>> {
        let gone = |err: &io::Error| {
            matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            )
        };
        let region = self
            .region
            .map(|region| region.try_clone_to_owned())
            .transpose()?;
        let frame = Handshake::frame(self.endpoint.store_file().try_clone()?.into(), region);
        match frame.write_to(&socket) {
            Err(err) if gone(&err) => return Ok(None),
            result => result?,
        }
        let mut replies = FrameReader::new(BufReader::new(socket.try_clone()?));
        let body = match replies.read_frame() {
            Ok(Some(body)) => body,
            Ok(None) => return Ok(None),
            Err(err) if gone(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        match HandshakeReply::decode(&body)? {
            HandshakeReply::Accepted {
                server_pid,
                direct_io,
            } => {
                if direct_io != self.endpoint.options().direct_io {
                    return Err(Error::InvalidArgument(
                        "direct_io differs from the running server's",
                    ));
                }
                Ok(Some(Connected {
                    socket,
                    replies,
                    server_pid,
                }))
            }
            HandshakeReply::Refused(reason) => Err(Error::Server(reason)),
        }
    }

    /// Runs `t4-server` and reads its status line. The process that prints
    /// it exits at once, so it is reaped here: a server forks first, and
    /// runs on in the child.
    fn start_server(&self) -> Result<ServerStatus> {
        let server = self.endpoint.server_options();
        let program = server
            .program
            .clone()
            .or_else(|| std::env::var_os("T4_SERVER").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("t4-server"));
        let args = ServerArgs::new(
            self.endpoint.path().to_path_buf(),
            self.endpoint.options(),
            server.idle_timeout,
        );
        let mut child = Command::new(&program)
            .args(args.to_args())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|err| Error::Server(format!("cannot run {}: {err}", program.display())))?;
        let stdout = child.stdout.take().expect("stdout is piped");
        let mut line = String::new();
        let read = BufReader::new(stdout).read_line(&mut line);
        let _ = child.wait();
        read?;
        Ok(ServerStatus::parse(&line))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_args_round_trip() {
        let options = MountOptions {
            queue_depth: 17,
            direct_io: false,
            ..MountOptions::default()
        };
        let args = ServerArgs::new(
            PathBuf::from("/tmp/a store.t4"),
            &options,
            Duration::from_millis(1500),
        );
        assert_eq!(ServerArgs::parse(args.to_args()).unwrap(), args);
        assert!(ServerArgs::parse(Vec::<OsString>::new()).is_err());
        assert!(ServerArgs::parse(["--store".into()]).is_err());
    }

    #[test]
    fn status_lines_round_trip() {
        for (status, line) in [
            (ServerStatus::Ready, "ready\n"),
            (ServerStatus::AlreadyRunning, "already-running\n"),
            (ServerStatus::Busy, "busy\n"),
            (ServerStatus::Failed("bad file".into()), "error: bad file\n"),
            (
                ServerStatus::Failed("t4-server exited without a status".into()),
                "",
            ),
        ] {
            assert_eq!(ServerStatus::parse(line), status);
        }
    }
}
