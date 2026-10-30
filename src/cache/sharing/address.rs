//! Where a cache's owner listens for its clients (`doc/11-shared-mode.md`,
//! decision 6): an address named by the first cache file's identity, so
//! that every path to the file names the same one.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(not(target_os = "linux"))]
use std::path::PathBuf;

use rustix::fs::Uid;

use crate::io::error::{Error, Result};

/// A file, by whatever path it is reached: its device and inode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    /// The identity of the file `file` is open on.
    pub(in crate::cache) fn of(file: &File) -> io::Result<Self> {
        let meta = file.metadata()?;
        Ok(Self {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
}

/// The address of the owner of the cache whose first cache file is one
/// file: on Linux an abstract socket's name, which no file holds and which
/// is gone with the owner's socket, however the owner exits; elsewhere a
/// socket file's path, in the user's temporary directory.
pub(super) struct OwnerAddress {
    #[cfg(target_os = "linux")]
    name: String,
    #[cfg(not(target_os = "linux"))]
    path: PathBuf,
}

impl OwnerAddress {
    /// The address of the owner of the cache whose first cache file is
    /// `file`.
    ///
    /// # Errors
    ///
    /// If the system doesn't name the user's temporary directory.
    pub(super) fn of(file: FileIdentity) -> io::Result<Self> {
        let FileIdentity { device, inode } = file;
        Ok(Self {
            #[cfg(target_os = "linux")]
            name: format!("t4/{device:x}/{inode:x}"),
            #[cfg(not(target_os = "linux"))]
            path: Self::user_temp_dir()?.join(format!("t4-{device:x}-{inode:x}.sock")),
        })
    }

    /// The user's temporary directory, as the system names it, whatever
    /// the process's environment says: shells set `TMPDIR` apart (a nix
    /// shell, `sudo`), and an owner and its client that read it would look
    /// in different places. Only the user may write in it, so no other
    /// user can listen at the user's owners' addresses.
    #[cfg(target_os = "macos")]
    fn user_temp_dir() -> io::Result<PathBuf> {
        use std::os::unix::ffi::OsStringExt;
        let mut path = vec![0_u8; libc::PATH_MAX as usize];
        // SAFETY: `path` has room for the `path.len()` bytes `confstr`
        // writes at most.
        let len = unsafe {
            libc::confstr(
                libc::_CS_DARWIN_USER_TEMP_DIR,
                path.as_mut_ptr().cast(),
                path.len(),
            )
        };
        // `len` counts the closing NUL; 0 if the system names none.
        if len == 0 {
            return Err(io::Error::last_os_error());
        }
        if len > path.len() {
            return Err(io::Error::other(
                "the user's temporary directory's path is too long",
            ));
        }
        path.truncate(len - 1);
        Ok(PathBuf::from(std::ffi::OsString::from_vec(path)))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn user_temp_dir() -> io::Result<PathBuf> {
        Ok(std::env::temp_dir())
    }

    /// Listens at the address, for the owner. It holds the cache files'
    /// lock, so no other process binds the address meanwhile: a socket file
    /// there is one an owner left as it died, and goes first.
    pub(super) fn listen(&self) -> io::Result<UnixListener> {
        #[cfg(target_os = "linux")]
        {
            UnixListener::bind_addr(&self.socket_address()?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            match std::fs::remove_file(&self.path) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
            UnixListener::bind(&self.path)
        }
    }

    /// A connection to the owner listening at the address, which runs as
    /// `user`. A client trusts no other user's (decision 8): on Linux any
    /// process may listen at an abstract name, first, and would be sent
    /// the client's descriptor of the cache file, and serve it any bytes.
    ///
    /// # Errors
    ///
    /// If the socket fails; [`Error::Busy`] if the process listening runs
    /// as another user.
    pub(super) fn connect(&self, user: Uid) -> Result<UnixStream> {
        let stream = self.stream()?;
        if Self::peer_user(&stream)? != user {
            return Err(Error::Busy);
        }
        Ok(stream)
    }

    /// Connects to the address and goes: wakes the owner's thread that
    /// accepts its clients.
    pub(super) fn wake(&self) {
        let _ = self.stream();
    }

    fn stream(&self) -> io::Result<UnixStream> {
        #[cfg(target_os = "linux")]
        {
            UnixStream::connect_addr(&self.socket_address()?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            UnixStream::connect(&self.path)
        }
    }

    /// The owner's listener gone, its socket file goes too, while the owner
    /// still holds the lock; an abstract name went with its socket.
    pub(super) fn remove(&self) {
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(&self.path);
    }

    /// The user the process at the other end of `stream` ran as when it
    /// connected or listened.
    #[cfg(target_os = "linux")]
    fn peer_user(stream: &UnixStream) -> io::Result<Uid> {
        Ok(rustix::net::sockopt::socket_peercred(stream)?.uid)
    }

    #[cfg(not(target_os = "linux"))]
    fn peer_user(stream: &UnixStream) -> io::Result<Uid> {
        use std::os::fd::AsRawFd;
        let (mut user, mut group) = (0, 0);
        // SAFETY: the descriptor is `stream`'s, open while it lives; the
        // call writes only `user` and `group`.
        if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut user, &mut group) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Uid::from_raw(user))
    }

    #[cfg(target_os = "linux")]
    fn socket_address(&self) -> io::Result<std::os::unix::net::SocketAddr> {
        use std::os::linux::net::SocketAddrExt;
        std::os::unix::net::SocketAddr::from_abstract_name(self.name.as_bytes())
    }
}
