use std::io;
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(not(target_os = "linux"))]
use std::path::PathBuf;

use rustix::fs::Uid;

use crate::cache::storage::FileIdentity;
use crate::io::error::{Error, Result};

/// On Linux an abstract socket's name: no file holds it, and it goes with the owner's
/// socket however the owner exits.
pub(in crate::cache::cache_stores::shared) struct OwnerAddress {
    #[cfg(target_os = "linux")]
    name: String,
    #[cfg(not(target_os = "linux"))]
    path: PathBuf,
}

impl OwnerAddress {
    pub(in crate::cache::cache_stores::shared) fn of(file: FileIdentity) -> io::Result<Self> {
        let (device, inode) = (file.device(), file.inode());
        Ok(Self {
            #[cfg(target_os = "linux")]
            name: format!("t4/{device:x}/{inode:x}"),
            #[cfg(not(target_os = "linux"))]
            path: Self::user_temp_dir()?.join(format!("t4-{device:x}-{inode:x}.sock")),
        })
    }

    /// Not `TMPDIR`, which shells set apart (a nix shell, `sudo`): owner and client would
    /// disagree. Only the user may write in it, so no other user can listen there.
    #[cfg(target_os = "macos")]
    fn user_temp_dir() -> io::Result<PathBuf> {
        use std::os::unix::ffi::OsStringExt;
        let mut path = vec![0_u8; libc::PATH_MAX as usize];
        // SAFETY: `path` has room for the `path.len()` bytes `confstr` writes at most.
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

    /// The owner holds the files' lock, so a socket file here is one a dead owner left.
    pub(in crate::cache::cache_stores::shared) fn listen(&self) -> io::Result<UnixListener> {
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

    /// On Linux any process may listen at an abstract name first, and would be sent the
    /// client's cache file descriptor: trust only `user` (`doc/11-shared-mode.md`, decision 8).
    pub(in crate::cache::cache_stores::shared) fn connect(&self, user: Uid) -> Result<UnixStream> {
        let stream = self.stream()?;
        if Self::peer_user(&stream)? != user {
            return Err(Error::Busy);
        }
        Ok(stream)
    }

    pub(in crate::cache::cache_stores::shared) fn wake(&self) {
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

    /// Called while the owner still holds the lock; an abstract name went with its socket.
    pub(in crate::cache::cache_stores::shared) fn remove(&self) {
        #[cfg(not(target_os = "linux"))]
        let _ = std::fs::remove_file(&self.path);
    }

    #[cfg(target_os = "linux")]
    fn peer_user(stream: &UnixStream) -> io::Result<Uid> {
        Ok(rustix::net::sockopt::socket_peercred(stream)?.uid)
    }

    #[cfg(not(target_os = "linux"))]
    fn peer_user(stream: &UnixStream) -> io::Result<Uid> {
        use std::os::fd::AsRawFd;
        let (mut user, mut group) = (0, 0);
        // SAFETY: `stream`'s descriptor is open; the call writes only `user` and `group`.
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
