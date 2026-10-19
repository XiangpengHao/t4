//! Where a store's server listens, and how servers keep each other out.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};

#[cfg(target_os = "linux")]
use std::os::{linux::net::SocketAddrExt, unix::net::SocketAddr};
#[cfg(not(target_os = "linux"))]
use std::{fs, path::PathBuf};

/// Which file a store is: its device and inode, the same through every
/// path that reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StoreIdentity {
    dev: u64,
    ino: u64,
}

impl StoreIdentity {
    pub(super) fn of(file: &File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
}

/// The address of one store's server, named after the store's identity.
///
/// On Linux it is an abstract socket, `t4/<dev>/<ino>`: no file to clean
/// up, and binding the name is what keeps a second server out. Elsewhere it
/// is a socket file in `$TMPDIR`, and servers keep each other out with a
/// `flock` on a lock file next to it.
pub(super) struct ServerAddress {
    #[cfg(target_os = "linux")]
    name: SocketAddr,
    #[cfg(not(target_os = "linux"))]
    socket_path: PathBuf,
    #[cfg(not(target_os = "linux"))]
    lock_path: PathBuf,
}

impl ServerAddress {
    #[cfg(target_os = "linux")]
    pub(super) fn new(store: StoreIdentity) -> io::Result<Self> {
        let StoreIdentity { dev, ino } = store;
        Ok(Self {
            name: SocketAddr::from_abstract_name(format!("t4/{dev:x}/{ino:x}"))?,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn new(store: StoreIdentity) -> io::Result<Self> {
        let StoreIdentity { dev, ino } = store;
        // `$TMPDIR` is per user on macOS, and short enough for the 104-byte
        // limit on a socket's path.
        let dir = std::env::temp_dir();
        Ok(Self {
            socket_path: dir.join(format!("t4-{dev:x}-{ino:x}.sock")),
            lock_path: dir.join(format!("t4-{dev:x}-{ino:x}.lock")),
        })
    }

    pub(super) fn connect(&self) -> io::Result<UnixStream> {
        #[cfg(target_os = "linux")]
        return UnixStream::connect_addr(&self.name);
        #[cfg(not(target_os = "linux"))]
        return UnixStream::connect(&self.socket_path);
    }

    /// Whether `err`, from [`Self::connect`], means no server is listening:
    /// nothing bound to the name, or a socket file a dead server left.
    pub(super) fn no_server(err: &io::Error) -> bool {
        matches!(
            err.kind(),
            io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
        )
    }

    /// Takes the address for this server, listening on it; `None` if
    /// another server holds it. It frees itself when the claim drops.
    #[cfg(target_os = "linux")]
    pub(super) fn claim(&self) -> io::Result<Option<ServerClaim>> {
        match UnixListener::bind_addr(&self.name) {
            Ok(listener) => Ok(Some(ServerClaim { listener })),
            Err(err) if err.kind() == io::ErrorKind::AddrInUse => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Takes the address for this server, listening on it; `None` if
    /// another server holds it. A socket file a killed server left behind
    /// is removed first, which is safe once the lock is ours. The lock
    /// file stays: deleting it would race a server about to lock it.
    #[cfg(not(target_os = "linux"))]
    pub(super) fn claim(&self) -> io::Result<Option<ServerClaim>> {
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock_path)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(std::fs::TryLockError::Error(err)) => return Err(err),
        }
        match fs::remove_file(&self.socket_path) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
            _ => {}
        }
        Ok(Some(ServerClaim {
            listener: UnixListener::bind(&self.socket_path)?,
            socket_path: self.socket_path.clone(),
            _lock: lock,
        }))
    }
}

/// A server's hold on its address, listening on it until dropped.
pub(super) struct ServerClaim {
    listener: UnixListener,
    #[cfg(not(target_os = "linux"))]
    socket_path: PathBuf,
    // Dropped after the socket file is gone, so the next server can't
    // remove the file of a live one.
    #[cfg(not(target_os = "linux"))]
    _lock: File,
}

impl ServerClaim {
    pub(super) fn listener(&self) -> &UnixListener {
        &self.listener
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for ServerClaim {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_claim_at_a_time() {
        let store = tempfile::tempfile().unwrap();
        let address = ServerAddress::new(StoreIdentity::of(&store).unwrap()).unwrap();
        assert!(ServerAddress::no_server(&address.connect().unwrap_err()));

        let claim = address.claim().unwrap().expect("nobody holds the address");
        assert!(address.claim().unwrap().is_none());
        address.connect().unwrap();

        drop(claim);
        assert!(ServerAddress::no_server(&address.connect().unwrap_err()));
        assert!(address.claim().unwrap().is_some());
    }
}
