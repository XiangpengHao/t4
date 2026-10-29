use std::fs::File;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use pollster::block_on;
use tempfile::TempDir;

use super::address::{FileIdentity, OwnerAddress};
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use crate::cache::storage_link::StorageLink;
use crate::cache::{T4Caching, T4CachingBuilder};
use crate::io::sync::Arc;

const MIB: u64 = 1 << 20;

/// A builder of a cache built for sharing, of 4 MiB of memory and 8 on
/// disk over `inner`, its one cache file in `dir`.
fn shared(inner: &Arc<InMemory>, dir: &TempDir) -> T4CachingBuilder {
    T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
        .with_sharing()
}

/// What `work` returns, run on a thread of its own: `None` if it takes
/// more than 5 s, so that a test fails rather than hangs.
fn within_5s<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(work());
    });
    finished.recv_timeout(Duration::from_secs(5)).ok()
}

/// Whether `done` holds within 5 s.
fn eventually(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// The connection of `store`, a client.
fn connection(store: &T4Caching) -> &super::client::OwnerConnection {
    match store.link() {
        StorageLink::Client(connection) => connection,
        StorageLink::Owner { .. } => panic!("a client"),
    }
}

/// Frames go whole, each answered as it was sent; the peer's close between
/// frames ends them, and anything else is an error.
#[test]
fn frames_cross_a_socket_whole() {
    let (one, other) = UnixStream::pair().unwrap();
    let (one, other) = (
        FrameSocket::new(one).unwrap(),
        FrameSocket::new(other).unwrap(),
    );
    let frames = [
        ClientFrame::Hello { version: 7 },
        ClientFrame::Land {
            read: 1,
            fetch: 2,
            bytes: Bytes::from(vec![9; 3 << 20]),
        },
    ];
    let sent = frames.clone();
    let sending = std::thread::spawn(move || {
        for frame in &sent {
            one.send(&frame.encode()).unwrap();
        }
        one
    });
    for frame in frames {
        let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
        assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    }
    drop(sending.join().unwrap());
    assert!(other.receive(FrameSocket::MOST_BYTES).unwrap().is_none());

    // A frame longer than the receiver takes, or cut short by a close.
    let (one, other) = UnixStream::pair().unwrap();
    let (mut one, other) = (one, FrameSocket::new(other).unwrap());
    one.write_all(&ClientFrame::EndRead { read: 1 }.encode())
        .unwrap();
    assert!(other.receive(4).is_err(), "longer than taken");
    let (mut one, other) = {
        let (one, other) = UnixStream::pair().unwrap();
        (one, FrameSocket::new(other).unwrap())
    };
    let frame = ClientFrame::EndRead { read: 1 }.encode();
    one.write_all(&frame[..frame.len() - 1]).unwrap();
    drop(one);
    assert!(other.receive(FrameSocket::MOST_BYTES).is_err(), "cut short");
}

/// A cache built for sharing on files another owns is its client: it
/// connects, and reads the store's bytes. One not built for sharing is
/// `Busy`.
#[test]
fn a_cache_built_for_sharing_on_owned_files_is_their_owners_client() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let data = Bytes::from((0..2 * MIB).map(|at| at as u8).collect::<Vec<_>>());
    block_on(inner.put(&path, data.clone().into())).unwrap();

    let owner = shared(&inner, &dir).build().unwrap();
    assert!(matches!(owner.link(), StorageLink::Owner { .. }));
    let client = shared(&inner, &dir).build().unwrap();
    assert!(!connection(&client).owner_gone());
    for store in [&owner, &client] {
        let read = block_on(store.get_range(&path, 10..2 * MIB - 10)).unwrap();
        assert_eq!(read, data.slice(10..2 * MIB as usize - 10));
    }

    let unshared = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
        .build()
        .unwrap_err();
    assert!(matches!(unshared, crate::Error::Busy), "{unshared}");
}

/// The owner dropped, its client sees it gone, the files' lock is free, and
/// a new cache built for sharing owns them and serves them at the same
/// address.
#[test]
fn an_owner_dropped_hands_its_files_to_the_next_cache() {
    let dir = Arc::new(tempfile::tempdir().unwrap());
    let inner = Arc::new(InMemory::new());
    let owner = shared(&inner, &dir).build().unwrap();
    let client = shared(&inner, &dir).build().unwrap();

    within_5s(move || drop(owner)).expect("the owner's drop ends");
    assert!(eventually(|| connection(&client).owner_gone()));
    let kept = Arc::clone(&dir);
    let (next_owner, next_client) = within_5s(move || {
        let next_owner = shared(&inner, &dir).build().unwrap();
        let next_client = shared(&inner, &dir).build().unwrap();
        (next_owner, next_client)
    })
    .expect("the next owner and its client are built");
    assert!(matches!(next_owner.link(), StorageLink::Owner { .. }));
    assert!(!connection(&next_client).owner_gone());
    drop((next_client, next_owner, kept));
}

/// How the owner answers `hello`, sent on a connection of its own: its
/// welcome, or `None` if it closed the connection.
fn answer_to(address: &OwnerAddress, hello: impl FnOnce(&FrameSocket)) -> Option<OwnerFrame> {
    let socket = FrameSocket::new(address.connect().unwrap()).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    hello(&socket);
    match socket.receive(64) {
        Ok(Some(answer)) => Some(OwnerFrame::decode(answer).unwrap()),
        Ok(None) => None,
        Err(error) => {
            let kind = error.kind();
            assert!(
                !matches!(
                    kind,
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "the owner neither answered nor closed: {error}"
            );
            None
        }
    }
}

/// The owner admits a client by a hello of its protocol's version, with a
/// descriptor of its first cache file open for writing it; it closes any
/// other connection, whatever it sent first.
#[test]
fn the_owner_admits_only_a_writer_of_its_first_cache_file() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let _owner = shared(&inner, &dir).build().unwrap();
    let cache_path = dir.path().join("cache");
    let writable = || {
        File::options()
            .read(true)
            .write(true)
            .open(&cache_path)
            .unwrap()
    };
    let address = OwnerAddress::of(FileIdentity::of(&writable()).unwrap());
    let other = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("other"))
        .unwrap();
    let read_only = File::open(&cache_path).unwrap();

    let welcome = answer_to(&address, |socket| {
        Hello::new(PROTOCOL_VERSION, writable())
            .send(socket)
            .unwrap();
    });
    let version = PROTOCOL_VERSION;
    assert_eq!(welcome, Some(OwnerFrame::Welcome { version }));
    let refused = [
        Hello::new(PROTOCOL_VERSION, other),
        Hello::new(PROTOCOL_VERSION, read_only),
        Hello::new(PROTOCOL_VERSION + 1, writable()),
    ];
    for hello in refused {
        assert_eq!(
            answer_to(&address, |socket| hello.send(socket).unwrap()),
            None
        );
    }
    // A frame with no descriptor, and a length no hello has.
    let bare = ClientFrame::Hello { version }.encode();
    let bare = answer_to(&address, |socket| socket.send(&bare).unwrap());
    assert_eq!(bare, None);
    let garbage = answer_to(&address, |socket| socket.send(&[0xff; 17]).unwrap());
    assert_eq!(garbage, None);
}
