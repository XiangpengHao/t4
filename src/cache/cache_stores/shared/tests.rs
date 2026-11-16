use std::fs::File;
use std::io::Write;
use std::num::NonZeroU8;
use std::ops::Range;
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use pollster::block_on;
use rustix::fs::Uid;
use rustix::process::geteuid;
use tempfile::TempDir;

use super::protocol::{
    ClientFrame, ClientRegion, FetchSpec, FrameReceiver, FrameSender, FrameSocket, Hello, NextStep,
    OwnerAddress, OwnerFrame, PROTOCOL_VERSION, SpanName,
};
use crate::cache::policy::{
    Admit, BlockSource, FixedPlacementPolicy, IoPhase, PlannedGet, ReadPlan,
};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::FileIdentity;
use crate::cache::storage::FileVersion;
use crate::cache::storage::LocatedRun;
use crate::cache::{FileVersionId, T4Caching, T4CachingBuilder};
use crate::io::sync::Arc;

const MIB: u64 = 1 << 20;

fn shared(inner: &Arc<InMemory>, dir: &TempDir) -> T4CachingBuilder {
    T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
        .with_multiprocess_sharing()
}

/// Unbounded: the CI step bounds a hung test.
fn wait_until(done: impl Fn() -> bool) {
    while !done() {
        std::thread::yield_now();
    }
}

fn owner_gone(store: &T4Caching) -> bool {
    store
        .cache_store()
        .owner_connection()
        .expect("a client")
        .owner_gone()
}

/// Among them a frame longer than the receiver reads ahead.
#[test]
fn frames_cross_a_socket_whole() {
    let (one, other) = UnixStream::pair().unwrap();
    let one = FrameSocket::new(one).unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    let frames = [
        ClientFrame::Hello {
            version: 7,
            region: false,
        },
        ClientFrame::Land {
            read: 1,
            fetch: 2,
            bytes: Bytes::from(vec![9; 3 << 20]),
        },
        ClientFrame::EndRead { read: 3 },
        ClientFrame::Finish { call: 4, read: 3 },
    ];
    let sent = frames.clone();
    let sending = std::thread::spawn(move || {
        let encoded: Vec<Vec<u8>> = sent.iter().map(ClientFrame::encode).collect();
        one.send_all(&encoded[..2]).unwrap();
        one.send_all(&encoded[2..]).unwrap();
        one
    });
    for frame in frames {
        let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
        assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    }
    drop(sending.join().unwrap());
    assert!(other.receive(FrameSocket::MOST_BYTES).unwrap().is_none());

    let (mut one, other) = UnixStream::pair().unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    one.write_all(&ClientFrame::EndRead { read: 1 }.encode())
        .unwrap();
    assert!(other.receive(4).is_err(), "longer than taken");
    let (mut one, other) = UnixStream::pair().unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    let frame = ClientFrame::EndRead { read: 1 }.encode();
    one.write_all(&frame[..frame.len() - 1]).unwrap();
    drop(one);
    assert!(other.receive(FrameSocket::MOST_BYTES).is_err(), "cut short");
}

#[test]
fn frames_sent_together_are_taken_from_one_read() {
    let (one, other) = UnixStream::pair().unwrap();
    let one = FrameSocket::new(one).unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    let frames = [
        ClientFrame::EndRead { read: 1 },
        ClientFrame::Finish { call: 2, read: 3 },
        ClientFrame::EndRead { read: 3 },
    ];
    let encoded: Vec<Vec<u8>> = frames.iter().map(ClientFrame::encode).collect();
    one.send_all(&encoded).unwrap();

    assert!(other.fill().unwrap());
    let mut bodies = Vec::new();
    for frame in &frames {
        let body = other.next_frame(FrameSocket::MOST_BYTES).unwrap();
        let body = body.expect("the frame came with the read");
        assert_eq!(&ClientFrame::decode(body.clone()).unwrap(), frame);
        bodies.push(body);
    }
    assert!(other.next_frame(FrameSocket::MOST_BYTES).unwrap().is_none());
    for pair in bodies.windows(2) {
        let after_first = pair[0].as_ptr_range().end;
        // The next frame's body follows its length.
        assert_eq!(after_first.wrapping_add(size_of::<u64>()), pair[1].as_ptr());
    }
}

#[test]
fn a_frame_half_come_is_held_till_the_rest_comes() {
    let (mut one, other) = UnixStream::pair().unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    let frame = ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: Bytes::from(vec![7; 3 << 20]),
    };
    let encoded = frame.encode();
    one.write_all(&encoded[..5]).unwrap();

    assert!(other.fill().unwrap());
    assert!(other.next_frame(FrameSocket::MOST_BYTES).unwrap().is_none());
    let sending = std::thread::spawn(move || {
        one.write_all(&encoded[5..]).unwrap();
        one
    });
    let body = loop {
        if let Some(body) = other.next_frame(FrameSocket::MOST_BYTES).unwrap() {
            break body;
        }
        assert!(other.fill().unwrap(), "the rest comes");
    };
    assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    drop(sending.join().unwrap());
    assert!(!other.fill().unwrap(), "closed between frames");
}

#[test]
fn a_senders_frames_go_in_order_whoever_writes_them() {
    let (one, other) = UnixStream::pair().unwrap();
    let sender =
        FrameSender::start(FrameSocket::new(one).unwrap(), Duration::from_micros(200)).unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    // 8 MiB, far more than the socket holds unread: most wait for the writer.
    let frames: Vec<ClientFrame> = (0..2048)
        .map(|read| ClientFrame::Land {
            read,
            fetch: 0,
            bytes: Bytes::from(vec![read as u8; 4096]),
        })
        .collect();
    let encoded: Vec<Vec<u8>> = frames.iter().map(ClientFrame::encode).collect();
    let mut rest = &encoded[..];
    for together in [1, 2, 3].into_iter().cycle() {
        if rest.is_empty() {
            break;
        }
        let (now, later) = rest.split_at(together.min(rest.len()));
        sender.send(now.to_vec());
        rest = later;
    }
    for frame in frames {
        let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
        assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    }

    drop(other);
    for read in 0..64 {
        sender.send(vec![ClientFrame::EndRead { read }.encode()]);
    }
    drop(sender);
}

#[test]
fn a_caller_waiting_for_room_holds_up_no_other() {
    let (one, other) = UnixStream::pair().unwrap();
    let wait = FrameSender::SEND_WAIT;
    let sender =
        std::sync::Arc::new(FrameSender::start(FrameSocket::new(one).unwrap(), wait).unwrap());
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    // Far more than the socket holds unread: its caller waits for room.
    let first = ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: Bytes::from(vec![7; 4 << 20]),
    };
    let then = ClientFrame::EndRead { read: 1 };

    let waiting = std::sync::Arc::clone(&sender);
    let encoded = first.encode();
    let (entered, resume) = sender.pause_next_caller();
    let first_sent = std::thread::spawn(move || waiting.send(vec![encoded]));
    entered.recv().unwrap();
    sender.send(vec![then.encode()]);
    resume.send(()).unwrap();
    first_sent.join().unwrap();

    for frame in [first, then] {
        let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
        assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    }
}

#[test]
fn a_callers_send_leaves_what_does_not_fit_to_its_writer() {
    let (one, _other) = UnixStream::pair().unwrap();
    let wait = FrameSender::SEND_WAIT;
    let sender = FrameSender::start(FrameSocket::new(one).unwrap(), wait).unwrap();
    let frame = ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: Bytes::from(vec![7; 4 << 20]),
    };
    sender.send(vec![frame.encode()]);
    assert!(
        sender.has_pending_frames(),
        "the writer holds what did not fit"
    );
}

#[test]
fn a_batch_longer_than_a_write_takes_goes_whole() {
    let (one, other) = UnixStream::pair().unwrap();
    let one = FrameSocket::new(one).unwrap();
    let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
    let frames: Vec<ClientFrame> = (0..5000)
        .map(|read| ClientFrame::EndRead { read })
        .collect();
    let sent = frames.clone();
    let sending = std::thread::spawn(move || {
        let encoded: Vec<Vec<u8>> = sent.iter().map(ClientFrame::encode).collect();
        one.send_all(&encoded).unwrap();
        one
    });
    for frame in frames {
        let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
        assert_eq!(ClientFrame::decode(body).unwrap(), frame);
    }
    drop(sending.join().unwrap());
}

#[test]
fn a_cache_built_for_sharing_on_owned_files_is_their_owners_client() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let data = Bytes::from((0..2 * MIB).map(|at| at as u8).collect::<Vec<_>>());
    block_on(inner.put(&path, data.clone().into())).unwrap();

    let owner = shared(&inner, &dir).build().unwrap();
    assert!(owner.cache_store().owned().is_some());
    let client = shared(&inner, &dir).build().unwrap();
    assert!(!owner_gone(&client));
    let clients_store = client.cache_store();
    assert!(
        clients_store
            .owner_connection()
            .expect("a client")
            .has_region()
    );
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

#[test]
fn an_owner_dropped_hands_its_files_to_the_next_cache() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let owner = shared(&inner, &dir).build().unwrap();
    let client = shared(&inner, &dir).build().unwrap();

    drop(owner);
    wait_until(|| owner_gone(&client));
    let next_owner = shared(&inner, &dir).build().unwrap();
    let next_client = shared(&inner, &dir).build().unwrap();
    assert!(next_owner.cache_store().owned().is_some());
    assert!(!owner_gone(&next_client));
    drop((next_client, next_owner));
}

fn answer_to(address: &OwnerAddress, hello: impl FnOnce(&FrameSocket)) -> Option<OwnerFrame> {
    let socket = FrameSocket::new(address.connect(geteuid()).unwrap()).unwrap();
    hello(&socket);
    match FrameReceiver::new(socket.try_clone().unwrap()).receive(64) {
        Ok(Some(answer)) => Some(OwnerFrame::decode(answer).unwrap()),
        Ok(None) => None,
        Err(_) => None,
    }
}

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
    let address = OwnerAddress::of(FileIdentity::of(&writable()).unwrap()).unwrap();
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
    let region = false;
    assert_eq!(welcome, Some(OwnerFrame::Welcome { version, region }));
    let refused = [
        Hello::new(PROTOCOL_VERSION, other),
        Hello::new(PROTOCOL_VERSION, read_only),
    ];
    for hello in refused {
        assert_eq!(
            answer_to(&address, |socket| hello.send(socket).unwrap()),
            None
        );
    }
    // A frame with no descriptor, and a length no hello has.
    let bare = ClientFrame::Hello { version, region }.encode();
    let bare = answer_to(&address, |socket| socket.send(&bare).unwrap());
    assert_eq!(bare, None);
    let garbage = answer_to(&address, |socket| socket.send(&[0xff; 18]).unwrap());
    assert_eq!(garbage, None);
}

#[test]
fn a_client_connects_only_to_an_owner_of_its_user() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let _owner = shared(&inner, &dir).build().unwrap();
    let cache_file = File::open(dir.path().join("cache")).unwrap();
    let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap()).unwrap();

    assert!(address.connect(geteuid()).is_ok());
    let another = Uid::from_raw(geteuid().as_raw() + 1);
    let refused = address.connect(another).unwrap_err();
    assert!(matches!(refused, crate::Error::Busy), "{refused}");
}

/// No other user keeps a user from caching.
#[test]
fn an_owner_whose_address_is_held_still_owns_its_cache() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let cache_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("cache"))
        .unwrap();
    let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap()).unwrap();
    let _holder = address.listen().unwrap();

    let owner = shared(&inner, &dir).build().unwrap();
    assert!(owner.cache_store().owned().is_some());
    let read = block_on(owner.get_range(&meta.location, 0..MIB)).unwrap();
    assert_eq!(read, data.slice(..MIB as usize));
}

#[test]
fn a_hellos_descriptors_close_on_exec() {
    let (client, owner) = UnixStream::pair().unwrap();
    let (client, owner) = (
        FrameSocket::new(client).unwrap(),
        FrameSocket::new(owner).unwrap(),
    );
    let region = ClientRegion::make().unwrap();
    let memory = region.descriptor().try_clone_to_owned().unwrap();
    let file = tempfile::tempfile().unwrap();
    Hello::new(PROTOCOL_VERSION, file)
        .with_region(memory)
        .send(&client)
        .unwrap();

    let hello = Hello::receive(&owner).unwrap();
    let memory = hello.into_region().expect("the region came");
    let flags = rustix::io::fcntl_getfd(&memory).unwrap();
    assert!(flags.contains(rustix::io::FdFlags::CLOEXEC), "{flags:?}");
}

#[test]
fn the_owner_tells_a_client_of_another_version_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let _owner = shared(&inner, &dir).build().unwrap();
    let cache_file = File::options()
        .read(true)
        .write(true)
        .open(dir.path().join("cache"))
        .unwrap();
    let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap()).unwrap();
    let socket = FrameSocket::new(address.connect(geteuid()).unwrap()).unwrap();

    Hello::new(PROTOCOL_VERSION + 1, cache_file)
        .send(&socket)
        .unwrap();
    let mut frames = FrameReceiver::new(socket.try_clone().unwrap());
    let welcome = OwnerFrame::decode(frames.receive(64).unwrap().expect("an answer")).unwrap();
    let (version, region) = (PROTOCOL_VERSION, false);
    assert_eq!(welcome, OwnerFrame::Welcome { version, region });
    let closed = frames.receive(64);
    assert!(
        matches!(closed, Ok(None)),
        "the owner closes the connection: {closed:?}"
    );
}

#[test]
fn a_cache_whose_owner_speaks_another_version_is_busy_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let cache_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join("cache"))
        .unwrap();
    cache_file.try_lock().unwrap();
    let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap()).unwrap();
    let listener = address.listen().unwrap();
    let other_owner = std::thread::spawn(move || {
        let (connection, _) = listener.accept().unwrap();
        let socket = FrameSocket::new(connection).unwrap();
        Hello::receive(&socket).unwrap();
        let (version, region) = (PROTOCOL_VERSION + 1, false);
        socket
            .send(&OwnerFrame::Welcome { version, region }.encode())
            .unwrap();
        listener
    });

    let refused = shared(&inner, &dir).build().unwrap_err();
    assert!(matches!(refused, crate::Error::Busy), "{refused}");
    let listener = other_owner.join().unwrap();
    listener.set_nonblocking(true).unwrap();
    let retry = listener.accept().unwrap_err();
    assert_eq!(
        retry.kind(),
        std::io::ErrorKind::WouldBlock,
        "no retry connection"
    );
    address.remove();
}

/// A region that could shrink (a file) is welcomed without it; a hello that claims a region
/// and sends none is closed.
#[test]
fn the_owner_maps_a_clients_region_that_cannot_shrink() {
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
    let address = OwnerAddress::of(FileIdentity::of(&writable()).unwrap()).unwrap();
    let version = PROTOCOL_VERSION;

    let region = ClientRegion::make().unwrap();
    let memory = region.descriptor().try_clone_to_owned().unwrap();
    let welcome = answer_to(&address, |socket| {
        let hello = Hello::new(version, writable()).with_region(memory);
        hello.send(socket).unwrap();
    });
    let region = true;
    assert_eq!(welcome, Some(OwnerFrame::Welcome { version, region }));

    let file = tempfile::tempfile().unwrap();
    file.set_len(1 << 20).unwrap();
    let welcome = answer_to(&address, |socket| {
        let hello = Hello::new(version, writable()).with_region(file.into());
        hello.send(socket).unwrap();
    });
    let region = false;
    assert_eq!(welcome, Some(OwnerFrame::Welcome { version, region }));

    let claimed = answer_to(&address, |socket| {
        let frame = ClientFrame::Hello {
            version,
            region: true,
        };
        Hello::new(version, writable())
            .send_as(socket, &frame.encode())
            .unwrap();
    });
    assert_eq!(claimed, None);
}

struct RawClient {
    socket: FrameSocket,
    frames: FrameReceiver,
    news: Vec<OwnerFrame>,
}

impl RawClient {
    fn admitted(dir: &TempDir) -> Self {
        Self::admitted_with(dir, None)
    }

    fn admitted_with_region(dir: &TempDir) -> (Self, std::sync::Arc<ClientRegion>) {
        let region = ClientRegion::make().unwrap();
        (Self::admitted_with(dir, Some(&region)), region)
    }

    fn admitted_with(dir: &TempDir, region: Option<&std::sync::Arc<ClientRegion>>) -> Self {
        let cache_file = File::options()
            .read(true)
            .write(true)
            .open(dir.path().join("cache"))
            .unwrap();
        let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap()).unwrap();
        let socket = FrameSocket::new(address.connect(geteuid()).unwrap()).unwrap();
        let mut hello = Hello::new(PROTOCOL_VERSION, cache_file);
        if let Some(region) = region {
            hello = hello.with_region(region.descriptor().try_clone_to_owned().unwrap());
        }
        hello.send(&socket).unwrap();
        let mut client = Self {
            frames: FrameReceiver::new(socket.try_clone().unwrap()),
            socket,
            news: Vec::new(),
        };
        let version = PROTOCOL_VERSION;
        let region = region.is_some();
        assert_eq!(client.answer(), OwnerFrame::Welcome { version, region });
        client
    }

    fn send(&self, frame: &ClientFrame) {
        self.socket.send(&frame.encode()).unwrap();
    }

    fn answer(&mut self) -> OwnerFrame {
        loop {
            let body = self.frames.receive(FrameSocket::MOST_BYTES).unwrap();
            let frame = OwnerFrame::decode(body.expect("an answer, not a close")).unwrap();
            if !matches!(frame, OwnerFrame::DiskRead { .. }) {
                return frame;
            }
            self.news.push(frame);
        }
    }

    fn closed(&mut self) -> bool {
        loop {
            match self.frames.receive(FrameSocket::MOST_BYTES) {
                Ok(Some(_)) => {}
                Ok(None) => return true,
                Err(error) => {
                    use std::io::ErrorKind::{TimedOut, WouldBlock};
                    return !matches!(error.kind(), WouldBlock | TimedOut);
                }
            }
        }
    }
}

fn put_file(inner: &InMemory, path: &Path, size: u64) -> (Bytes, ObjectMeta) {
    let data = Bytes::from((0..size).map(|at| (at % 251) as u8).collect::<Vec<_>>());
    block_on(inner.put(path, data.clone().into())).unwrap();
    (data, block_on(inner.head(path)).unwrap())
}

fn start_read(read: u64, meta: &ObjectMeta, range: Range<u64>) -> ClientFrame {
    ClientFrame::StartRead {
        call: read * 100,
        read,
        path: Bytes::copy_from_slice(meta.location.as_ref().as_bytes()),
        e_tag: meta.e_tag.clone().unwrap(),
        size: meta.size,
        range,
        intent: None,
        output: None,
    }
}

fn start_read_into(
    read: u64,
    meta: &ObjectMeta,
    range: Range<u64>,
    output: SpanName,
) -> ClientFrame {
    let ClientFrame::StartRead {
        call,
        read,
        path,
        e_tag,
        size,
        range,
        intent,
        ..
    } = start_read(read, meta, range)
    else {
        unreachable!("a start");
    };
    ClientFrame::StartRead {
        call,
        read,
        path,
        e_tag,
        size,
        range,
        intent,
        output: Some(output),
    }
}

#[test]
fn the_owner_serves_a_read_into_its_clients_span() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, 2 * MIB);
    let _owner = shared(&inner, &dir).build().unwrap();
    let (mut client, region) = RawClient::admitted_with_region(&dir);

    let range = 100..MIB + 50;
    let span = region.span(ReadOutput::padded_len(&range)).unwrap();
    client.send(&start_read_into(1, &meta, range.clone(), span.name()));
    assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
    let plan = ReadPlan::new(vec![PlannedGet::once(0..2)]);
    client.send(&ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan,
    });
    assert!(matches!(client.answer(), OwnerFrame::Next { .. }));
    client.send(&ClientFrame::Finish { call: 4, read: 1 });
    client.send(&ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: data.clone(),
    });
    client.send(&ClientFrame::Commit {
        call: 3,
        read: 1,
        fetch: 0,
    });
    assert_eq!(client.answer(), OwnerFrame::Committed { call: 3 });
    let asked = range.clone();
    assert_eq!(client.answer(), OwnerFrame::ServedInSpan { call: 4, asked });
    let within = 100..100 + (range.end - range.start) as usize;
    assert_eq!(span.into_bytes(within), data.slice(100..MIB as usize + 50));

    // A barrier after the read's frames: no `Ended` may come before the client's `EndRead`.
    client.send(&ClientFrame::KnownSize {
        call: 5,
        path: Bytes::copy_from_slice(meta.location.as_ref().as_bytes()),
    });
    assert_eq!(
        client.answer(),
        OwnerFrame::KnownSize {
            call: 5,
            size: Some(meta.size)
        }
    );
    client.send(&ClientFrame::EndRead { read: 1 });
    assert_eq!(client.answer(), OwnerFrame::Ended { read: 1 });

    let short = region.span(1).unwrap();
    client.send(&start_read_into(2, &meta, 0..2 * MIB, short.name()));
    assert!(client.closed());
}

#[test]
fn an_output_span_off_a_page_boundary_closes_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (_data, meta) = put_file(&inner, &Path::from("a"), MIB);
    let _owner = shared(&inner, &dir).build().unwrap();
    let (mut client, region) = RawClient::admitted_with_region(&dir);

    let page = crate::PAGE_SIZE_U64;
    let span = region
        .span(ReadOutput::padded_len(&(0..MIB)) + page)
        .unwrap();
    let whole = span.name();
    let off_a_page = SpanName::new(whole.offset() + 512, whole.len() - 512);
    client.send(&start_read_into(1, &meta, 0..MIB, off_a_page));
    assert!(client.closed());
}

fn read_listed(
    store: &T4Caching,
    meta: &ObjectMeta,
    range: Range<u64>,
) -> object_store::Result<Bytes> {
    block_on(store.open_listed(meta.clone()).read(range, None))
}

#[test]
fn the_owner_serves_and_caches_a_clients_cold_read() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, 2 * MIB);
    let owner = shared(&inner, &dir).build().unwrap();
    let mut client = RawClient::admitted(&dir);

    client.send(&start_read(1, &meta, 10..2 * MIB));
    let OwnerFrame::Started {
        call: 100,
        version,
        next,
    } = client.answer()
    else {
        panic!("the read started");
    };
    let remote = vec![BlockSource::Remote, BlockSource::Remote];
    assert_eq!(
        next,
        NextStep::Plan(LocatedRun::new(0, 10..2 * MIB, remote))
    );
    assert_eq!(version.size(), 2 * MIB);
    let plan = ReadPlan::new(vec![PlannedGet::once(0..2)]);
    client.send(&ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan,
    });
    let fetch = FetchSpec::new(0..2 * MIB, NonZeroU8::MIN, None);
    let next = NextStep::Fetch(vec![fetch]);
    assert_eq!(client.answer(), OwnerFrame::Next { call: 2, next });

    client.send(&ClientFrame::Finish { call: 4, read: 1 });
    for piece in [0..MIB / 3, MIB / 3..2 * MIB] {
        let bytes = data.slice(piece.start as usize..piece.end as usize);
        client.send(&ClientFrame::Land {
            read: 1,
            fetch: 0,
            bytes,
        });
    }
    client.send(&ClientFrame::Commit {
        call: 3,
        read: 1,
        fetch: 0,
    });
    assert_eq!(client.answer(), OwnerFrame::Committed { call: 3 });
    let bytes = Some(data.slice(10..2 * MIB as usize));
    assert_eq!(client.answer(), OwnerFrame::Served { call: 4, bytes });
    client.send(&ClientFrame::EndRead { read: 1 });

    block_on(inner.delete(&path)).unwrap();
    assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);
}

#[test]
fn the_owner_lands_a_fetch_staged_in_its_clients_region() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, 2 * MIB);
    let (_, other) = put_file(&inner, &Path::from("b"), MIB);
    let owner = shared(&inner, &dir).build().unwrap();
    let (mut client, region) = RawClient::admitted_with_region(&dir);

    client.send(&start_read(1, &meta, 10..2 * MIB));
    assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
    let plan = ReadPlan::new(vec![PlannedGet::once(0..2)]);
    client.send(&ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan,
    });
    assert!(matches!(client.answer(), OwnerFrame::Next { .. }));
    client.send(&ClientFrame::Finish { call: 4, read: 1 });
    let mut span = region.span(2 * MIB).unwrap();
    span.copy_in(0, &data);
    let at = span.name().offset();
    for piece in [0..MIB / 3, MIB / 3..2 * MIB] {
        client.send(&ClientFrame::LandStaged {
            read: 1,
            fetch: 0,
            staged: SpanName::new(at + piece.start, piece.end - piece.start),
        });
    }
    client.send(&ClientFrame::Commit {
        call: 3,
        read: 1,
        fetch: 0,
    });
    assert_eq!(client.answer(), OwnerFrame::Committed { call: 3 });
    let bytes = Some(data.slice(10..2 * MIB as usize));
    assert_eq!(client.answer(), OwnerFrame::Served { call: 4, bytes });
    client.send(&ClientFrame::EndRead { read: 1 });
    assert_eq!(client.answer(), OwnerFrame::Ended { read: 1 });
    drop(span);
    block_on(inner.delete(&path)).unwrap();
    assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);

    client.send(&start_read(2, &other, 0..MIB));
    assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
    let plan = ReadPlan::new(vec![PlannedGet::once(0..1)]);
    client.send(&ClientFrame::TakeGets {
        call: 5,
        read: 2,
        plan,
    });
    assert!(matches!(client.answer(), OwnerFrame::Next { .. }));
    client.send(&ClientFrame::LandStaged {
        read: 2,
        fetch: 0,
        staged: SpanName::new(ClientRegion::BYTES - 10, 20),
    });
    assert!(client.closed());
}

fn socket_bytes(cache: &T4Caching) -> (u64, u64) {
    cache
        .cache_store()
        .owner_connection()
        .expect("a client")
        .socket_bytes()
}

#[test]
fn a_clients_cold_read_moves_only_control_over_its_socket() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let owner = shared(&inner, &dir).build().unwrap();
    let client = shared(&inner, &dir).build().unwrap();

    let (sent, taken) = socket_bytes(&client);
    assert_eq!(read_listed(&client, &meta, 0..2 * MIB).unwrap(), data);
    let (sent, taken) = {
        let (now_sent, now_taken) = socket_bytes(&client);
        (now_sent - sent, now_taken - taken)
    };
    assert!(sent + taken < 64 << 10, "{sent} bytes sent, {taken} taken");
    block_on(inner.delete(&meta.location)).unwrap();
    assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);

    let (data, meta) = put_file(&inner, &Path::from("b"), 2 * MIB);
    let cache_store = client.cache_store();
    let connection = cache_store.owner_connection().expect("a client");
    let region = connection.region().unwrap();
    let held: Vec<_> = std::iter::from_fn(|| region.span(1)).collect();
    let (sent, taken) = socket_bytes(&client);
    assert_eq!(read_listed(&client, &meta, 0..2 * MIB).unwrap(), data);
    let (now_sent, now_taken) = socket_bytes(&client);
    assert!(now_sent - sent > 2 * MIB, "the GET's bytes in frames");
    assert!(now_taken - taken > 2 * MIB, "the read's bytes in a frame");
    drop(held);
}

#[test]
fn the_owner_serves_a_clients_hits_and_tells_it_of_its_disk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, 2 * MIB);
    let owner = shared(&inner, &dir)
        .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
        .build()
        .unwrap();
    assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);
    let mut client = RawClient::admitted(&dir);

    client.send(&start_read(1, &meta, 5..2 * MIB - 5));
    let OwnerFrame::Started {
        call: 100, next, ..
    } = client.answer()
    else {
        panic!("the read started");
    };
    assert_eq!(next, NextStep::Fetch(Vec::new()), "nothing to fetch");
    client.send(&ClientFrame::Finish { call: 1, read: 1 });
    let bytes = Some(data.slice(5..2 * MIB as usize - 5));
    assert_eq!(client.answer(), OwnerFrame::Served { call: 1, bytes });
    client.send(&ClientFrame::EndRead { read: 1 });

    let phases: Vec<_> = client
        .news
        .iter()
        .map(|news| match news {
            OwnerFrame::DiskRead {
                drive: 0, phase, ..
            } => match phase {
                IoPhase::Started => "started",
                IoPhase::Finished { .. } => "finished",
                _ => "other",
            },
            _ => "not a disk read of drive 0",
        })
        .collect();
    assert_eq!(phases, ["started", "started", "finished", "finished"]);
}

#[test]
fn the_owner_answers_a_hot_read_started_and_finished_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let owner = shared(&inner, &dir).build().unwrap();
    assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);
    let started_with_nothing_to_fetch = |answer: OwnerFrame, call| match answer {
        OwnerFrame::Started { call: of, next, .. } => of == call && next == NextStep::Fetch(vec![]),
        _ => false,
    };

    let mut client = RawClient::admitted(&dir);
    client.send(&start_read(1, &meta, 5..MIB));
    client.send(&ClientFrame::Finish { call: 1, read: 1 });
    assert!(started_with_nothing_to_fetch(client.answer(), 100));
    let bytes = Some(data.slice(5..MIB as usize));
    assert_eq!(client.answer(), OwnerFrame::Served { call: 1, bytes });

    let (mut client, region) = RawClient::admitted_with_region(&dir);
    let span = region.span(ReadOutput::padded_len(&(5..MIB))).unwrap();
    client.send(&start_read_into(2, &meta, 5..MIB, span.name()));
    client.send(&ClientFrame::Finish { call: 3, read: 2 });
    assert!(started_with_nothing_to_fetch(client.answer(), 200));
    let asked = 5..MIB;
    assert_eq!(client.answer(), OwnerFrame::ServedInSpan { call: 3, asked });
    assert_eq!(
        span.into_bytes(5..MIB as usize),
        data.slice(5..MIB as usize)
    );
}

#[test]
fn the_owner_holds_a_finish_sent_while_it_plans() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let _owner = shared(&inner, &dir).build().unwrap();
    let mut client = RawClient::admitted(&dir);

    client.send(&start_read(1, &meta, 10..2 * MIB));
    client.send(&ClientFrame::Finish { call: 1, read: 1 });
    let OwnerFrame::Started {
        call: 100,
        next: NextStep::Plan(_),
        ..
    } = client.answer()
    else {
        panic!("a run to plan");
    };
    let plan = ReadPlan::new(vec![PlannedGet::once(0..2)]);
    client.send(&ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan,
    });
    let OwnerFrame::Next {
        call: 2,
        next: NextStep::Fetch(fetches),
    } = client.answer()
    else {
        panic!("the read's GETs");
    };
    assert_eq!(fetches.len(), 1);
    client.send(&ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: data.clone(),
    });
    client.send(&ClientFrame::Commit {
        call: 3,
        read: 1,
        fetch: 0,
    });
    assert_eq!(client.answer(), OwnerFrame::Committed { call: 3 });
    let bytes = Some(data.slice(10..2 * MIB as usize));
    assert_eq!(client.answer(), OwnerFrame::Served { call: 1, bytes });
}

#[test]
fn a_second_finish_in_a_round_closes_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (_data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let _owner = shared(&inner, &dir).build().unwrap();
    let mut client = RawClient::admitted(&dir);

    client.send(&start_read(1, &meta, 0..2 * MIB));
    client.send(&ClientFrame::Finish { call: 1, read: 1 });
    client.send(&ClientFrame::Finish { call: 2, read: 1 });
    assert!(client.closed());
}

/// A cache not built for sharing holds the files, so a stand-in owner answers by hand.
#[test]
fn a_clients_read_sends_its_finish_with_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), MIB);
    let _holder = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
        .build()
        .unwrap();
    let first_file = FileIdentity::of(&File::open(dir.path().join("cache")).unwrap()).unwrap();
    let address = OwnerAddress::of(first_file).unwrap();
    let listener = address.listen().unwrap();
    let served = data.clone();
    let (start_seen, started) = mpsc::channel();
    let (proceed, finish_queued) = mpsc::channel();
    let stand_in = std::thread::spawn(move || {
        let socket = FrameSocket::new(listener.accept().unwrap().0).unwrap();
        Hello::receive(&socket).unwrap().admit(first_file).unwrap();
        let welcome = OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
            region: false,
        };
        socket.send(&welcome.encode()).unwrap();
        let mut frames = FrameReceiver::new(socket.try_clone().unwrap());
        let mut frame = || {
            let body = frames.receive(FrameSocket::MOST_BYTES).ok().flatten()?;
            Some(ClientFrame::decode(body).unwrap())
        };
        let Some(ClientFrame::StartRead {
            call, read, size, ..
        }) = frame()
        else {
            panic!("a read's start");
        };
        start_seen.send(()).unwrap();
        finish_queued.recv().unwrap();
        // Block on the finish before answering the start.
        let before_any_answer = frame();
        let version = FileVersion::named(FileVersionId::new(0), size).unwrap();
        let next = NextStep::Fetch(Vec::new());
        socket
            .send(
                &OwnerFrame::Started {
                    call,
                    version,
                    next,
                }
                .encode(),
            )
            .unwrap();
        let Some(ClientFrame::Finish { call, .. }) = before_any_answer.clone() else {
            panic!("the read's finish");
        };
        let bytes = Some(served);
        socket
            .send(&OwnerFrame::Served { call, bytes }.encode())
            .unwrap();
        (read, before_any_answer)
    });
    let client = shared(&inner, &dir).build().unwrap();
    let reading = client.clone();
    let read = std::thread::spawn(move || read_listed(&reading, &meta, 0..MIB).unwrap());
    started.recv().unwrap();
    let connection = client.cache_store().owner_connection().unwrap();
    assert_eq!(
        connection.calls_waiting(),
        [0, 1],
        "start and finish queued before any answer"
    );
    proceed.send(()).unwrap();
    assert_eq!(read.join().unwrap(), data, "served by the stand-in");
    let (read, before_any_answer) = stand_in.join().unwrap();
    address.remove();
    assert!(
        matches!(before_any_answer, Some(ClientFrame::Finish { read: of, .. }) if of == read),
        "the finish went before any answer: {before_any_answer:?}"
    );
}

#[test]
fn a_client_gone_mid_read_hands_its_claims_to_their_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, MIB);
    let owner = Arc::new(shared(&inner, &dir).build().unwrap());
    let mut client = RawClient::admitted(&dir);
    client.send(&start_read(1, &meta, 0..MIB));
    assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
    let plan = ReadPlan::new(vec![PlannedGet::once(0..1)]);
    client.send(&ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan,
    });
    assert!(matches!(client.answer(), OwnerFrame::Next { .. }));

    let (done, finished) = mpsc::channel();
    let waiting = Arc::clone(&owner);
    std::thread::spawn(move || {
        let _ = done.send(read_listed(&waiting, &meta, 0..MIB));
    });
    wait_until(|| {
        owner
            .cache_store()
            .owned()
            .unwrap()
            .fetch_waiters_registered()
            == 1
    });
    drop(client);
    let read = finished.recv();
    assert_eq!(read.expect("the waiter fetches").unwrap(), data);
}

#[test]
fn a_clients_misuse_closes_its_connection() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let path = Path::from("a");
    let (data, meta) = put_file(&inner, &path, MIB);
    let owner = shared(&inner, &dir).build().unwrap();
    let plan = || ReadPlan::new(vec![PlannedGet::once(0..1)]);
    let land = || ClientFrame::Land {
        read: 1,
        fetch: 0,
        bytes: data.clone(),
    };
    let take_gets = || ClientFrame::TakeGets {
        call: 2,
        read: 1,
        plan: plan(),
    };
    let misuses: Vec<(&str, Vec<ClientFrame>, ClientFrame)> = vec![
        ("GETs of no read", vec![], take_gets()),
        ("bytes of no read", vec![], land()),
        (
            "a read's number twice",
            vec![start_read(1, &meta, 0..1)],
            start_read(1, &meta, 0..1),
        ),
        (
            "bytes before the GETs",
            vec![start_read(1, &meta, 0..1)],
            land(),
        ),
        (
            "GETs twice",
            vec![start_read(1, &meta, 0..1), take_gets()],
            take_gets(),
        ),
        (
            "bytes of no fetch",
            vec![start_read(1, &meta, 0..1), take_gets()],
            ClientFrame::Land {
                read: 1,
                fetch: 1,
                bytes: data.clone(),
            },
        ),
        (
            "bytes after the commit",
            vec![
                start_read(1, &meta, 0..1),
                take_gets(),
                land(),
                ClientFrame::Commit {
                    call: 3,
                    read: 1,
                    fetch: 0,
                },
            ],
            land(),
        ),
        (
            "a second hello",
            vec![],
            ClientFrame::Hello {
                version: PROTOCOL_VERSION,
                region: false,
            },
        ),
        (
            "a read ended twice",
            vec![start_read(1, &meta, 0..1), ClientFrame::EndRead { read: 1 }],
            ClientFrame::EndRead { read: 1 },
        ),
    ];
    for (misuse, setup, breaking) in misuses {
        let mut client = RawClient::admitted(&dir);
        let mut answered = vec![];
        for frame in setup {
            let answers = !matches!(
                frame,
                ClientFrame::Land { .. } | ClientFrame::EndRead { .. }
            );
            client.send(&frame);
            if answers {
                answered.push(client.answer());
            }
        }
        client.send(&breaking);
        assert!(client.closed(), "{misuse}: closed, after {answered:?}");
    }
    assert_eq!(read_listed(&owner, &meta, 0..MIB).unwrap(), data);
    let mut client = RawClient::admitted(&dir);
    client.send(&start_read(1, &meta, 0..MIB));
    assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
}
