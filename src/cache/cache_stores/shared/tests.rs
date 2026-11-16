#[cfg(feature = "shuttle")]
use super::protocol::FrameSocket as UnixStream;
use crate::io::sync::mpsc;
use std::io::Write;
use std::num::NonZeroU8;
use std::ops::Range;
#[cfg(not(feature = "shuttle"))]
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
#[cfg(not(feature = "shuttle"))]
use pollster::block_on;
use rustix::fs::Uid;
use rustix::process::geteuid;
#[cfg(feature = "shuttle")]
use shuttle::future::block_on;

use super::protocol::{
    ClientFrame, ClientRegion, FetchSpec, FrameReceiver, FrameSender, FrameSocket, Hello,
    MOST_READ_BYTES, NextStep, OwnerAddress, OwnerFrame, PROTOCOL_VERSION, SpanName,
};
use crate::cache::policy::{
    Admit, BlockSource, FixedPlacementPolicy, IoPhase, PlannedGet, ReadPlan,
};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::FileVersion;
use crate::cache::storage::LocatedRun;
use crate::cache::{FileVersionId, T4Caching, T4CachingBuilder};
use crate::io::file_system::{FileIdentity, FileSystem, OpenFile};
use crate::io::sync::Arc;

const MIB: u64 = 1 << 20;

fn shared(inner: &Arc<InMemory>, dir: &TestDirectory) -> T4CachingBuilder {
    let builder = T4Caching::builder(Arc::clone(inner) as Arc<dyn ObjectStore>)
        .with_memory_capacity(4 * MIB)
        .with_disk_file(dir.path().join("cache"), 8 * MIB)
        .with_multiprocess_sharing();
    #[cfg(feature = "shuttle")]
    let builder = (builder.with_test_transport(dir.transport.clone()))
        .with_test_file_system(dir.file_system.clone());
    builder
}

/// No deadline: shuttle fails a schedule that stalls here.
fn wait_until(done: impl Fn() -> bool) {
    while !done() {
        crate::io::sync::thread::yield_now();
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
    crate::io::test_schedule::TestSchedule::check(|| {
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
        let sending = crate::io::sync::thread::spawn(move || {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn frames_sent_together_are_taken_from_one_read() {
    crate::io::test_schedule::TestSchedule::once(|| {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_frame_half_come_is_held_till_the_rest_comes() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (mut one, other) = UnixStream::pair().unwrap();
        let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
        let frame = ClientFrame::Land {
            read: 1,
            fetch: 0,
            bytes: Bytes::from(vec![7; 3 << 20]),
        };
        let encoded = frame.encode();
        one.write_all(&encoded[..5]).unwrap();

        let mut other = {
            assert!(other.fill().unwrap());
            assert!(other.next_frame(FrameSocket::MOST_BYTES).unwrap().is_none());
            other
        };
        let sending = crate::io::sync::thread::spawn(move || {
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_senders_frames_go_in_order_whoever_writes_them() {
    crate::io::test_schedule::TestSchedule::check(|| {
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
        let sender = {
            let mut rest = &encoded[..];
            for together in [1, 2, 3].into_iter().cycle() {
                if rest.is_empty() {
                    break;
                }
                let (now, later) = rest.split_at(together.min(rest.len()));
                sender.send(now.to_vec());
                rest = later;
            }
            sender
        };
        for frame in frames {
            let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
            assert_eq!(ClientFrame::decode(body).unwrap(), frame);
        }

        drop(other);
        let sender = {
            for read in 0..64 {
                sender.send(vec![ClientFrame::EndRead { read }.encode()]);
            }
            sender
        };
        drop(sender);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_caller_waiting_for_room_holds_up_no_other() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (one, other) = UnixStream::pair().unwrap();
        let wait = Duration::from_secs(1);
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
        let first_sent = crate::io::sync::thread::spawn(move || waiting.send(vec![encoded]));
        first_sent.join().unwrap();
        sender.send(vec![then.encode()]);

        for frame in [first, then] {
            let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
            assert_eq!(ClientFrame::decode(body).unwrap(), frame);
        }
    });
}

#[cfg(target_os = "linux")]
#[cfg(feature = "shuttle")]
#[test]
fn a_callers_send_waits_for_no_room_on_linux() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (one, _other) = UnixStream::pair().unwrap();
        let wait = Duration::from_secs(1);
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
    });
}

#[test]
fn a_batch_longer_than_a_write_takes_goes_whole() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (one, other) = UnixStream::pair().unwrap();
        let one = FrameSocket::new(one).unwrap();
        let mut other = FrameReceiver::new(FrameSocket::new(other).unwrap());
        let frames: Vec<ClientFrame> = (0..5000)
            .map(|read| ClientFrame::EndRead { read })
            .collect();
        let sent = frames.clone();
        let sending = crate::io::sync::thread::spawn(move || {
            let encoded: Vec<Vec<u8>> = sent.iter().map(ClientFrame::encode).collect();
            one.send_all(&encoded).unwrap();
            one
        });
        for frame in frames {
            let body = other.receive(FrameSocket::MOST_BYTES).unwrap().unwrap();
            assert_eq!(ClientFrame::decode(body).unwrap(), frame);
        }
        drop(sending.join().unwrap());
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_cache_built_for_sharing_on_owned_files_is_their_owners_client() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
            .with_test_file_system(dir.file_system.clone())
            .build()
            .unwrap_err();
        assert!(matches!(unshared, crate::Error::Busy), "{unshared}");
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn an_owner_dropped_hands_its_files_to_the_next_cache() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = Arc::new(TestDirectory::new());
        let inner = Arc::new(InMemory::new());
        let owner = shared(&inner, &dir).build().unwrap();
        let client = shared(&inner, &dir).build().unwrap();

        drop(owner);
        wait_until(|| owner_gone(&client));
        let kept = Arc::clone(&dir);
        let (next_owner, next_client) = {
            let next_owner = shared(&inner, &dir).build().unwrap();
            let next_client = shared(&inner, &dir).build().unwrap();
            (next_owner, next_client)
        };
        assert!(next_owner.cache_store().owned().is_some());
        assert!(!owner_gone(&next_client));
        drop((next_client, next_owner, kept));
    });
}

fn answer_to(address: &OwnerAddress, hello: impl FnOnce(&FrameSocket)) -> Option<OwnerFrame> {
    let socket = FrameSocket::new(address.connect(geteuid()).unwrap()).unwrap();
    hello(&socket);
    match FrameReceiver::new(socket.try_clone().unwrap()).receive(64) {
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

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_admits_only_a_writer_of_its_first_cache_file() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let _owner = shared(&inner, &dir).build().unwrap();
        let cache_path = dir.path().join("cache");
        let writable = || dir.file_system.open_existing(&cache_path, true).unwrap();
        let address = OwnerAddress::of(
            writable().identity().unwrap(),
            #[cfg(feature = "shuttle")]
            &dir.transport,
        )
        .unwrap();
        let other = (dir.file_system)
            .open_cache_file(&dir.path().join("other"), false)
            .unwrap();
        let read_only = dir.file_system.open_existing(&cache_path, false).unwrap();

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
    });
}

#[cfg(not(feature = "shuttle"))]
#[test]
fn a_client_connects_only_to_an_owner_of_its_user() {
    let dir = TestDirectory::new();
    let inner = Arc::new(InMemory::new());
    let _owner = shared(&inner, &dir).build().unwrap();
    let cache_file = (dir.file_system)
        .open_existing(&dir.path().join("cache"), false)
        .unwrap();
    let address = OwnerAddress::of(
        cache_file.identity().unwrap(),
        #[cfg(feature = "shuttle")]
        &dir.transport,
    )
    .unwrap();

    assert!(address.connect(geteuid()).is_ok());
    let another = Uid::from_raw(geteuid().as_raw() + 1);
    let refused = address.connect(another).unwrap_err();
    assert!(matches!(refused, crate::Error::Busy), "{refused}");
}

/// No other user keeps a user from caching.
#[cfg(not(feature = "shuttle"))]
#[test]
fn an_owner_whose_address_is_held_still_owns_its_cache() {
    let dir = TestDirectory::new();
    let inner = Arc::new(InMemory::new());
    let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
    let cache_file = (dir.file_system)
        .open_cache_file(&dir.path().join("cache"), false)
        .unwrap();
    let address = OwnerAddress::of(
        cache_file.identity().unwrap(),
        #[cfg(feature = "shuttle")]
        &dir.transport,
    )
    .unwrap();
    let _holder = address.listen().unwrap();

    let owner = shared(&inner, &dir).build().unwrap();
    assert!(owner.cache_store().owned().is_some());
    let read = block_on(owner.get_range(&meta.location, 0..MIB)).unwrap();
    assert_eq!(read, data.slice(..MIB as usize));
}

#[cfg(not(feature = "shuttle"))]
#[test]
fn a_hellos_descriptors_close_on_exec() {
    let (client, owner) = UnixStream::pair().unwrap();
    let (client, owner) = (
        FrameSocket::new(client).unwrap(),
        FrameSocket::new(owner).unwrap(),
    );
    let region = ClientRegion::make().unwrap();
    let memory = region.descriptor().try_clone_to_owned().unwrap();
    let file = OpenFile::from(tempfile::tempfile().unwrap());
    Hello::new(PROTOCOL_VERSION, file)
        .with_region(memory)
        .send(&client)
        .unwrap();

    let hello = Hello::receive(&owner).unwrap();
    let memory = hello.into_region().expect("the region came");
    let flags = rustix::io::fcntl_getfd(&memory).unwrap();
    assert!(flags.contains(rustix::io::FdFlags::CLOEXEC), "{flags:?}");
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_tells_a_client_of_another_version_its_own() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let _owner = shared(&inner, &dir).build().unwrap();
        let cache_file = (dir.file_system)
            .open_existing(&dir.path().join("cache"), true)
            .unwrap();
        let address = OwnerAddress::of(
            cache_file.identity().unwrap(),
            #[cfg(feature = "shuttle")]
            &dir.transport,
        )
        .unwrap();
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_cache_whose_owner_speaks_another_version_is_busy_at_once() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let cache_file = (dir.file_system)
            .open_cache_file(&dir.path().join("cache"), false)
            .unwrap();
        cache_file.try_lock().unwrap();
        let address = OwnerAddress::of(
            cache_file.identity().unwrap(),
            #[cfg(feature = "shuttle")]
            &dir.transport,
        )
        .unwrap();
        let listener = address.listen().unwrap();
        let other_owner = crate::io::sync::thread::spawn(move || {
            let (connection, _) = listener.accept().unwrap();
            let socket = FrameSocket::new(connection).unwrap();
            Hello::receive(&socket).unwrap();
            let (version, region) = (PROTOCOL_VERSION + 1, false);
            socket
                .send(&OwnerFrame::Welcome { version, region }.encode())
                .unwrap();
        });

        let refused = shared(&inner, &dir).build().unwrap_err();
        assert!(matches!(refused, crate::Error::Busy), "{refused}");
        other_owner.join().unwrap();
        address.remove();
    });
}

/// A region that could shrink (a file) is welcomed without it; a hello that claims a region
/// and sends none is closed.
#[cfg(not(feature = "shuttle"))]
#[test]
fn the_owner_maps_a_clients_region_that_cannot_shrink() {
    let dir = TestDirectory::new();
    let inner = Arc::new(InMemory::new());
    let _owner = shared(&inner, &dir).build().unwrap();
    let cache_path = dir.path().join("cache");
    let writable = || dir.file_system.open_existing(&cache_path, true).unwrap();
    let address = OwnerAddress::of(
        writable().identity().unwrap(),
        #[cfg(feature = "shuttle")]
        &dir.transport,
    )
    .unwrap();
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
    fn admitted(dir: &TestDirectory) -> Self {
        Self::admitted_with(dir, None)
    }

    fn admitted_with_region(dir: &TestDirectory) -> (Self, std::sync::Arc<ClientRegion>) {
        let region = ClientRegion::make().unwrap();
        (Self::admitted_with(dir, Some(&region)), region)
    }

    fn admitted_with(dir: &TestDirectory, region: Option<&std::sync::Arc<ClientRegion>>) -> Self {
        let cache_file = (dir.file_system)
            .open_existing(&dir.path().join("cache"), true)
            .unwrap();
        let address = OwnerAddress::of(
            cache_file.identity().unwrap(),
            #[cfg(feature = "shuttle")]
            &dir.transport,
        )
        .unwrap();
        let socket = FrameSocket::new(address.connect(geteuid()).unwrap()).unwrap();
        let mut hello = Hello::new(PROTOCOL_VERSION, cache_file);
        if let Some(region) = region {
            #[cfg(not(feature = "shuttle"))]
            let descriptor = region.descriptor().try_clone_to_owned().unwrap();
            #[cfg(feature = "shuttle")]
            let descriptor = region.descriptor();
            hello = hello.with_region(descriptor);
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

    /// A round trip fences earlier frames; an unexpected end fails immediately.
    fn quiet(&mut self) -> bool {
        self.send(&ClientFrame::KnownSize {
            call: u64::MAX,
            path: Bytes::new(),
        });
        matches!(self.answer(), OwnerFrame::KnownSize { call: u64::MAX, .. })
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

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_serves_a_read_into_its_clients_span() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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

        assert!(client.quiet(), "no end before the client's");
        client.send(&ClientFrame::EndRead { read: 1 });
        assert_eq!(client.answer(), OwnerFrame::Ended { read: 1 });

        let short = region.span(1).unwrap();
        client.send(&start_read_into(2, &meta, 0..2 * MIB, short.name()));
        assert!(client.closed());
    });
}

/// A client may name a version of any size: planned, a read of all of it would take the owner's
/// memory, for bytes that come back in no frame.
#[cfg(feature = "shuttle")]
#[test]
fn a_read_longer_than_a_client_asks_fails_at_once() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let (_data, meta) = put_file(&inner, &Path::from("a"), MIB);
        let _owner = shared(&inner, &dir).build().unwrap();
        let mut client = RawClient::admitted(&dir);

        let size = MOST_READ_BYTES + 1;
        let ClientFrame::StartRead {
            call,
            read,
            path,
            e_tag,
            ..
        } = start_read(1, &meta, 0..size)
        else {
            unreachable!("a start");
        };
        client.send(&ClientFrame::StartRead {
            call,
            read,
            path,
            e_tag,
            size,
            range: 0..size,
            intent: None,
            output: None,
        });
        assert_eq!(client.answer(), OwnerFrame::Failed { call });
        assert!(client.quiet(), "the client is still served");
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn an_output_span_off_a_page_boundary_closes_the_connection() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

/// Landed from its output's own bytes, a fetch would copy them onto themselves: undefined, and
/// a panic where debug assertions check it.
#[cfg(feature = "shuttle")]
#[test]
fn a_fetch_staged_in_its_reads_output_closes_the_connection() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let path = Path::from("a");
        let (data, meta) = put_file(&inner, &path, MIB);
        let owner = shared(&inner, &dir).build().unwrap();
        let (mut client, region) = RawClient::admitted_with_region(&dir);

        let mut output = region.span(ReadOutput::padded_len(&(0..MIB))).unwrap();
        client.send(&start_read_into(1, &meta, 0..MIB, output.name()));
        assert!(matches!(client.answer(), OwnerFrame::Started { .. }));
        let plan = ReadPlan::new(vec![PlannedGet::once(0..1)]);
        client.send(&ClientFrame::TakeGets {
            call: 2,
            read: 1,
            plan,
        });
        assert!(matches!(client.answer(), OwnerFrame::Next { .. }));
        output.copy_in(0, &data);
        let staged = SpanName::new(output.name().offset(), MIB);
        client.send(&ClientFrame::LandStaged {
            read: 1,
            fetch: 0,
            staged,
        });
        assert!(client.closed());
        assert_eq!(read_listed(&owner, &meta, 0..MIB).unwrap(), data);
    });
}

fn read_listed(
    store: &T4Caching,
    meta: &ObjectMeta,
    range: Range<u64>,
) -> object_store::Result<Bytes> {
    let result = block_on(store.open_listed(meta.clone()).read(range, None));
    if let Some(cache) = store.cache_store().owned() {
        cache.wait_for_disk_writes();
    }
    result
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_serves_and_caches_a_clients_cold_read() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_lands_a_fetch_staged_in_its_clients_region() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

fn socket_bytes(cache: &T4Caching) -> (u64, u64) {
    cache
        .cache_store()
        .owner_connection()
        .expect("a client")
        .socket_bytes()
}

#[cfg(feature = "shuttle")]
#[test]
fn a_clients_cold_read_moves_only_control_over_its_socket() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
        wait_until(|| region.free_pages() == ClientRegion::BYTES / crate::PAGE_SIZE_U64);
        let held = region.span(ClientRegion::BYTES).unwrap();
        let (sent, taken) = socket_bytes(&client);
        assert_eq!(read_listed(&client, &meta, 0..2 * MIB).unwrap(), data);
        let (now_sent, now_taken) = socket_bytes(&client);
        assert!(now_sent - sent > 2 * MIB, "the GET's bytes in frames");
        assert!(now_taken - taken > 2 * MIB, "the read's bytes in a frame");
        drop(held);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_serves_a_clients_hits_and_tells_it_of_its_disk_reads() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_answers_a_hot_read_started_and_finished_at_once() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let (data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
        let owner = shared(&inner, &dir).build().unwrap();
        assert_eq!(read_listed(&owner, &meta, 0..2 * MIB).unwrap(), data);
        let started_with_nothing_to_fetch = |answer: OwnerFrame, call| match answer {
            OwnerFrame::Started { call: of, next, .. } => {
                of == call && next == NextStep::Fetch(vec![])
            }
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn the_owner_holds_a_finish_sent_while_it_plans() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_second_finish_in_a_round_closes_the_connection() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let (_data, meta) = put_file(&inner, &Path::from("a"), 2 * MIB);
        let _owner = shared(&inner, &dir).build().unwrap();
        let mut client = RawClient::admitted(&dir);

        client.send(&start_read(1, &meta, 0..2 * MIB));
        client.send(&ClientFrame::Finish { call: 1, read: 1 });
        client.send(&ClientFrame::Finish { call: 2, read: 1 });
        assert!(client.closed());
    });
}

/// A cache not built for sharing holds the files, so a stand-in owner answers by hand.
#[cfg(feature = "shuttle")]
#[test]
fn a_clients_read_sends_its_finish_with_its_start() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let (data, meta) = put_file(&inner, &Path::from("a"), MIB);
        let _holder = T4Caching::builder(Arc::clone(&inner) as Arc<dyn ObjectStore>)
            .with_memory_capacity(4 * MIB)
            .with_disk_file(dir.path().join("cache"), 8 * MIB)
            .with_test_file_system(dir.file_system.clone())
            .build()
            .unwrap();
        let first_file = (dir.file_system)
            .open_existing(&dir.path().join("cache"), false)
            .and_then(|file| file.identity())
            .unwrap();
        let address = OwnerAddress::of(
            first_file,
            #[cfg(feature = "shuttle")]
            &dir.transport,
        )
        .unwrap();
        let listener = address.listen().unwrap();
        let served = data.clone();
        let stand_in = crate::io::sync::thread::spawn(move || {
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
            // The finish must follow the start before any answer. Shuttle
            // reports a deadlock if the client waits for an answer first.
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
            let Some(ClientFrame::Finish { call, .. }) =
                before_any_answer.clone().or_else(&mut frame)
            else {
                panic!("the read's finish");
            };
            let bytes = Some(served);
            socket
                .send(&OwnerFrame::Served { call, bytes }.encode())
                .unwrap();
            (read, before_any_answer)
        });
        let client = shared(&inner, &dir).build().unwrap();
        let bytes = read_listed(&client, &meta, 0..MIB).unwrap();
        assert_eq!(bytes, data, "served by the stand-in");
        let (read, before_any_answer) = stand_in.join().unwrap();
        address.remove();
        assert!(
            matches!(before_any_answer, Some(ClientFrame::Finish { read: of, .. }) if of == read),
            "the finish went before any answer: {before_any_answer:?}"
        );
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_client_gone_mid_read_hands_its_claims_to_their_waiters() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
        crate::io::sync::thread::spawn(move || {
            let _ = done.send(read_listed(&waiting, &meta, 0..MIB));
        });
        wait_until(|| owner.cache_store().owned().unwrap().waiting_reads() == 1);
        assert!(finished.try_recv().is_err());

        drop(client);
        let read = finished.recv();
        assert_eq!(read.expect("the waiter fetches").unwrap(), data);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_clients_misuse_closes_its_connection() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
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
    });
}

/// Where the test's caches keep their files: under shuttle, on a simulated disk they share.
struct TestDirectory {
    #[cfg(not(feature = "shuttle"))]
    directory: tempfile::TempDir,
    file_system: FileSystem,
    #[cfg(feature = "shuttle")]
    transport: super::protocol::Transport,
}
impl TestDirectory {
    fn new() -> Self {
        Self {
            #[cfg(not(feature = "shuttle"))]
            directory: tempfile::tempdir().unwrap(),
            file_system: FileSystem::default(),
            #[cfg(feature = "shuttle")]
            transport: Default::default(),
        }
    }
    fn path(&self) -> &std::path::Path {
        #[cfg(not(feature = "shuttle"))]
        let path = self.directory.path();
        #[cfg(feature = "shuttle")]
        let path = std::path::Path::new("/simulated");
        path
    }
}

#[cfg(feature = "shuttle")]
#[test]
fn an_absent_owner_exhausts_the_builders_wait_on_the_manual_clock() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let dir = TestDirectory::new();
        let inner = Arc::new(InMemory::new());
        let _holder = T4Caching::builder(inner.clone())
            .with_memory_capacity(MIB)
            .with_disk_file(dir.path().join("cache"), MIB)
            .with_test_file_system(dir.file_system.clone())
            .build()
            .unwrap();
        let clock = dir.transport.clock();
        let started = clock.now();
        assert!(matches!(
            shared(&inner, &dir).build(),
            Err(crate::Error::Busy)
        ));
        assert_eq!(clock.now() - started, T4CachingBuilder::OWNER_WAIT);
    });
}

#[cfg(feature = "shuttle")]
#[test]
fn a_silent_hello_expires_only_when_the_test_advances_time() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let transport = super::protocol::Transport::default();
        let file = FileIdentity::new(1, 1);
        let address = OwnerAddress::of(file, &transport).unwrap();
        let listener = address.listen().unwrap();
        let client = address.connect(geteuid()).unwrap();
        let (socket, _) = listener.accept().unwrap();
        socket.set_read_timeout(Some(Hello::WAIT)).unwrap();
        let (done, result) = mpsc::channel();
        let serving = crate::io::sync::thread::spawn(move || {
            done.send(Hello::receive(&socket).is_err()).unwrap();
        });
        wait_until(|| transport.clock().timers_waiting() == 1);
        assert!(result.try_recv().is_err());
        transport
            .clock()
            .advance(Hello::WAIT - Duration::from_nanos(1));
        assert!(result.try_recv().is_err());
        transport.clock().advance(Duration::from_nanos(1));
        assert!(result.recv().unwrap());
        serving.join().unwrap();
        drop(client);
        address.remove();
    });
}
