use std::fs::File;
use std::io::Write;
use std::num::NonZeroU8;
use std::ops::Range;
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use pollster::block_on;
use tempfile::TempDir;

use super::address::{FileIdentity, OwnerAddress};
use super::frame_socket::FrameSocket;
use super::frames::{ClientFrame, FetchSpec, NextStep, OwnerFrame, PROTOCOL_VERSION};
use super::handshake::Hello;
use super::region::{ClientRegion, SpanName};
use crate::cache::object_cache::LocatedRun;
use crate::cache::policy::{
    Admit, BlockSource, FixedPlacementPolicy, IoPhase, PlannedGet, ReadPlan,
};
use crate::cache::read_output::ReadOutput;
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

/// Whether the owner of `store`, a client, has gone.
fn owner_gone(store: &T4Caching) -> bool {
    match &*store.link() {
        StorageLink::Client(connection) => connection.owner_gone(),
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
        ClientFrame::Hello {
            version: 7,
            region: false,
        },
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
    assert!(matches!(*owner.link(), StorageLink::Owner { .. }));
    let client = shared(&inner, &dir).build().unwrap();
    assert!(!owner_gone(&client));
    match &*client.link() {
        StorageLink::Client(connection) => assert!(connection.has_region()),
        StorageLink::Owner { .. } => panic!("a client"),
    }
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
    assert!(eventually(|| owner_gone(&client)));
    let kept = Arc::clone(&dir);
    let (next_owner, next_client) = within_5s(move || {
        let next_owner = shared(&inner, &dir).build().unwrap();
        let next_client = shared(&inner, &dir).build().unwrap();
        (next_owner, next_client)
    })
    .expect("the next owner and its client are built");
    assert!(matches!(*next_owner.link(), StorageLink::Owner { .. }));
    assert!(!owner_gone(&next_client));
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
    let region = false;
    assert_eq!(welcome, Some(OwnerFrame::Welcome { version, region }));
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
    let bare = ClientFrame::Hello { version, region }.encode();
    let bare = answer_to(&address, |socket| socket.send(&bare).unwrap());
    assert_eq!(bare, None);
    let garbage = answer_to(&address, |socket| socket.send(&[0xff; 18]).unwrap());
    assert_eq!(garbage, None);
}

/// The owner maps a client's region that can't shrink, and welcomes the
/// client with it; it welcomes a client whose region could shrink without
/// it, its reads' bytes in the frames. A hello that says it sent a region
/// and sent none closes the connection.
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
    let address = OwnerAddress::of(FileIdentity::of(&writable()).unwrap());
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

/// A client of an owner's that speaks frames itself, as a test drives it:
/// admitted, then each frame sent and each answer taken by hand.
struct RawClient {
    socket: FrameSocket,
    /// The news of disk reads taken so far, between answers.
    news: Vec<OwnerFrame>,
}

impl RawClient {
    /// A client of the owner of the cache file in `dir`, admitted.
    fn admitted(dir: &TempDir) -> Self {
        Self::admitted_with(dir, None)
    }

    /// A client of the owner of the cache file in `dir`, admitted with a
    /// region of its own, which the owner mapped.
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
        let address = OwnerAddress::of(FileIdentity::of(&cache_file).unwrap());
        let socket = FrameSocket::new(address.connect().unwrap()).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut hello = Hello::new(PROTOCOL_VERSION, cache_file);
        if let Some(region) = region {
            hello = hello.with_region(region.descriptor().try_clone_to_owned().unwrap());
        }
        hello.send(&socket).unwrap();
        let mut client = Self {
            socket,
            news: Vec::new(),
        };
        let version = PROTOCOL_VERSION;
        let region = region.is_some();
        assert_eq!(client.answer(), OwnerFrame::Welcome { version, region });
        client
    }

    /// Whether the owner sends nothing for 100 ms.
    fn quiet(&self) -> bool {
        let short = Some(Duration::from_millis(100));
        self.socket.set_read_timeout(short).unwrap();
        let quiet = match self.socket.receive(FrameSocket::MOST_BYTES) {
            Err(error) => matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            Ok(_) => false,
        };
        let usual = Some(Duration::from_secs(5));
        self.socket.set_read_timeout(usual).unwrap();
        quiet
    }

    fn send(&self, frame: &ClientFrame) {
        self.socket.send(&frame.encode()).unwrap();
    }

    /// The owner's next answer, the news before it kept.
    fn answer(&mut self) -> OwnerFrame {
        loop {
            let body = self.socket.receive(FrameSocket::MOST_BYTES).unwrap();
            let frame = OwnerFrame::decode(body.expect("an answer, not a close")).unwrap();
            if !matches!(frame, OwnerFrame::DiskRead { .. }) {
                return frame;
            }
            self.news.push(frame);
        }
    }

    /// Whether the owner closes the connection within 5 s, whatever it
    /// sends before.
    fn closed(&self) -> bool {
        loop {
            match self.socket.receive(FrameSocket::MOST_BYTES) {
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

/// A file of `size` bytes at `path` in `inner`, each byte a function of
/// its offset: its bytes, and what the store says of it.
fn put_file(inner: &InMemory, path: &Path, size: u64) -> (Bytes, ObjectMeta) {
    let data = Bytes::from((0..size).map(|at| (at % 251) as u8).collect::<Vec<_>>());
    block_on(inner.put(path, data.clone().into())).unwrap();
    (data, block_on(inner.head(path)).unwrap())
}

/// A client's start of a read, numbered `read`, of `range` of the file
/// `meta` names; its call numbered `read` times 100.
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

/// The same start of a read, its output the span `output`.
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

/// A client's read served into the span it named for its output: the
/// bytes in the span, where an output of the read has them, and none in
/// the frame. The owner ends the read, and says so, only once the client
/// ends it. A span too short for its read breaks the protocol.
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

    assert!(client.quiet(), "no end before the client's");
    client.send(&ClientFrame::EndRead { read: 1 });
    assert_eq!(client.answer(), OwnerFrame::Ended { read: 1 });

    let short = region.span(1).unwrap();
    client.send(&start_read_into(2, &meta, 0..2 * MIB, short.name()));
    assert!(client.closed());
}

/// An output span off a page boundary breaks the protocol: the owner's
/// reads from disk land in an output a page at a time, past the page
/// cache, from page-aligned memory only.
#[test]
fn an_output_span_off_a_page_boundary_closes_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemory::new());
    let (_data, meta) = put_file(&inner, &Path::from("a"), MIB);
    let _owner = shared(&inner, &dir).build().unwrap();
    let (client, region) = RawClient::admitted_with_region(&dir);

    let page = crate::PAGE_SIZE_U64;
    let span = region
        .span(ReadOutput::padded_len(&(0..MIB)) + page)
        .unwrap();
    let whole = span.name();
    let off_a_page = SpanName::new(whole.offset() + 512, whole.len() - 512);
    client.send(&start_read_into(1, &meta, 0..MIB, off_a_page));
    assert!(client.closed());
}

/// `range` of the file `meta` names, read through `store`'s cache, opened
/// as listed: no HEAD.
fn read_listed(
    store: &T4Caching,
    meta: &ObjectMeta,
    range: Range<u64>,
) -> object_store::Result<Bytes> {
    block_on(store.open_listed(meta.clone()).read(range, None))
}

/// A client's cold read, call by call: the owner locates its run, takes
/// the client's GETs, lands their bytes as they come, caches them and
/// serves the read. The owner holds the blocks then: its own read of them,
/// the file gone from the store, is a hit.
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

/// A client's fetch staged in its region: each `LandStaged` names the next
/// bytes in the span, which the owner lands from its mapping as its own
/// fetches' bytes, and caches. A span past the region breaks the protocol.
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

/// The bytes `cache`'s connection to its owner has sent and taken so far,
/// `cache` a client.
fn socket_bytes(cache: &T4Caching) -> (u64, u64) {
    match &*cache.link() {
        StorageLink::Client(connection) => connection.socket_bytes(),
        StorageLink::Owner { .. } => panic!("a client"),
    }
}

/// A client's cold read moves only control over its socket: its GET's
/// bytes are staged in its region, and the read's bytes served in it; the
/// owner caches the blocks. With no span free, a read's bytes, fetched
/// and served, go in the frames.
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
    let link = client.link();
    let StorageLink::Client(connection) = &*link else {
        panic!("a client");
    };
    let region = connection.region().unwrap();
    let held: Vec<_> = std::iter::from_fn(|| region.span(1)).collect();
    let (sent, taken) = socket_bytes(&client);
    assert_eq!(read_listed(&client, &meta, 0..2 * MIB).unwrap(), data);
    let (now_sent, now_taken) = socket_bytes(&client);
    assert!(now_sent - sent > 2 * MIB, "the GET's bytes in frames");
    assert!(now_taken - taken > 2 * MIB, "the read's bytes in a frame");
    drop(held);
}

/// A client's read of blocks the owner holds is served with nothing to
/// fetch; and a read of blocks on disk tells the client of each read from
/// disk made for it, for its read policy.
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

    // Each block's pages read from drive 0: started, then finished.
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

/// A client gone mid-read, holding the claims of its GETs, gives them up:
/// a read of the owner's that waited for them fetches the blocks itself.
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
    let waited = finished.recv_timeout(Duration::from_millis(200));
    assert!(
        waited.is_err(),
        "the owner's read waits for the client's claim"
    );
    drop(client);
    let read = finished.recv_timeout(Duration::from_secs(5));
    assert_eq!(read.expect("the waiter fetches").unwrap(), data);
}

/// A frame the protocol doesn't allow where it comes closes the client's
/// connection, and nothing else: the owner serves its next client.
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
    // Each misuse: the frames that set it up, each answered, then the one
    // that breaks the protocol.
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
            vec![ClientFrame::EndRead { read: 1 }],
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
