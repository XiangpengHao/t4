//! A cache shared by an owner and its client (`doc/11-shared-mode.md`),
//! both in this process, as the lock conflicts within a process too: each
//! over a store of its own, which holds the same file under the same e_tag,
//! as two processes' stores reach one bucket.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::*;
use crate::cache::policy::FixedPlacementPolicy;

/// A process's cache, built by `build` over a store of its own that holds
/// `data` at `path`; the store, and what it says of the file, asked of it
/// directly.
fn process(
    path: &Path,
    data: &Bytes,
    build: impl FnOnce(&Arc<CountingStore>) -> T4CachingBuilder,
) -> (T4Caching, Arc<CountingStore>, ObjectMeta) {
    let inner = Arc::new(CountingStore::default());
    put(inner.as_ref(), path, data);
    let cache = build(&inner).build().unwrap();
    let meta = listed(&inner, path);
    (cache, inner, meta)
}

/// What `store` says of the file at `path`, asked of it directly.
fn listed(store: &CountingStore, path: &Path) -> ObjectMeta {
    let meta = block_on(store.head(path)).unwrap();
    store.take_asked();
    meta
}

/// `range` of the file `meta` names, read through `cache`, opened as
/// listed: no HEAD.
fn read_listed(cache: &T4Caching, meta: &ObjectMeta, range: Range<u64>) -> Bytes {
    block_on(cache.open_listed(meta.clone()).read(range, None)).unwrap()
}

/// A client's cold read fetches through its own store, and the owner caches
/// what it fetched: the owner's read of it asks its store nothing.
#[test]
fn a_clients_cold_read_is_cached_for_its_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 1);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(
        owners_meta.e_tag, clients_meta.e_tag,
        "one file, one version"
    );
    assert!(matches!(*client.link(), StorageLink::Client(_)));

    let read = read_listed(&client, &clients_meta, 10..2 * MIB);
    assert_eq!(read, data.slice(10..));
    assert_eq!(clients.take_asked(), [bounded(0..2 * MIB)]);
    assert_eq!(read_listed(&owner, &owners_meta, 0..2 * MIB), data);
    assert_eq!(
        owners.take_asked(),
        [],
        "the owner holds the client's fetch"
    );
}

/// A block the owner holds is served to its client, whose store is asked
/// nothing; the bytes keep the alignment an owner's read has, their
/// address agreeing with their file offset mod 4 KiB.
#[test]
fn a_block_the_owner_holds_is_served_to_its_client() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 2);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(read_listed(&owner, &owners_meta, 0..2 * MIB), data);
    assert_eq!(owners.take_asked(), [bounded(0..2 * MIB)]);

    let read = read_listed(&client, &clients_meta, 4097..MIB + 3);
    assert_eq!(read, data.slice(4097..MIB as usize + 3));
    assert_eq!(read.as_ptr() as u64 % 4096, 4097 % 4096);
    assert_eq!(clients.take_asked(), []);
}

/// Reads of one block in the owner and its client, at once, fetch it once:
/// the owner's waits for the client's GET, and takes the block from where
/// it landed.
#[test]
fn reads_of_one_block_across_owner_and_client_fetch_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 3);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = clients.gate_next_get();
    let (client_done, client_read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = client_done.send(read_listed(&client, &clients_meta, 0..MIB));
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while clients.gets_out_now() == 0 {
        assert!(Instant::now() < deadline, "the client's GET goes out");
        std::thread::sleep(Duration::from_millis(1));
    }

    let (owner_done, owner_read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = owner_done.send(read_listed(&owner, &owners_meta, 0..MIB));
    });
    let early = owner_read.recv_timeout(Duration::from_millis(200));
    assert!(
        early.is_err(),
        "the owner's read waits for the client's GET"
    );
    drop(open);
    let timeout = Duration::from_secs(5);
    assert_eq!(client_read.recv_timeout(timeout).unwrap(), data);
    assert_eq!(owner_read.recv_timeout(timeout).unwrap(), data);
    assert_eq!(owners.take_asked(), [], "one GET, the client's");
}

/// A client's read policy hears its own GETs, and each read from disk its
/// owner makes for it.
#[test]
fn a_clients_read_policy_hears_its_gets_and_its_owners_disk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("on_disk");
    let data = file_bytes(MIB, 4);
    let on_disk = |inner: &Arc<CountingStore>| {
        builder(inner, &dir)
            .with_sharing()
            .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
    };
    let (owner, _, owners_meta) = process(&path, &data, on_disk);
    let reads = ScriptedReads::new(|read| FetchRuns::default().plan(read));
    let (client, clients, clients_meta) = process(&path, &data, |inner| {
        on_disk(inner).with_read_policy(reads.clone())
    });
    assert_eq!(read_listed(&owner, &owners_meta, 0..MIB), data);

    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    let disk_read = |phase| Heard::DiskRead {
        drive: 0,
        bytes: MIB as u32,
        phase,
    };
    assert_eq!(reads.heard(), [disk_read("started"), disk_read("finished")]);

    let fetched = Path::from("fetched");
    let other = file_bytes(MIB, 5);
    put(clients.as_ref(), &fetched, &other);
    let meta = listed(&clients, &fetched);
    reads.heard.lock().unwrap().clear();
    assert_eq!(read_listed(&client, &meta, 0..MIB), other);
    let get = |phase| Heard::Get {
        bytes: 0..MIB,
        racer: 0,
        phase,
    };
    let phases = ["started", "answered", "finished", "landed"].map(get);
    assert_eq!(reads.heard(), phases);
}

/// A client whose owner went reads from its store, the right bytes: the
/// cache may lose anything, and its callers see none of it.
#[test]
fn a_client_whose_owner_went_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 6);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(read_listed(&owner, &owners_meta, 0..MIB), data);
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "served by its owner");

    drop(owner);
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [bounded(0..MIB)]);
}

/// `range` of the file `meta` names, read through `cache` on a thread of
/// its own: its bytes, once read, come through the receiver returned.
fn read_on_thread(cache: T4Caching, meta: ObjectMeta, range: Range<u64>) -> mpsc::Receiver<Bytes> {
    let (done, read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(read_listed(&cache, &meta, range));
    });
    read
}

/// The owner gone while its client's GET is out: the client's read still
/// returns the right bytes, from its store, as the bytes its GET brought
/// had nowhere to go but the owner.
#[test]
fn a_clients_read_whose_owner_goes_mid_get_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 7);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = clients.gate_next_get();
    let read = read_on_thread(client, clients_meta, 0..MIB);
    wait_until(|| clients.gets_out_now() == 1, "the client's GET goes out");

    drop(owner);
    drop(open);
    assert_eq!(read.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    assert_eq!(
        clients.take_asked(),
        [bounded(0..MIB), bounded(0..MIB)],
        "its GET, then the read from its store"
    );
}

/// The owner gone while its client waits for a block another client is
/// fetching: the waiting read's call fails, and it reads from its store at
/// once; the fetching read, once its GET is in, from its own.
#[test]
fn a_clients_read_waiting_on_another_when_the_owner_goes_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 8);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (fetching, fetchings, fetchings_meta) = process(&path, &data, shared);
    let (waiting, waitings, waitings_meta) = process(&path, &data, shared);
    let open = fetchings.gate_next_get();
    let fetched = read_on_thread(fetching, fetchings_meta, 0..MIB);
    wait_until(
        || fetchings.gets_out_now() == 1,
        "the fetching GET goes out",
    );
    let waited = read_on_thread(waiting, waitings_meta, 0..MIB);
    let early = waited.recv_timeout(Duration::from_millis(200));
    assert!(early.is_err(), "the read waits for the other client's GET");

    drop(owner);
    assert_eq!(waited.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    assert_eq!(waitings.take_asked(), [bounded(0..MIB)]);
    drop(open);
    assert_eq!(fetched.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    assert_eq!(fetchings.take_asked(), [bounded(0..MIB), bounded(0..MIB)]);
}

/// Whether `cache` owns its files now.
fn owns(cache: &T4Caching) -> bool {
    matches!(*cache.link(), StorageLink::Owner { .. })
}

/// Whether `cache` reads through an owner that is still there.
fn reads_through_an_owner(cache: &T4Caching) -> bool {
    matches!(&*cache.link(), StorageLink::Client(owner) if !owner.owner_gone())
}

/// A client whose owner went comes to own the cache at a read: from then
/// on it caches what it reads, and a cache built for sharing on its files
/// is its client.
#[test]
fn a_client_whose_owner_went_comes_to_own_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 9);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);

    drop(owner);
    wait_until(
        || {
            assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
            owns(&client)
        },
        "a read makes the client the owner",
    );
    clients.take_asked();
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "the read that took over cached");

    let (next, nexts, nexts_meta) = process(&path, &data, shared);
    assert!(reads_through_an_owner(&next));
    assert_eq!(read_listed(&next, &nexts_meta, 0..MIB), data);
    assert_eq!(
        nexts.take_asked(),
        [],
        "served by the client that took over"
    );
}

/// Two clients whose owner went: one comes to own the cache, and the other
/// reads through it.
#[test]
fn two_clients_whose_owner_went_one_owns_and_the_other_reads_through_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 10);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (first, firsts, firsts_meta) = process(&path, &data, shared);
    let (second, seconds, seconds_meta) = process(&path, &data, shared);

    drop(owner);
    wait_until(
        || {
            assert_eq!(read_listed(&first, &firsts_meta, 0..MIB), data);
            assert_eq!(read_listed(&second, &seconds_meta, 0..MIB), data);
            let first_owns = owns(&first) && reads_through_an_owner(&second);
            first_owns || owns(&second) && reads_through_an_owner(&first)
        },
        "one owns the cache, and the other reads through it",
    );
    let (owning, owners) = if owns(&first) {
        (&first, &firsts)
    } else {
        (&second, &seconds)
    };
    let (reading, readers) = if owns(&first) {
        (&second, &seconds)
    } else {
        (&first, &firsts)
    };
    let fresh = Path::from("b");
    let other = file_bytes(MIB, 11);
    put(owners.as_ref(), &fresh, &other);
    put(readers.as_ref(), &fresh, &other);
    let (owners_meta, readers_meta) = (listed(owners, &fresh), listed(readers, &fresh));
    readers.take_asked();
    assert_eq!(read_listed(owning, &owners_meta, 0..MIB), other);
    assert_eq!(read_listed(reading, &readers_meta, 0..MIB), other);
    assert_eq!(readers.take_asked(), [], "served by the new owner");
}

/// A client whose files a cache not built for sharing took once its owner
/// went: it can neither own them nor reach an owner, and reads from its
/// store, no read waiting long for an owner.
#[test]
fn a_client_that_finds_no_owner_reads_from_its_store_without_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 12);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    drop(owner);
    // A child process another test starts meanwhile holds the dropped
    // owner's descriptors, and so its lock, until its own program starts:
    // the files come free within moments.
    let others = Arc::new(CountingStore::default());
    let mut _unshared = None;
    wait_until(
        || {
            _unshared = builder(&others, &dir).build().ok();
            _unshared.is_some()
        },
        "the dropped owner's files come free",
    );

    let started = Instant::now();
    for _ in 0..3 {
        assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    }
    assert!(started.elapsed() < Duration::from_secs(1), "no read waits");
    assert!(!owns(&client) && !reads_through_an_owner(&client));
    assert_eq!(clients.take_asked().len(), 3, "each read from its store");
}

/// The pages of `cache`'s region free, `cache` a client whose owner mapped
/// its region.
fn region_free_pages(cache: &T4Caching) -> u64 {
    match &*cache.link() {
        StorageLink::Client(owner) => owner.region_free_pages().expect("a region mapped"),
        StorageLink::Owner { .. } => panic!("a client"),
    }
}

/// Whether `bytes` lie in `cache`'s region, `cache` a client.
fn in_region(cache: &T4Caching, bytes: &Bytes) -> bool {
    match &*cache.link() {
        StorageLink::Client(owner) => owner.region_holds(bytes),
        StorageLink::Owner { .. } => panic!("a client"),
    }
}

/// A client's reads are served in its region: their bytes are in spans of
/// it, where an output has them (their address agrees with their file
/// offset mod 4 KiB), and right; a span is the caller's while it holds the
/// bytes, and every span is free once the reads are dropped.
#[test]
fn a_clients_reads_are_served_in_its_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(3 * MIB, 21);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (_owner, _, _) = process(&path, &data, shared);
    let (client, _, clients_meta) = process(&path, &data, shared);
    let all = region_free_pages(&client);

    let cold = 10..2 * MIB + 20;
    let hot = 4096 + 3..MIB;
    let reads: Vec<_> = [cold, hot]
        .into_iter()
        .map(|range| {
            let read = read_listed(&client, &clients_meta, range.clone());
            assert_eq!(read, data.slice(range.start as usize..range.end as usize));
            assert!(in_region(&client, &read), "{range:?} in a span");
            assert_eq!(read.as_ptr() as u64 % 4096, range.start % 4096);
            read
        })
        .collect();
    assert!(
        region_free_pages(&client) < all,
        "the reads hold their spans"
    );
    drop(reads);
    wait_until(|| region_free_pages(&client) == all, "every span is free");
}

/// A client's read in flight when its owner goes: the read goes to its
/// store, and its spans come back with the owner gone.
#[test]
fn a_clients_spans_come_back_once_its_owner_goes() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 22);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let all = region_free_pages(&client);
    let open = clients.gate_next_get();
    let read = read_on_thread(client.clone(), clients_meta, 0..MIB);
    wait_until(|| clients.gets_out_now() == 1, "the client's GET goes out");
    assert!(region_free_pages(&client) < all, "the read's span is lent");

    drop(owner);
    drop(open);
    assert_eq!(read.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    wait_until(|| region_free_pages(&client) == all, "every span is free");
}

/// The calls `cache`, a client, waits on its owner for, by number.
fn calls_waiting(cache: &T4Caching) -> Vec<u64> {
    match &*cache.link() {
        StorageLink::Client(owner) => owner.calls_waiting(),
        StorageLink::Owner { .. } => panic!("a client"),
    }
}

/// A client's read given up while it waits for its owner, its `Finish`
/// unanswered: the call leaves the client's table with the read, and the
/// connection serves the client's next read.
#[test]
fn a_clients_read_given_up_leaves_no_call_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 13);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = owners.gate_next_get();
    let owned = read_on_thread(owner.clone(), owners_meta, 0..MIB);
    wait_until(|| owners.gets_out_now() == 1, "the owner's GET goes out");

    let file = client.open_listed(clients_meta.clone());
    let mut read = Box::pin(file.read(0..MIB, None));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // Its start and its GETs answered, its `Finish` waits on the owner's
    // GET.
    wait_until(
        || {
            assert!(read.as_mut().poll(&mut context).is_pending());
            calls_waiting(&client) == [2]
        },
        "the read's Finish waits",
    );
    drop(read);
    assert_eq!(
        calls_waiting(&client),
        [],
        "the call given up with its read"
    );

    drop(open);
    assert_eq!(owned.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    assert!(reads_through_an_owner(&client));
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "served by the owner");
}

/// A client's read given up while its GET is out, part of its body landed:
/// its claim ends with it, so the owner's read of the block fetches it
/// itself rather than wait; no call is left waiting, and the connection
/// stays whole.
#[test]
fn a_clients_read_given_up_mid_get_abandons_its_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 14);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    clients.stall_next_body(MIB as usize / 2);

    let file = client.open_listed(clients_meta.clone());
    let mut read = Box::pin(file.read(0..MIB, None));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    wait_until(
        || {
            assert!(read.as_mut().poll(&mut context).is_pending());
            clients.stalled.load(Ordering::Relaxed)
        },
        "the client's GET stalls halfway",
    );
    drop(read);
    assert_eq!(calls_waiting(&client), []);

    let owned = read_on_thread(owner.clone(), owners_meta, 0..MIB);
    assert_eq!(owned.recv_timeout(Duration::from_secs(5)).unwrap(), data);
    assert_eq!(
        owners.take_asked(),
        [bounded(0..MIB)],
        "the owner's own GET"
    );
    assert!(reads_through_an_owner(&client));
    clients.take_asked();
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "served by the owner");
}

/// A client's GET that fails, the file changed since the client opened it:
/// its fetch is abandoned, so the owner's work for the read ends, and the
/// read ends, in bounded time, with the store's answer.
#[test]
fn a_clients_failed_get_abandons_its_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 15);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_sharing();
    let (_owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    put(clients.as_ref(), &path, &file_bytes(MIB, 16));

    let (done, read) = mpsc::channel();
    std::thread::spawn(move || {
        let file = client.open_listed(clients_meta);
        let _ = done.send(block_on(file.read(0..MIB, None)));
    });
    let read = read.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        matches!(read, Err(object_store::Error::Precondition { .. })),
        "{read:?}"
    );
}
