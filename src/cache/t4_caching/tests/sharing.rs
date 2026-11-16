//! Owner and client in one process: the lock conflicts within a process too.

use std::sync::mpsc;
use std::time::Duration;

use super::*;
use crate::cache::policy::FixedPlacementPolicy;

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

fn listed(store: &CountingStore, path: &Path) -> ObjectMeta {
    let meta = block_on(store.head(path)).unwrap();
    store.take_asked();
    meta
}

fn read_listed(cache: &T4Caching, meta: &ObjectMeta, range: Range<u64>) -> Bytes {
    block_on(cache.open_listed(meta.clone()).read(range, None)).unwrap()
}

#[test]
fn a_clients_cold_read_is_cached_for_its_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 1);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(
        owners_meta.e_tag, clients_meta.e_tag,
        "one file, one version"
    );
    assert!(client.cache_store().owner_connection().is_some());

    let read = read_listed(&client, &clients_meta, 10..2 * MIB);
    assert_eq!(read, data.slice(10..));
    assert_eq!(clients.take_asked(), [bounded(0..2 * MIB)]);
    assert_eq!(read_listed(&owner, &owners_meta, 0..2 * MIB), data);
    assert_eq!(
        owners.take_asked(),
        Vec::<Asked>::new(),
        "the owner holds the client's fetch"
    );
}

#[test]
fn a_block_the_owner_holds_is_served_to_its_client() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 2);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(read_listed(&owner, &owners_meta, 0..2 * MIB), data);
    assert_eq!(owners.take_asked(), [bounded(0..2 * MIB)]);

    let read = read_listed(&client, &clients_meta, 4097..MIB + 3);
    assert_eq!(read, data.slice(4097..MIB as usize + 3));
    assert_eq!(read.as_ptr() as u64 % 4096, 4097 % 4096);
    assert_eq!(clients.take_asked(), Vec::<Asked>::new());
}

#[test]
fn reads_of_one_block_across_owner_and_client_fetch_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 3);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = clients.gate_next_get();
    let (client_done, client_read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = client_done.send(read_listed(&client, &clients_meta, 0..MIB));
    });
    wait_until(|| clients.gets_out_now() == 1, "the client's GET goes out");
    let waiting = owner.clone();

    let (owner_done, owner_read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = owner_done.send(read_listed(&waiting, &owners_meta, 0..MIB));
    });
    wait_until(
        || fetch_waiters_registered(&owner) == 1,
        "the owner's read waits for the client's GET",
    );
    drop(open);
    assert_eq!(client_read.recv().unwrap(), data);
    assert_eq!(owner_read.recv().unwrap(), data);
    assert_eq!(owners.take_asked(), [], "one GET, the client's");
}

#[test]
fn a_clients_read_policy_hears_its_gets_and_its_owners_disk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("on_disk");
    let data = file_bytes(MIB, 4);
    let on_disk = |inner: &Arc<CountingStore>| {
        builder(inner, &dir)
            .with_multiprocess_sharing()
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

#[test]
fn a_client_whose_owner_went_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 6);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, _, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    assert_eq!(read_listed(&owner, &owners_meta, 0..MIB), data);
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "served by its owner");

    drop(owner);
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [bounded(0..MIB)]);
}

fn read_on_thread(cache: T4Caching, meta: ObjectMeta, range: Range<u64>) -> mpsc::Receiver<Bytes> {
    let (done, read) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(read_listed(&cache, &meta, range));
    });
    read
}

/// Its GET's bytes had nowhere to go but the owner, so it reads again from its store.
#[test]
fn a_clients_read_whose_owner_goes_mid_get_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 7);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = clients.gate_next_get();
    let read = read_on_thread(client, clients_meta, 0..MIB);
    wait_until(|| clients.gets_out_now() == 1, "the client's GET goes out");

    drop(owner);
    drop(open);
    assert_eq!(read.recv().unwrap(), data);
    assert_eq!(
        clients.take_asked(),
        [bounded(0..MIB), bounded(0..MIB)],
        "its GET, then the read from its store"
    );
}

#[test]
fn a_clients_read_waiting_on_another_when_the_owner_goes_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 8);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
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
    wait_until(
        || fetch_waiters_registered(&owner) == 1,
        "the read waits for the other client's GET",
    );

    drop(owner);
    assert_eq!(waited.recv().unwrap(), data);
    assert_eq!(waitings.take_asked(), [bounded(0..MIB)]);
    drop(open);
    assert_eq!(fetched.recv().unwrap(), data);
    assert_eq!(fetchings.take_asked(), [bounded(0..MIB), bounded(0..MIB)]);
}

fn owns(cache: &T4Caching) -> bool {
    cache.cache_store().owned().is_some()
}

fn reads_through_an_owner(cache: &T4Caching) -> bool {
    (cache.cache_store().owner_connection()).is_some_and(|owner| !owner.owner_gone())
}

#[test]
fn a_client_whose_owner_went_comes_to_own_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 9);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
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
        Vec::<Asked>::new(),
        "served by the client that took over"
    );
}

#[test]
fn two_clients_whose_owner_went_one_owns_and_the_other_reads_through_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 10);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
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

/// The client's own cache names `c` with the id the owner gave `b`, so a merge would mix files.
#[test]
fn a_get_waiting_as_its_owner_goes_merges_with_none_of_the_next_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (b, c) = (Path::from("b"), Path::from("c"));
    let (b_data, c_data) = (file_bytes(3 * MIB, 13), file_bytes(3 * MIB, 14));
    // Two blocks' GETs out at a time: room for two waiting to go as one.
    let shared = |inner: &Arc<CountingStore>| {
        builder(inner, &dir)
            .with_network_rate(2 * MIB, Duration::from_secs(1))
            .with_multiprocess_sharing()
    };
    let (owner, _, _) = process(&b, &b_data, shared);
    let (client, clients, b_meta) = process(&b, &b_data, shared);
    put(clients.as_ref(), &c, &c_data);
    // Too long to cache: a read of it hands the cache over and names no version.
    let long = Path::from("l".repeat(1025));
    let long_data = file_bytes(10, 15);
    put(clients.as_ref(), &long, &long_data);
    let (c_meta, long_meta) = (listed(&clients, &c), listed(&clients, &long));

    let open = clients.gate_next_get();
    let first = read_on_thread(client.clone(), b_meta.clone(), 0..2 * MIB);
    wait_until(|| clients.gets_out_now() == 1, "b's first blocks go out");
    let third = read_on_thread(client.clone(), b_meta, 2 * MIB..3 * MIB);
    wait_until(
        || client.network.gets_waiting() == 1,
        "b's third block waits",
    );

    drop(owner);
    wait_until(
        || {
            assert_eq!(read_listed(&client, &long_meta, 0..10), long_data);
            owns(&client)
        },
        "a read makes the client the owner",
    );
    // `c`'s second block, just before `b`'s third.
    let second = read_on_thread(client.clone(), c_meta, MIB..2 * MIB);
    wait_until(
        || client.network.gets_waiting() == 2,
        "c's second block waits",
    );

    drop(open);
    assert_eq!(
        second.recv().unwrap(),
        c_data.slice(MIB as usize..2 * MIB as usize)
    );
    assert_eq!(third.recv().unwrap(), b_data.slice(2 * MIB as usize..));
    assert_eq!(first.recv().unwrap(), b_data.slice(..2 * MIB as usize));
}

#[test]
fn a_client_that_comes_to_own_the_cache_takes_its_owners_disk_tier() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(2 * MIB, 16);
    let on_disk = |inner: &Arc<CountingStore>| {
        builder(inner, &dir)
            .with_multiprocess_sharing()
            .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
    };
    let (owner, _, owners_meta) = process(&path, &data, on_disk);
    let (client, clients, clients_meta) = process(&path, &data, on_disk);
    // Too long to cache: a read of it hands the cache over and caches nothing.
    let long = Path::from("l".repeat(1025));
    let long_data = file_bytes(10, 17);
    put(clients.as_ref(), &long, &long_data);
    let long_meta = listed(&clients, &long);
    assert_eq!(read_listed(&owner, &owners_meta, 0..2 * MIB), data);

    drop(owner);
    wait_until(
        || {
            assert_eq!(read_listed(&client, &long_meta, 0..10), long_data);
            owns(&client)
        },
        "a read makes the client the owner",
    );
    clients.take_asked();
    assert_eq!(block_on(client.known_size(&path)), Some(2 * MIB));
    assert_eq!(read_listed(&client, &clients_meta, 0..2 * MIB), data);
    assert_eq!(
        clients.take_asked(),
        [],
        "the owner's disk tier, taken over"
    );
}

#[test]
fn a_client_asks_its_owner_the_size_it_knows() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB + 3, 18);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, _, owners_meta) = process(&path, &data, shared);
    let (client, clients, _) = process(&path, &data, shared);
    assert!(reads_through_an_owner(&client));
    assert_eq!(block_on(client.known_size(&path)), None);

    assert_eq!(
        read_listed(&owner, &owners_meta, 0..MIB),
        data.slice(..MIB as usize)
    );
    assert_eq!(block_on(client.known_size(&path)), Some(MIB + 3));
    assert_eq!(clients.take_asked(), [], "asked of the owner alone");
}

#[test]
fn a_client_shut_down_leaves_its_owner_be_and_takes_over_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 19);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    client.shut_down().unwrap();
    assert!(reads_through_an_owner(&client));

    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [bounded(0..MIB)]);
    assert_eq!(read_listed(&owner, &owners_meta, 0..MIB), data);
    assert_eq!(owners.take_asked(), [], "the owner caches still");

    drop(owner);
    wait_until(|| !reads_through_an_owner(&client), "the owner goes");
    // This first read tries a hand-off: none was tried before the owner went.
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert!(!owns(&client), "it took over nothing");
}

/// A cache not built for sharing takes the files once the owner goes.
#[test]
fn a_client_that_finds_no_owner_reads_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 12);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    drop(owner);
    // Another test's child process may hold the dropped owner's lock until it execs.
    let others = Arc::new(CountingStore::default());
    let mut _unshared = None;
    wait_until(
        || {
            _unshared = builder(&others, &dir).build().ok();
            _unshared.is_some()
        },
        "the dropped owner's files come free",
    );

    for _ in 0..3 {
        assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    }
    assert!(!owns(&client) && !reads_through_an_owner(&client));
    assert_eq!(clients.take_asked().len(), 3, "each read from its store");
}

fn region_free_pages(cache: &T4Caching) -> u64 {
    cache
        .cache_store()
        .owner_connection()
        .expect("a client")
        .region_free_pages()
        .expect("a region mapped")
}

fn in_region(cache: &T4Caching, bytes: &Bytes) -> bool {
    cache
        .cache_store()
        .owner_connection()
        .expect("a client")
        .region_holds(bytes)
}

#[test]
fn a_clients_reads_are_served_in_its_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(3 * MIB, 21);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
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

#[test]
fn a_clients_spans_come_back_once_its_owner_goes() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 22);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let all = region_free_pages(&client);
    let open = clients.gate_next_get();
    let read = read_on_thread(client.clone(), clients_meta, 0..MIB);
    wait_until(|| clients.gets_out_now() == 1, "the client's GET goes out");
    assert!(region_free_pages(&client) < all, "the read's span is lent");

    drop(owner);
    drop(open);
    assert_eq!(read.recv().unwrap(), data);
    wait_until(|| region_free_pages(&client) == all, "every span is free");
}

/// The owner ends a read only once its last fetch drops: abandoning a read the owner ended
/// breaks the protocol.
#[test]
fn a_fetch_dropped_after_its_read_gives_its_span_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 23);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (_owner, _, _) = process(&path, &data, shared);
    let (client, _, meta) = process(&path, &data, shared);
    let all = region_free_pages(&client);
    let cache_store = client.cache_store();
    let connection = cache_store.owner_connection().expect("a client");
    let e_tag = meta.e_tag.clone().unwrap();
    let reads = FetchRuns::default();
    let path = path.as_ref().as_bytes();
    let started = cache_store.start_read(path, &e_tag, meta.size, 0..MIB, None, &reads);
    let mut read = block_on(started).unwrap().expect("a cacheable file");
    while let Some(run) = read.locate_next_run().unwrap() {
        let gets = run.planned_by(&reads, &read.file(), None);
        block_on(read.take_gets(&gets)).unwrap();
    }
    let fetches = read.fetches().unwrap();
    assert!(!fetches.is_empty(), "the read fetches its block");

    drop(read);
    // The answer follows every frame sent before it on this connection.
    assert_eq!(block_on(cache_store.known_size(path)), Some(meta.size));
    assert!(
        region_free_pages(&client) < all,
        "the fetch still holds its spans"
    );
    drop(fetches);
    wait_until(|| region_free_pages(&client) == all, "every span is free");
    // The final round trip also processes the fetch's abandon and end.
    assert_eq!(block_on(cache_store.known_size(path)), Some(meta.size));
    assert!(!connection.owner_gone(), "the original connection kept");
}

fn calls_waiting(cache: &T4Caching) -> Vec<u64> {
    cache
        .cache_store()
        .owner_connection()
        .expect("a client")
        .calls_waiting()
}

#[test]
fn a_clients_read_whose_wait_ends_uncached_fetches_in_a_second_round() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 17);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = owners.gate_next_get();
    let (owned, owners_read) = mpsc::channel();
    let file = owner.open_listed(owners_meta.clone());
    std::thread::spawn(move || {
        let _ = owned.send(block_on(file.read(0..MIB, None)).is_err());
    });
    wait_until(|| owners.gets_out_now() == 1, "the owner's GET goes out");

    let clients_read = read_on_thread(client.clone(), clients_meta, 0..MIB);
    wait_until(
        || calls_waiting(&client) == [1],
        "the client's finish waits",
    );
    // The owner's GET finds the file gone, so its fetch caches nothing.
    block_on(owners.delete(&path)).unwrap();
    drop(open);
    assert_eq!(owners_read.recv(), Ok(true));
    let read = clients_read.recv();
    assert_eq!(read.unwrap(), data);
    assert_eq!(
        clients.take_asked(),
        [bounded(0..MIB)],
        "fetched once, itself"
    );
    assert!(reads_through_an_owner(&client));
    // The owner's store lost the file, so only the client's fetch could have cached this.
    assert_eq!(read_listed(&owner, &owners_meta, 0..MIB), data);
}

#[test]
fn a_clients_read_given_up_leaves_no_call_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 13);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (owner, owners, owners_meta) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    let open = owners.gate_next_get();
    let owned = read_on_thread(owner.clone(), owners_meta, 0..MIB);
    wait_until(|| owners.gets_out_now() == 1, "the owner's GET goes out");

    let file = client.open_listed(clients_meta.clone());
    let mut read = Box::pin(file.read(0..MIB, None));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // Its `Finish` is call 1, sent with its start; it waits on the owner's GET.
    wait_until(
        || {
            assert!(read.as_mut().poll(&mut context).is_pending());
            calls_waiting(&client) == [1]
        },
        "the read's Finish waits",
    );
    drop(read);
    assert_eq!(
        calls_waiting(&client),
        Vec::<u64>::new(),
        "the call given up with its read"
    );

    drop(open);
    assert_eq!(owned.recv().unwrap(), data);
    assert!(reads_through_an_owner(&client));
    assert_eq!(read_listed(&client, &clients_meta, 0..MIB), data);
    assert_eq!(clients.take_asked(), [], "served by the owner");
}

#[test]
fn a_clients_read_given_up_mid_get_abandons_its_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 14);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
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
    assert_eq!(calls_waiting(&client), Vec::<u64>::new());

    let owned = read_on_thread(owner.clone(), owners_meta, 0..MIB);
    assert_eq!(owned.recv().unwrap(), data);
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

#[test]
fn a_clients_failed_get_abandons_its_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("a");
    let data = file_bytes(MIB, 15);
    let shared = |inner: &Arc<CountingStore>| builder(inner, &dir).with_multiprocess_sharing();
    let (_owner, _, _) = process(&path, &data, shared);
    let (client, clients, clients_meta) = process(&path, &data, shared);
    put(clients.as_ref(), &path, &file_bytes(MIB, 16));

    let (done, read) = mpsc::channel();
    std::thread::spawn(move || {
        let file = client.open_listed(clients_meta);
        let _ = done.send(block_on(file.read(0..MIB, None)));
    });
    let read = read.recv().unwrap();
    assert!(
        matches!(read, Err(object_store::Error::Precondition { .. })),
        "{read:?}"
    );
}
