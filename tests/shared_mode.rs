//! Shared mode against real `t4-server` processes (`doc/02-multi-process.md`).

#![cfg(unix)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::task::{Context, Waker};
use std::thread;
use std::time::{Duration, Instant};

use pollster::block_on;
use t4::{Error, MountMode, MountOptions, ServerOptions, Store};

fn shared_options(idle_timeout: Duration) -> MountOptions {
    MountOptions {
        queue_depth: 8,
        direct_io: false,
        mode: MountMode::Shared(ServerOptions {
            idle_timeout,
            program: Some(PathBuf::from(env!("CARGO_BIN_EXE_t4-server"))),
        }),
    }
}

fn in_process_options() -> MountOptions {
    MountOptions {
        queue_depth: 8,
        direct_io: false,
        mode: MountMode::InProcess,
    }
}

fn mount_shared(path: &Path) -> t4::Result<Store> {
    block_on(t4::mount_with_options(
        path,
        shared_options(Duration::from_millis(200)),
    ))
}

/// Retries an in-process mount while a server still holds the store.
fn mount_in_process_once_free(path: &Path) -> Store {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match block_on(t4::mount_with_options(path, in_process_options())) {
            Ok(store) => return store,
            Err(Error::Busy) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => panic!("in-process mount failed: {err}"),
        }
    }
}

#[test]
fn clients_share_one_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.t4");
    let a = mount_shared(&path).unwrap();
    let b = mount_shared(&path).unwrap();
    assert_eq!(a.__server_pid(), b.__server_pid());

    block_on(a.put(b"k", vec![7; 5000])).unwrap();
    assert_eq!(block_on(b.get(b"k")).unwrap(), vec![7; 5000]);
    assert_eq!(block_on(b.get_range(b"k", 4090, 10)).unwrap(), vec![7; 10]);
    assert!(matches!(
        block_on(b.get_range(b"k", 4999, 2)),
        Err(Error::RangeOutOfBounds)
    ));
    assert_eq!(block_on(a.len()).unwrap(), 1);

    assert!(block_on(b.remove(b"k")).unwrap());
    assert!(matches!(block_on(a.get(b"k")), Err(Error::NotFound)));
    assert!(block_on(a.is_empty()).unwrap());
}

#[test]
fn values_of_every_size_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sizes.t4");
    let client = mount_shared(&path).unwrap();
    // Past a page, past a get's first buffer, and past what the connection's
    // shared region holds: a value that gets a memfd of its own.
    let sizes = [0, 1, 4095, 4096, 4097, 200 << 10, 20 << 20];
    let values: Vec<Vec<u8>> = sizes
        .iter()
        .map(|size| (0..*size).map(|i| (i % 251) as u8).collect())
        .collect();
    for (key, value) in values.iter().enumerate() {
        let key = [key as u8];
        block_on(client.put(key, value.clone())).unwrap();
        // Twice: the first get of a size may lend too small a buffer.
        for _ in 0..2 {
            assert_eq!(block_on(client.get(&key)).unwrap(), *value);
        }
        let (start, len) = (value.len() / 3, value.len() / 2);
        let range = block_on(client.get_range(&key, start as u64, len as u64)).unwrap();
        assert_eq!(range, value[start..start + len]);
    }

    // The server wrote the bytes to the file.
    drop(client);
    let owner = mount_in_process_once_free(&path);
    for (key, value) in values.iter().enumerate() {
        assert_eq!(block_on(owner.get(&[key as u8])).unwrap(), *value);
    }
}

#[test]
fn concurrent_calls_share_one_connection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.t4");
    let client = mount_shared(&path).unwrap();
    let threads: Vec<_> = (0..8_u8)
        .map(|thread| {
            let client = client.clone();
            thread::spawn(move || {
                for i in 0..40_u8 {
                    let size = (usize::from(thread) * 40 + usize::from(i)) * 7919 % (300 << 10);
                    let value = vec![thread ^ i; size];
                    block_on(client.put([thread, i], value.clone())).unwrap();
                    assert_eq!(block_on(client.get(&[thread, i])).unwrap(), value);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(block_on(client.len()).unwrap(), 8 * 40);
}

#[test]
fn in_process_and_shared_mounts_exclude_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exclusive.t4");

    let owner = block_on(t4::mount_with_options(&path, in_process_options())).unwrap();
    block_on(owner.put(b"k", b"in-process".to_vec())).unwrap();
    assert!(matches!(mount_shared(&path), Err(Error::Busy)));
    drop(owner);

    let client = mount_shared(&path).unwrap();
    assert_eq!(block_on(client.get(b"k")).unwrap(), b"in-process");
    assert!(matches!(
        block_on(t4::mount_with_options(&path, in_process_options())),
        Err(Error::Busy)
    ));
}

#[test]
fn racing_clients_start_one_server() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.t4");
    let clients: Vec<_> = (0..8_u8)
        .map(|i| {
            let path = path.clone();
            thread::spawn(move || {
                let store = mount_shared(&path).unwrap();
                block_on(store.put(vec![i], vec![i; 100])).unwrap();
                store
            })
        })
        .map(|client| client.join().unwrap())
        .collect();

    let server = clients[0].__server_pid();
    for (i, client) in clients.iter().enumerate() {
        assert_eq!(client.__server_pid(), server);
        assert_eq!(
            block_on(client.get(&[i as u8])).unwrap(),
            vec![i as u8; 100]
        );
    }
    assert_eq!(block_on(clients[0].len()).unwrap(), clients.len());
}

#[test]
fn idle_server_exits_and_frees_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idle.t4");
    let client = mount_shared(&path).unwrap();
    block_on(client.put(b"k", b"kept".to_vec())).unwrap();
    drop(client);

    let owner = mount_in_process_once_free(&path);
    assert_eq!(block_on(owner.get(b"k")).unwrap(), b"kept");
}

#[test]
fn cancelled_mount_lets_the_server_exit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancelled.t4");
    // Polled once, the mount connects on a thread of its own; dropped, its
    // connection must not keep the server up.
    let mut cancelled = Box::pin(t4::mount_with_options(
        &path,
        shared_options(Duration::from_millis(200)),
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);

    let client = mount_shared(&path).unwrap();
    block_on(client.put(b"k", b"v".to_vec())).unwrap();
    drop(client);
    let owner = mount_in_process_once_free(&path);
    assert_eq!(block_on(owner.get(b"k")).unwrap(), b"v");
}

#[test]
fn killed_server_is_replaced_and_keeps_acknowledged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.t4");
    let client = mount_shared(&path).unwrap();
    block_on(client.put(b"k", b"acknowledged".to_vec())).unwrap();
    let killed = client.__server_pid().unwrap();
    // SAFETY: plain syscall; `killed` is the server this test started.
    assert_eq!(unsafe { libc::kill(killed as i32, libc::SIGKILL) }, 0);

    // The first call may still go to the dead server, and fail with it.
    let got = match block_on(client.get(b"k")) {
        Err(Error::ServerGone) => block_on(client.get(b"k")),
        other => other,
    };
    assert_eq!(got.unwrap(), b"acknowledged");
    assert_ne!(client.__server_pid(), Some(killed));
}

#[test]
fn direct_io_must_match_the_running_server() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("options.t4");
    let _client = mount_shared(&path).unwrap();
    let mut options = shared_options(Duration::from_millis(200));
    options.direct_io = true;
    assert!(matches!(
        block_on(t4::mount_with_options(&path, options)),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn snapshot_through_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.t4");
    let target = dir.path().join("snapshot.t4");
    let client = mount_shared(&path).unwrap();
    block_on(client.put(b"a", b"alpha".to_vec())).unwrap();
    block_on(client.put(b"b", b"beta".to_vec())).unwrap();
    assert!(block_on(client.remove(b"b")).unwrap());

    block_on(client.snapshot(&target)).unwrap();
    assert!(matches!(
        block_on(client.snapshot(&target)),
        Err(Error::InvalidArgument(_))
    ));

    let snapshot = block_on(t4::mount_with_options(&target, in_process_options())).unwrap();
    assert_eq!(block_on(snapshot.get(b"a")).unwrap(), b"alpha");
    assert_eq!(block_on(snapshot.len()).unwrap(), 1);
}

#[test]
fn missing_server_program_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = shared_options(Duration::from_millis(200));
    options.mode = MountMode::Shared(ServerOptions {
        program: Some(dir.path().join("no-such-server")),
        ..ServerOptions::default()
    });
    match block_on(t4::mount_with_options(dir.path().join("s.t4"), options)) {
        Err(Error::Server(reason)) => assert!(reason.contains("cannot run"), "{reason}"),
        other => panic!("expected a server error, got {other:?}"),
    }
}
