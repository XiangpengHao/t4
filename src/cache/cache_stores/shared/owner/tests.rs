#[cfg(feature = "shuttle")]
use super::FrameSocket as UnixStream;
use std::io::Write;
#[cfg(not(feature = "shuttle"))]
use std::os::unix::net::UnixStream;
use std::task::Wake;

use super::SessionWaker;
use crate::cache::cache_stores::shared::protocol::{ClientFrame, FrameSocket};

/// Many wakes before a wait end that wait, not the next.
#[test]
fn a_session_waits_for_its_socket_or_a_wake() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (mut client, socket) = UnixStream::pair().unwrap();
        let socket = FrameSocket::new(socket).unwrap();
        let waker = std::sync::Arc::new(SessionWaker::new().unwrap());

        waker.wake_by_ref();
        assert!(
            !waker.wait_for(&socket).unwrap(),
            "woken, the socket not ready"
        );
        let waking = std::sync::Arc::clone(&waker);
        let woken = crate::io::sync::thread::spawn(move || {
            waking.wake_by_ref();
        });
        assert!(!waker.wait_for(&socket).unwrap());
        woken.join().unwrap();

        for _ in 0..3 {
            waker.wake_by_ref();
        }
        assert!(!waker.wait_for(&socket).unwrap());
        let sending = crate::io::sync::thread::spawn(move || {
            client
                .write_all(&ClientFrame::EndRead { read: 1 }.encode())
                .unwrap();
            client
        });
        assert!(waker.wait_for(&socket).unwrap(), "the frame came");
        drop(sending.join().unwrap());
        assert!(waker.wait_for(&socket).unwrap(), "the close came");
    });
}

#[test]
fn a_frame_queued_wakes_the_session_from_another_thread_only() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let (mut client, socket) = UnixStream::pair().unwrap();
        let socket = FrameSocket::new(socket).unwrap();
        let waker = std::sync::Arc::new(SessionWaker::new().unwrap());

        let waking = std::sync::Arc::clone(&waker);
        crate::io::sync::thread::spawn(move || waking.wake_to_send())
            .join()
            .unwrap();
        assert!(
            !waker.wait_for(&socket).unwrap(),
            "woken from another thread"
        );
        waker.wake_to_send();
        let sending = crate::io::sync::thread::spawn(move || {
            client
                .write_all(&ClientFrame::EndRead { read: 1 }.encode())
                .unwrap();
            client
        });
        assert!(waker.wait_for(&socket).unwrap(), "not woken by its own");
        drop(sending.join().unwrap());
    });
}
