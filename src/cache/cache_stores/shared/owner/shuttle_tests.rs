//! Drive the production session loop through the shutdown ordering of 57bf283.
use std::num::NonZeroU32;
use std::sync::Arc as Shared;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::{ClientSession, FrameReceiver, FrameSocket};
use crate::cache::cache_stores::shared::protocol::{ClientFrame, NextStep, OwnerFrame};
use crate::cache::policy::{ClockPolicy, PlacementPolicy, PlannedGet, ReadPlan};
use crate::cache::storage::{CacheFiles, FileIdentity, ObjectCache};
use crate::io::io_worker::IoWorker;
use crate::io::simulated::SimDisk;
use crate::io::sync::{Arc, atomic::AtomicBool, thread};
use crate::io::test_schedule::TestSchedule;

struct SessionPeer {
    socket: FrameSocket,
    frames: FrameReceiver,
    serving: thread::JoinHandle<()>,
}
impl SessionPeer {
    fn start(cache: Arc<ObjectCache>, stopping: Shared<AtomicBool>) -> Self {
        let (socket, owner) = FrameSocket::pair().unwrap();
        let serving = thread::spawn(move || {
            let session = ClientSession {
                socket: Shared::new(owner),
                stopping,
                cache,
                first_file: FileIdentity::new(1, 1),
            };
            let mut frames = FrameReceiver::new(session.socket.try_clone().unwrap());
            session.serve_reads(&mut frames, None).unwrap();
            session.socket.shut_down();
        });
        Self {
            frames: FrameReceiver::new(socket.try_clone().unwrap()),
            socket,
            serving,
        }
    }
    fn send(&self, frame: ClientFrame) {
        self.socket.send(&frame.encode()).unwrap();
    }
    fn answer(&mut self) -> Option<OwnerFrame> {
        self.frames
            .receive(FrameSocket::MOST_BYTES)
            .unwrap()
            .map(|body| OwnerFrame::decode(body).unwrap())
    }
    fn start_read(&self) {
        self.send(ClientFrame::StartRead {
            call: 1,
            read: 1,
            path: Bytes::from_static(b"a"),
            e_tag: "v1".into(),
            size: 64,
            range: 0..64,
            intent: None,
            output: None,
        });
    }
    fn cache() -> Arc<ObjectCache> {
        let io = IoWorker::simulated(NonZeroU32::MIN, vec![SimDisk::new()]).unwrap();
        let disk = CacheFiles::on(io, NonZeroU32::MIN, 1).unwrap();
        let policy: Arc<dyn PlacementPolicy> =
            Arc::new(ClockPolicy::new(NonZeroU32::MIN, NonZeroU32::MIN));
        Arc::new(ObjectCache::new(NonZeroU32::MIN, disk, policy).unwrap())
    }
}

#[test]
fn a_stopping_owner_never_hands_a_departing_sessions_claim_to_another_client() {
    TestSchedule::iterations(16, || {
        let cache = SessionPeer::cache();
        let stopping = Shared::new(AtomicBool::new(false));
        let mut fetching = SessionPeer::start(Arc::clone(&cache), Shared::clone(&stopping));
        fetching.start_read();
        assert!(matches!(
            fetching.answer(),
            Some(OwnerFrame::Started {
                next: NextStep::Plan(_),
                ..
            })
        ));
        fetching.send(ClientFrame::TakeGets {
            call: 2,
            read: 1,
            plan: ReadPlan::new(vec![PlannedGet::once(0..1)]),
        });
        assert!(matches!(
            fetching.answer(),
            Some(OwnerFrame::Next {
                next: NextStep::Fetch(_),
                ..
            })
        ));

        let mut waiting = SessionPeer::start(Arc::clone(&cache), Shared::clone(&stopping));
        waiting.start_read();
        assert!(matches!(
            waiting.answer(),
            Some(OwnerFrame::Started {
                next: NextStep::Plan(_),
                ..
            })
        ));
        waiting.send(ClientFrame::TakeGets {
            call: 2,
            read: 1,
            plan: ReadPlan::new(vec![]),
        });
        assert!(matches!(
            waiting.answer(),
            Some(OwnerFrame::Next {
                next: NextStep::Fetch(_),
                ..
            })
        ));
        waiting.send(ClientFrame::Finish { call: 3, read: 1 });
        // A claim already in flight is found while locating the read. The
        // waiting session must register before the claiming session ends.
        while cache.waiting_reads() == 0 {
            thread::yield_now();
        }
        stopping.store(true, Ordering::Release);
        fetching.socket.shut_down();
        fetching.serving.join().unwrap();

        // Keep the later socket open. This is a legal pause between the
        // listener's successive shutdowns. The fixed loop closes it; the
        // reverted loop sends a fresh fetch for the abandoned claim.
        let answer = waiting.answer();
        waiting.socket.shut_down();
        waiting.serving.join().unwrap();
        drop(fetching.socket);
        drop(fetching.frames);
        drop(waiting.socket);
        drop(waiting.frames);
        drop(cache);
        assert_eq!(
            answer, None,
            "a departing owner must not authorize another GET"
        );
    });
}
