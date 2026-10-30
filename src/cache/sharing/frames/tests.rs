use super::*;
use crate::cache::BlockKey;

/// One of each client frame, each field set apart from its default.
fn client_frames() -> Vec<ClientFrame> {
    let race = NonZeroU8::new(3).unwrap();
    vec![
        ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            region: false,
        },
        ClientFrame::Hello {
            version: PROTOCOL_VERSION,
            region: true,
        },
        ClientFrame::StartRead {
            call: 1,
            read: 2,
            path: Bytes::from_static(b"data/a.vortex"),
            e_tag: "\"9b2cf535f27731c974343645a3985328\"".to_owned(),
            size: 5 << 20,
            range: 100..3 << 20,
            intent: Some(ReadIntent::Throughput),
        },
        ClientFrame::StartRead {
            call: 3,
            read: 4,
            path: Bytes::new(),
            e_tag: String::new(),
            size: 0,
            range: 0..0,
            intent: None,
        },
        ClientFrame::StartRead {
            call: 4,
            read: 5,
            path: Bytes::from_static(b"b"),
            e_tag: "v2".to_owned(),
            size: 1,
            range: 0..1,
            intent: Some(ReadIntent::Latency),
        },
        ClientFrame::StartRead {
            call: 5,
            read: 6,
            path: Bytes::from_static(b"c"),
            e_tag: "v3".to_owned(),
            size: 1,
            range: 0..1,
            intent: Some(ReadIntent::Warm),
        },
        ClientFrame::TakeGets {
            call: 5,
            read: 2,
            plan: ReadPlan::new(vec![
                PlannedGet::once(0..2),
                PlannedGet::new(2..3, race).with_late_race(Duration::from_millis(40)),
            ]),
        },
        ClientFrame::TakeGets {
            call: 6,
            read: 2,
            plan: ReadPlan::default(),
        },
        ClientFrame::Land {
            read: 2,
            fetch: 1,
            bytes: Bytes::from_static(b"the next bytes"),
        },
        ClientFrame::Commit {
            call: 7,
            read: 2,
            fetch: 1,
        },
        ClientFrame::Abandon { read: 2, fetch: 0 },
        ClientFrame::Finish { call: 8, read: 2 },
        ClientFrame::EndRead { read: u64::MAX },
    ]
}

/// One of each owner frame, and of each next step and I/O phase.
fn owner_frames() -> Vec<OwnerFrame> {
    let version = FileVersion::named(FileVersionId::new(41), 5 << 20).unwrap();
    let run = LocatedRun::new(
        3,
        (3 << 20) + 7..5 << 20,
        vec![
            BlockSource::Memory,
            BlockSource::Disk,
            BlockSource::Remote,
            BlockSource::InFlight { requests: 2 },
        ],
    );
    let fetches = vec![
        FetchSpec::new(0..2 << 20, NonZeroU8::MIN, None),
        FetchSpec::new(
            2 << 20..5 << 20,
            NonZeroU8::new(2).unwrap(),
            Some(Duration::from_micros(1500)),
        ),
    ];
    let after = Duration::from_nanos(123_456_789);
    let phases = [
        IoPhase::Started,
        IoPhase::Answered { after },
        IoPhase::Finished {
            bytes: 1 << 20,
            after,
        },
        IoPhase::Landed {
            answered: after,
            received: after * 2,
            landing: after / 3,
            after: after * 4,
            waited: Duration::ZERO,
        },
        IoPhase::Failed { after },
        IoPhase::Cancelled { after },
    ];
    let mut frames = vec![
        OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
            region: false,
        },
        OwnerFrame::Welcome {
            version: PROTOCOL_VERSION,
            region: true,
        },
        OwnerFrame::Started {
            call: 1,
            version,
            next: NextStep::Plan(run.clone()),
        },
        OwnerFrame::Started {
            call: 2,
            version,
            next: NextStep::Fetch(Vec::new()),
        },
        OwnerFrame::Uncacheable { call: 3 },
        OwnerFrame::Next {
            call: 4,
            next: NextStep::Fetch(fetches),
        },
        OwnerFrame::Committed { call: 5 },
        OwnerFrame::Retry {
            call: 6,
            next: NextStep::Plan(run),
        },
        OwnerFrame::Served {
            call: 7,
            bytes: Some(Bytes::from_static(b"a read's bytes")),
        },
        OwnerFrame::Served {
            call: 8,
            bytes: None,
        },
        OwnerFrame::Failed { call: 9 },
    ];
    frames.extend(phases.map(|phase| OwnerFrame::DiskRead {
        drive: 2,
        bytes: 256 << 10,
        phase,
    }));
    frames
}

/// A frame's body: its bytes after its length, which must be theirs.
fn body(frame: &[u8]) -> Bytes {
    let (length, body) = frame.split_at(8);
    assert_eq!(
        u64::from_le_bytes(length.try_into().unwrap()),
        body.len() as u64
    );
    Bytes::copy_from_slice(body)
}

#[test]
fn every_frame_decodes_as_it_was_encoded() {
    for frame in client_frames() {
        assert_eq!(ClientFrame::decode(body(&frame.encode())).unwrap(), frame);
    }
    for frame in owner_frames() {
        assert_eq!(OwnerFrame::decode(body(&frame.encode())).unwrap(), frame);
    }
}

/// A frame cut anywhere short, or with a byte past its end, is malformed:
/// a field that ends early, or a byte no field takes.
#[test]
fn a_frame_of_the_wrong_length_is_malformed() {
    for whole in client_frames().iter().map(|frame| body(&frame.encode())) {
        for cut in 0..whole.len() {
            assert!(ClientFrame::decode(whole.slice(..cut)).is_err());
        }
        let longer = Bytes::from([&whole[..], &[0]].concat());
        assert!(ClientFrame::decode(longer).is_err());
    }
    for whole in owner_frames().iter().map(|frame| body(&frame.encode())) {
        for cut in 0..whole.len() {
            assert!(OwnerFrame::decode(whole.slice(..cut)).is_err());
        }
        let longer = Bytes::from([&whole[..], &[0]].concat());
        assert!(OwnerFrame::decode(longer).is_err());
    }
}

/// `frame`'s body, its bytes from `at` replaced by `with`.
fn patched(frame: &[u8], at: usize, with: &[u8]) -> Bytes {
    let mut body = body(frame).to_vec();
    body[at..at + with.len()].copy_from_slice(with);
    Bytes::from(body)
}

#[test]
fn a_value_no_field_takes_is_malformed() {
    let hello = ClientFrame::Hello {
        version: 1,
        region: false,
    }
    .encode();
    let flag = patched(&hello, 1 + 4 + 4, &[2]);
    assert!(ClientFrame::decode(flag).is_err(), "region flag");
    assert!(
        ClientFrame::decode(patched(&hello, 0, &[0])).is_err(),
        "tag"
    );
    let magic = patched(&hello, 1, b"t4sX");
    assert!(ClientFrame::decode(magic).is_err(), "magic");

    let start = ClientFrame::StartRead {
        call: 1,
        read: 2,
        path: Bytes::from_static(b"p"),
        e_tag: "e".to_owned(),
        size: 9,
        range: 1..5,
        intent: None,
    }
    .encode();
    // The tag, call and read; the path's length and its byte; the e_tag's
    // length, then its byte.
    let e_tag = 1 + 8 + 8 + 8 + 1 + 8;
    let invalid = patched(&start, e_tag, &[0xff]);
    assert!(ClientFrame::decode(invalid).is_err(), "UTF-8");
    let range = e_tag + 1 + 8;
    let backwards = [5u64.to_le_bytes(), 1u64.to_le_bytes()].concat();
    let backwards = patched(&start, range, &backwards);
    assert!(ClientFrame::decode(backwards).is_err(), "range");
    let intent = patched(&start, range + 16, &[4]);
    assert!(ClientFrame::decode(intent).is_err(), "intent");

    let take = ClientFrame::TakeGets {
        call: 1,
        read: 2,
        plan: ReadPlan::new(vec![PlannedGet::once(0..1)]),
    }
    .encode();
    // The tag, call, read and count; the GET's blocks, then its race.
    let count = 1 + 8 + 8;
    let race = count + 4 + 8;
    assert!(
        ClientFrame::decode(patched(&take, race, &[0])).is_err(),
        "race"
    );
    let many = patched(&take, count, &u32::MAX.to_le_bytes());
    assert!(ClientFrame::decode(many).is_err(), "count");

    let started = OwnerFrame::Started {
        call: 1,
        version: FileVersion::named(FileVersionId::new(0), 1).unwrap(),
        next: NextStep::Fetch(Vec::new()),
    }
    .encode();
    let id = patched(&started, 9, &FileVersionId::LIMIT.to_le_bytes());
    assert!(OwnerFrame::decode(id).is_err(), "version id");
    let size = patched(&started, 17, &u64::MAX.to_le_bytes());
    assert!(OwnerFrame::decode(size).is_err(), "size");

    let next = OwnerFrame::Next {
        call: 1,
        next: NextStep::Plan(LocatedRun::new(0, 0..1, vec![BlockSource::Remote])),
    }
    .encode();
    // The tag, call and step; the run's first block and its range; then
    // its count of sources, and its one.
    let first = 1 + 8 + 1;
    let sources = first + 4 + 16;
    let mut no_block = body(&next).to_vec();
    no_block.truncate(sources);
    no_block.extend_from_slice(&0u32.to_le_bytes());
    let no_block = Bytes::from(no_block);
    assert!(OwnerFrame::decode(no_block).is_err(), "a run of no block");
    let source = patched(&next, sources + 4, &[4]);
    assert!(OwnerFrame::decode(source).is_err(), "source");
    let past = patched(&next, first, &BlockKey::INDEX_LIMIT.to_le_bytes());
    assert!(
        OwnerFrame::decode(past).is_err(),
        "past a file's last block"
    );
}

/// A fetch's bytes are a slice of the frame they came in, not a copy.
#[test]
fn landed_bytes_are_the_frames_own() {
    let frame = body(
        &ClientFrame::Land {
            read: 1,
            fetch: 0,
            bytes: Bytes::from(vec![7; 4096]),
        }
        .encode(),
    );
    let ClientFrame::Land { bytes, .. } = ClientFrame::decode(frame.clone()).unwrap() else {
        panic!("a land frame");
    };
    assert!(frame.as_ptr_range().contains(&bytes.as_ptr()));
}
