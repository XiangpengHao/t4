use std::task::Context;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use futures_util::task::noop_waker_ref;

use super::*;

const MIB: u64 = 1 << 20;
const ONE: NonZeroU8 = NonZeroU8::MIN;

/// A queue letting `most` bytes be in flight, merging into `merged_up_to`.
fn queue(most: u64, merged_up_to: u64) -> Arc<GetQueue> {
    Arc::new(GetQueue::new(NonZeroU64::new(most).unwrap(), merged_up_to))
}

fn file(number: u64) -> FileVersionId {
    FileVersionId::new(number)
}

/// The turn of a GET waiting for it, polled once: `None` while it waits.
fn polled<T>(waiting: &mut BoxFuture<'_, T>) -> Option<T> {
    match waiting
        .as_mut()
        .poll(&mut Context::from_waker(noop_waker_ref()))
    {
        Poll::Ready(turn) => Some(turn),
        Poll::Pending => None,
    }
}

/// A turn that sends its GET: its place, and the GETs joined to it.
fn sent(turn: GetTurn) -> (GetPlace, JoinedParts) {
    match turn {
        GetTurn::Send { place, joined } => (place, joined),
        GetTurn::Joined(_) => panic!("joined another GET"),
    }
}

fn joined(turn: GetTurn) -> JoinedGet {
    match turn {
        GetTurn::Joined(get) => get,
        GetTurn::Send { .. } => panic!("sent its own GET"),
    }
}

/// A GET's turn with room free and none waiting: at once.
fn at_once(
    queue: &Arc<GetQueue>,
    file: FileVersionId,
    bytes: Range<u64>,
) -> (GetPlace, JoinedParts) {
    sent(
        queue
            .turn(file, bytes, ONE)
            .now_or_never()
            .expect("room free"),
    )
}

/// A file's bytes, each a function of its offset.
fn body(len: u64) -> Bytes {
    (0..len)
        .map(|at| (at % 251) as u8)
        .collect::<Vec<_>>()
        .into()
}

fn concatenated(pieces: Vec<Bytes>) -> Bytes {
    pieces.concat().into()
}

#[test]
fn gets_past_the_cap_wait_their_turn_first_come_first_served() {
    let queue = queue(2 * MIB, 0);
    let first = at_once(&queue, file(1), 0..MIB);
    let second = at_once(&queue, file(1), 4 * MIB..5 * MIB);
    let mut third = queue.turn(file(1), 8 * MIB..9 * MIB, ONE).boxed();
    let mut fourth = queue.turn(file(2), 0..MIB, ONE).boxed();
    assert!(polled(&mut third).is_none());
    assert!(polled(&mut fourth).is_none());
    assert_eq!((queue.in_flight(), queue.waiting()), (2 * MIB, 2));

    drop(second);
    let third = sent(polled(&mut third).expect("first to wait, first to go"));
    assert!(polled(&mut fourth).is_none(), "no room yet");
    drop(first);
    let fourth = sent(polled(&mut fourth).expect("room given back"));
    assert_eq!((queue.in_flight(), queue.waiting()), (2 * MIB, 0));
    drop((third, fourth));
    assert_eq!(queue.in_flight(), 0);
}

/// A GET's bytes count once for each racer; a GET bigger than the cap goes
/// once nothing is in flight.
#[test]
fn a_gets_bytes_count_once_for_each_racer() {
    let two = NonZeroU8::new(2).unwrap();
    let queue = queue(3 * MIB, 0);
    let first = sent(queue.turn(file(1), 0..MIB, two).now_or_never().unwrap());
    let mut second = queue.turn(file(1), MIB..2 * MIB, two).boxed();
    assert!(polled(&mut second).is_none(), "2 + 2 MiB past 3");
    drop(first);
    let second = sent(polled(&mut second).unwrap());
    assert_eq!(queue.in_flight(), 2 * MIB);
    drop(second);

    let alone = at_once(&queue, file(1), 0..8 * MIB);
    assert_eq!(queue.in_flight(), 8 * MIB);
    drop(alone);
}

#[test]
fn a_get_given_up_on_leaves_the_queue_or_gives_its_place_back() {
    let queue = queue(MIB, 0);
    let first = at_once(&queue, file(1), 0..MIB);
    let mut second = queue.turn(file(1), MIB..2 * MIB, ONE).boxed();
    let mut third = queue.turn(file(1), 4 * MIB..5 * MIB, ONE).boxed();
    assert!(polled(&mut second).is_none());
    assert!(polled(&mut third).is_none());

    // Before its turn: it leaves the queue.
    drop(second);
    assert_eq!(queue.waiting(), 1);
    // Once its turn came, before it took it: its place goes to none, and
    // back.
    drop(first);
    assert_eq!((queue.in_flight(), queue.waiting()), (MIB, 0));
    drop(third);
    assert_eq!(queue.in_flight(), 0);
}

/// When a waiting GET's turn comes, those of the file's bytes just before
/// and after it join it: the one with the last bytes sends the GET, in the
/// place of them all, and the others are handed theirs as the body brings
/// them all.
#[test]
fn waiting_gets_of_adjacent_bytes_go_as_one() {
    let queue = queue(4 * MIB, 4 * MIB);
    let first = at_once(&queue, file(9), 0..4 * MIB);
    let mut middle = queue.turn(file(1), MIB..2 * MIB, ONE).boxed();
    let mut low = queue.turn(file(1), 0..MIB, ONE).boxed();
    let mut high = queue.turn(file(1), 2 * MIB..3 * MIB, ONE).boxed();
    let mut other_file = queue.turn(file(2), 3 * MIB..4 * MIB, ONE).boxed();
    for waiting in [&mut middle, &mut low, &mut high, &mut other_file] {
        assert!(polled(waiting).is_none());
    }

    drop(first);
    let (place, mut parts) = sent(polled(&mut high).expect("the last bytes send"));
    assert_eq!(parts.start(), Some(0));
    let mut middle = joined(polled(&mut middle).unwrap()).bytes().boxed();
    let mut low = joined(polled(&mut low).unwrap()).bytes().boxed();
    let other_file = sent(polled(&mut other_file).expect("room for it beside"));
    assert_eq!((queue.in_flight(), queue.waiting()), (4 * MIB, 0));

    let body = body(3 * MIB);
    let mut piece = body.slice(..(3 * MIB / 2) as usize);
    parts.take_front(&mut piece);
    assert!(piece.is_empty(), "the joined GETs' bytes");
    let low = polled(&mut low).expect("all in").expect("handed");
    assert_eq!(concatenated(low), body.slice(..MIB as usize));
    assert!(polled(&mut middle).is_none(), "half in");

    let mut piece = body.slice((3 * MIB / 2) as usize..);
    parts.take_front(&mut piece);
    assert_eq!(piece, body.slice(2 * MIB as usize..), "the sender's own");
    let middle = polled(&mut middle).expect("all in").expect("handed");
    assert_eq!(
        concatenated(middle),
        body.slice(MIB as usize..2 * MIB as usize)
    );

    drop((place, parts));
    assert_eq!(queue.in_flight(), MIB, "the merged GET's place, whole");
    drop(other_file);
}

/// GETs merge up to the size, as long as their bytes fit in flight, and
/// only GETs of one request.
#[test]
fn waiting_gets_merge_up_to_a_size_and_the_room_left() {
    for (most, merged_up_to) in [(4 * MIB, 2 * MIB), (2 * MIB, 4 * MIB)] {
        let queue = queue(most, merged_up_to);
        let first = at_once(&queue, file(9), 0..most);
        let mut low = queue.turn(file(1), 0..MIB, ONE).boxed();
        let mut middle = queue.turn(file(1), MIB..2 * MIB, ONE).boxed();
        let mut high = queue.turn(file(1), 2 * MIB..3 * MIB, ONE).boxed();
        for waiting in [&mut low, &mut middle, &mut high] {
            assert!(polled(waiting).is_none());
        }

        drop(first);
        let (place, parts) = sent(polled(&mut middle).expect("2 MiB at most"));
        assert_eq!(parts.start(), Some(0));
        let _low = joined(polled(&mut low).unwrap());
        let high = polled(&mut high);
        assert_eq!(
            high.is_some(),
            most == 4 * MIB,
            "beside it if there is room"
        );
        drop((place, parts, high));
    }

    let queue = queue(MIB, 4 * MIB);
    let first = at_once(&queue, file(9), 0..MIB);
    let mut low = queue.turn(file(1), 0..MIB, ONE).boxed();
    let mut racing = queue
        .turn(file(1), MIB..2 * MIB, NonZeroU8::new(2).unwrap())
        .boxed();
    assert!(polled(&mut low).is_none());
    assert!(polled(&mut racing).is_none());
    drop(first);
    let (place, parts) = sent(polled(&mut low).unwrap());
    assert_eq!(parts.start(), None, "a racing GET merges with none");
    drop((place, parts));
    let _racing = sent(polled(&mut racing).unwrap());
}

/// A merged GET given up on before its body is in, or before its sender
/// took its turn, hands its joined GETs nothing: they wait again.
#[test]
fn a_merged_get_given_up_on_leaves_its_joined_gets_to_wait_again() {
    for taken in [true, false] {
        let queue = queue(2 * MIB, 4 * MIB);
        let first = at_once(&queue, file(9), 0..2 * MIB);
        let mut low = queue.turn(file(1), 0..MIB, ONE).boxed();
        let mut high = queue.turn(file(1), MIB..2 * MIB, ONE).boxed();
        assert!(polled(&mut low).is_none());
        assert!(polled(&mut high).is_none());

        drop(first);
        let mut low = joined(polled(&mut low).unwrap()).bytes().boxed();
        if taken {
            let (place, mut parts) = sent(polled(&mut high).unwrap());
            let mut piece = body(MIB / 2);
            parts.take_front(&mut piece);
            drop((place, parts));
        } else {
            drop(high);
        }
        assert_eq!(polled(&mut low), Some(None));
        assert_eq!(queue.in_flight(), 0, "its place given back");
    }
}

/// The first waiting GET waits for room for the GETs that join it too:
/// room for itself alone doesn't send it.
#[test]
fn a_merged_get_waits_for_room_for_all_its_bytes() {
    let queue = queue(4 * MIB, 4 * MIB);
    let big = at_once(&queue, file(9), 0..3 * MIB);
    let small = at_once(&queue, file(9), 3 * MIB..4 * MIB);
    let mut low = queue.turn(file(1), 0..MIB, ONE).boxed();
    let mut high = queue.turn(file(1), MIB..2 * MIB, ONE).boxed();
    assert!(polled(&mut low).is_none());
    assert!(polled(&mut high).is_none());

    drop(small);
    assert!(polled(&mut low).is_none(), "room for one of the two");
    assert!(polled(&mut high).is_none());
    drop(big);
    let (place, parts) = sent(polled(&mut high).expect("room for both"));
    assert_eq!(parts.start(), Some(0));
    let _low = joined(polled(&mut low).unwrap());
    assert_eq!(queue.in_flight(), 2 * MIB);
    drop((place, parts));
}
