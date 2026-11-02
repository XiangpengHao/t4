//! The queue under shuttle: GETs of a file's adjacent bytes taking turns,
//! merging and handing their bytes over, one giving up as it waits, while
//! places are given back.

use std::num::{NonZeroU8, NonZeroU64};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{Either, ready, select};
use shuttle::future::block_on;
use shuttle::scheduler::PctScheduler;

use super::{GetQueue, GetTurn};
use crate::cache::FileVersionId;
use crate::io::sync::{Arc, thread};

/// A GET's bytes: its block's number throughout.
const BLOCK: u64 = 8;
/// The bytes in flight at most: two GETs'.
const MOST: u64 = 2 * BLOCK;

fn explore(test: impl Fn() + Send + Sync + 'static) {
    shuttle::Runner::new(PctScheduler::new(3, 1000), shuttle::Config::new()).run(test);
}

/// The bytes of blocks `from..to`, each its number throughout.
fn blocks(from: u64, to: u64) -> Bytes {
    (from..to)
        .flat_map(|block| vec![block as u8; BLOCK as usize])
        .collect::<Vec<_>>()
        .into()
}

/// GET `block` of the file, until it has its bytes, sent or handed over;
/// while it holds a place, the bytes in flight are within the cap.
async fn fetch(queue: &Arc<GetQueue>, block: u64) -> Bytes {
    let own = block * BLOCK..(block + 1) * BLOCK;
    loop {
        match queue
            .turn(FileVersionId::new(1), own.clone(), NonZeroU8::MIN)
            .await
        {
            GetTurn::Send { place, mut joined } => {
                assert!(queue.in_flight() <= MOST);
                let from = joined.start().unwrap_or(own.start) / BLOCK;
                let mut body = blocks(from, block + 1);
                joined.take_front(&mut body);
                drop(place);
                return body;
            }
            GetTurn::Joined(get) => {
                if let Some(pieces) = get.bytes().await {
                    return pieces.concat().into();
                }
            }
        }
    }
}

#[test]
fn shuttle_gets_take_turns_merge_and_hand_their_bytes_over() {
    explore(|| {
        let queue = Arc::new(GetQueue::new(NonZeroU64::new(MOST).unwrap(), 2 * BLOCK));
        let fetches: Vec<_> = (0..4)
            .map(|block| {
                let queue = Arc::clone(&queue);
                thread::spawn(move || {
                    let got = block_on(fetch(&queue, block));
                    assert_eq!(got, blocks(block, block + 1), "block {block}'s own bytes");
                })
            })
            .collect();
        // One more GET that gives up if its turn isn't there at once.
        let quitter = {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let own = 9 * BLOCK..10 * BLOCK;
                let turn = queue
                    .turn(FileVersionId::new(1), own, NonZeroU8::MIN)
                    .boxed();
                if let Either::Left((turn, _)) = block_on(select(turn, ready(()))) {
                    drop(turn);
                }
            })
        };
        for fetch in fetches {
            fetch.join().unwrap();
        }
        quitter.join().unwrap();
        assert_eq!((queue.in_flight(), queue.waiting()), (0, 0));
    });
}
