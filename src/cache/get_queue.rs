//! [`GetQueue`]: the cap on the bytes t4's GETs have in flight to the inner
//! store (`doc/06-read-modes.md`, decision 11). A GET takes its place, its
//! bytes once for each of its racers, before it is sent, and gives it back
//! once its body is in; past the cap, GETs wait their turn, first come,
//! first served. A GET bigger than the cap goes once nothing else is in
//! flight. A late racer goes beside the cap: it is sent because a GET in
//! flight is slow, and waiting would make it later.
//!
//! Waiting GETs may go as one, if the queue merges: the first GET waiting
//! goes with the waiting GETs of a request each for the file's bytes just
//! before and after it, up to a size and the cap, once there is room for
//! them all. The one with the last bytes sends the merged GET, in the place
//! of them all, and lands its own bytes as they come; each of the others is
//! handed its bytes once they are all in ([`JoinedParts`], [`JoinedGet`]).
//! If that GET fails or is given up on, the GETs joined to it wait their
//! turn again.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::num::{NonZeroU8, NonZeroU64};
use std::ops::Range;
use std::task::{Poll, Waker};

use bytes::Bytes;

use super::FileVersionId;
use crate::io::sync::{Arc, Mutex, MutexGuard};

/// The GETs in flight, at most `most` bytes of them, a GET's racers' each
/// counted, and those waiting for a place.
#[derive(Debug)]
pub(super) struct GetQueue {
    most: u64,
    /// The most bytes waiting GETs go as, merged; 0 if they go apart.
    merged_up_to: u64,
    state: Mutex<QueueState>,
}

/// Where a GET's turn sends it.
pub(super) enum GetTurn {
    /// It goes now, in `place`, for its own bytes and, before them, those
    /// of the GETs `joined` to it.
    Send {
        place: GetPlace,
        joined: JoinedParts,
    },
    /// It joined another GET, which fetches its bytes: they come from here.
    Joined(JoinedGet),
}

impl GetQueue {
    /// A queue that lets `most` bytes be in flight, and merges waiting GETs
    /// into GETs of `merged_up_to` bytes at most, or none if 0.
    pub(super) fn new(most: NonZeroU64, merged_up_to: u64) -> Self {
        Self {
            most: most.get(),
            merged_up_to,
            state: Mutex::default(),
        }
    }

    /// The turn of a GET of `requests` racers for `bytes` of `file`: at
    /// once if its place is free and no GET waits, else once those that
    /// came before it have gone.
    pub(super) async fn turn(
        self: &Arc<Self>,
        file: FileVersionId,
        bytes: Range<u64>,
        requests: NonZeroU8,
    ) -> GetTurn {
        let cost = (bytes.end - bytes.start) * u64::from(requests.get());
        let mut waiting = {
            let mut state = self.state();
            if state.waiting.is_empty() && state.has_room(cost, self.most) {
                state.in_flight += cost;
                return GetTurn::Send {
                    place: GetPlace::new(self, cost),
                    joined: JoinedParts::none(),
                };
            }
            let merges =
                (requests == NonZeroU8::MIN && self.merged_up_to > 0).then_some((file, bytes));
            let slot = Arc::new(TurnSlot::default());
            let number = state.wait(cost, merges, Arc::clone(&slot));
            WaitingTurn {
                queue: Arc::clone(self),
                number,
                slot,
            }
        };
        waiting.taken().await
    }

    /// A place of `bytes` given back: the GETs waiting next go, as many as
    /// there is room for, each with those that join it.
    fn give_back(self: &Arc<Self>, bytes: u64) {
        let turns = {
            let mut state = self.state();
            state.in_flight -= bytes;
            self.admit(&mut state)
        };
        for (slot, turn) in turns {
            slot.hand(turn);
        }
    }

    /// The turns of the waiting GETs there is room for, first come, first
    /// served: the first waiting, with those that join it, once there is
    /// room for them all.
    fn admit(self: &Arc<Self>, state: &mut QueueState) -> Vec<(Arc<TurnSlot>, GetTurn)> {
        let mut turns = Vec::new();
        while let Some(&number) = state.order.front() {
            let Some(next) = state.waiting.get(&number) else {
                state.order.pop_front();
                continue;
            };
            let group = self.group(state, number, next);
            let cost = group.iter().map(|member| state.waiting[member].cost).sum();
            if !state.has_room(cost, self.most) {
                break;
            }
            state.order.pop_front();
            state.in_flight += cost;
            turns.extend(Self::sent(state, group, GetPlace::new(self, cost)));
        }
        turns
    }

    /// The waiting GETs that go with `first`, waiting GET `number`, in the
    /// order of their bytes: itself and, if it may merge, the GETs of the
    /// file's bytes just before and after it, as long as all of them come
    /// to `merged_up_to` and the cap at most.
    fn group(&self, state: &QueueState, number: u64, first: &WaitingGet) -> Vec<u64> {
        let Some((file, bytes)) = &first.merges else {
            return vec![number];
        };
        let most = self.merged_up_to.min(self.most);
        let mut span = bytes.clone();
        let mut group = VecDeque::from([number]);
        loop {
            let fits = |member: u64| state.waiting[&member].cost + span.end - span.start <= most;
            let bytes_of = |member: u64| state.waiting[&member].merged_bytes().expect("indexed");
            if let Some(below) = state
                .by_end
                .get(&(*file, span.start))
                .copied()
                .filter(|&member| fits(member))
            {
                span.start = bytes_of(below).start;
                group.push_front(below);
            } else if let Some(above) = state
                .by_start
                .get(&(*file, span.end))
                .copied()
                .filter(|&member| fits(member))
            {
                span.end = bytes_of(above).end;
                group.push_back(above);
            } else {
                break;
            }
        }
        group.into()
    }

    /// The turns of `group`, the waiting GETs of a file's adjacent bytes in
    /// their order, or a GET alone, taken out of the queue: the last sends
    /// the GET, in `place`, and each of the others is handed its bytes from
    /// it.
    fn sent(
        state: &mut QueueState,
        group: Vec<u64>,
        place: GetPlace,
    ) -> Vec<(Arc<TurnSlot>, GetTurn)> {
        let mut members: Vec<WaitingGet> = group
            .into_iter()
            .map(|member| state.take(member).expect("waiting in the group"))
            .collect();
        let sender = members.pop().expect("one GET at least");
        let mut turns = Vec::with_capacity(members.len() + 1);
        let mut parts = VecDeque::with_capacity(members.len());
        for member in members {
            let handoff = Arc::new(Handoff::default());
            let bytes = member.merged_bytes().expect("merged").clone();
            parts.push_back(JoinedPart::new(bytes, Arc::clone(&handoff)));
            turns.push((member.turn, GetTurn::Joined(JoinedGet { handoff })));
        }
        let joined = JoinedParts { parts };
        turns.push((sender.turn, GetTurn::Send { place, joined }));
        turns
    }

    /// Takes waiting GET `number` out of the queue, its future dropped,
    /// unless its turn came.
    fn leave(self: &Arc<Self>, number: u64) {
        let turns = {
            let mut state = self.state();
            if state.take(number).is_none() {
                return;
            }
            // A GET too big for the room left held up those behind it.
            self.admit(&mut state)
        };
        for (slot, turn) in turns {
            slot.hand(turn);
        }
    }

    /// Bytes in flight now.
    #[cfg(test)]
    pub(super) fn in_flight(&self) -> u64 {
        self.state().in_flight
    }

    /// GETs waiting now.
    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.state().waiting.len()
    }

    fn state(&self) -> MutexGuard<'_, QueueState> {
        // A panic between a count and its GET's entry would leave them
        // apart: nothing in a change panics but an allocation.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The bytes in flight, and the GETs waiting.
#[derive(Debug, Default)]
struct QueueState {
    /// Bytes in flight, over every GET holding a place.
    in_flight: u64,
    /// The number the next GET to wait gets.
    next: u64,
    /// The waiting GETs' numbers in the order they came. One that left,
    /// joined to another or given up on, stays until it reaches the front.
    order: VecDeque<u64>,
    waiting: HashMap<u64, WaitingGet>,
    /// The waiting GETs that may merge, by their file and first byte, and
    /// by their file and the byte past their last.
    by_start: HashMap<(FileVersionId, u64), u64>,
    by_end: HashMap<(FileVersionId, u64), u64>,
}

impl QueueState {
    /// Whether `bytes` more fit in flight: under the cap, or as the only
    /// GET.
    fn has_room(&self, bytes: u64, most: u64) -> bool {
        self.in_flight == 0 || self.in_flight + bytes <= most
    }

    /// A GET of `cost` bytes in flight waits, its turn to come in `turn`:
    /// its number.
    fn wait(
        &mut self,
        cost: u64,
        merges: Option<(FileVersionId, Range<u64>)>,
        turn: Arc<TurnSlot>,
    ) -> u64 {
        let number = self.next;
        self.next += 1;
        if let Some((file, bytes)) = &merges {
            self.by_start.insert((*file, bytes.start), number);
            self.by_end.insert((*file, bytes.end), number);
        }
        self.order.push_back(number);
        self.waiting
            .insert(number, WaitingGet { cost, merges, turn });
        number
    }

    /// Waiting GET `number`, out of the queue, if it waits still.
    fn take(&mut self, number: u64) -> Option<WaitingGet> {
        let waiting = self.waiting.remove(&number)?;
        if let Some((file, bytes)) = &waiting.merges {
            for (index, at) in [
                (&mut self.by_start, bytes.start),
                (&mut self.by_end, bytes.end),
            ] {
                if index.get(&(*file, at)) == Some(&number) {
                    index.remove(&(*file, at));
                }
            }
        }
        Some(waiting)
    }
}

/// A GET waiting its turn.
#[derive(Debug)]
struct WaitingGet {
    /// Its bytes in flight once sent: its bytes, once for each racer.
    cost: u64,
    /// Its file and bytes, if it may merge: a GET of one request, in a
    /// queue that merges.
    merges: Option<(FileVersionId, Range<u64>)>,
    turn: Arc<TurnSlot>,
}

impl WaitingGet {
    fn merged_bytes(&self) -> Option<&Range<u64>> {
        self.merges.as_ref().map(|(_, bytes)| bytes)
    }
}

/// A waiting GET's future's hold on its place in the queue: dropped before
/// its turn came, the GET leaves the queue.
struct WaitingTurn {
    queue: Arc<GetQueue>,
    number: u64,
    slot: Arc<TurnSlot>,
}

impl WaitingTurn {
    /// The turn, once it came.
    async fn taken(&mut self) -> GetTurn {
        poll_fn(|cx| {
            let mut slot = self.slot.state();
            match slot.turn.take() {
                Some(turn) => Poll::Ready(turn),
                None => {
                    slot.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }
}

impl Drop for WaitingTurn {
    fn drop(&mut self) {
        // A turn handed to the slot meanwhile is dropped with it: its place
        // given back, the GETs joined to it to wait again.
        self.queue.leave(self.number);
    }
}

/// Where a waiting GET's turn is handed to it.
#[derive(Debug, Default)]
struct TurnSlot {
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    turn: Option<GetTurn>,
    waker: Option<Waker>,
}

impl std::fmt::Debug for SlotState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotState")
            .field("handed", &self.turn.is_some())
            .finish_non_exhaustive()
    }
}

impl TurnSlot {
    /// Hands the GET its turn, and wakes it.
    fn hand(&self, turn: GetTurn) {
        let waker = {
            let mut slot = self.state();
            slot.turn = Some(turn);
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn state(&self) -> MutexGuard<'_, SlotState> {
        // Every change is a field set whole: a panic leaves the slot whole.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A GET's place in flight, its bytes, given back when dropped.
pub(super) struct GetPlace {
    queue: Arc<GetQueue>,
    bytes: u64,
}

impl GetPlace {
    fn new(queue: &Arc<GetQueue>, bytes: u64) -> Self {
        Self {
            queue: Arc::clone(queue),
            bytes,
        }
    }
}

impl Drop for GetPlace {
    fn drop(&mut self) {
        self.queue.give_back(self.bytes);
    }
}

/// The GETs joined to a GET that is sent: the bytes each of them asked
/// for, before the sender's own, in the file's order.
pub(super) struct JoinedParts {
    parts: VecDeque<JoinedPart>,
}

impl JoinedParts {
    fn none() -> Self {
        Self {
            parts: VecDeque::new(),
        }
    }

    /// The file's byte the joined GETs' bytes start at, if any joined.
    pub(super) fn start(&self) -> Option<u64> {
        self.parts.front().map(|part| part.bytes.start)
    }

    /// Takes from the front of `piece`, the merged GET's next bytes, what
    /// the joined GETs lack yet, handing each its bytes once it has them
    /// all: what is left of `piece` is the sender's.
    pub(super) fn take_front(&mut self, piece: &mut Bytes) {
        while let Some(part) = self.parts.front_mut() {
            if !part.take_front(piece) {
                return;
            }
            self.parts.pop_front().expect("found at the front").hand();
        }
    }
}

/// A joined GET's bytes, as the merged GET brings them, until handed over.
struct JoinedPart {
    bytes: Range<u64>,
    received: u64,
    pieces: Vec<Bytes>,
    handoff: Arc<Handoff>,
}

impl JoinedPart {
    fn new(bytes: Range<u64>, handoff: Arc<Handoff>) -> Self {
        Self {
            bytes,
            received: 0,
            pieces: Vec::new(),
            handoff,
        }
    }

    /// Takes from the front of `piece` what the part lacks: whether it has
    /// all its bytes now.
    fn take_front(&mut self, piece: &mut Bytes) -> bool {
        let lacks = self.bytes.end - self.bytes.start - self.received;
        let len = piece
            .len()
            .min(usize::try_from(lacks).unwrap_or(usize::MAX));
        let taken = piece.split_to(len);
        self.received += taken.len() as u64;
        if !taken.is_empty() {
            self.pieces.push(taken);
        }
        self.received == self.bytes.end - self.bytes.start
    }

    fn hand(mut self) {
        self.handoff.end(Some(std::mem::take(&mut self.pieces)));
    }
}

impl Drop for JoinedPart {
    fn drop(&mut self) {
        // Handed over already, this changes nothing; else the merged GET
        // failed or was given up on, and the joined GET waits again.
        self.handoff.end(None);
    }
}

/// A GET joined to another, waiting for its bytes.
pub(super) struct JoinedGet {
    handoff: Arc<Handoff>,
}

impl JoinedGet {
    /// Its bytes, in order, once the GET it joined has them all; `None` if
    /// that GET failed or was given up on.
    pub(super) async fn bytes(self) -> Option<Vec<Bytes>> {
        poll_fn(|cx| {
            let mut handoff = self.handoff.state();
            match &mut *handoff {
                Handed::Not(waker) => {
                    *waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                Handed::Bytes(pieces) => Poll::Ready(Some(std::mem::take(pieces))),
                Handed::Never => Poll::Ready(None),
            }
        })
        .await
    }
}

/// Where a merged GET hands a joined GET its bytes.
#[derive(Debug)]
struct Handoff {
    state: Mutex<Handed>,
}

impl Default for Handoff {
    fn default() -> Self {
        Self {
            state: Mutex::new(Handed::Not(None)),
        }
    }
}

#[derive(Debug)]
enum Handed {
    /// Not yet, the joined GET's waker if it waits.
    Not(Option<Waker>),
    Bytes(Vec<Bytes>),
    /// The merged GET failed or was given up on.
    Never,
}

impl Handoff {
    /// Hands over `pieces`, or, if `None`, that none will come, unless
    /// handed already; wakes the joined GET.
    fn end(&self, pieces: Option<Vec<Bytes>>) {
        let waker = {
            let mut handed = self.state();
            let Handed::Not(waker) = &mut *handed else {
                return;
            };
            let waker = waker.take();
            *handed = pieces.map_or(Handed::Never, Handed::Bytes);
            waker
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn state(&self) -> MutexGuard<'_, Handed> {
        // Every change is the state set whole: a panic leaves it whole.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
