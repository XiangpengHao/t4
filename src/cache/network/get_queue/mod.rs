//! Late racers bypass the cap: waiting would make a slow GET slower.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::num::{NonZeroU8, NonZeroU64};
use std::ops::Range;
use std::task::{Poll, Waker};

use bytes::Bytes;

use crate::cache::cache_stores::FileVersionKey;
use crate::io::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
pub(super) struct GetQueue {
    most: u64,
    merged_up_to: u64,
    state: Mutex<QueueState>,
}

pub(super) enum GetTurn {
    Send {
        place: GetPlace,
        joined: JoinedParts,
    },
    Joined(JoinedGet),
}

impl GetQueue {
    pub(super) fn new(most: NonZeroU64, merged_up_to: u64) -> Self {
        Self {
            most: most.get(),
            merged_up_to,
            state: Mutex::default(),
        }
    }

    pub(super) async fn turn(
        self: &Arc<Self>,
        file: FileVersionKey,
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

    /// First come, first served: a GET without room holds up those behind it.
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

    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn in_flight(&self) -> u64 {
        self.state().in_flight
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn waiting(&self) -> usize {
        self.state().waiting.len()
    }

    fn state(&self) -> MutexGuard<'_, QueueState> {
        // Poison is ignored: nothing between a count and its GET's entry panics but an allocation.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug, Default)]
struct QueueState {
    in_flight: u64,
    next: u64,
    /// A GET that left stays here until it reaches the front.
    order: VecDeque<u64>,
    waiting: HashMap<u64, WaitingGet>,
    by_start: HashMap<(FileVersionKey, u64), u64>,
    by_end: HashMap<(FileVersionKey, u64), u64>,
}

impl QueueState {
    /// A GET bigger than the cap goes alone, or it would never go.
    fn has_room(&self, bytes: u64, most: u64) -> bool {
        self.in_flight == 0 || self.in_flight + bytes <= most
    }

    fn wait(
        &mut self,
        cost: u64,
        merges: Option<(FileVersionKey, Range<u64>)>,
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

#[derive(Debug)]
struct WaitingGet {
    cost: u64,
    merges: Option<(FileVersionKey, Range<u64>)>,
    turn: Arc<TurnSlot>,
}

impl WaitingGet {
    fn merged_bytes(&self) -> Option<&Range<u64>> {
        self.merges.as_ref().map(|(_, bytes)| bytes)
    }
}

struct WaitingTurn {
    queue: Arc<GetQueue>,
    number: u64,
    slot: Arc<TurnSlot>,
}

impl WaitingTurn {
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
        // A turn handed meanwhile drops with the slot, giving its place back.
        self.queue.leave(self.number);
    }
}

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

pub(super) struct JoinedParts {
    parts: VecDeque<JoinedPart>,
}

impl JoinedParts {
    fn none() -> Self {
        Self {
            parts: VecDeque::new(),
        }
    }

    pub(super) fn start(&self) -> Option<u64> {
        self.parts.front().map(|part| part.bytes.start)
    }

    pub(super) fn take_front(&mut self, piece: &mut Bytes) {
        while let Some(part) = self.parts.front_mut() {
            if !part.take_front(piece) {
                return;
            }
            self.parts.pop_front().expect("found at the front").hand();
        }
    }
}

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
        // A no-op once handed; else the merged GET failed and the joined GET waits again.
        self.handoff.end(None);
    }
}

pub(super) struct JoinedGet {
    handoff: Arc<Handoff>,
}

impl JoinedGet {
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
    Not(Option<Waker>),
    Bytes(Vec<Bytes>),
    Never,
}

impl Handoff {
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

#[cfg(all(test, feature = "shuttle"))]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
