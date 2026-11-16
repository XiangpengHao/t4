//! [`TestPolicy`]: a policy the cache's tests steer. It decides what the
//! test set, picks victims oldest admitted first, and records every hook
//! the cache calls.

use std::collections::VecDeque;

use crate::cache::policy::{
    Admit, BlockRead, DiskHit, MemoryVictim, PlacementPolicy, Tier, VictimAction,
};
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::Mutex;

/// A hook the cache called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Heard {
    MemoryHit(ChunkId),
    DiskHit(ChunkId),
    Remote(BlockKey),
    Admit(Tier, ChunkId, BlockKey),
}

pub(super) struct TestPolicy {
    admit: Mutex<Admit>,
    disk_hit: Mutex<DiskHit>,
    victim_action: Mutex<VictimAction>,
    /// Each tier's admitted chunks, oldest first: its victims, in order.
    memory: Mutex<VecDeque<ChunkId>>,
    disk: Mutex<VecDeque<ChunkId>>,
    heard: Mutex<Vec<Heard>>,
}

impl TestPolicy {
    /// Remote reads go to memory, disk hits stay, memory victims go to disk.
    pub(super) fn new() -> Self {
        Self {
            admit: Mutex::new(Admit::Memory),
            disk_hit: Mutex::new(DiskHit::Stay),
            victim_action: Mutex::new(VictimAction::ToDisk),
            memory: Mutex::new(VecDeque::new()),
            disk: Mutex::new(VecDeque::new()),
            heard: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn set_admit(&self, admit: Admit) {
        *self.admit.lock().unwrap() = admit;
    }

    pub(super) fn set_disk_hit(&self, disk_hit: DiskHit) {
        *self.disk_hit.lock().unwrap() = disk_hit;
    }

    // The shuttle tests don't steer or check by it.
    #[cfg(not(feature = "shuttle"))]
    pub(super) fn set_victim_action(&self, action: VictimAction) {
        *self.victim_action.lock().unwrap() = action;
    }

    /// Every hook the cache called, in order.
    // The shuttle tests don't steer or check by it.
    #[cfg(not(feature = "shuttle"))]
    pub(super) fn heard(&self) -> Vec<Heard> {
        self.heard.lock().unwrap().clone()
    }

    /// The chunks of `tier` it would pick, oldest first.
    // The shuttle tests don't steer or check by it.
    #[cfg(not(feature = "shuttle"))]
    pub(super) fn queued(&self, tier: Tier) -> Vec<ChunkId> {
        self.queue(tier).lock().unwrap().iter().copied().collect()
    }

    fn queue(&self, tier: Tier) -> &Mutex<VecDeque<ChunkId>> {
        match tier {
            Tier::Memory => &self.memory,
            Tier::Disk => &self.disk,
        }
    }

    fn hear(&self, heard: Heard) {
        self.heard.lock().unwrap().push(heard);
    }

    fn pop(&self, tier: Tier, count: usize) -> Vec<ChunkId> {
        let mut queue = self.queue(tier).lock().unwrap();
        let count = count.min(queue.len());
        queue.drain(..count).collect()
    }
}

impl PlacementPolicy for TestPolicy {
    fn on_memory_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) {
        self.hear(Heard::MemoryHit(chunk));
    }

    fn on_disk_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) -> DiskHit {
        self.hear(Heard::DiskHit(chunk));
        *self.disk_hit.lock().unwrap()
    }

    fn on_remote_read(&self, read: &BlockRead<'_>) -> Admit {
        self.hear(Heard::Remote(read.block()));
        *self.admit.lock().unwrap()
    }

    fn on_admit(&self, tier: Tier, chunk: ChunkId, block: BlockKey) {
        self.hear(Heard::Admit(tier, chunk, block));
        self.queue(tier).lock().unwrap().push_back(chunk);
    }

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim> {
        let action = *self.victim_action.lock().unwrap();
        self.pop(Tier::Memory, count)
            .into_iter()
            .map(|chunk| MemoryVictim::new(chunk, action))
            .collect()
    }

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId> {
        self.pop(Tier::Disk, count)
    }
}
