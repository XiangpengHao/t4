use std::collections::HashMap;

use verified::input_kv::{T4Key, ValueRef};

use crate::disk::{WalCommit, WalTombstoneCommit};
use crate::io::error::{Error, Result};
use crate::io::sync::{RwLock, RwLockReadGuard};

// ---------------------------------------------------------------------------
// Index
//
// The in-memory view over the WAL. Each entry carries the LSN of the WAL
// record that produced it, so `apply_put` / `apply_remove` can enforce the
// store's core invariant:
//
//     for every key K, index[K] reflects the WAL record for K with the
//     highest LSN
//
// The WAL's LSN order is authoritative. If the index lock is acquired in
// a different order than LSNs were assigned, the LSN check rejects the
// out-of-order writer; the "loser" releases its own value space. Both
// orderings converge to the same final state, matching what `Wal::replay`
// would reconstruct after a remount.
//
// Tombstones live in the index (not just on disk) so that a put whose
// LSN is older than a concurrent remove's LSN can be rejected even when
// the key wasn't present beforehand. Their cost is one entry per
// deleted key; they're dropped on remount because replay produces only
// live entries.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum IndexEntry {
    Live { lsn: u64, vref: ValueRef },
    Tomb { lsn: u64 },
}

impl IndexEntry {
    fn lsn(&self) -> u64 {
        match self {
            Self::Live { lsn, .. } | Self::Tomb { lsn } => *lsn,
        }
    }

    fn live_vref(&self) -> Option<ValueRef> {
        match self {
            Self::Live { vref, .. } => Some(*vref),
            Self::Tomb { .. } => None,
        }
    }
}

pub(crate) enum ApplyPutOutcome {
    /// Our LSN was the newest; the index now points at our vref.
    /// `displaced` is the previous vref if the key was Live before.
    Inserted { displaced: Option<ValueRef> },
    /// A newer LSN already won the race. Our vref is shadowed by the
    /// newer WAL record — release it so the hole list stays consistent
    /// with what replay would derive.
    Rejected { our_vref: ValueRef },
}

pub(crate) enum ApplyRemoveOutcome {
    /// Our tombstone won and the key was Live; release the old vref.
    Removed { displaced: ValueRef },
    /// Our tombstone won but the key wasn't Live (absent, or already a
    /// tombstone with a smaller LSN).
    MarkedAbsent,
    /// A newer LSN already won the race; nothing to release.
    Rejected,
}

#[derive(Debug)]
pub(crate) struct Index {
    map: RwLock<HashMap<T4Key, IndexEntry>>,
}

impl Index {
    pub(crate) fn from_replay(initial: HashMap<T4Key, ValueRef>) -> Self {
        // Replay produces only Live entries; they all predate any
        // live operation, so lsn=0 is a safe lower bound (the WAL's
        // post-replay `next_lsn` is strictly greater than any LSN we
        // can newly allocate, so live ops always displace replayed
        // entries — unless a fresh remove for that key gets to them
        // first, which is also correct).
        let map = initial
            .into_iter()
            .map(|(k, v)| (k, IndexEntry::Live { lsn: 0, vref: v }))
            .collect();
        Self {
            map: RwLock::new(map),
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<ValueRef>> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map.get(key).and_then(IndexEntry::live_vref))
    }

    pub(crate) fn apply_put(&self, key: T4Key, commit: WalCommit<'_>) -> Result<ApplyPutOutcome> {
        let (lsn, vref) = commit.into_parts();
        let mut map = self.map.write().map_err(|_| Error::LockPoisoned)?;
        if let Some(existing) = map.get(&key)
            && existing.lsn() >= lsn
        {
            return Ok(ApplyPutOutcome::Rejected { our_vref: vref });
        }
        let displaced = map
            .insert(key, IndexEntry::Live { lsn, vref })
            .and_then(|e| e.live_vref());
        Ok(ApplyPutOutcome::Inserted { displaced })
    }

    pub(crate) fn apply_remove(
        &self,
        key: T4Key,
        commit: WalTombstoneCommit,
    ) -> Result<ApplyRemoveOutcome> {
        let lsn = commit.lsn();
        let mut map = self.map.write().map_err(|_| Error::LockPoisoned)?;
        if let Some(existing) = map.get(&key)
            && existing.lsn() >= lsn
        {
            return Ok(ApplyRemoveOutcome::Rejected);
        }
        let displaced = map
            .insert(key, IndexEntry::Tomb { lsn })
            .and_then(|e| e.live_vref());
        match displaced {
            Some(vref) => Ok(ApplyRemoveOutcome::Removed { displaced: vref }),
            None => Ok(ApplyRemoveOutcome::MarkedAbsent),
        }
    }

    pub(crate) fn len(&self) -> Result<usize> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map
            .values()
            .filter(|e| matches!(e, IndexEntry::Live { .. }))
            .count())
    }

    pub(crate) fn is_empty(&self) -> Result<bool> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map.values().all(|e| !matches!(e, IndexEntry::Live { .. })))
    }

    /// Acquire a read-locked view of the index. The returned guard blocks
    /// concurrent puts/removes for its lifetime, so vrefs yielded by
    /// `iter_live` stay valid while the caller reads their backing slots
    /// asynchronously.
    pub(crate) fn read_locked(&self) -> Result<LockedView<'_>> {
        let guard = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(LockedView { guard })
    }
}

pub(crate) struct LockedView<'a> {
    guard: RwLockReadGuard<'a, HashMap<T4Key, IndexEntry>>,
}

impl LockedView<'_> {
    pub(crate) fn iter_live(&self) -> impl Iterator<Item = (&T4Key, ValueRef)> + '_ {
        self.guard
            .iter()
            .filter_map(|(k, e)| e.live_vref().map(|v| (k, v)))
    }
}
