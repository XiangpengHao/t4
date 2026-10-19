use std::mem;
use std::ops::ControlFlow;

use crate::disk::{WalCommit, WalTombstoneCommit};
use crate::input_kv::{T4Key, ValueRef};
use crate::wal::replay::ReplayIndex;
use bbtree::BBTreeMap;
use bbtree::bbtree_map::InsertError;

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
// The WAL's LSN order is authoritative. If writers reach the index in a
// different order than LSNs were assigned, the LSN check — made under the
// key's leaf latch, in one step with the change — rejects the out-of-order
// writer; the "loser" releases its own value space. Both orderings
// converge to the same final state, matching what `Wal::replay` would
// reconstruct after a remount.
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

/// Keys are boxed byte strings: bbtree's default strategy gives them inline
/// keys and hash tags, for inserts and for lookups by `&[u8]` alike. A
/// `T4Key` newtype would need `WithInlineKey`, whose lookups by `&[u8]`
/// compare full keys.
pub(crate) struct Index {
    map: BBTreeMap<Box<[u8]>, IndexEntry>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index").finish_non_exhaustive()
    }
}

impl Index {
    pub(crate) fn new() -> Self {
        Self {
            map: BBTreeMap::new(),
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueRef> {
        self.map.get_copied(key).and_then(|e| e.live_vref())
    }

    pub(crate) fn apply_put(&self, key: T4Key, commit: WalCommit<'_>) -> ApplyPutOutcome {
        let (lsn, vref) = commit.into_parts();
        let ours = IndexEntry::Live { lsn, vref };
        let outcome = self
            .map
            .update_or_insert(Self::map_key(key), ours, |_, stored, ours| {
                if stored.lsn() >= lsn {
                    return ApplyPutOutcome::Rejected { our_vref: vref };
                }
                ApplyPutOutcome::Inserted {
                    displaced: mem::replace(stored, ours).live_vref(),
                }
            });
        match outcome {
            Ok(Some(outcome)) => outcome,
            Ok(None) => ApplyPutOutcome::Inserted { displaced: None },
            Err(error) => Self::insert_failed(error),
        }
    }

    pub(crate) fn apply_remove(
        &self,
        key: T4Key,
        commit: WalTombstoneCommit,
    ) -> ApplyRemoveOutcome {
        let lsn = commit.lsn();
        let ours = IndexEntry::Tomb { lsn };
        let outcome = self
            .map
            .update_or_insert(Self::map_key(key), ours, |_, stored, ours| {
                if stored.lsn() >= lsn {
                    return ApplyRemoveOutcome::Rejected;
                }
                match mem::replace(stored, ours).live_vref() {
                    Some(displaced) => ApplyRemoveOutcome::Removed { displaced },
                    None => ApplyRemoveOutcome::MarkedAbsent,
                }
            });
        match outcome {
            Ok(Some(outcome)) => outcome,
            Ok(None) => ApplyRemoveOutcome::MarkedAbsent,
            Err(error) => Self::insert_failed(error),
        }
    }

    pub(crate) fn len(&self) -> usize {
        let mut live = 0;
        self.map.scan::<[u8], ()>(.., |_, entry| {
            if matches!(entry, IndexEntry::Live { .. }) {
                live += 1;
            }
            ControlFlow::Continue(())
        });
        live
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map
            .scan::<[u8], ()>(.., |_, entry| match entry {
                IndexEntry::Live { .. } => ControlFlow::Break(()),
                IndexEntry::Tomb { .. } => ControlFlow::Continue(()),
            })
            .is_none()
    }

    /// The live entries, in key order. It holds nothing between items, so
    /// it is no snapshot by itself: the caller pauses writes for one
    /// (`DiskData::pause_writes`).
    pub(crate) fn iter_live(&self) -> impl Iterator<Item = (Box<[u8]>, ValueRef)> + '_ {
        self.map
            .range_cloned::<[u8], _>(..)
            .filter_map(|(key, entry)| entry.live_vref().map(|vref| (key, vref)))
    }

    fn map_key(key: T4Key) -> Box<[u8]> {
        key.into_bytes().into_boxed_slice()
    }

    /// The WAL holds the record already, so an index without it would
    /// disagree with what replay rebuilds: there is no state to fall back
    /// to, as there is none when a std map's insert runs out of memory.
    fn insert_failed(_: InsertError<Box<[u8]>, IndexEntry>) -> ! {
        eprintln!("t4: out of memory inserting into the index");
        std::process::abort()
    }
}

/// Replay reads the WAL alone, in LSN order: each record replaces what came
/// before it, and a tombstone forgets its key.
impl ReplayIndex for Index {
    fn insert(&mut self, key: T4Key, lsn: u64, value: ValueRef) -> Option<ValueRef> {
        let entry = IndexEntry::Live { lsn, vref: value };
        match self.map.insert(Self::map_key(key), entry) {
            Ok(old) => old.and_then(|e| e.live_vref()),
            Err(error) => Self::insert_failed(error),
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueRef> {
        self.map.remove(key).and_then(|e| e.live_vref())
    }
}
