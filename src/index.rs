use std::mem;
use std::ops::ControlFlow;

use crate::disk::{Extent, FreedExtents, WalCommit, WalTombstoneCommit};
use crate::input_kv::{T4Key, ValueRef};
use crate::io::sync::Arc;
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
// writer, whose entry leaves at once. Both orderings converge to the same
// final state, matching what `Wal::replay` would reconstruct after a
// remount.
//
// A live entry holds its value's extent. An entry that leaves the index
// drops its handle, and the extent's space goes back to the allocator once
// no read holds it either.
//
// Tombstones live in the index (not just on disk) so that a put whose
// LSN is older than a concurrent remove's LSN can be rejected even when
// the key wasn't present beforehand. Their cost is one entry per
// deleted key; they're dropped on remount because replay produces only
// live entries.
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum IndexEntry {
    Live { lsn: u64, extent: Arc<Extent> },
    Tomb { lsn: u64 },
}

impl IndexEntry {
    fn lsn(&self) -> u64 {
        match self {
            Self::Live { lsn, .. } | Self::Tomb { lsn } => *lsn,
        }
    }

    fn live_extent(&self) -> Option<&Arc<Extent>> {
        match self {
            Self::Live { extent, .. } => Some(extent),
            Self::Tomb { .. } => None,
        }
    }

    /// Keeps whichever of `self` and `other` comes from the newer record,
    /// and returns the one that loses.
    fn keep_newer(&mut self, other: Self) -> Self {
        if self.lsn() >= other.lsn() {
            other
        } else {
            mem::replace(self, other)
        }
    }

    /// The location of an entry replay replaced, which replay frees itself.
    fn into_replaced(self) -> Option<ValueRef> {
        match self {
            Self::Live { extent, .. } => Some(Extent::into_replaced(extent)),
            Self::Tomb { .. } => None,
        }
    }
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

    /// The extent of `key`'s live value: a handle that keeps its space from
    /// being reused while the caller reads it.
    pub(crate) fn get(&self, key: &[u8]) -> Option<Arc<Extent>> {
        self.map
            .read(key, |_, entry| entry.live_extent().cloned())
            .flatten()
    }

    pub(crate) fn apply_put(&self, key: T4Key, commit: WalCommit) {
        let (lsn, extent) = commit.into_parts();
        self.apply(key, IndexEntry::Live { lsn, extent });
    }

    /// Whether the tombstone took a live value's place.
    pub(crate) fn apply_remove(&self, key: T4Key, commit: WalTombstoneCommit) -> bool {
        let lsn = commit.lsn();
        let left = self.apply(key, IndexEntry::Tomb { lsn });
        matches!(left, Some(IndexEntry::Live { .. }))
    }

    /// Puts `ours` in `key`'s place unless the entry there is newer, and
    /// returns the entry that left the index: the one replaced, or ours.
    /// It drops in the caller, out of the leaf's latch.
    fn apply(&self, key: T4Key, ours: IndexEntry) -> Option<IndexEntry> {
        let left = self
            .map
            .update_or_insert(Self::map_key(key), ours, |_, stored, ours| {
                stored.keep_newer(ours)
            });
        match left {
            Ok(left) => left,
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
    pub(crate) fn iter_live(&self) -> impl Iterator<Item = (Box<[u8]>, Arc<Extent>)> + '_ {
        self.map
            .range_cloned::<[u8], _>(..)
            .filter_map(|(key, entry)| match entry {
                IndexEntry::Live { extent, .. } => Some((key, extent)),
                IndexEntry::Tomb { .. } => None,
            })
    }

    /// The index as replay fills it, its extents going back through `freed`.
    pub(crate) fn replay<'a>(&'a self, freed: &'a FreedExtents) -> IndexReplay<'a> {
        IndexReplay { index: self, freed }
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

/// An index being replayed into: replay hands it value locations, which
/// it keeps as extents.
pub(crate) struct IndexReplay<'a> {
    index: &'a Index,
    freed: &'a FreedExtents,
}

/// Replay reads the WAL alone, in LSN order: each record replaces what came
/// before it, and a tombstone forgets its key. Replay frees what it
/// replaces itself, so those extents leave without freeing it again.
impl ReplayIndex for IndexReplay<'_> {
    fn insert(&mut self, key: T4Key, lsn: u64, value: ValueRef) -> Option<ValueRef> {
        let entry = IndexEntry::Live {
            lsn,
            extent: Extent::new(value, self.freed),
        };
        match self.index.map.insert(Index::map_key(key), entry) {
            Ok(old) => old.and_then(IndexEntry::into_replaced),
            Err(error) => Index::insert_failed(error),
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueRef> {
        self.index
            .map
            .remove(key)
            .and_then(IndexEntry::into_replaced)
    }
}
