//! `paths`: each object's path, to the version of it the cache holds blocks
//! of (`doc/03-cache.md`, decisions 4 and 5).

use std::sync::atomic::{AtomicU64, Ordering};

use bbtree::BBTreeMap;

use super::InsertOrAbort;
use crate::cache::{BLOCK_SIZE, BlockKey, FileId};

/// A version of an object the cache can hold: the id it gave the version,
/// and the object's size, which a read of it is clamped to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct ResolvedFile {
    id: FileId,
    /// At most [`Self::MAX_SIZE`].
    size: u64,
}

impl ResolvedFile {
    /// The largest object the cache holds, 16 TiB less a block: its block
    /// numbers stay below the last a [`BlockKey`] holds, so the number one
    /// past its last block is a block number too.
    const MAX_SIZE: u64 = (BlockKey::INDEX_LIMIT - 1) as u64 * BLOCK_SIZE as u64;

    pub(in crate::cache) fn id(self) -> FileId {
        self.id
    }

    pub(in crate::cache) fn size(self) -> u64 {
        self.size
    }
}

/// Why the cache can't hold an object: reads of it go to the object store
/// alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) enum UncacheableObject {
    /// Its path is longer than [`PathTable::MAX_PATH_LEN`].
    PathTooLong,
    /// It has no e_tag, so nothing tells a new version of it from the one
    /// cached, and the cache would return the old version's bytes.
    NoETag,
    /// It is larger than the 16 TiB a block number reaches.
    TooLarge,
}

/// A path's entry: the version of its object the cache holds blocks of,
/// under the id the cache gave it.
struct FileEntry {
    id: FileId,
    e_tag: Box<str>,
    size: u64,
}

impl FileEntry {
    /// Whether this is the version with `e_tag` and `size`. The size is
    /// part of it too: an object store that kept an e_tag across a change of
    /// size would have the cache return the wrong bytes.
    fn is_version(&self, e_tag: &str, size: u64) -> bool {
        &*self.e_tag == e_tag && self.size == size
    }
}

/// Each path's file, and the ids that new files get.
///
/// Keys are boxed byte strings, as `Index`'s are: bbtree's default strategy
/// keeps their bytes outside its nodes, so a long path changes no node size
/// (decision 4), and gives them inline keys for lookups by `&[u8]`.
pub(in crate::cache) struct PathTable {
    paths: BBTreeMap<Box<[u8]>, FileEntry>,
    /// The id the next new file gets. It only grows, so no id is handed out
    /// twice (decision 5).
    next_id: AtomicU64,
}

impl PathTable {
    /// The longest path the cache holds: object stores' limit, and what a
    /// snapshot's file record has room for (decisions 4 and 10).
    pub(in crate::cache) const MAX_PATH_LEN: usize = 1024;

    pub(in crate::cache) fn new() -> Self {
        Self {
            paths: BBTreeMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    /// The file of the version of the object at `path` with `e_tag` and
    /// `size`: the id the path's entry has for it, or, for a new path or a
    /// new version, a fresh id that takes the path's entry (decision 5). The
    /// version it replaces leaves its blocks behind, for eviction to reclaim.
    ///
    /// Each resolution is one step to the others: two of the same version get
    /// the same id, unless a resolution of another version took the path
    /// between them.
    pub(in crate::cache) fn resolve(
        &self,
        path: &[u8],
        e_tag: Option<&str>,
        size: u64,
    ) -> Result<ResolvedFile, UncacheableObject> {
        let e_tag = Self::check_cacheable(path, e_tag, size)?;
        let held = self
            .paths
            .read(path, |_, entry| {
                entry.is_version(e_tag, size).then_some(entry.id)
            })
            .flatten();
        if let Some(id) = held {
            return Ok(ResolvedFile { id, size });
        }
        // A resolution of the same version that raced this one may put it in
        // first: its id stands, and this fresh one is never handed out.
        let fresh = FileEntry {
            id: self.take_fresh_id(),
            e_tag: e_tag.into(),
            size,
        };
        let fresh_id = fresh.id;
        let id = self
            .paths
            .update_or_insert(path.into(), fresh, |_, stored, fresh| {
                if !stored.is_version(e_tag, size) {
                    *stored = fresh;
                }
                stored.id
            })
            .or_abort()
            .unwrap_or(fresh_id);
        Ok(ResolvedFile { id, size })
    }

    /// Puts back a file record of the shutdown snapshot (decision 10), which
    /// holds one a path. No fresh id is handed out at or below `id` from
    /// then on (decision 5).
    pub(in crate::cache) fn insert_from_snapshot(
        &self,
        path: &[u8],
        e_tag: &str,
        size: u64,
        id: FileId,
    ) -> Result<(), UncacheableObject> {
        Self::check_cacheable(path, Some(e_tag), size)?;
        // Below `FileId::LIMIT`: no overflow.
        self.next_id.fetch_max(id.get() + 1, Ordering::Relaxed);
        let entry = FileEntry {
            id,
            e_tag: e_tag.into(),
            size,
        };
        self.paths.insert(path.into(), entry).or_abort();
        Ok(())
    }

    /// An id no file had. Every change to `next_id` is a read-modify-write,
    /// and those are ordered on it alone: `Relaxed` hands out each id once.
    fn take_fresh_id(&self) -> FileId {
        let mut id = self.next_id.load(Ordering::Relaxed);
        loop {
            // Not `fetch_add`, which would go on past the ids a `FileId`
            // holds: `FileId::new` refuses them, so `id + 1` never overflows.
            let fresh = FileId::new(id);
            match self.next_id.compare_exchange_weak(
                id,
                id + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return fresh,
                Err(current) => id = current,
            }
        }
    }

    /// The e_tag of the object at `path`, if the cache can hold the object.
    fn check_cacheable<'e>(
        path: &[u8],
        e_tag: Option<&'e str>,
        size: u64,
    ) -> Result<&'e str, UncacheableObject> {
        if path.len() > Self::MAX_PATH_LEN {
            return Err(UncacheableObject::PathTooLong);
        }
        if size > ResolvedFile::MAX_SIZE {
            return Err(UncacheableObject::TooLarge);
        }
        e_tag.ok_or(UncacheableObject::NoETag)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Barrier;

    use super::*;

    fn id_of(table: &PathTable, path: &str, e_tag: &str, size: u64) -> FileId {
        table
            .resolve(path.as_bytes(), Some(e_tag), size)
            .unwrap()
            .id()
    }

    #[test]
    fn the_same_version_resolves_to_the_same_id() {
        let table = PathTable::new();
        let file = table.resolve(b"a/b.parquet", Some("v1"), 100).unwrap();
        assert_eq!(file.size(), 100);
        assert_eq!(id_of(&table, "a/b.parquet", "v1", 100), file.id());
        assert_ne!(id_of(&table, "a/c.parquet", "v1", 100), file.id());
    }

    #[test]
    fn a_new_version_takes_a_fresh_id_and_the_path() {
        let table = PathTable::new();
        let v1 = id_of(&table, "p", "v1", 100);
        let v2 = id_of(&table, "p", "v2", 100);
        assert_ne!(v1, v2);
        assert_eq!(id_of(&table, "p", "v2", 100), v2);
        // The path holds one version: going back to the first is new again.
        let v1_again = id_of(&table, "p", "v1", 100);
        assert_ne!(v1_again, v1);
        assert_ne!(v1_again, v2);
        // A different size under the same e_tag is another version too.
        assert_ne!(id_of(&table, "p", "v1", 101), v1_again);
    }

    #[test]
    fn objects_the_cache_cannot_hold_are_refused() {
        let table = PathTable::new();
        assert_eq!(
            table.resolve(b"p", None, 100),
            Err(UncacheableObject::NoETag)
        );
        let longest = vec![b'x'; PathTable::MAX_PATH_LEN];
        assert!(table.resolve(&longest, Some("v1"), 100).is_ok());
        let too_long = vec![b'x'; PathTable::MAX_PATH_LEN + 1];
        assert_eq!(
            table.resolve(&too_long, Some("v1"), 100),
            Err(UncacheableObject::PathTooLong)
        );
        assert!(
            table
                .resolve(b"p", Some("v1"), ResolvedFile::MAX_SIZE)
                .is_ok()
        );
        assert_eq!(
            table.resolve(b"p", Some("v1"), ResolvedFile::MAX_SIZE + 1),
            Err(UncacheableObject::TooLarge)
        );
    }

    #[test]
    fn fresh_ids_never_repeat() {
        let table = PathTable::new();
        let mut seen = HashSet::new();
        for round in 0..50 {
            for path in ["a", "b", "c"] {
                let id = id_of(&table, path, &format!("v{round}"), 10);
                assert!(seen.insert(id), "{id:?} handed out twice");
            }
        }
    }

    #[test]
    fn snapshot_files_resolve_and_fresh_ids_start_past_them() {
        let table = PathTable::new();
        table
            .insert_from_snapshot(b"p", "v1", 100, FileId::new(41))
            .unwrap();
        table
            .insert_from_snapshot(b"q", "v1", 100, FileId::new(7))
            .unwrap();
        assert_eq!(id_of(&table, "p", "v1", 100), FileId::new(41));
        assert_eq!(id_of(&table, "q", "v1", 100), FileId::new(7));
        // Past the largest id loaded, not the last.
        assert_eq!(id_of(&table, "r", "v1", 100), FileId::new(42));

        let too_long = vec![b'x'; PathTable::MAX_PATH_LEN + 1];
        assert_eq!(
            table.insert_from_snapshot(&too_long, "v1", 100, FileId::new(50)),
            Err(UncacheableObject::PathTooLong)
        );
    }

    #[test]
    fn concurrent_resolutions_of_one_version_share_one_id() {
        const THREADS: usize = 8;
        let table = PathTable::new();
        for round in 0..200 {
            let path = format!("objects/{round}");
            let barrier = Barrier::new(THREADS);
            let ids: Vec<FileId> = std::thread::scope(|scope| {
                let resolvers: Vec<_> = (0..THREADS)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            id_of(&table, &path, "v1", 10)
                        })
                    })
                    .collect();
                resolvers
                    .into_iter()
                    .map(|resolver| resolver.join().unwrap())
                    .collect()
            });
            assert!(ids.iter().all(|id| *id == ids[0]), "{ids:?}");
            assert_eq!(id_of(&table, &path, "v1", 10), ids[0]);
        }
    }

    #[test]
    fn concurrent_resolutions_of_two_versions_never_share_an_id() {
        const THREADS: usize = 8;
        let table = PathTable::new();
        for round in 0..200 {
            let path = format!("objects/{round}");
            let barrier = Barrier::new(THREADS);
            let resolved: Vec<(&str, FileId)> = std::thread::scope(|scope| {
                let resolvers: Vec<_> = (0..THREADS)
                    .map(|thread| {
                        let e_tag = if thread % 2 == 0 { "v1" } else { "v2" };
                        let (table, path, barrier) = (&table, &path, &barrier);
                        scope.spawn(move || {
                            barrier.wait();
                            (e_tag, id_of(table, path, e_tag, 10))
                        })
                    })
                    .collect();
                resolvers
                    .into_iter()
                    .map(|resolver| resolver.join().unwrap())
                    .collect()
            });
            for (e_tag, id) in &resolved {
                for (other_e_tag, other_id) in &resolved {
                    assert!(e_tag == other_e_tag || id != other_id, "{resolved:?}");
                }
            }
        }
    }
}
