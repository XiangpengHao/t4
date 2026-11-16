use std::ops::{ControlFlow, Range};

use bbtree::BBTreeMap;

use crate::cache::InsertOrAbort;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, FileVersionId};
use crate::io::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) struct FileVersion {
    id: FileVersionId,
    size: u64,
}

impl FileVersion {
    /// Less a block, so the number one past an object's last block is a block number too.
    const MAX_SIZE: u64 = (BlockKey::INDEX_LIMIT - 1) as u64 * BLOCK_SIZE_U64;

    pub(in crate::cache) fn named(id: FileVersionId, size: u64) -> Option<Self> {
        (size <= Self::MAX_SIZE).then_some(Self { id, size })
    }

    pub(in crate::cache) fn id(self) -> FileVersionId {
        self.id
    }

    pub(in crate::cache) fn size(self) -> u64 {
        self.size
    }

    pub(in crate::cache) fn block_range(self, index: u32) -> Range<u64> {
        let start = u64::from(index) * BLOCK_SIZE_U64;
        assert!(start < self.size, "block {index} is past the file's end");
        start..(start + BLOCK_SIZE_U64).min(self.size)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache) enum UncacheableObject {
    PathTooLong,
    /// Nothing would tell a new version from the cached one.
    NoETag,
    TooLarge,
}

struct FileEntry {
    id: FileVersionId,
    e_tag: Box<str>,
    size: u64,
}

impl FileEntry {
    /// Size too: a store that kept an e_tag across a size change would serve wrong bytes.
    fn is_version(&self, e_tag: &str, size: u64) -> bool {
        &*self.e_tag == e_tag && self.size == size
    }
}

/// Boxed keys: bbtree keeps their bytes out of its nodes, so long paths change no node size.
pub(in crate::cache) struct PathTable {
    paths: BBTreeMap<Box<[u8]>, FileEntry>,
    next_id: AtomicU64,
}

impl PathTable {
    /// Object stores' limit.
    pub(in crate::cache) const MAX_PATH_LEN: usize = 1024;

    pub(in crate::cache) fn new() -> Self {
        Self {
            paths: BBTreeMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    /// A replaced version leaves its blocks behind, for eviction to reclaim.
    pub(in crate::cache) fn resolve(
        &self,
        path: &[u8],
        e_tag: Option<&str>,
        size: u64,
    ) -> Result<FileVersion, UncacheableObject> {
        let e_tag = Self::check_cacheable(path, e_tag, size)?;
        let held = self
            .paths
            .read(path, |_, entry| {
                entry.is_version(e_tag, size).then_some(entry.id)
            })
            .flatten();
        if let Some(id) = held {
            return Ok(FileVersion { id, size });
        }
        // A racing resolution of this version may win: its id stands, and this one is wasted.
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
        Ok(FileVersion { id, size })
    }

    pub(in crate::cache) fn known_size(&self, path: &[u8]) -> Option<u64> {
        self.paths.read(path, |_, entry| entry.size)
    }

    pub(in crate::cache) fn scan(&self, mut found: impl FnMut(&[u8], &str, FileVersion)) {
        self.paths.scan::<[u8], ()>(.., |path, entry| {
            let version = FileVersion {
                id: entry.id,
                size: entry.size,
            };
            found(path, &entry.e_tag, version);
            ControlFlow::Continue(())
        });
    }

    pub(in crate::cache) fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    /// Only as the cache opens, before any read resolves a path.
    pub(in crate::cache) fn restore(&self, path: &[u8], e_tag: &str, version: FileVersion) {
        let entry = FileEntry {
            id: version.id,
            e_tag: e_tag.into(),
            size: version.size,
        };
        let held = self.paths.insert(path.into(), entry).or_abort();
        assert!(held.is_none(), "a snapshot names a path once");
    }

    pub(in crate::cache) fn resume_ids_at(&self, next_id: u64) {
        assert!(self.next_id() <= next_id, "ids only grow");
        self.next_id.store(next_id, Ordering::Relaxed);
    }

    /// `Relaxed` hands out each id once: every change to `next_id` is a read-modify-write.
    fn take_fresh_id(&self) -> FileVersionId {
        let mut id = self.next_id.load(Ordering::Relaxed);
        loop {
            // Not `fetch_add`, which would go on past the ids a `FileVersionId`
            // holds: `FileVersionId::new` refuses them, so `id + 1` never overflows.
            let fresh = FileVersionId::new(id);
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

    fn check_cacheable<'e>(
        path: &[u8],
        e_tag: Option<&'e str>,
        size: u64,
    ) -> Result<&'e str, UncacheableObject> {
        if path.len() > Self::MAX_PATH_LEN {
            return Err(UncacheableObject::PathTooLong);
        }
        if size > FileVersion::MAX_SIZE {
            return Err(UncacheableObject::TooLarge);
        }
        e_tag.ok_or(UncacheableObject::NoETag)
    }
}

// Std threads, which shuttle's atomics refuse.
#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::collections::HashSet;
    use std::sync::Barrier;

    use super::*;

    fn id_of(table: &PathTable, path: &str, e_tag: &str, size: u64) -> FileVersionId {
        table
            .resolve(path.as_bytes(), Some(e_tag), size)
            .unwrap()
            .id()
    }

    #[test]
    fn blocks_are_whole_but_the_last() {
        let block = BLOCK_SIZE_U64;
        let version = PathTable::new()
            .resolve(b"p", Some("v1"), 2 * block + 10)
            .unwrap();
        assert_eq!(version.block_range(0), 0..block);
        assert_eq!(version.block_range(1), block..2 * block);
        assert_eq!(version.block_range(2), 2 * block..2 * block + 10);
    }

    #[test]
    #[should_panic(expected = "past the file's end")]
    fn no_block_lies_past_the_end() {
        let version = PathTable::new()
            .resolve(b"p", Some("v1"), BLOCK_SIZE_U64)
            .unwrap();
        version.block_range(1);
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
                .resolve(b"p", Some("v1"), FileVersion::MAX_SIZE)
                .is_ok()
        );
        assert_eq!(
            table.resolve(b"p", Some("v1"), FileVersion::MAX_SIZE + 1),
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
    fn concurrent_resolutions_of_one_version_share_one_id() {
        const THREADS: usize = 8;
        let table = PathTable::new();
        for round in 0..200 {
            let path = format!("objects/{round}");
            let barrier = Barrier::new(THREADS);
            let ids: Vec<FileVersionId> = std::thread::scope(|scope| {
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
            let resolved: Vec<(&str, FileVersionId)> = std::thread::scope(|scope| {
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
