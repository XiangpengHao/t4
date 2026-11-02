//! What the cache keeps across a restart (`doc/10-spiral-integration.md`,
//! decision 7): its disk tier, captured as the cache drops and restored as
//! the next cache on its files opens. Memory's blocks go with the process.

use super::super::index::Location;
use super::super::snapshot::{DiskSnapshot, VersionOnDisk};
use super::ObjectCache;
use crate::cache::BlockKey;
use crate::cache::policy::Tier;

impl ObjectCache {
    /// What the disk tier holds now: each path's version with blocks on
    /// disk, those blocks' chunks, and the id the next version takes.
    ///
    /// Each block it names was written whole before it was published, and
    /// so before the index named its chunk; a block being written isn't
    /// named yet. A version a newer one took the path from is left out:
    /// no read finds it again.
    pub(super) fn capture_snapshot(&self) -> DiskSnapshot {
        let mut held = Vec::new();
        self.paths.scan(|path, e_tag, version| {
            held.push((Box::<[u8]>::from(path), Box::<str>::from(e_tag), version));
        });
        let versions = held
            .into_iter()
            .filter_map(|(path, e_tag, version)| {
                let blocks = self.blocks.on_disk(version);
                (!blocks.is_empty()).then(|| VersionOnDisk::new(path, e_tag, version, blocks))
            })
            .collect();
        DiskSnapshot::new(self.paths.next_id(), versions)
    }

    /// Puts back what `snapshot` says the disk tier held, as the cache
    /// opens: each version's entry, each block's entry and chunk, and each
    /// chunk told to the placement policy as admitted, as a fill tells it.
    /// The snapshot was checked whole as it was read: each chunk in it once,
    /// and in the tier.
    pub(super) fn restore(&self, snapshot: &DiskSnapshot) {
        self.paths.resume_ids_at(snapshot.next_id());
        for on_disk in snapshot.versions() {
            let version = on_disk.version();
            self.paths.restore(on_disk.path(), on_disk.e_tag(), version);
            for &(index, chunk) in on_disk.blocks() {
                let key = BlockKey::new(version.id(), index);
                self.disk.chunks().restore(chunk, key);
                let placed = self.blocks.insert_if_absent(key, Location::Disk(chunk));
                assert!(placed, "a snapshot names a block once");
                self.placement.on_admit(Tier::Disk, chunk, key);
            }
        }
    }

    /// The size of the version the cache holds of the file at `path`, if it
    /// holds one: resolved by a read since it opened, or restored.
    pub(in crate::cache) fn known_size(&self, path: &[u8]) -> Option<u64> {
        self.paths.known_size(path)
    }
}

/// The disk tier kept as the cache drops, for the next cache on its files,
/// before the files close and their lock goes with them: the cache drops
/// when nothing reads through it any more. A snapshot that can't be kept
/// leaves the next cache cold.
impl Drop for ObjectCache {
    fn drop(&mut self) {
        if let Some(snapshot) = self.disk.snapshot() {
            let _ = snapshot.keep(&self.capture_snapshot());
        }
    }
}
