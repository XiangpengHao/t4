use super::super::index::Location;
use super::super::snapshot::{DiskSnapshot, VersionOnDisk};
use super::ObjectCache;
use crate::cache::BlockKey;
use crate::cache::policy::Tier;

impl ObjectCache {
    /// Safe mid-write: the index names a chunk only once its block is written whole.
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

    /// The snapshot was checked as it was read: each chunk in it once, and in the tier.
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

    /// For a process that exits without dropping the cache. The tier closes first, so
    /// the snapshot names only chunks no write will touch.
    pub(in crate::cache) fn shut_down(&self) -> std::io::Result<()> {
        self.disk.chunks().close();
        if let Some(writer) = &self.writer {
            writer.stop();
        }
        match self.disk.snapshot() {
            Some(snapshot) => snapshot.keep(&self.capture_snapshot()),
            None => Ok(()),
        }
    }

    pub(in crate::cache) fn known_size(&self, path: &[u8]) -> Option<u64> {
        self.paths.known_size(path)
    }
}

/// Kept again after `shut_down`: writes in flight then may have published blocks since.
impl Drop for ObjectCache {
    fn drop(&mut self) {
        if self.writer.is_some() {
            let _ = self.shut_down();
        }
    }
}
