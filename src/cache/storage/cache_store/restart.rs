use super::super::index::Location;
use super::super::snapshot::DiskSnapshot;
use super::ObjectCache;
use crate::cache::BlockKey;
use crate::cache::policy::Tier;

impl ObjectCache {
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

    /// Closing and joining precede the clean mark: no chunk it names can be overwritten.
    pub(in crate::cache) fn shut_down(&self) -> std::io::Result<()> {
        self.disk.chunks().close();
        if let Some(writer) = &self.writer {
            writer.stop();
        }
        match self.disk.log() {
            Some(log) => log.finish(self.paths.next_id()),
            None => Ok(()),
        }
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn replay_log(
        first: &std::path::Path,
        chunks: u32,
    ) -> Option<DiskSnapshot> {
        let file = std::fs::File::open(first).unwrap();
        super::super::snapshot::PersistenceLog::beside(first, &[file], chunks, Default::default())
            .unwrap()
            .unwrap()
            .inspect()
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn abandon_persistence(&self) {
        if let Some(log) = self.disk.log() {
            log.abandon();
        }
    }

    pub(in crate::cache) fn known_size(&self, path: &[u8]) -> Option<u64> {
        self.paths.known_size(path)
    }
}

/// The writer is joined before the log and its cache-file locks close.
impl Drop for ObjectCache {
    fn drop(&mut self) {
        if self.writer.is_some() {
            let _ = self.shut_down();
        }
    }
}
