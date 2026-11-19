use super::super::index::Location;
use super::super::persistence::LogBase;
use super::{ObjectCache, Tiers};
use crate::cache::BlockKey;
use crate::cache::policy::Tier;

impl Tiers {
    /// The base was checked as it was replayed: each chunk in it once, and in the tier.
    pub(super) fn restore(&self, base: &LogBase) {
        self.paths.resume_ids_at(base.next_id());
        for on_disk in base.versions() {
            let version = on_disk.version();
            self.paths.restore(on_disk.path(), on_disk.e_tag(), version);
            for &(index, chunk) in on_disk.blocks() {
                let key = BlockKey::new(version.id(), index);
                self.disk.chunks().restore(chunk, key);
                let placed = self.blocks.insert_if_absent(key, Location::Disk(chunk));
                assert!(placed, "a base names a block once");
                self.placement.on_admit(Tier::Disk, chunk, key);
            }
        }
    }
}

impl ObjectCache {
    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn replay_log(
        log: &std::path::Path,
        first: &std::path::Path,
        chunks: u32,
    ) -> Option<LogBase> {
        let file_system = crate::io::file_system::FileSystem::default();
        let file = file_system.open_existing(first, false).unwrap();
        super::super::persistence::PersistenceLog::at(&file_system, log, &[file], chunks)
            .unwrap()
            .unwrap()
            .replay()
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache) fn abandon_persistence(&self) {
        if let Some(log) = self.tiers.disk.log() {
            log.abandon();
        }
    }

    pub(in crate::cache) fn known_size(&self, path: &[u8]) -> Option<u64> {
        self.tiers.paths.known_size(path)
    }
}

/// Joins the writer, once it has written what waits, before the tier, the log and the cache
/// files' locks close, then appends its last adds.
impl Drop for ObjectCache {
    fn drop(&mut self) {
        let tiers = &self.tiers;
        self.writer.stop();
        tiers.disk.chunks().close();
        if let Some(log) = tiers.disk.log() {
            let _ = log.finish();
        }
    }
}
