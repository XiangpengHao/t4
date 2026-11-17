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
    /// Closing and joining precede the clean mark: no chunk it names can be overwritten.
    pub(in crate::cache) fn shut_down(&self) -> std::io::Result<()> {
        let tiers = &self.tiers;
        tiers.disk.chunks().close();
        self.writer.stop();
        match tiers.disk.log() {
            Some(log) => log.finish(tiers.paths.next_id()),
            None => Ok(()),
        }
    }

    #[cfg(all(test, not(feature = "shuttle")))]
    pub(in crate::cache) fn replay_log(first: &std::path::Path, chunks: u32) -> Option<LogBase> {
        let file_system = crate::io::file_system::FileSystem::default();
        let file = file_system.open_existing(first, false).unwrap();
        super::super::persistence::PersistenceLog::beside(&file_system, first, &[file], chunks)
            .unwrap()
            .unwrap()
            .inspect()
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

/// The writer is joined before the log and its cache-file locks close.
impl Drop for ObjectCache {
    fn drop(&mut self) {
        let _ = self.shut_down();
    }
}
