//! A snapshot of arbitrary bytes taken back, for the fuzz target `snapshot`.

use std::fs::File;
use std::num::NonZeroU32;
use std::path::Path;

use super::ObjectCache;
use crate::cache::policy::{ClockPolicy, PlacementPolicy, Tier};
use crate::cache::storage::CacheFiles;
use crate::cache::storage::snapshot::DiskSnapshot;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

pub struct FuzzedSnapshot {
    body: Vec<u8>,
}

impl FuzzedSnapshot {
    const DISK_CHUNKS: NonZeroU32 = NonZeroU32::new(8).unwrap();

    pub fn new(body: &[u8]) -> Self {
        Self {
            body: body.to_vec(),
        }
    }

    /// Panics unless the body, after a valid header, is refused, or is a
    /// snapshot a cache takes back with its index and tiers agreeing. The
    /// cache's file is made in `dir`.
    pub fn check(&self, dir: &Path) {
        let Some(snapshot) = DiskSnapshot::fuzzed(&self.body, Self::DISK_CHUNKS.get()) else {
            return;
        };
        let cache = Self::cache(dir);
        cache.restore(&snapshot);
        let in_use = cache
            .check_at_rest()
            .unwrap_or_else(|error| panic!("{snapshot:?} taken back: {error}"));
        let blocks: usize = snapshot
            .versions()
            .iter()
            .map(|version| version.blocks().len())
            .sum();
        assert_eq!(in_use.len(), blocks, "{snapshot:?} taken back: {in_use:?}");
        assert!(in_use.iter().all(|&(tier, _)| tier == Tier::Disk));
        for version in snapshot.versions() {
            let size = version.version().size();
            assert_eq!(cache.known_size(version.path()), Some(size));
        }
    }

    fn cache(dir: &Path) -> ObjectCache {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("cache"))
            .expect("a cache file");
        let io = IoWorker::new(NonZeroU32::MIN, vec![file]).expect("an I/O worker");
        let disk = CacheFiles::on(io, NonZeroU32::MIN, Self::DISK_CHUNKS.get()).expect("a tier");
        let placement: Arc<dyn PlacementPolicy> =
            Arc::new(ClockPolicy::new(NonZeroU32::MIN, Self::DISK_CHUNKS));
        ObjectCache::new(NonZeroU32::MIN, disk, placement).expect("a cache")
    }
}
