use std::fs::File;
use std::num::NonZeroU32;
use std::path::Path;

use super::ObjectCache;
use crate::cache::policy::{ClockPolicy, PlacementPolicy, Tier};
use crate::cache::storage::persistence::{LogBase, VersionOnDisk};
use crate::cache::storage::{CacheFiles, FileVersion};
use crate::cache::{ChunkId, FileVersionId};
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

pub struct FuzzedLogBase {
    body: Vec<u8>,
}

pub struct FuzzedVersion {
    id: u64,
    size: u64,
    path: Vec<u8>,
    e_tag: String,
    blocks: Vec<(u32, u32)>,
}

impl FuzzedVersion {
    /// `blocks` are (block number, chunk number) pairs.
    pub fn new(id: u64, size: u64, path: Vec<u8>, e_tag: String, blocks: Vec<(u32, u32)>) -> Self {
        Self {
            id,
            size,
            path,
            e_tag,
            blocks,
        }
    }

    fn on_disk(self) -> VersionOnDisk {
        let id = FileVersionId::new(self.id % FileVersionId::LIMIT);
        let version = FileVersion::named(id, self.size).expect("a size a cache holds");
        let blocks = self
            .blocks
            .into_iter()
            .map(|(index, chunk)| (index, ChunkId::new(chunk)))
            .collect();
        VersionOnDisk::new(self.path.into(), self.e_tag.into(), version, blocks)
    }
}

impl FuzzedLogBase {
    const DISK_CHUNKS: NonZeroU32 = NonZeroU32::new(8).unwrap();

    /// `body` follows a valid header.
    pub fn new(body: &[u8]) -> Self {
        Self {
            body: body.to_vec(),
        }
    }

    pub fn of_versions(next_id: u64, versions: Vec<FuzzedVersion>) -> Self {
        let versions = versions.into_iter().map(FuzzedVersion::on_disk).collect();
        Self {
            body: LogBase::new(next_id, versions).fuzzed_body(Self::DISK_CHUNKS.get()),
        }
    }

    pub fn check(&self, dir: &Path) {
        let Some(base) = LogBase::fuzzed(&self.body, Self::DISK_CHUNKS.get()) else {
            return;
        };
        let cache = Self::cache(dir);
        cache.restore(&base);
        let in_use = cache
            .check_at_rest()
            .unwrap_or_else(|error| panic!("{base:?} taken back: {error}"));
        let blocks: usize = base
            .versions()
            .iter()
            .map(|version| version.blocks().len())
            .sum();
        assert_eq!(in_use.len(), blocks, "{base:?} taken back: {in_use:?}");
        assert!(in_use.iter().all(|&(tier, _)| tier == Tier::Disk));
        for version in base.versions() {
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
