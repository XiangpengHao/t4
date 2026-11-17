use std::num::NonZeroU32;
use std::ops::Range;
use std::path::Path;

use bytes::Bytes;

use super::ObjectCache;
use crate::cache::policy::{ClockPolicy, PlacementPolicy, Tier};
use crate::cache::storage::persistence::{LogBase, VersionOnDisk};
use crate::cache::storage::{CacheFiles, FileVersion};
use crate::cache::{BLOCK_SIZE_U64, ChunkId, FileVersionId};
use crate::io::file_system::FileSystem;
use crate::io::io_worker::IoWorker;
use crate::io::sync::Arc;

/// A version's bytes over a range, from its path, e-tag and size; `None` for a version the store
/// never held.
pub type VersionBytes<'a> = dyn Fn(&[u8], &str, u64, Range<u64>) -> Option<Bytes> + 'a;

impl ObjectCache {
    /// A build after a kill now would serve each block the log names from the chunk it names:
    /// that chunk must hold what `expected` gives for the block's version and range.
    pub(in crate::cache) async fn check_what_a_kill_keeps(
        &self,
        expected: &VersionBytes<'_>,
    ) -> Result<(), String> {
        self.wait_for_disk_writes();
        let Some(log) = self.tiers.disk.log() else {
            return Ok(());
        };
        let base = log.replay().ok_or_else(|| {
            "the log no longer replays: a build after a kill would start cold".to_owned()
        })?;
        for version in base.versions() {
            let size = version.version().size();
            for &(index, chunk) in version.blocks() {
                let named = || {
                    let path = String::from_utf8_lossy(version.path());
                    let e_tag = version.e_tag();
                    format!("{chunk:?}, named for block {index} of {path} at e-tag {e_tag}")
                };
                let start = u64::from(index) * BLOCK_SIZE_U64;
                let range = start..size.min(start + BLOCK_SIZE_U64);
                let want = expected(version.path(), version.e_tag(), size, range)
                    .ok_or_else(|| format!("{}: a version the store never held", named()))?;
                let held = (self.tiers.disk.read_whole_chunk(chunk).await)
                    .map_err(|error| format!("{}: {error}", named()))?;
                if held.as_slice()[..want.len()] != want[..] {
                    return Err(format!("{} holds other bytes", named()));
                }
            }
        }
        Ok(())
    }
}

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
        cache.tiers.restore(&base);
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
        let file = (FileSystem::default())
            .open_cache_file(&dir.join("cache"), false)
            .expect("a cache file");
        let io = IoWorker::new(NonZeroU32::MIN, vec![file]).expect("an I/O worker");
        let disk = CacheFiles::on(io, NonZeroU32::MIN, Self::DISK_CHUNKS.get()).expect("a tier");
        let placement: Arc<dyn PlacementPolicy> =
            Arc::new(ClockPolicy::new(NonZeroU32::MIN, Self::DISK_CHUNKS));
        ObjectCache::new(NonZeroU32::MIN, disk, placement).expect("a cache")
    }
}
