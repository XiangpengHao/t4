use std::ops::Range;

use bytes::Bytes;

use super::ObjectCache;
use crate::cache::policy::Tier;
use crate::cache::storage::index::Location;
use crate::cache::{BLOCK_SIZE_U64, ChunkId};

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

    pub(in crate::cache) fn check_at_rest(
        &self,
    ) -> std::result::Result<Vec<(Tier, ChunkId)>, String> {
        self.wait_for_disk_writes();
        if !self.in_flight.is_empty() {
            return Err("a block is still in flight".to_owned());
        }
        let mut memory =
            (self.tiers.memory.blocks_at_rest()).map_err(|error| format!("memory {error}"))?;
        let mut disk =
            (self.tiers.disk.chunks().blocks_at_rest()).map_err(|error| format!("disk {error}"))?;
        let mut in_use = Vec::new();
        for (block, location) in self.tiers.blocks.entries() {
            let (chunks, tier, chunk) = match location {
                Location::Memory(chunk) => (&mut memory, Tier::Memory, chunk),
                Location::Disk(chunk) => (&mut disk, Tier::Disk, chunk),
            };
            in_use.push((tier, chunk));
            // Taken, so that a second entry naming the chunk finds none.
            let held = chunks
                .get_mut(chunk.index() as usize)
                .and_then(Option::take);
            if held != Some(block) {
                return Err(format!(
                    "{block:?} is at {location:?}, which holds {held:?}"
                ));
            }
        }
        for (tier, chunks) in [("memory", memory), ("disk", disk)] {
            if let Some((index, block)) = (0..)
                .zip(chunks)
                .find_map(|(index, block)| Some((index, block?)))
            {
                return Err(format!(
                    "{tier} chunk {index} holds {block:?}, which no entry names"
                ));
            }
        }
        Ok(in_use)
    }
}
