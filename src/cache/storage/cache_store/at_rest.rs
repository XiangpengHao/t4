//! The cache store at rest: whether its index and both tiers agree, for
//! the tests and the fuzzer to check once every read, fill and eviction is
//! done.

use super::ObjectCache;
use crate::cache::ChunkId;
use crate::cache::policy::Tier;
use crate::cache::storage::index::Location;

impl ObjectCache {
    /// Whether the index and both tiers agree, as they do once every read,
    /// fill and eviction is done, and the I/O each started: no block in
    /// flight, each entry names
    /// a chunk that holds its block, each chunk that holds a block has an
    /// entry naming it, and every other chunk is free. If so, the chunks
    /// that hold a block, each with its tier; `Err` says where they don't.
    pub(in crate::cache) fn check_at_rest(
        &self,
    ) -> std::result::Result<Vec<(Tier, ChunkId)>, String> {
        if !self.in_flight.is_empty() {
            return Err("a block is still in flight".to_owned());
        }
        let mut memory =
            (self.memory.blocks_at_rest()).map_err(|error| format!("memory {error}"))?;
        let mut disk =
            (self.disk.chunks().blocks_at_rest()).map_err(|error| format!("disk {error}"))?;
        let mut in_use = Vec::new();
        for (block, location) in self.blocks.entries() {
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
