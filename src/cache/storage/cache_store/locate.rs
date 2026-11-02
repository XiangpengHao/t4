//! Where a block a read needs is: held in a tier, being fetched by another
//! read, or remote, claimed for the read's own fetch.

use std::num::NonZeroU8;
use std::ops::Range;

use crate::cache::BlockKey;
use crate::cache::policy::{Admit, BlockRead, ReadIntent};
use crate::cache::storage::disk::DiskChunkReader;
use crate::cache::storage::in_flight::{BlockFetch, FetchClaim};
use crate::cache::storage::index::{Location, PlannedBlock};
use crate::cache::storage::memory::MemoryChunkReader;
use crate::io::sync::Arc;

use super::partial_read::{BlockFate, FetchBlockPlan};
use super::{CacheableFile, ObjectCache};

impl ObjectCache {
    /// Where `block` is, for a read of `context`: held in a tier, being
    /// fetched by another read, or remote, claimed for the read's own fetch.
    pub(super) fn locate(&self, block: &PlannedBlock, context: &BlockRead<'_>) -> Located<'_> {
        let key = block.key();
        let mut location = block.location();
        loop {
            match self.hold(key, location) {
                HeldBlock::Memory(reader) => return Located::Memory(reader),
                HeldBlock::Disk(reader) => return Located::Disk(reader),
                HeldBlock::Remote => {}
            }
            if let Some(fetch) = self.in_flight.get(key) {
                return Located::InFlight(fetch);
            }
            let admit = self.placement.on_remote_read(context);
            let claim = match self.in_flight.claim(key, admit) {
                Ok(claim) => claim,
                Err(fetch) => return Located::InFlight(fetch),
            };
            // A fetch may have cached the block since the index said it was
            // remote, where the index names it now: not the entry just found
            // stale, a chunk an eviction closed. The claim ends with the
            // block there, for any read that joined it meanwhile, and this
            // read takes it from there too.
            match self.blocks.get(key) {
                Some(cached) if Some(cached) != location => {
                    claim.ended(Some(cached));
                    location = Some(cached);
                }
                _ => {
                    return Located::Remote {
                        admit,
                        claim: Some(claim),
                    };
                }
            }
        }
    }

    /// What becomes of block `index` of `file`, fetched by a GET racing
    /// `race`, widened past the read of `range`: dropped if a tier holds it
    /// or another read is fetching it, else placed as the placement policy
    /// says, and claimed for the GET.
    pub(super) fn widened_block(
        &self,
        file: &CacheableFile<'_>,
        range: &Range<u64>,
        intent: Option<ReadIntent>,
        index: u32,
        race: NonZeroU8,
    ) -> FetchBlockPlan {
        let key = BlockKey::new(file.version.id(), index);
        if self.blocks.get(key).is_some() || self.in_flight.get(key).is_some() {
            return FetchBlockPlan::dropped();
        }
        let context = BlockRead::new(
            key,
            file.version.size(),
            range.clone(),
            file.path,
            intent,
            false,
        );
        let admit = self.placement.on_remote_read(&context);
        match self.in_flight.claim(key, admit) {
            Err(_) => FetchBlockPlan::dropped(),
            Ok(mut claim) => match self.blocks.get(key) {
                Some(cached) => {
                    claim.ended(Some(cached));
                    FetchBlockPlan::dropped()
                }
                None => {
                    claim.fetched_by(race);
                    FetchBlockPlan::new(BlockFate::Widened(admit), Some(claim))
                }
            },
        }
    }

    /// Holds the chunk with `key`'s block, the index having said it is at
    /// `location`.
    ///
    /// An entry may name a chunk that no longer holds the block: eviction
    /// closed the chunk, and is about to drop the entry, or moved the block
    /// to the other tier, or the chunk already holds another block. So the
    /// index is read again, until it names the chunk that holds the block,
    /// or no change.
    pub(super) fn hold(&self, key: BlockKey, mut location: Option<Location>) -> HeldBlock<'_> {
        loop {
            let held = match location {
                Some(Location::Memory(chunk)) => {
                    self.memory.read(chunk, key).map(HeldBlock::Memory)
                }
                Some(Location::Disk(chunk)) => {
                    self.disk.chunks().read(chunk, key).map(HeldBlock::Disk)
                }
                None => return HeldBlock::Remote,
            };
            if let Some(held) = held {
                return held;
            }
            let again = self.blocks.get(key);
            if again == location {
                return HeldBlock::Remote;
            }
            location = again;
        }
    }
}

/// A block a read holds while it takes its bytes, or found remote: in
/// neither memory nor disk.
pub(super) enum HeldBlock<'cache> {
    Memory(MemoryChunkReader<'cache>),
    Disk(DiskChunkReader<'cache>),
    Remote,
}

/// Where a block a read needs is ([`ObjectCache::locate`]).
pub(super) enum Located<'c> {
    Memory(MemoryChunkReader<'c>),
    Disk(DiskChunkReader<'c>),
    /// Another read is fetching it.
    InFlight(Arc<BlockFetch>),
    /// No tier holds it, nor is another read fetching it: the read fetches
    /// it, claimed if it is to be cached.
    Remote {
        admit: Admit,
        claim: Option<FetchClaim>,
    },
}
