use std::num::NonZeroU8;
use std::ops::Range;

use bytes::Bytes;

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
                return Located::in_flight(fetch);
            }
            let admit = self.tiers.placement.on_remote_read(context);
            let claim = match self.in_flight.claim(key, admit) {
                Ok(claim) => claim,
                Err(fetch) => return Located::in_flight(fetch),
            };
            // A fetch may have cached the block since `hold` found it remote;
            // the stale entry `hold` gave up on doesn't count.
            match self.tiers.blocks.get(key) {
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

    pub(super) fn widened_block(
        &self,
        file: &CacheableFile<'_>,
        range: &Range<u64>,
        intent: Option<ReadIntent>,
        index: u32,
        race: NonZeroU8,
    ) -> FetchBlockPlan {
        let key = BlockKey::new(file.version.id(), index);
        if self.tiers.blocks.get(key).is_some() || self.in_flight.get(key).is_some() {
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
        let admit = self.tiers.placement.on_remote_read(&context);
        match self.in_flight.claim(key, admit) {
            Err(_) => FetchBlockPlan::dropped(),
            Ok(mut claim) => match self.tiers.blocks.get(key) {
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

    /// Retries while the index changes: an entry may name a chunk eviction closed or reused.
    pub(super) fn hold(&self, key: BlockKey, mut location: Option<Location>) -> HeldBlock<'_> {
        loop {
            let held = match location {
                Some(Location::Memory(chunk)) => {
                    self.tiers.memory.read(chunk, key).map(HeldBlock::Memory)
                }
                Some(Location::Disk(chunk)) => self
                    .tiers
                    .disk
                    .chunks()
                    .read(chunk, key)
                    .map(HeldBlock::Disk),
                None => return HeldBlock::Remote,
            };
            if let Some(held) = held {
                return held;
            }
            let again = self.tiers.blocks.get(key);
            if again == location {
                return HeldBlock::Remote;
            }
            location = again;
        }
    }
}

pub(super) enum HeldBlock<'cache> {
    Memory(MemoryChunkReader<'cache>),
    Disk(DiskChunkReader<'cache>),
    Remote,
}

pub(super) enum Located<'c> {
    Memory(MemoryChunkReader<'c>),
    Disk(DiskChunkReader<'c>),
    /// Fetched and bound for disk, its write not yet done.
    Landed(Bytes),
    InFlight(Arc<BlockFetch>),
    Remote {
        admit: Admit,
        claim: Option<FetchClaim>,
    },
}

impl Located<'_> {
    fn in_flight(fetch: Arc<BlockFetch>) -> Self {
        match fetch.landed() {
            Some(bytes) => Self::Landed(bytes),
            None => Self::InFlight(fetch),
        }
    }
}
