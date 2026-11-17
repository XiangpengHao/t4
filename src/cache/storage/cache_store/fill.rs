use std::num::{NonZeroU8, NonZeroU32};
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;

use crate::PAGE_SIZE_U32;
use crate::cache::policy::{Admit, Tier};
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::in_flight::FetchClaim;
use crate::cache::storage::index::{FileVersion, Location};
use crate::cache::storage::memory::LentMemoryChunk;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, FileVersionId};
use crate::io::aligned_buf::AlignedBuf;
use crate::io::error::{Error, Result};

use super::ObjectCache;
use super::disk_writer::LandedBlock;
use super::partial_read::{BlockFate, FetchBlockPlan, PartialRead, PlannedFetch};

impl ObjectCache {
    pub(in crate::cache) fn receive(
        &self,
        read: &mut PartialRead,
        fetch: Range<u64>,
    ) -> Result<FetchedBlocks<'_>> {
        let at = read
            .fetches
            .iter()
            .position(|planned| planned.bytes == fetch)
            .expect("a fetch is one the read planned, taken once");
        let PlannedFetch {
            race,
            late,
            first,
            blocks: planned,
            ..
        } = read.fetches.remove(at);
        let mut blocks = Vec::with_capacity(planned.len());
        for (index, FetchBlockPlan { fate, claim }) in (first..).zip(planned) {
            let (landing, output) = match fate {
                BlockFate::Asked(admit) => (
                    Landing::Due(admit),
                    read.lend_for_block(read.version.block_range(index))?,
                ),
                BlockFate::Widened(admit) => (Landing::Due(admit), None),
                BlockFate::Dropped => (Landing::Nowhere, None),
            };
            blocks.push(FetchedBlock {
                landing,
                output,
                claim,
            });
        }
        Ok(FetchedBlocks {
            cache: self,
            version: read.version,
            range: fetch,
            race,
            late,
            received: 0,
            first,
            blocks,
        })
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(in crate::cache) async fn take_fetched(
        &self,
        read: &mut PartialRead,
        fetch: Range<u64>,
        fetched: &[u8],
    ) -> Result<()> {
        let mut blocks = self.receive(read, fetch)?;
        blocks.land(fetched).await?;
        blocks.finish().await
    }

    /// A block bound for disk lands in pages of its own, outside the memory tier: every fetch
    /// in flight lands its blocks however small the tier.
    async fn landing(&self, key: BlockKey, admit: Admit, len: usize) -> Landing<'_> {
        if self.tiers.blocks.get(key).is_some() {
            return Landing::Nowhere;
        }
        match admit {
            Admit::Memory => {
                let pages = u32::try_from(len.next_multiple_of(PAGE_SIZE_U32 as usize))
                    .ok()
                    .and_then(NonZeroU32::new)
                    .expect("a block is a chunk's bytes or fewer, one or more");
                // An eviction's failed write leaves the block uncached, not the read failed.
                match self.tiers.allocate_memory().await {
                    Ok(Some(writer)) => {
                        let (chunk, lent) = writer.lend_for_read(&self.tiers.memory, pages);
                        Landing::Memory { chunk, lent }
                    }
                    Ok(None) | Err(_) => Landing::Nowhere,
                }
            }
            Admit::Disk if !self.tiers.disk.takes_a_block() => Landing::Nowhere,
            Admit::Disk => match ReadOutput::new(0..len as u64) {
                Ok(pages) => Landing::Disk(pages),
                Err(_) => Landing::Nowhere,
            },
            Admit::Discard => Landing::Passing(Vec::with_capacity(len)),
        }
    }

    /// Ends its fetch once written; reads find it meanwhile, and the queue's room paces the
    /// fetch.
    async fn write_landed(&self, key: BlockKey, pages: ReadOutput, claim: Option<FetchClaim>) {
        let Some(landed) = LandedBlock::new(key, pages, claim) else {
            return;
        };
        if let Err(landed) = self.tiers.pending.enqueue_landed(landed).await {
            self.tiers.leave_uncached(landed);
        }
    }

    async fn cache_landed(&self, key: BlockKey, landing: Landing<'_>) {
        match landing {
            Landing::Memory { chunk, lent } => {
                // Published before the index names it: a read that finds the entry finds it open.
                let reader = lent.publish(chunk, key);
                if self
                    .tiers
                    .blocks
                    .insert_if_absent(key, Location::Memory(reader.chunk()))
                {
                    self.tiers
                        .placement
                        .on_admit(Tier::Memory, reader.chunk(), key);
                } else {
                    // Another fetch cached it first; the chunk goes back when `reader` drops.
                    reader.unpublish();
                }
            }
            Landing::Due(_) | Landing::Disk(_) | Landing::Passing(_) | Landing::Nowhere => {}
        }
    }
}

/// Each byte is copied twice: the output is the caller's, never the cache's memory.
pub(in crate::cache) struct FetchedBlocks<'c> {
    cache: &'c ObjectCache,
    version: FileVersion,
    /// Whole blocks, from a block's start.
    range: Range<u64>,
    race: NonZeroU8,
    late: Option<Duration>,
    received: u64,
    first: u32,
    blocks: Vec<FetchedBlock<'c>>,
}

struct FetchedBlock<'c> {
    landing: Landing<'c>,
    output: Option<FetchedOutput>,
    claim: Option<FetchClaim>,
}

impl FetchedBlocks<'_> {
    pub(in crate::cache) fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    pub(in crate::cache) fn file(&self) -> FileVersionId {
        self.version.id()
    }

    pub(in crate::cache) fn race(&self) -> NonZeroU8 {
        self.race
    }

    pub(in crate::cache) fn late_race(&self) -> Option<Duration> {
        self.late
    }

    pub(in crate::cache) async fn land(&mut self, mut piece: &[u8]) -> Result<()> {
        let len = self.range.end - self.range.start;
        if self.received + piece.len() as u64 > len {
            return Err(Self::wrong_length(&self.range, "more"));
        }
        while !piece.is_empty() {
            let index = PartialRead::offset(self.received / BLOCK_SIZE_U64);
            let within = PartialRead::offset(self.received % BLOCK_SIZE_U64);
            if within == 0 {
                self.decide_landing(index).await;
            }
            let taken = piece.len().min(self.block_len(index) - within);
            let at = self.range.start + self.received;
            let block = &mut self.blocks[index];
            if let Some(output) = &mut block.output {
                output.copy_in(at, &piece[..taken]);
            }
            block.landing.copy_in(within, &piece[..taken]);
            piece = &piece[taken..];
            self.received += taken as u64;
            if within + taken == self.block_len(index) {
                self.settle(index).await;
            }
        }
        Ok(())
    }

    /// Every block is cached as its last byte lands; a fetch that brought fewer bytes than it
    /// holds leaves its unfinished blocks uncached.
    pub(in crate::cache) async fn finish(self) -> Result<()> {
        if self.received != self.range.end - self.range.start {
            return Err(Self::wrong_length(&self.range, "fewer"));
        }
        Ok(())
    }

    /// Caches a block whose bytes are all in, so that it may go to disk while the fetch goes on.
    async fn settle(&mut self, index: usize) {
        let key = self.key(index);
        let block = &mut self.blocks[index];
        // Released before caching: the read may take its bytes already.
        drop(block.output.take());
        let claim = block.claim.take();
        match std::mem::replace(&mut block.landing, Landing::Nowhere) {
            Landing::Passing(bytes) => {
                if let Some(claim) = claim {
                    claim.passed(Bytes::from(bytes));
                }
            }
            Landing::Disk(pages) => self.cache.write_landed(key, pages, claim).await,
            landing => {
                self.cache.cache_landed(key, landing).await;
                // Ends with wherever a tier holds the block, even if another fetch put it there.
                if let Some(claim) = claim {
                    claim.ended(self.cache.tiers.blocks.get(key));
                }
            }
        }
    }

    async fn decide_landing(&mut self, index: usize) {
        if let Landing::Due(admit) = self.blocks[index].landing {
            let key = self.key(index);
            let landing = self.cache.landing(key, admit, self.block_len(index)).await;
            self.blocks[index].landing = landing;
        }
    }

    fn key(&self, index: usize) -> BlockKey {
        let number = self.first + u32::try_from(index).expect("a fetch's blocks fit a u32");
        BlockKey::new(self.version.id(), number)
    }

    fn block_len(&self, index: usize) -> usize {
        let start = index as u64 * BLOCK_SIZE_U64;
        PartialRead::offset((self.range.end - self.range.start - start).min(BLOCK_SIZE_U64))
    }

    fn wrong_length(range: &Range<u64>, more_or_fewer: &str) -> Error {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("the fetch of {range:?} brought {more_or_fewer} bytes than it holds"),
        ))
    }
}

pub(super) struct FetchedOutput {
    pub(super) pages: AlignedBuf,
    pub(super) start: u64,
}

impl FetchedOutput {
    fn copy_in(&mut self, at: u64, bytes: &[u8]) {
        let end = self.start + self.pages.len() as u64;
        let overlap = at.max(self.start)..(at + bytes.len() as u64).min(end);
        if overlap.is_empty() {
            return;
        }
        let from = PartialRead::offset(overlap.start - at);
        let len = PartialRead::offset(overlap.end - overlap.start);
        self.pages.copy_in(
            PartialRead::offset(overlap.start - self.start),
            &bytes[from..from + len],
        );
    }
}

enum Landing<'c> {
    Due(Admit),
    Memory {
        chunk: AlignedBuf,
        lent: LentMemoryChunk<'c>,
    },
    Disk(ReadOutput),
    /// A discarded block's bytes, kept for the reads waiting on its fetch.
    Passing(Vec<u8>),
    Nowhere,
}

impl Landing<'_> {
    fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        match self {
            Self::Memory { chunk, .. } => chunk.copy_in(at, bytes),
            Self::Disk(pages) => pages.copy_in(at as u64, bytes),
            Self::Passing(passing) => {
                debug_assert_eq!(passing.len(), at, "a block's bytes come in order");
                passing.extend_from_slice(bytes);
            }
            Self::Due(_) | Self::Nowhere => {}
        }
    }
}
