//! A fill: a read's fetched bytes taken as they come in, into the read's
//! output and each block into its place in the cache ([`FetchedBlocks`]).

use std::num::{NonZeroU8, NonZeroU32};
use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::join_all;

use crate::PAGE_SIZE_U32;
use crate::buffer::AlignedBuf;
use crate::cache::policy::{Admit, Tier};
use crate::cache::storage::disk::DiskChunkWriter;
use crate::cache::storage::in_flight::FetchClaim;
use crate::cache::storage::index::{FileVersion, Location};
use crate::cache::storage::memory::LentMemoryChunk;
use crate::cache::{BLOCK_SIZE_U64, BlockKey, FileVersionId};
use crate::io::error::{Error, Result};
use crate::io::sync::Arc;

use super::ObjectCache;
use super::partial_read::{BlockFate, FetchBlockPlan, PartialRead, PlannedFetch};

impl ObjectCache {
    /// Takes `fetch`, the bytes of one of `read`'s GETs, as they come in
    /// ([`FetchedBlocks`]): each block the read asked for into its output,
    /// over the pages of it the block covers, and each block whole into its
    /// place in the cache, where the placement policy said it goes; the
    /// bytes of blocks the cache holds already, or another read is fetching,
    /// dropped. Each fetch is taken once.
    ///
    /// # Errors
    ///
    /// If the output's pages can't be lent.
    ///
    /// # Panics
    ///
    /// If `fetch` isn't the bytes of one of the read's GETs not taken yet.
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

    /// Takes `fetched`, the file's bytes of `fetch`, in one piece, as a
    /// fetch whose answer comes in one does: for tests, which have the
    /// bytes at hand.
    #[cfg(test)]
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

    /// Where `key`'s block, `len` bytes, lands as it is fetched: a chunk of
    /// the tier `admit` names, lent to take its bytes; a buffer, for the
    /// reads waiting on its fetch, if the policy discards it. Nowhere if a
    /// tier holds it already, or that tier has no room even after evicting.
    async fn landing(&self, key: BlockKey, admit: Admit, len: usize) -> Landing<'_> {
        if self.blocks.get(key).is_some() {
            return Landing::Nowhere;
        }
        let pages = u32::try_from(len.next_multiple_of(PAGE_SIZE_U32 as usize))
            .ok()
            .and_then(NonZeroU32::new)
            .expect("a block is a chunk's bytes or fewer, one or more");
        match admit {
            Admit::Memory => match self.allocate_memory().await {
                Ok(Some(writer)) => {
                    let (chunk, lent) = writer.lend_for_read(&self.memory, pages);
                    Landing::Memory { chunk, lent }
                }
                // No room, or an eviction's write failed: the block goes
                // uncached, as one the cache fails to keep.
                Ok(None) | Err(_) => Landing::Nowhere,
            },
            Admit::Disk => {
                let Some(writer) = self.allocate_disk() else {
                    return Landing::Nowhere;
                };
                let hold = writer.hold_for_write(self.disk.chunks());
                match AlignedBuf::new_zeroed(pages) {
                    Ok(pages) => Landing::Disk {
                        pages: pages.holding(Arc::new(hold)),
                        writer,
                    },
                    Err(_) => Landing::Nowhere,
                }
            }
            Admit::Discard => Landing::Passing(Vec::with_capacity(len)),
        }
    }

    /// Caches `key`'s block where it landed, all of it in: publishes its
    /// memory chunk, or writes its disk chunk and then publishes that. A
    /// block that a tier came to hold meanwhile, or whose write fails, goes
    /// uncached.
    async fn cache_landed(&self, key: BlockKey, landing: Landing<'_>) {
        match landing {
            Landing::Memory { chunk, lent } => {
                // Published before the index names it, so that a read that
                // finds the entry finds the chunk open.
                let reader = lent.publish(chunk, key);
                if self
                    .blocks
                    .insert_if_absent(key, Location::Memory(reader.chunk()))
                {
                    self.placement.on_admit(Tier::Memory, reader.chunk(), key);
                } else {
                    // Another fetch of the block cached it first. No entry
                    // names this chunk: it goes back when `reader` drops.
                    reader.unpublish();
                }
            }
            Landing::Disk { pages, writer } => {
                if self.disk.write(&writer, pages).await.is_err() {
                    return;
                }
                let reader = writer.publish(key);
                if self
                    .blocks
                    .insert_if_absent(key, Location::Disk(reader.chunk()))
                {
                    self.placement.on_admit(Tier::Disk, reader.chunk(), key);
                } else {
                    reader.unpublish();
                }
            }
            Landing::Due(_) | Landing::Passing(_) | Landing::Nowhere => {}
        }
    }
}

/// A fetch's bytes as they come in ([`ObjectCache::receive`]), each put
/// where it goes: into the read's output, over the pages of it the fetch
/// covers; and each block's into its place in the cache, which it takes as
/// the block's first byte comes. [`Self::finish`] caches the blocks once
/// every byte is in. Dropped before, as when the fetch fails, it caches
/// nothing, and gives the chunks it took back.
///
/// Each byte is copied twice: into the output, and into the cache. The
/// output is the caller's, never the cache's memory.
pub(in crate::cache) struct FetchedBlocks<'c> {
    cache: &'c ObjectCache,
    version: FileVersion,
    /// The file's bytes the fetch holds: whole blocks, from a block's start.
    range: Range<u64>,
    race: NonZeroU8,
    /// When one more request races the fetch, if it hasn't finished.
    late: Option<Duration>,
    /// How many of them have come in.
    received: u64,
    /// The first block's number.
    first: u32,
    /// Where each block's bytes go, the first block's first.
    blocks: Vec<FetchedBlock<'c>>,
}

/// Where one fetched block's bytes go: its place in the cache, and the
/// read's output over its pages, if the read asked for it; and the requests
/// the read holds in its fetch, if it is to be cached.
struct FetchedBlock<'c> {
    landing: Landing<'c>,
    output: Option<FetchedOutput>,
    claim: Option<FetchClaim>,
}

impl FetchedBlocks<'_> {
    /// The file's bytes the fetch holds.
    pub(in crate::cache) fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// The version of the file they are of.
    pub(in crate::cache) fn file(&self) -> FileVersionId {
        self.version.id()
    }

    /// How many identical requests race for them.
    pub(in crate::cache) fn race(&self) -> NonZeroU8 {
        self.race
    }

    /// When one more request races them, if they aren't all in.
    pub(in crate::cache) fn late_race(&self) -> Option<Duration> {
        self.late
    }

    /// Takes `piece`, the fetch's next bytes.
    ///
    /// # Errors
    ///
    /// If the fetch brings more bytes than it holds.
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
        }
        Ok(())
    }

    /// Caches the blocks where they landed, once every byte is in, and ends
    /// the read's requests in their fetches.
    ///
    /// # Errors
    ///
    /// If the fetch brought fewer bytes than it holds: nothing is cached.
    pub(in crate::cache) async fn finish(self) -> Result<()> {
        if self.received != self.range.end - self.range.start {
            return Err(Self::wrong_length(&self.range, "fewer"));
        }
        let Self {
            cache,
            version,
            first,
            blocks,
            ..
        } = self;
        let cached = (first..).zip(blocks).map(|(index, block)| async move {
            let FetchedBlock {
                landing,
                output,
                claim,
            } = block;
            // The read's output over the block is whole: the read may take
            // its bytes.
            drop(output);
            let key = BlockKey::new(version.id(), index);
            let passed = match landing {
                Landing::Passing(bytes) => Some(Bytes::from(bytes)),
                landing => {
                    cache.cache_landed(key, landing).await;
                    None
                }
            };
            // The fetch ends with the block where a tier holds it, whether
            // this fetch or another put it there, or, one no tier keeps, with
            // its bytes, for the reads waiting.
            if let Some(claim) = claim {
                match passed {
                    Some(bytes) => claim.passed(bytes),
                    None => claim.ended(cache.blocks.get(key)),
                }
            }
        });
        join_all(cached).await;
        Ok(())
    }

    /// Decides where block `index`, its first byte come, lands.
    async fn decide_landing(&mut self, index: usize) {
        if let Landing::Due(admit) = self.blocks[index].landing {
            let number = self.first + u32::try_from(index).expect("a fetch's blocks fit a u32");
            let key = BlockKey::new(self.version.id(), number);
            let landing = self.cache.landing(key, admit, self.block_len(index)).await;
            self.blocks[index].landing = landing;
        }
    }

    /// Block `index`'s bytes: a block's, but for the file's last, which may
    /// be shorter.
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

/// The pages of a read's output a fetch covers, lent to it, and the file's
/// byte they start at.
pub(super) struct FetchedOutput {
    pub(super) pages: AlignedBuf,
    pub(super) start: u64,
}

impl FetchedOutput {
    /// Copies what of `bytes`, the file's from `at`, falls in these pages.
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

/// Where one fetched block's bytes land.
enum Landing<'c> {
    /// Not yet decided: where the read said the block goes.
    Due(Admit),
    /// A memory chunk, lent as a buffer until the block is in.
    Memory {
        chunk: AlignedBuf,
        lent: LentMemoryChunk<'c>,
    },
    /// A buffer the write to a disk chunk takes once the block is in.
    Disk {
        pages: AlignedBuf,
        writer: DiskChunkWriter<'c>,
    },
    /// A buffer the bytes of a block no tier keeps collect in, for the
    /// reads waiting on its fetch to take theirs from; the block goes
    /// uncached.
    Passing(Vec<u8>),
    /// Nowhere: the block goes uncached.
    Nowhere,
}

impl Landing<'_> {
    /// Copies `bytes` into the block's place from its byte `at`. A block's
    /// bytes come in order.
    fn copy_in(&mut self, at: usize, bytes: &[u8]) {
        match self {
            Self::Memory { chunk, .. } => chunk.copy_in(at, bytes),
            Self::Disk { pages, .. } => pages.copy_in(at, bytes),
            Self::Passing(passing) => {
                debug_assert_eq!(passing.len(), at, "a block's bytes come in order");
                passing.extend_from_slice(bytes);
            }
            Self::Due(_) | Self::Nowhere => {}
        }
    }
}
