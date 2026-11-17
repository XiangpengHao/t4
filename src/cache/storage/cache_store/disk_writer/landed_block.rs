use std::any::Any;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::Arc;

use bytes::Bytes;

use crate::cache::BlockKey;
use crate::cache::read_output::ReadOutput;
use crate::cache::storage::in_flight::FetchClaim;
use crate::cache::storage::index::Location;
use crate::io::aligned_buf::AlignedBuf;

/// A fetched block bound for disk, in pages of its own outside the memory tier, so that every
/// fetch in flight lands as many as it brings. Reads that find it share its pages with its write.
pub(in crate::cache) struct LandedBlock {
    block: BlockKey,
    bytes: Bytes,
    pages: NonZeroU32,
    /// Its fetch, kept live until the write is done: reads find the block here meanwhile.
    claim: Option<FetchClaim>,
}

impl LandedBlock {
    /// `None` if the landing's pages are still lent, which they never are.
    pub(in crate::cache) fn new(
        block: BlockKey,
        landing: ReadOutput,
        claim: Option<FetchClaim>,
    ) -> Option<Self> {
        let pages = u32::try_from(ReadOutput::padded_len(&landing.asked()))
            .ok()
            .and_then(NonZeroU32::new)
            .expect("a block is a page or more, and fits a u32");
        let bytes = landing.into_bytes()?;
        debug_assert!((bytes.as_ptr() as usize).is_multiple_of(crate::PAGE_SIZE));
        if let Some(claim) = &claim {
            claim.landed(bytes.clone());
        }
        Some(Self {
            block,
            bytes,
            pages,
            claim,
        })
    }

    pub(in crate::cache) fn block(&self) -> BlockKey {
        self.block
    }

    /// Its pages, for the write, kept alive along with `with` until the write lets go.
    pub(in crate::cache) fn write_buffer(&self, with: impl Any + Send + Sync) -> AlignedBuf {
        let start = NonNull::new(self.bytes.as_ptr().cast_mut()).expect("bytes are never null");
        // SAFETY: `bytes` start a page-aligned allocation of `pages` bytes, its rounding zeroed
        // (`ReadOutput`), which the clone keeps; nothing writes it, as no `Bytes` can.
        unsafe { AlignedBuf::lent(start, self.pages, Arc::new((self.bytes.clone(), with))) }
    }

    /// Its fetch ends: on disk, or wherever the index holds it, if anywhere.
    pub(in crate::cache) fn ended(self, location: Option<Location>) {
        if let Some(claim) = self.claim {
            claim.ended(location);
        }
    }
}
