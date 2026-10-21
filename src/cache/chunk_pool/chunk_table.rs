use std::sync::PoisonError;

use super::free_chunks::FreeChunks;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::io::sync::{Mutex, MutexGuard};

/// A tier's chunks (`doc/03-cache.md`, decisions 2 and 8): which are free,
/// and for each, the block it holds, an access bit for the replacement
/// policy, and the count of readers holding it.
///
/// A chunk goes round:
/// 1. free: [`Self::allocate`] leases it to one writer, a [`ChunkLease`];
/// 2. published: [`ChunkLease::publish`] names the block it holds, and any
///    number of readers pin it ([`Self::pin_block`]), each a [`ChunkPin`];
/// 3. unpublished: [`ChunkPin::unpublish`] stops new pins, and the chunk is
///    free again when its last pin drops. A lease dropped unpublished frees
///    its chunk at once.
///
/// So a chunk is freed once no block is published in it and no reader holds
/// it, and exactly once: by whichever drop leaves it so. Its owner changes
/// only while it is free or leased, so it holds still under a pin.
pub(super) struct ChunkTable {
    free: Mutex<FreeChunks>,
    slots: Box<[ChunkSlot]>,
}

impl ChunkTable {
    /// Set while the chunk holds a published block: readers may pin it.
    const PUBLISHED: u32 = 1 << 31;
    /// Set by reads, cleared by the replacement policy's sweep.
    const ACCESSED: u32 = 1 << 30;
    /// The bits counting the chunk's pins.
    const READERS: u32 = Self::ACCESSED - 1;

    /// `chunk_count` chunks, all free.
    pub(super) fn new(chunk_count: u32) -> Self {
        Self {
            free: Mutex::new(FreeChunks::new(chunk_count)),
            slots: (0..chunk_count).map(|_| ChunkSlot::new()).collect(),
        }
    }

    pub(super) fn chunk_count(&self) -> u32 {
        // `new` made one slot per chunk of a `u32` count.
        self.slots.len() as u32
    }

    pub(super) fn free_count(&self) -> u32 {
        self.free_chunks().free_count()
    }

    /// A free chunk, leased to one writer; `None` when every chunk is taken.
    pub(super) fn allocate(&self) -> Option<ChunkLease<'_>> {
        let chunk = self.free_chunks().allocate()?;
        Some(ChunkLease { table: self, chunk })
    }

    /// Pins `chunk` to read `block` from it, and marks it accessed. `None`
    /// if the chunk doesn't hold `block`: the index entry that named it is
    /// stale, and the caller looks again.
    pub(super) fn pin_block(&self, chunk: ChunkId, block: BlockKey) -> Option<ChunkPin<'_>> {
        let pin = self.pin(chunk)?;
        if pin.block() != block {
            return None;
        }
        self.mark_accessed(chunk);
        Some(pin)
    }

    /// Pins `chunk` with whatever block it holds, as eviction does: not an
    /// access. `None` if no block is published in it.
    pub(super) fn pin(&self, chunk: ChunkId) -> Option<ChunkPin<'_>> {
        let slot = self.slot(chunk);
        let mut state = slot.state.load(Ordering::Relaxed);
        loop {
            if state & Self::PUBLISHED == 0 {
                return None;
            }
            assert_ne!(
                state & Self::READERS,
                Self::READERS,
                "{chunk:?} has too many readers"
            );
            // Acquire: sees the owner, and the bytes, its writer published.
            match slot.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => state = current,
            }
        }
        Some(ChunkPin {
            table: self,
            chunk,
            block: slot.owner(),
        })
    }

    /// Clears `chunk`'s access bit, and returns it: whether a read pinned
    /// the chunk since the last call.
    pub(super) fn take_accessed(&self, chunk: ChunkId) -> bool {
        let state = self
            .slot(chunk)
            .state
            .fetch_and(!Self::ACCESSED, Ordering::Relaxed);
        state & Self::ACCESSED != 0
    }

    /// Publishes `block` in leased `chunk`, its writer holding it as a
    /// reader. The chunk starts unaccessed.
    fn publish(&self, chunk: ChunkId, block: BlockKey) {
        let slot = self.slot(chunk);
        let state = slot.state.load(Ordering::Relaxed);
        assert_eq!(
            state & (Self::PUBLISHED | Self::READERS),
            0,
            "{chunk:?} published while it holds a block"
        );
        slot.set_owner(block);
        // Release: the owner, and the bytes the writer wrote, come before
        // any pin that sees `PUBLISHED`.
        slot.state.store(Self::PUBLISHED | 1, Ordering::Release);
    }

    /// Whether this call unpublished `chunk`: of two evictions of one
    /// chunk, one goes on.
    fn unpublish(&self, chunk: ChunkId) -> bool {
        // Relaxed: unpublishing hands nothing over; the last unpin does.
        let state = self
            .slot(chunk)
            .state
            .fetch_and(!Self::PUBLISHED, Ordering::Relaxed);
        state & Self::PUBLISHED != 0
    }

    /// Drops a pin, and frees the chunk if that leaves it unpublished with
    /// no reader.
    fn unpin(&self, chunk: ChunkId) {
        // Release: this reader is done with the chunk before it is freed.
        // Acquire: if this call frees it, so is every other reader.
        let state = self.slot(chunk).state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(
            state & Self::READERS,
            0,
            "{chunk:?} unpinned more than pinned"
        );
        if state & (Self::PUBLISHED | Self::READERS) == 1 {
            self.free_chunks().free(chunk);
        }
    }

    fn mark_accessed(&self, chunk: ChunkId) {
        let state = &self.slot(chunk).state;
        // A hot chunk is marked already: the load spares its cache line a
        // write per read.
        if state.load(Ordering::Relaxed) & Self::ACCESSED == 0 {
            state.fetch_or(Self::ACCESSED, Ordering::Relaxed);
        }
    }

    fn slot(&self, chunk: ChunkId) -> &ChunkSlot {
        &self.slots[chunk.index() as usize]
    }

    fn free_chunks(&self) -> MutexGuard<'_, FreeChunks> {
        // `FreeChunks` checks each change before making it: a panic leaves
        // it whole.
        self.free.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One chunk's entry in a [`ChunkTable`].
struct ChunkSlot {
    /// [`ChunkTable::PUBLISHED`], [`ChunkTable::ACCESSED`], and the count of
    /// pins.
    state: AtomicU32,
    /// The block the chunk holds, as [`BlockKey::packed`] gives it: set
    /// before it is published and read under a pin, so never both at once.
    /// An atomic only to share it without `unsafe`, ordered by `state`.
    owner: AtomicU64,
}

impl ChunkSlot {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            owner: AtomicU64::new(0),
        }
    }

    fn owner(&self) -> BlockKey {
        BlockKey::from_packed(self.owner.load(Ordering::Relaxed))
    }

    fn set_owner(&self, block: BlockKey) {
        self.owner.store(block.packed(), Ordering::Relaxed);
    }
}

/// A chunk leased to its one writer, until [`Self::publish`] hands it to
/// readers. Dropped unpublished, it frees the chunk.
pub(super) struct ChunkLease<'table> {
    table: &'table ChunkTable,
    chunk: ChunkId,
}

impl<'table> ChunkLease<'table> {
    pub(super) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    /// Publishes the chunk as holding `block`, which the writer has written:
    /// readers may pin it from now on, and the writer goes on as one of
    /// them.
    pub(super) fn publish(self, block: BlockKey) -> ChunkPin<'table> {
        let (table, chunk) = (self.table, self.chunk);
        // From here the pin frees the chunk, not the lease.
        std::mem::forget(self);
        table.publish(chunk, block);
        ChunkPin {
            table,
            chunk,
            block,
        }
    }
}

impl Drop for ChunkLease<'_> {
    fn drop(&mut self) {
        self.table.free_chunks().free(self.chunk);
    }
}

/// A reader's hold on a chunk: until it drops, the chunk is not freed, so
/// it holds `block`.
pub(super) struct ChunkPin<'table> {
    table: &'table ChunkTable,
    chunk: ChunkId,
    block: BlockKey,
}

impl ChunkPin<'_> {
    pub(super) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    pub(super) fn block(&self) -> BlockKey {
        self.block
    }

    /// Unpublishes the chunk, for eviction: no new reader pins it, and it is
    /// freed when its last pin drops, this one included. Whether this call
    /// did it: of two evictions of one chunk, one goes on.
    pub(super) fn unpublish(&self) -> bool {
        self.table.unpublish(self.chunk)
    }
}

impl Drop for ChunkPin<'_> {
    fn drop(&mut self) {
        self.table.unpin(self.chunk);
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;

    fn block(file: u64) -> BlockKey {
        BlockKey::new(FileVersionId::new(file), 0)
    }

    #[test]
    fn allocates_until_exhausted_and_a_dropped_lease_frees() {
        let table = ChunkTable::new(3);
        let leases: Vec<_> = std::iter::from_fn(|| table.allocate()).collect();
        assert_eq!(leases.len(), 3);
        assert!(table.allocate().is_none());
        assert_eq!(table.free_count(), 0);
        drop(leases);
        assert_eq!(table.free_count(), 3);
    }

    #[test]
    fn a_published_chunk_pins_for_its_block_only() {
        let table = ChunkTable::new(2);
        let chunk = table.allocate().unwrap().publish(block(1)).chunk();
        assert_eq!(table.free_count(), 1, "published, with no reader, it stays");
        assert_eq!(table.pin_block(chunk, block(1)).unwrap().block(), block(1));
        assert!(table.pin_block(chunk, block(2)).is_none(), "a stale entry");
        assert!(table.pin(ChunkId::new(1)).is_none(), "a free chunk");
    }

    #[test]
    fn a_held_pin_keeps_an_unpublished_chunk_from_being_freed() {
        let table = ChunkTable::new(1);
        let chunk = table.allocate().unwrap().publish(block(1)).chunk();
        let reader = table.pin_block(chunk, block(1)).unwrap();

        let evictor = table.pin(chunk).unwrap();
        assert!(evictor.unpublish());
        assert!(!evictor.unpublish(), "one eviction goes on");
        drop(evictor);
        assert!(table.pin(chunk).is_none(), "no new pin once unpublished");
        assert_eq!(table.free_count(), 0, "the reader still holds it");
        assert!(table.allocate().is_none());

        drop(reader);
        assert_eq!(table.free_count(), 1);
        let reused = table.allocate().unwrap().publish(block(2));
        assert_eq!(reused.chunk(), chunk);
        assert!(table.pin_block(chunk, block(1)).is_none());
    }

    #[test]
    fn reads_mark_the_chunk_accessed_and_eviction_pins_do_not() {
        let table = ChunkTable::new(1);
        let chunk = table.allocate().unwrap().publish(block(1)).chunk();
        assert!(!table.take_accessed(chunk), "published unaccessed");
        drop(table.pin(chunk));
        assert!(!table.take_accessed(chunk));
        drop(table.pin_block(chunk, block(2)));
        assert!(!table.take_accessed(chunk), "a stale read is no access");
        drop(table.pin_block(chunk, block(1)));
        assert!(table.take_accessed(chunk));
        assert!(!table.take_accessed(chunk), "taking clears it");
    }
}
