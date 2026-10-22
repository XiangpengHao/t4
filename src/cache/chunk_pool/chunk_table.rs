use std::sync::PoisonError;

use super::free_chunks::FreeChunks;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::io::sync::{Arc, Mutex, MutexGuard};

/// A tier's chunks (`doc/03-cache.md`, decisions 2 and 8): which are free,
/// and for each, the block it holds and the count of its holders. What a
/// replacement policy keeps of hits is its own (`policy`).
///
/// A chunk goes round:
/// 1. free: [`Self::allocate`] leases it to one writer, a [`ChunkLease`];
/// 2. published: [`ChunkLease::publish`] names the block it holds, and any
///    number of readers pin it ([`Self::pin_block`]), each a [`ChunkPin`];
/// 3. unpublished: [`ChunkPin::unpublish`] stops new pins, and the chunk is
///    free again when its last holder drops.
///
/// A holder is the lease, a pin, or a hold a direct I/O in flight keeps
/// ([`OwnedChunkHold`]), which a lease or a pin takes. So a chunk is freed
/// once no block is published in it and nothing holds it, and exactly once:
/// by whichever drop leaves it so. A lease dropped unpublished frees its
/// chunk once the I/O it started is done. The owner changes only while the
/// chunk is free or leased, so it holds still under a pin.
pub(super) struct ChunkTable {
    free: Mutex<FreeChunks>,
    slots: Box<[ChunkSlot]>,
}

impl ChunkTable {
    /// Set while the chunk holds a published block: readers may pin it.
    const PUBLISHED: u32 = 1 << 31;
    /// The bits counting the chunk's holders.
    const READERS: u32 = Self::PUBLISHED - 1;

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

    #[cfg(test)]
    pub(super) fn free_count(&self) -> u32 {
        self.free_chunks().free_count()
    }

    /// The block published in each chunk, `None` for a free one, as they
    /// are once nothing holds any: no writer, reader or I/O. `Err` names a
    /// chunk held still, or one the free bitmap has wrong.
    #[cfg(any(test, feature = "__fuzz"))]
    pub(super) fn blocks_at_rest(&self) -> Result<Vec<Option<BlockKey>>, String> {
        let free = self.free_chunks();
        (0..self.chunk_count())
            .map(ChunkId::new)
            .map(|chunk| {
                let slot = self.slot(chunk);
                // Acquire: sees the owner its writer published.
                let state = slot.state.load(Ordering::Acquire);
                if state & Self::READERS != 0 {
                    return Err(format!("{chunk:?} is held"));
                }
                match (state & Self::PUBLISHED != 0, free.is_free(chunk)) {
                    (true, false) => Ok(Some(slot.owner())),
                    (false, true) => Ok(None),
                    (true, true) => Err(format!("{chunk:?} holds a block, yet is free")),
                    (false, false) => Err(format!(
                        "{chunk:?} is taken, though it holds no block and nothing holds it"
                    )),
                }
            })
            .collect()
    }

    /// A free chunk, leased to one writer; `None` when every chunk is taken.
    pub(super) fn allocate(&self) -> Option<ChunkLease<'_>> {
        let chunk = self.free_chunks().allocate()?;
        // The lease is the chunk's one holder, and it is unpublished: no pin
        // gets in. The free bitmap's lock orders this after the last holder
        // of the chunk's previous round.
        self.slot(chunk).state.store(1, Ordering::Relaxed);
        Some(ChunkLease { table: self, chunk })
    }

    /// Pins `chunk` to read `block` from it. `None` if the chunk doesn't
    /// hold `block`: the index entry that named it is stale, and the caller
    /// looks again.
    pub(super) fn pin_block(&self, chunk: ChunkId, block: BlockKey) -> Option<ChunkPin<'_>> {
        self.pin(chunk).filter(|pin| pin.block() == block)
    }

    /// Pins `chunk` with whatever block it holds, as eviction does. `None`
    /// if no block is published in it.
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

    /// Publishes `block` in leased `chunk`: its holders stay, the lease
    /// going on as a reader.
    fn publish(&self, chunk: ChunkId, block: BlockKey) {
        let slot = self.slot(chunk);
        slot.set_owner(block);
        // Release: the owner, and the bytes the writer wrote, come before
        // any pin that sees `PUBLISHED`.
        let state = slot.state.fetch_or(Self::PUBLISHED, Ordering::Release);
        assert_eq!(
            state & Self::PUBLISHED,
            0,
            "{chunk:?} published while it holds a block"
        );
    }

    /// Counts one more holder of `chunk`, which the caller holds already,
    /// by a lease or a pin: so the chunk can't be freed under the count.
    fn add_holder(&self, chunk: ChunkId) {
        // Relaxed: the caller's hold already orders the chunk for it.
        let state = self.slot(chunk).state.fetch_add(1, Ordering::Relaxed);
        assert_ne!(
            state & Self::READERS,
            Self::READERS,
            "{chunk:?} has too many holders"
        );
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

    /// Drops a holder, and frees the chunk if that leaves it unpublished
    /// with none.
    fn unpin(&self, chunk: ChunkId) {
        // Release: this holder is done with the chunk before it is freed.
        // Acquire: if this call frees it, so is every other holder.
        let state = self.slot(chunk).state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(
            state & Self::READERS,
            0,
            "{chunk:?} released more than held"
        );
        if state & (Self::PUBLISHED | Self::READERS) == 1 {
            self.free_chunks().free(chunk);
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
    /// [`ChunkTable::PUBLISHED`], and the count of holders.
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
/// readers. Dropped unpublished, it frees the chunk, once the holds it took
/// for I/O are gone too.
pub(super) struct ChunkLease<'table> {
    table: &'table ChunkTable,
    chunk: ChunkId,
}

impl<'table> ChunkLease<'table> {
    pub(super) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    /// A hold on the chunk for a direct I/O that writes it: until the hold
    /// drops, the chunk isn't freed, whatever becomes of the lease.
    pub(super) fn hold_owned<T: ChunkTier>(&self, tier: &Arc<T>) -> OwnedChunkHold<T> {
        OwnedChunkHold::new(self.table, self.chunk, tier)
    }

    /// Publishes the chunk as holding `block`, which the writer has written:
    /// readers may pin it from now on, and the writer goes on as one of
    /// them.
    pub(super) fn publish(self, block: BlockKey) -> ChunkPin<'table> {
        let (table, chunk) = (self.table, self.chunk);
        // The lease's count goes on as the pin's.
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
        self.table.unpin(self.chunk);
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

    /// A hold on the chunk for a direct I/O that reads it: until the hold
    /// drops, the chunk isn't freed, whatever becomes of the pin.
    pub(super) fn hold_owned<T: ChunkTier>(&self, tier: &Arc<T>) -> OwnedChunkHold<T> {
        OwnedChunkHold::new(self.table, self.chunk, tier)
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

/// A tier: what owns a chunk table, and hands it to the holds that keep the
/// tier alive.
pub(super) trait ChunkTier: Send + Sync + 'static {
    fn chunk_table(&self) -> &ChunkTable;
}

/// A hold on a chunk that keeps its tier alive, for a direct I/O in flight:
/// its buffer, handed to the I/O worker, outlives any borrow of the tier,
/// and the future that started it may be dropped before it is done. Until
/// the hold drops, the chunk isn't freed.
pub(super) struct OwnedChunkHold<T: ChunkTier> {
    tier: Arc<T>,
    chunk: ChunkId,
}

impl<T: ChunkTier> OwnedChunkHold<T> {
    /// One more holder of `chunk` of `table`, which the caller holds, by a
    /// lease or a pin, and which `tier` owns.
    fn new(table: &ChunkTable, chunk: ChunkId, tier: &Arc<T>) -> Self {
        assert!(
            std::ptr::eq(table, tier.chunk_table()),
            "a hold names the tier its chunk is in"
        );
        table.add_holder(chunk);
        Self {
            tier: Arc::clone(tier),
            chunk,
        }
    }
}

impl<T: ChunkTier> Drop for OwnedChunkHold<T> {
    fn drop(&mut self) {
        self.tier.chunk_table().unpin(self.chunk);
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;

    /// A tier of one table, for owned holds.
    impl ChunkTier for ChunkTable {
        fn chunk_table(&self) -> &ChunkTable {
            self
        }
    }

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
    fn a_hold_for_io_outlives_its_lease() {
        let table = Arc::new(ChunkTable::new(1));
        let lease = table.allocate().unwrap();
        let hold = lease.hold_owned(&table);
        drop(lease);
        assert_eq!(table.free_count(), 0, "the write is still in flight");
        assert!(table.pin(ChunkId::new(0)).is_none(), "never published");
        drop(hold);
        assert_eq!(table.free_count(), 1);
    }

    #[test]
    fn a_hold_for_io_outlives_the_close_and_the_pin() {
        let table = Arc::new(ChunkTable::new(1));
        let chunk = table.allocate().unwrap().publish(block(1)).chunk();
        let pin = table.pin(chunk).unwrap();
        let hold = pin.hold_owned(&table);
        assert!(pin.unpublish());
        drop(pin);
        assert_eq!(table.free_count(), 0, "the read is still in flight");
        drop(hold);
        assert_eq!(table.free_count(), 1);
    }

    #[test]
    fn a_lease_published_keeps_its_holds() {
        let table = Arc::new(ChunkTable::new(1));
        let lease = table.allocate().unwrap();
        let hold = lease.hold_owned(&table);
        let pin = lease.publish(block(1));
        assert!(pin.unpublish());
        drop(pin);
        assert_eq!(table.free_count(), 0);
        drop(hold);
        assert_eq!(table.free_count(), 1);
    }

    #[test]
    #[should_panic(expected = "names the tier")]
    fn a_hold_on_another_tiers_chunk_panics() {
        let (table, other) = (ChunkTable::new(1), Arc::new(ChunkTable::new(1)));
        let lease = table.allocate().unwrap();
        let _hold = lease.hold_owned(&other);
    }
}
