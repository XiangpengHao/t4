use std::sync::PoisonError;

use super::free_chunks::FreeChunks;
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use crate::io::sync::{Arc, Mutex, MutexGuard};

pub(in crate::cache::storage) struct ChunkTable {
    free: Mutex<FreeChunks>,
    slots: Box<[ChunkSlot]>,
}

impl ChunkTable {
    const PUBLISHED: u32 = 1 << 31;
    const READERS: u32 = Self::PUBLISHED - 1;

    pub(in crate::cache::storage) fn new(chunk_count: u32) -> Self {
        Self {
            free: Mutex::new(FreeChunks::new(chunk_count)),
            slots: (0..chunk_count).map(|_| ChunkSlot::new()).collect(),
        }
    }

    pub(in crate::cache::storage) fn chunk_count(&self) -> u32 {
        // `new` made one slot per chunk of a `u32` count.
        self.slots.len() as u32
    }

    #[cfg(test)]
    pub(in crate::cache::storage) fn free_count(&self) -> u32 {
        self.free_chunks().free_count()
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(in crate::cache::storage) fn blocks_at_rest(
        &self,
    ) -> Result<Vec<Option<BlockKey>>, String> {
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

    /// Only as the cache opens, before anything else holds a chunk.
    pub(in crate::cache::storage) fn restore(&self, chunk: ChunkId, block: BlockKey) {
        self.free_chunks().take(chunk);
        let slot = self.slot(chunk);
        slot.set_owner(block);
        // Release: the owner comes before any pin that sees `PUBLISHED`.
        slot.state.store(Self::PUBLISHED, Ordering::Release);
    }

    pub(in crate::cache::storage) fn close(&self) {
        self.free_chunks().close();
    }

    pub(in crate::cache::storage) fn is_closed(&self) -> bool {
        self.free_chunks().is_closed()
    }

    pub(in crate::cache::storage) fn allocate(&self) -> Option<ChunkLease<'_>> {
        let chunk = self.free_chunks().allocate()?;
        // Relaxed: the free bitmap's lock orders this after the chunk's last holder.
        self.slot(chunk).state.store(1, Ordering::Relaxed);
        Some(ChunkLease { table: self, chunk })
    }

    pub(in crate::cache::storage) fn pin_block(
        &self,
        chunk: ChunkId,
        block: BlockKey,
    ) -> Option<ChunkPin<'_>> {
        self.pin(chunk).filter(|pin| pin.block() == block)
    }

    pub(in crate::cache::storage) fn pin(&self, chunk: ChunkId) -> Option<ChunkPin<'_>> {
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

    fn publish(&self, chunk: ChunkId, block: BlockKey) {
        let slot = self.slot(chunk);
        slot.set_owner(block);
        // Release: the owner and the bytes come before any pin that sees `PUBLISHED`.
        let state = slot.state.fetch_or(Self::PUBLISHED, Ordering::Release);
        assert_eq!(
            state & Self::PUBLISHED,
            0,
            "{chunk:?} published while it holds a block"
        );
    }

    fn add_holder(&self, chunk: ChunkId) {
        // Relaxed: the caller's hold already orders the chunk for it.
        let state = self.slot(chunk).state.fetch_add(1, Ordering::Relaxed);
        assert_ne!(
            state & Self::READERS,
            Self::READERS,
            "{chunk:?} has too many holders"
        );
    }

    fn unpublish(&self, chunk: ChunkId) -> bool {
        // Relaxed: unpublishing hands nothing over; the last unpin does.
        let state = self
            .slot(chunk)
            .state
            .fetch_and(!Self::PUBLISHED, Ordering::Relaxed);
        state & Self::PUBLISHED != 0
    }

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
        // `FreeChunks` checks each change before making it: a panic leaves it whole.
        self.free.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct ChunkSlot {
    state: AtomicU32,
    /// Set while unpublished, read while pinned: atomic only to share it without `unsafe`.
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

pub(in crate::cache::storage) struct ChunkLease<'table> {
    table: &'table ChunkTable,
    chunk: ChunkId,
}

impl<'table> ChunkLease<'table> {
    pub(in crate::cache::storage) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    pub(in crate::cache::storage) fn hold_owned<T: ChunkTier>(
        &self,
        tier: &Arc<T>,
    ) -> OwnedChunkHold<T> {
        OwnedChunkHold::new(self.table, self.chunk, tier)
    }

    pub(in crate::cache::storage) fn publish(self, block: BlockKey) -> ChunkPin<'table> {
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

pub(in crate::cache::storage) struct ChunkPin<'table> {
    table: &'table ChunkTable,
    chunk: ChunkId,
    block: BlockKey,
}

impl ChunkPin<'_> {
    pub(in crate::cache::storage) fn chunk(&self) -> ChunkId {
        self.chunk
    }

    pub(in crate::cache::storage) fn block(&self) -> BlockKey {
        self.block
    }

    pub(in crate::cache::storage) fn hold_owned<T: ChunkTier>(
        &self,
        tier: &Arc<T>,
    ) -> OwnedChunkHold<T> {
        OwnedChunkHold::new(self.table, self.chunk, tier)
    }

    pub(in crate::cache::storage) fn unpublish(&self) -> bool {
        self.table.unpublish(self.chunk)
    }
}

impl Drop for ChunkPin<'_> {
    fn drop(&mut self) {
        self.table.unpin(self.chunk);
    }
}

pub(in crate::cache::storage) trait ChunkTier:
    Send + Sync + 'static
{
    fn chunk_table(&self) -> &ChunkTable;
}

/// Keeps its tier alive: a direct I/O can outlive the future that started it.
pub(in crate::cache::storage) struct OwnedChunkHold<T: ChunkTier> {
    tier: Arc<T>,
    chunk: ChunkId,
}

impl<T: ChunkTier> OwnedChunkHold<T> {
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
