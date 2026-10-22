use crate::cache::ChunkId;

/// One bit per chunk of a tier, set while the chunk is taken
/// (`doc/03-cache.md`, decision 2). Any free chunk serves any block, so an
/// allocation is any clear bit: nothing fragments, and nothing merges.
#[derive(Debug)]
pub(super) struct FreeChunks {
    /// Chunk `i`'s bit is bit `i % 64` of word `i / 64`. The bits past the
    /// last chunk are set, so no allocation finds them.
    taken: Box<[u64]>,
    chunk_count: u32,
    free_count: u32,
    /// The word the last allocation or free touched: where the next
    /// allocation starts looking.
    cursor: usize,
}

impl FreeChunks {
    const WORD_BITS: usize = u64::BITS as usize;

    /// `chunk_count` chunks, all free.
    pub(super) fn new(chunk_count: u32) -> Self {
        let chunks = chunk_count as usize;
        let mut taken = vec![0; chunks.div_ceil(Self::WORD_BITS)].into_boxed_slice();
        let used_in_last_word = chunks % Self::WORD_BITS;
        if used_in_last_word != 0 {
            taken[chunks / Self::WORD_BITS] = u64::MAX << used_in_last_word;
        }
        Self {
            taken,
            chunk_count,
            free_count: chunk_count,
            cursor: 0,
        }
    }

    /// Takes a free chunk, `None` if every chunk is taken: the first clear
    /// bit from the cursor on, wrapping around, skipping full words.
    pub(super) fn allocate(&mut self) -> Option<ChunkId> {
        if self.free_count == 0 {
            return None;
        }
        let word_index = (self.cursor..self.taken.len())
            .chain(0..self.cursor)
            .find(|&word_index| self.taken[word_index] != u64::MAX)
            .expect("the free count says a chunk is free");
        let bit = self.taken[word_index].trailing_ones();
        self.taken[word_index] |= 1 << bit;
        self.free_count -= 1;
        self.cursor = word_index;
        // Below `chunk_count`, a `u32`: the bits past it are never clear.
        Some(ChunkId::new((word_index * Self::WORD_BITS) as u32 + bit))
    }

    /// Gives `chunk` back. A chunk freed twice could go to two blocks at
    /// once, so that panics. The next allocation starts at it: in a full
    /// cache, frees and allocations take turns.
    pub(super) fn free(&mut self, chunk: ChunkId) {
        let (word_index, mask) = self.bit_of(chunk);
        assert_ne!(self.taken[word_index] & mask, 0, "{chunk:?} freed twice");
        self.taken[word_index] &= !mask;
        self.free_count += 1;
        self.cursor = word_index;
    }

    #[cfg(test)]
    pub(super) fn free_count(&self) -> u32 {
        self.free_count
    }

    /// The word holding `chunk`'s bit, and the bit's mask in it.
    fn bit_of(&self, chunk: ChunkId) -> (usize, u64) {
        // The padding bits past the last chunk are set: freeing one would
        // hand out a chunk that doesn't exist.
        assert!(
            chunk.index() < self.chunk_count,
            "{chunk:?} is past the tier's {} chunks",
            self.chunk_count
        );
        let index = chunk.index() as usize;
        (index / Self::WORD_BITS, 1 << (index % Self::WORD_BITS))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Two whole words and part of a third: allocations cross words, and the
    /// last word has padding.
    const CHUNKS: u32 = 2 * 64 + 5;

    fn allocate_all(free: &mut FreeChunks) -> BTreeSet<u32> {
        std::iter::from_fn(|| free.allocate())
            .map(ChunkId::index)
            .collect()
    }

    #[test]
    fn allocates_each_chunk_once_until_exhausted() {
        for chunk_count in [0, 1, 64, 128, CHUNKS] {
            let mut free = FreeChunks::new(chunk_count);
            assert_eq!(allocate_all(&mut free), (0..chunk_count).collect());
            assert_eq!(free.free_count(), 0);
            assert_eq!(free.allocate(), None);
        }
    }

    #[test]
    fn freed_chunks_are_allocated_again() {
        let mut free = FreeChunks::new(CHUNKS);
        allocate_all(&mut free);
        for index in [3, 64, CHUNKS - 1] {
            free.free(ChunkId::new(index));
        }
        assert_eq!(free.free_count(), 3);
        assert_eq!(allocate_all(&mut free), BTreeSet::from([3, 64, CHUNKS - 1]));
    }

    #[test]
    #[should_panic(expected = "freed twice")]
    fn freeing_a_free_chunk_panics() {
        let mut free = FreeChunks::new(CHUNKS);
        let chunk = free.allocate().unwrap();
        free.free(chunk);
        free.free(chunk);
    }

    #[test]
    #[should_panic(expected = "past the tier")]
    fn freeing_a_padding_bit_panics() {
        let mut free = FreeChunks::new(CHUNKS);
        free.free(ChunkId::new(CHUNKS));
    }
}
