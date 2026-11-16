use vstd::prelude::*;

use crate::cache::{ChunkId, check};

verus! {

#[derive(Debug)]
pub(super) struct FreeChunks {
    /// The bits past the last chunk are set, so no allocation finds them.
    taken: Vec<u64>,
    chunk_count: u32,
    free_count: u32,
    cursor: usize,
    closed: bool,
}

impl View for FreeChunks {
    type V = Set<int>;

    closed spec fn view(&self) -> Set<int> {
        Self::free_in(self.chunk_count, self.taken@)
    }
}

impl FreeChunks {
    const WORD_BITS: usize = 64;

    pub(super) closed spec fn chunks(self) -> Set<int> {
        Set::range(0, self.chunk_count as int)
    }

    pub(super) closed spec fn spec_is_closed(&self) -> bool {
        self.closed
    }

    spec fn free_in(chunk_count: u32, taken: Seq<u64>) -> Set<int> {
        Set::range(0, chunk_count as int).filter(|chunk: int| !Self::is_taken(taken, chunk))
    }

    spec fn is_taken(taken: Seq<u64>, chunk: int) -> bool {
        (taken[chunk / 64] >> (chunk % 64) as u64) & 1 == 1
    }

    pub(super) closed spec fn wf(self) -> bool {
        &&& self.chunk_count <= self.taken@.len() * 64 < self.chunk_count + 64
        &&& forall|chunk: int|
            self.chunk_count <= chunk < self.taken@.len() * 64 ==> #[trigger] Self::is_taken(
                self.taken@,
                chunk,
            )
        &&& self.free_count == self@.len()
        &&& self.taken@.len() == 0 || self.cursor < self.taken@.len()
    }

    pub(super) fn new(chunk_count: u32) -> (free: Self)
        ensures
            free.wf(),
            free@ == free.chunks(),
            free.chunks() == Set::range(0, chunk_count as int),
            !free.is_closed(),
    {
        let chunks = chunk_count as usize;
        let used_in_last_word = chunks % Self::WORD_BITS;
        // `chunks.div_ceil(Self::WORD_BITS)`, which Verus has no spec for.
        let words = chunks / Self::WORD_BITS + if used_in_last_word == 0 {
            0
        } else {
            1
        };
        let mut taken = vec![0; words];
        if used_in_last_word != 0 {
            taken[chunks / Self::WORD_BITS] = u64::MAX << used_in_last_word;
        }
        let free = Self { taken, chunk_count, free_count: chunk_count, cursor: 0, closed: false };
        proof {
            assert forall|chunk: int|
                0 <= chunk < free.taken@.len() * 64 implies #[trigger] Self::is_taken(
                free.taken@,
                chunk,
            ) == (chunk >= chunk_count) by {
                let (word, bit) = (free.taken@[chunk / 64], (chunk % 64) as u64);
                let used = used_in_last_word as u64;
                if used != 0 && chunk / 64 == chunks / 64 {
                    assert(((word >> bit) & 1 == 1) == (bit >= used)) by (bit_vector)
                        requires
                            word == u64::MAX << used,
                            used < 64,
                            bit < 64,
                    ;
                } else {
                    assert((word >> bit) & 1 == 0) by (bit_vector)
                        requires
                            word == 0,
                    ;
                }
            }
            assert(free@ =~= free.chunks());
        }
        free
    }

    pub(super) fn allocate(&mut self) -> (chunk: Option<ChunkId>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).chunks() == old(self).chunks(),
            final(self).is_closed() == old(self).is_closed(),
            match chunk {
                Some(chunk) => old(self)@.contains(chunk@ as int) && final(self)@ == old(
                    self,
                )@.remove(chunk@ as int),
                None => final(self)@ == old(self)@ && (old(self)@.is_empty() || old(
                    self,
                ).is_closed()),
            },
    {
        if self.free_count == 0 || self.closed {
            return None;
        }
        let word_index = self.word_with_a_free_chunk();
        let bit = self.taken[word_index].trailing_ones();
        let ghost chunk = word_index * 64 + bit;
        proof {
            broadcast use vstd::std_specs::bits::axiom_u64_trailing_ones;

            assert(!Self::is_taken(self.taken@, chunk));
        }
        self.take_bit(word_index, 1 << bit, Ghost(chunk));
        self.cursor = word_index;
        Some(ChunkId::new((word_index * Self::WORD_BITS) as u32 + bit))
    }

    fn word_with_a_free_chunk(&self) -> (word_index: usize)
        requires
            self.wf(),
            self.free_count > 0,
        ensures
            word_index < self.taken@.len(),
            self.taken@[word_index as int] != u64::MAX,
    {
        let ghost free_word = self@.choose() / 64;
        proof {
            let chunk = self@.choose();
            assert(self@.len() != 0);
            assert(self@.contains(chunk));
            let (word, bit) = (self.taken@[free_word], (chunk % 64) as u64);
            assert(word != u64::MAX) by (bit_vector)
                requires
                    (word >> bit) & 1 != 1,
                    bit < 64,
            ;
        }
        let mut word_index = self.cursor;
        while self.taken[word_index] == u64::MAX
            invariant
                self.wf(),
                0 <= free_word < self.taken@.len(),
                self.taken@[free_word] != u64::MAX,
                word_index < self.taken@.len(),
            decreases
                    if word_index <= free_word {
                        free_word - word_index
                    } else {
                        free_word + self.taken@.len() - word_index
                    },
        {
            word_index =
            if word_index + 1 == self.taken.len() {
                0
            } else {
                word_index + 1
            };
        }
        word_index
    }

    pub(super) fn close(&mut self)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@,
            final(self).chunks() == old(self).chunks(),
            final(self).is_closed(),
    {
        self.closed = true;
    }

    #[verifier::when_used_as_spec(spec_is_closed)]
    pub(super) fn is_closed(&self) -> (closed: bool)
        ensures
            closed == self.spec_is_closed(),
    {
        self.closed
    }

    pub(super) fn take(&mut self, chunk: ChunkId)
        requires
            old(self).wf(),
            old(self)@.contains(chunk@ as int),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.remove(chunk@ as int),
            final(self).chunks() == old(self).chunks(),
            final(self).is_closed() == old(self).is_closed(),
    {
        let (word_index, mask) = self.bit_of(chunk);
        check(self.taken[word_index] & mask == 0, "a chunk taken twice");
        self.take_bit(word_index, mask, Ghost(chunk@ as int));
    }

    /// The next allocation starts here: in a full cache, frees and allocations take turns.
    pub(super) fn free(&mut self, chunk: ChunkId)
        requires
            old(self).wf(),
            old(self).chunks().contains(chunk@ as int),
            !old(self)@.contains(chunk@ as int),
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.insert(chunk@ as int),
            final(self).chunks() == old(self).chunks(),
            final(self).is_closed() == old(self).is_closed(),
    {
        let (word_index, mask) = self.bit_of(chunk);
        check(self.taken[word_index] & mask != 0, "a chunk freed twice");
        let ghost old_self = *self;
        self.taken[word_index] &= !mask;
        proof {
            let index = chunk@ as int;
            let bit = (index % 64) as u64;
            assert forall|other: int|
                0 <= other < self.taken@.len() * 64 implies #[trigger] Self::is_taken(
                self.taken@,
                other,
            ) == (Self::is_taken(old_self.taken@, other) && other != index) by {
                if other / 64 == index / 64 {
                    let (word, other_bit) = (old_self.taken@[other / 64], (other % 64) as u64);
                    assert(((word & !mask) >> other_bit) & 1 == 1 <==> ((word >> other_bit) & 1 == 1
                        && other_bit != bit)) by (bit_vector)
                        requires
                            mask == 1u64 << bit,
                            bit < 64,
                            other_bit < 64,
                    ;
                }
            }
            assert(self@ =~= old_self@.insert(index));
            vstd::set_lib::lemma_len_subset(self@, self.chunks());
        }
        self.free_count += 1;
        self.cursor = word_index;
    }

    fn take_bit(&mut self, word_index: usize, mask: u64, Ghost(chunk): Ghost<int>)
        requires
            old(self).wf(),
            old(self)@.contains(chunk),
            word_index == chunk / 64,
            mask == 1u64 << (chunk % 64) as u64,
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.remove(chunk),
            final(self).chunks() == old(self).chunks(),
            final(self).is_closed() == old(self).is_closed(),
            final(self).cursor == old(self).cursor,
            final(self).taken@.len() == old(self).taken@.len(),
    {
        let ghost old_self = *self;
        self.taken[word_index] |= mask;
        proof {
            let bit = (chunk % 64) as u64;
            assert forall|other: int|
                0 <= other < self.taken@.len() * 64 implies #[trigger] Self::is_taken(
                self.taken@,
                other,
            ) == (Self::is_taken(old_self.taken@, other) || other == chunk) by {
                if other / 64 == chunk / 64 {
                    let (word, other_bit) = (old_self.taken@[other / 64], (other % 64) as u64);
                    assert(((word | mask) >> other_bit) & 1 == 1 <==> ((word >> other_bit) & 1 == 1
                        || other_bit == bit)) by (bit_vector)
                        requires
                            mask == 1u64 << bit,
                            bit < 64,
                            other_bit < 64,
                    ;
                }
            }
            assert(self@ =~= old_self@.remove(chunk));
        }
        self.free_count -= 1;
    }

    fn bit_of(&self, chunk: ChunkId) -> (bit: (usize, u64))
        requires
            self.wf(),
            chunk@ < self.chunk_count,
        ensures
            bit.0 == chunk@ / 64,
            bit.0 < self.taken@.len(),
            bit.1 == 1u64 << (chunk@ % 64) as u64,
            (self.taken@[bit.0 as int] & bit.1 == 0) == !Self::is_taken(self.taken@, chunk@ as int),
    {
        // Padding bits are set: freeing one would hand out a chunk that doesn't exist.
        check(chunk.index() < self.chunk_count, "a chunk past the tier's chunks");
        let index = chunk.index() as usize;
        let (word_index, mask) = (index / Self::WORD_BITS, 1u64 << (index % Self::WORD_BITS));
        proof {
            let (word, bit) = (self.taken@[word_index as int], (index % 64) as u64);
            assert((word & mask == 0) == ((word >> bit) & 1 != 1)) by (bit_vector)
                requires
                    mask == 1u64 << bit,
                    bit < 64,
            ;
        }
        (word_index, mask)
    }
}

} // verus!

impl FreeChunks {
    #[cfg(test)]
    pub(super) fn free_count(&self) -> u32 {
        self.free_count
    }

    #[cfg(any(all(test, feature = "shuttle"), feature = "__fuzz"))]
    pub(super) fn is_free(&self, chunk: ChunkId) -> bool {
        let (word_index, mask) = self.bit_of(chunk);
        self.taken[word_index] & mask == 0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Allocations cross words, and the last word has padding.
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
    fn a_closed_tier_hands_out_no_chunk() {
        let mut free = FreeChunks::new(CHUNKS);
        let chunk = free.allocate().unwrap();
        free.close();
        assert_eq!(free.allocate(), None);
        free.free(chunk);
        assert_eq!(free.free_count(), CHUNKS);
        assert_eq!(free.allocate(), None);
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
