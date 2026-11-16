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

    /// The words hold the tier's chunks, and their bits past it are set.
    spec fn padded(chunk_count: u32, taken: Seq<u64>) -> bool {
        &&& chunk_count <= taken.len() * 64 < chunk_count + 64
        &&& forall|chunk: int|
            chunk_count <= chunk < taken.len() * 64 ==> #[trigger] Self::is_taken(taken, chunk)
    }

    pub(super) closed spec fn wf(self) -> bool {
        &&& Self::padded(self.chunk_count, self.taken@)
        &&& self.free_count == self@.len()
        &&& self.taken@.len() == 0 || self.cursor < self.taken@.len()
    }

    pub(super) fn new(chunk_count: u32) -> (free: Self)
        ensures
            free.wf(),
            free@ == Set::range(0, chunk_count as int),
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
        proof {
            Self::lemma_fresh(chunk_count, taken@);
        }
        Self { taken, chunk_count, free_count: chunk_count, cursor: 0, closed: false }
    }

    pub(super) fn allocate(&mut self) -> (chunk: Option<ChunkId>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
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
        proof {
            Self::lemma_first_clear_bit(self.taken@, word_index as int);
        }
        self.take_bit(word_index, 1 << bit, Ghost(word_index * 64 + bit));
        self.cursor = word_index;
        Some(ChunkId::new((word_index * Self::WORD_BITS) as u32 + bit))
    }

    #[verifier::loop_isolation(false)]
    fn word_with_a_free_chunk(&self) -> (word_index: usize)
        requires
            self.wf(),
            self.free_count > 0,
        ensures
            word_index < self.taken@.len(),
            self.taken@[word_index as int] != u64::MAX,
    {
        proof {
            self.lemma_a_word_has_a_free_chunk();
        }
        let mut word_index = self.cursor;
        while self.taken[word_index] == u64::MAX
            invariant
                word_index < self.taken@.len(),
            decreases self.words_to_a_free_chunk(word_index as int),
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
    {
        let (word_index, mask) = self.bit_of(chunk);
        check(self.taken[word_index] & mask != 0, "a chunk freed twice");
        self.taken[word_index] &= !mask;
        proof {
            Self::lemma_free_bit(old(self).taken@, self.chunk_count, chunk@ as int);
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
            final(self).cursor == old(self).cursor,
            final(self).taken@.len() == old(self).taken@.len(),
    {
        self.taken[word_index] |= mask;
        proof {
            Self::lemma_take_bit(old(self).taken@, self.chunk_count, chunk);
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
        proof {
            Self::lemma_mask(self.taken@[(index / 64) as int], (index % 64) as u64);
        }
        (index / Self::WORD_BITS, 1u64 << (index % Self::WORD_BITS))
    }
}

#[cfg(verus_only)]
mod proofs {
    use super::*;

    impl FreeChunks {
        pub(super) proof fn lemma_fresh(chunk_count: u32, taken: Seq<u64>)
            requires
                chunk_count <= taken.len() * 64 < chunk_count + 64,
                forall|word: int|
                    0 <= word < taken.len() ==> #[trigger] taken[word] == if word * 64 + 64
                        <= chunk_count {
                        0
                    } else {
                        u64::MAX << (chunk_count % 64) as u64
                    },
            ensures
                Self::padded(chunk_count, taken),
                Self::free_in(chunk_count, taken) == Set::range(0, chunk_count as int),
        {
            assert forall|chunk: int|
                0 <= chunk < taken.len() * 64 implies #[trigger] Self::is_taken(taken, chunk) == (
            chunk >= chunk_count) by {
                let (word, bit) = (taken[chunk / 64], (chunk % 64) as u64);
                let used = (chunk_count % 64) as u64;
                if chunk / 64 * 64 + 64 <= chunk_count {
                    assert((word >> bit) & 1 == 0) by (bit_vector)
                        requires
                            word == 0,
                    ;
                } else {
                    assert(((word >> bit) & 1 == 1) == (bit >= used)) by (bit_vector)
                        requires
                            word == u64::MAX << used,
                            used < 64,
                            bit < 64,
                    ;
                }
            }
            assert(Self::free_in(chunk_count, taken) =~= Set::range(0, chunk_count as int));
        }

        pub(super) open spec fn word_of_a_free_chunk(self) -> int {
            self@.choose() / 64
        }

        pub(super) open spec fn words_to_a_free_chunk(self, word: int) -> int {
            let target = self.word_of_a_free_chunk();
            if word <= target {
                target - word
            } else {
                target + self.taken@.len() - word
            }
        }

        pub(super) proof fn lemma_a_word_has_a_free_chunk(self)
            requires
                self.wf(),
                self.free_count > 0,
            ensures
                0 <= self.word_of_a_free_chunk() < self.taken@.len(),
                self.taken@[self.word_of_a_free_chunk()] != u64::MAX,
        {
            let chunk = self@.choose();
            assert(self@.len() != 0);
            assert(self@.contains(chunk));
            let (word, bit) = (self.taken@[chunk / 64], (chunk % 64) as u64);
            assert(word != u64::MAX) by (bit_vector)
                requires
                    (word >> bit) & 1 != 1,
                    bit < 64,
            ;
        }

        pub(super) proof fn lemma_first_clear_bit(taken: Seq<u64>, word: int)
            requires
                0 <= word < taken.len(),
                taken[word] != u64::MAX,
            ensures
                taken[word].trailing_ones() < 64,
                !Self::is_taken(taken, word * 64 + taken[word].trailing_ones()),
        {
            broadcast use vstd::std_specs::bits::axiom_u64_trailing_ones;

        }

        pub(super) proof fn lemma_take_bit(taken: Seq<u64>, chunk_count: u32, chunk: int)
            requires
                Self::padded(chunk_count, taken),
                Self::free_in(chunk_count, taken).contains(chunk),
            ensures
                ({
                    let word = taken[chunk / 64] | (1u64 << (chunk % 64) as u64);
                    let after = taken.update(chunk / 64, word);
                    &&& Self::padded(chunk_count, after)
                    &&& Self::free_in(chunk_count, after) == Self::free_in(
                        chunk_count,
                        taken,
                    ).remove(chunk)
                }),
        {
            let bit = (chunk % 64) as u64;
            let mask = 1u64 << bit;
            let after = taken.update(chunk / 64, taken[chunk / 64] | mask);
            assert forall|other: int|
                0 <= other < taken.len() * 64 implies #[trigger] Self::is_taken(after, other) == (
            Self::is_taken(taken, other) || other == chunk) by {
                if other / 64 == chunk / 64 {
                    let (word, other_bit) = (taken[other / 64], (other % 64) as u64);
                    assert(((word | mask) >> other_bit) & 1 == 1 <==> ((word >> other_bit) & 1 == 1
                        || other_bit == bit)) by (bit_vector)
                        requires
                            mask == 1u64 << bit,
                            bit < 64,
                            other_bit < 64,
                    ;
                }
            }
            assert(Self::free_in(chunk_count, after) =~= Self::free_in(chunk_count, taken).remove(
                chunk,
            ));
        }

        pub(super) proof fn lemma_free_bit(taken: Seq<u64>, chunk_count: u32, chunk: int)
            requires
                Self::padded(chunk_count, taken),
                0 <= chunk < chunk_count,
                !Self::free_in(chunk_count, taken).contains(chunk),
            ensures
                ({
                    let word = taken[chunk / 64] & !(1u64 << (chunk % 64) as u64);
                    let after = taken.update(chunk / 64, word);
                    &&& Self::padded(chunk_count, after)
                    &&& Self::free_in(chunk_count, after) == Self::free_in(
                        chunk_count,
                        taken,
                    ).insert(chunk)
                    &&& Self::free_in(chunk_count, taken).len() < chunk_count
                }),
        {
            let bit = (chunk % 64) as u64;
            let mask = 1u64 << bit;
            let after = taken.update(chunk / 64, taken[chunk / 64] & !mask);
            assert forall|other: int|
                0 <= other < taken.len() * 64 implies #[trigger] Self::is_taken(after, other) == (
            Self::is_taken(taken, other) && other != chunk) by {
                if other / 64 == chunk / 64 {
                    let (word, other_bit) = (taken[other / 64], (other % 64) as u64);
                    assert(((word & !mask) >> other_bit) & 1 == 1 <==> ((word >> other_bit) & 1 == 1
                        && other_bit != bit)) by (bit_vector)
                        requires
                            mask == 1u64 << bit,
                            bit < 64,
                            other_bit < 64,
                    ;
                }
            }
            assert(Self::free_in(chunk_count, after) =~= Self::free_in(chunk_count, taken).insert(
                chunk,
            ));
            vstd::set_lib::lemma_len_subset(
                Self::free_in(chunk_count, after),
                Set::range(0, chunk_count as int),
            );
        }

        pub(super) proof fn lemma_mask(word: u64, bit: u64)
            requires
                bit < 64,
            ensures
                (word & (1u64 << bit) == 0) == ((word >> bit) & 1 != 1),
        {
            assert((word & (1u64 << bit) == 0) == ((word >> bit) & 1 != 1)) by (bit_vector)
                requires
                    bit < 64,
            ;
        }
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
