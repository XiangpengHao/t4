//! [`ClockPolicy`]: the default policy, a lock-free CLOCK for each tier.

use std::num::NonZeroU32;

use super::{Admit, BlockRead, DiskHit, MemoryVictim, PlacementPolicy, Tier, VictimAction};
use crate::cache::{BlockKey, ChunkId};
use crate::io::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// The default policy: a CLOCK for each tier.
///
/// A fetched block goes to memory, and memory's victims to disk. A block
/// read from disk moves to memory on its second hit since the disk's hand
/// last passed it: one read of a cold block pushes nothing out of memory.
pub struct ClockPolicy {
    memory: Clock,
    disk: Clock,
}

impl ClockPolicy {
    /// A CLOCK for each tier, of `memory_chunks` and `disk_chunks` chunks.
    pub fn new(memory_chunks: NonZeroU32, disk_chunks: NonZeroU32) -> Self {
        Self {
            memory: Clock::new(memory_chunks),
            disk: Clock::new(disk_chunks),
        }
    }

    fn clock(&self, tier: Tier) -> &Clock {
        match tier {
            Tier::Memory => &self.memory,
            Tier::Disk => &self.disk,
        }
    }
}

impl PlacementPolicy for ClockPolicy {
    fn on_memory_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) {
        self.memory.mark_referenced(chunk);
    }

    fn on_disk_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) -> DiskHit {
        if self.disk.mark_referenced(chunk) {
            DiskHit::Promote
        } else {
            DiskHit::Stay
        }
    }

    fn on_remote_read(&self, _read: &BlockRead<'_>) -> Admit {
        Admit::Memory
    }

    fn on_admit(&self, tier: Tier, chunk: ChunkId, _block: BlockKey) {
        self.clock(tier).admit(chunk);
    }

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim> {
        self.memory
            .pick(count)
            .into_iter()
            .map(|chunk| MemoryVictim::new(chunk, VictimAction::ToDisk))
            .collect()
    }

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId> {
        self.disk.pick(count)
    }
}

/// One tier's CLOCK, over its chunk numbers: for each chunk, whether it
/// holds a block the hand may take, and whether a hit gave it a second
/// chance since the hand last passed. The hand sweeps a word of 64 chunks
/// at a time.
///
/// Every atomic here is relaxed. The cache checks each victim against its
/// chunk table, so the policy hands no memory over through these words: it
/// only keeps its own bits whole. And that takes atomicity, not ordering:
/// the read-modify-writes of one word are in one order, each seeing the one
/// before. So a claim takes an admit's bit at most once, and no bit set is
/// lost to another change of its word. A hit racing a sweep lands on either
/// side of it: a chunk gets a second chance more or less, nothing worse.
struct Clock {
    /// Chunk `i`'s bits are bit `i % 64` of word `i / 64`. The bits past
    /// the last chunk are never set: every chunk is checked against
    /// `chunk_count` first.
    words: Box<[ClockWord]>,
    /// The words swept so far: the next is `hand % words.len()`. Concurrent
    /// picks take different words, unless they sweep more than a rotation
    /// between them.
    hand: AtomicUsize,
    chunk_count: u32,
}

impl Clock {
    const WORD_BITS: u32 = u64::BITS;

    /// `chunk_count` chunks, none holding a block.
    fn new(chunk_count: NonZeroU32) -> Self {
        let word_count = chunk_count.get().div_ceil(Self::WORD_BITS);
        Self {
            words: (0..word_count).map(|_| ClockWord::default()).collect(),
            hand: AtomicUsize::new(0),
            chunk_count: chunk_count.get(),
        }
    }

    /// Marks `chunk` referenced, and returns whether it was already: hit
    /// since the hand last passed.
    fn mark_referenced(&self, chunk: ChunkId) -> bool {
        let (word, bit) = self.bit_of(chunk);
        // A hot chunk is marked already: the load spares its cache line a
        // write per hit.
        if word.referenced.load(Ordering::Relaxed) & bit != 0 {
            return true;
        }
        // A hit since the load may have marked it: then this one is the
        // second.
        word.referenced.fetch_or(bit, Ordering::Relaxed) & bit != 0
    }

    /// `chunk` took a block: the hand takes it on its next pass, unless a
    /// hit comes first. So a block read once goes soon, and a scan leaves
    /// the blocks read again.
    fn admit(&self, chunk: ChunkId) {
        let (word, bit) = self.bit_of(chunk);
        // A hit on the chunk's last block gives the new one no second chance.
        word.referenced.fetch_and(!bit, Ordering::Relaxed);
        word.occupied.fetch_or(bit, Ordering::Relaxed);
    }

    /// Up to `count` chunks the hand takes, each this call's alone until it
    /// is admitted again. Fewer if the hand finds no more in two rotations:
    /// the first clears every second chance, so the second takes every
    /// chunk not hit again meanwhile.
    fn pick(&self, count: usize) -> Vec<ChunkId> {
        let mut victims = Vec::new();
        for _ in 0..2 * self.words.len() {
            if victims.len() == count {
                break;
            }
            let word_index = self.hand.fetch_add(1, Ordering::Relaxed) % self.words.len();
            self.sweep(word_index, count, &mut victims);
        }
        victims
    }

    /// Passes the hand over word `word_index`: clears its chunks' second
    /// chances, and claims those that had none and hold a block, until
    /// `victims` holds `count`.
    fn sweep(&self, word_index: usize, count: usize, victims: &mut Vec<ChunkId>) {
        let word = &self.words[word_index];
        let referenced = word.referenced.swap(0, Ordering::Relaxed);
        let mut candidates = word.occupied.load(Ordering::Relaxed) & !referenced;
        while candidates != 0 && victims.len() < count {
            let offset = candidates.trailing_zeros();
            candidates &= candidates - 1;
            let bit = 1 << offset;
            // Another pick may have claimed it since the load: it is this
            // one's only if this call cleared the bit.
            if word.occupied.fetch_and(!bit, Ordering::Relaxed) & bit != 0 {
                // Below `chunk_count`, a `u32`: the bits past it are never
                // set.
                let first_chunk = word_index as u32 * Self::WORD_BITS;
                victims.push(ChunkId::new(first_chunk + offset));
            }
        }
    }

    /// The word holding `chunk`'s bits, and its bit in them.
    fn bit_of(&self, chunk: ChunkId) -> (&ClockWord, u64) {
        assert!(
            chunk.index() < self.chunk_count,
            "{chunk:?} is past the tier's {} chunks",
            self.chunk_count
        );
        let index = chunk.index();
        let word = &self.words[(index / Self::WORD_BITS) as usize];
        (word, 1 << (index % Self::WORD_BITS))
    }
}

/// 64 chunks' bits in a [`Clock`]. The two side by side: an admit and a
/// sweep change both, in one cache line, as the alignment to the pair's
/// size keeps it from straddling two.
#[derive(Default)]
#[repr(align(16))]
struct ClockWord {
    /// Set by a hit; cleared by the hand's pass, and by an admit.
    referenced: AtomicU64,
    /// Set by an admit; cleared by the pick that claims the chunk.
    occupied: AtomicU64,
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::cache::FileVersionId;
    use crate::io::sync::atomic::{AtomicBool, fence};

    /// A policy whose tiers have `chunk_count` chunks each.
    fn policy(chunk_count: u32) -> ClockPolicy {
        let chunk_count = NonZeroU32::new(chunk_count).unwrap();
        ClockPolicy::new(chunk_count, chunk_count)
    }

    fn block() -> BlockKey {
        BlockKey::new(FileVersionId::new(1), 0)
    }

    fn read() -> BlockRead<'static> {
        BlockRead::new(block(), 1, 0..1, b"a", None, true)
    }

    fn admit(policy: &ClockPolicy, tier: Tier, chunks: impl IntoIterator<Item = u32>) {
        for chunk in chunks {
            policy.on_admit(tier, ChunkId::new(chunk), block());
        }
    }

    fn hit(policy: &ClockPolicy, chunk: u32) {
        policy.on_memory_hit(ChunkId::new(chunk), &read());
    }

    fn disk_hit(policy: &ClockPolicy, chunk: u32) -> DiskHit {
        policy.on_disk_hit(ChunkId::new(chunk), &read())
    }

    /// Up to `count` memory victims, each moved to disk: their chunk
    /// numbers, sorted.
    fn pick(policy: &ClockPolicy, count: usize) -> Vec<u32> {
        let mut chunks: Vec<_> = policy
            .pick_memory_victims(count)
            .into_iter()
            .map(|victim| {
                assert_eq!(victim.action(), VictimAction::ToDisk);
                victim.chunk().index()
            })
            .collect();
        chunks.sort_unstable();
        chunks
    }

    #[test]
    fn a_fetched_block_goes_to_memory_and_picks_take_admitted_chunks_only() {
        let policy = policy(200);
        assert_eq!(policy.on_remote_read(&read()), Admit::Memory);
        admit(&policy, Tier::Memory, [3, 64, 130, 199]);
        assert_eq!(pick(&policy, 10), [3, 64, 130, 199]);
        assert!(policy.pick_disk_victims(10).is_empty(), "nothing on disk");
    }

    #[test]
    fn a_chunk_is_picked_once_and_some_picks_find_fewer_than_asked() {
        let policy = policy(10);
        admit(&policy, Tier::Memory, 0..10);
        assert_eq!(pick(&policy, 4), [0, 1, 2, 3]);
        assert_eq!(pick(&policy, 4), [4, 5, 6, 7]);
        assert_eq!(pick(&policy, 4), [8, 9]);
        assert!(pick(&policy, 4).is_empty());
        assert!(pick(&policy, 0).is_empty());
    }

    #[test]
    fn a_victim_admitted_again_is_picked_again_unreferenced() {
        let policy = policy(2);
        admit(&policy, Tier::Memory, [0, 1]);
        assert_eq!(pick(&policy, 2), [0, 1]);
        assert!(pick(&policy, 2).is_empty());
        // A read of the block chunk 1 held, racing its eviction.
        hit(&policy, 1);
        admit(&policy, Tier::Memory, [0, 1]);
        hit(&policy, 0);
        assert_eq!(pick(&policy, 1), [1], "the admit cleared the stale hit");
        assert_eq!(pick(&policy, 1), [0]);
    }

    #[test]
    fn a_chunk_hit_since_the_hand_passed_survives_one_sweep() {
        // One word: every pick sweeps it.
        let policy = policy(64);
        admit(&policy, Tier::Memory, 0..4);
        hit(&policy, 0);
        assert_eq!(pick(&policy, 3), [1, 2, 3]);
        // The sweep took its second chance, and no hit gave it another.
        assert_eq!(pick(&policy, 1), [0]);
    }

    #[test]
    fn a_chunk_hit_before_every_pick_goes_last() {
        // Two words: a pick sweeps the other between its two passes of
        // chunk 0's word.
        let policy = policy(100);
        admit(&policy, Tier::Memory, 0..100);
        let mut others = Vec::new();
        for _ in 1..100 {
            hit(&policy, 0);
            others.extend(pick(&policy, 1));
        }
        others.sort_unstable();
        assert_eq!(others, (1..100).collect::<Vec<_>>());
        // Alone, it goes on the second rotation, hit or not.
        hit(&policy, 0);
        assert_eq!(pick(&policy, 1), [0]);
    }

    #[test]
    fn a_tier_of_a_partial_word_picks_its_chunks_only() {
        let policy = policy(70);
        admit(&policy, Tier::Memory, 0..70);
        assert_eq!(pick(&policy, 100), (0..70).collect::<Vec<_>>());
    }

    #[test]
    #[should_panic(expected = "past the tier's 70 chunks")]
    fn a_chunk_past_the_tier_panics() {
        admit(&policy(70), Tier::Memory, [70]);
    }

    #[test]
    fn a_disk_block_moves_to_memory_on_its_second_hit() {
        let policy = policy(2);
        admit(&policy, Tier::Disk, [0, 1]);
        assert_eq!(disk_hit(&policy, 0), DiskHit::Stay);
        assert_eq!(disk_hit(&policy, 0), DiskHit::Promote);
        // The hand's pass clears the hits: the next is a first again.
        assert_eq!(policy.pick_disk_victims(1), [ChunkId::new(1)]);
        assert_eq!(disk_hit(&policy, 0), DiskHit::Stay);
        // As does an admit: a new block starts unreferenced.
        admit(&policy, Tier::Disk, [0]);
        assert_eq!(disk_hit(&policy, 0), DiskHit::Stay);
        assert_eq!(disk_hit(&policy, 0), DiskHit::Promote);
        assert!(pick(&policy, 2).is_empty(), "memory holds none");
    }

    /// Four pickers on one word, each giving its victims back as the cache
    /// does when it reuses a chunk, against a hitter and an admitter of fresh
    /// chunks: no two pickers hold one chunk at once, and no admission is
    /// lost.
    #[test]
    fn concurrent_pickers_never_hold_one_chunk_at_once() {
        const CHUNKS: u32 = 64;
        let policy = policy(CHUNKS);
        let held: Vec<_> = (0..CHUNKS).map(|_| AtomicBool::new(false)).collect();
        admit(&policy, Tier::Memory, 0..CHUNKS / 2);
        std::thread::scope(|scope| {
            scope.spawn(|| admit(&policy, Tier::Memory, CHUNKS / 2..CHUNKS));
            scope.spawn(|| {
                for _ in 0..1000 {
                    (0..CHUNKS).for_each(|chunk| hit(&policy, chunk));
                }
            });
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..20_000 {
                        let victims = pick(&policy, 2);
                        // The policy's atomics are relaxed: these fences,
                        // paired through a give-back's admit and the next
                        // claim, which reads it, order one picker's flags
                        // before the next one's.
                        fence(Ordering::Acquire);
                        for &chunk in &victims {
                            let taken = held[chunk as usize].swap(true, Ordering::Relaxed);
                            assert!(!taken, "two pickers hold chunk {chunk}");
                        }
                        for &chunk in &victims {
                            held[chunk as usize].store(false, Ordering::Relaxed);
                        }
                        fence(Ordering::Release);
                        admit(&policy, Tier::Memory, victims);
                    }
                });
            }
        });
        // Alone, two rotations take every chunk: each was given back.
        let every_chunk: Vec<_> = (0..CHUNKS).collect();
        assert_eq!(pick(&policy, CHUNKS as usize), every_chunk);
    }
}
