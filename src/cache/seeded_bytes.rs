use std::ops::Range;

/// A file's bytes for a fuzz harness: each a function of the seed and its offset, so that no two
/// seeds agree for long. Made a word at a time: coverage instrumentation traces each comparison
/// a loop makes, so making them byte by byte took a fuzz input much of its time.
#[derive(Clone, Copy, Debug)]
pub(in crate::cache) struct SeededBytes(u64);

impl SeededBytes {
    const WORD: usize = size_of::<u64>();

    pub(in crate::cache) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(in crate::cache) fn over(self, range: Range<u64>) -> Vec<u8> {
        let word = Self::WORD as u64;
        let len = range.end.saturating_sub(range.start) as usize;
        let first = range.start / word;
        let skipped = (range.start % word) as usize;
        let mut bytes = vec![0; (skipped + len).next_multiple_of(Self::WORD)];
        let (words, _) = bytes.as_chunks_mut::<{ Self::WORD }>();
        for (index, into) in (first..).zip(words) {
            *into = self.word(index);
        }
        bytes.drain(..skipped);
        bytes.truncate(len);
        bytes
    }

    fn word(self, index: u64) -> [u8; Self::WORD] {
        let mixed = (index ^ self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .wrapping_mul(0xD1B5_4A32_D192_ED03);
        mixed.to_le_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::SeededBytes;

    #[test]
    fn any_range_is_the_whole_files_bytes_there() {
        let seeded = SeededBytes::new(7);
        let whole = seeded.over(0..200);
        assert_eq!(whole.len(), 200);
        for start in 0..80 {
            for end in start..start + 80 {
                let part = &whole[start as usize..end as usize];
                assert_eq!(seeded.over(start..end), part, "{start}..{end}");
            }
        }
        assert_ne!(SeededBytes::new(8).over(0..200), whole);
    }
}
