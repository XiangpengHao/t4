//! What a read may answer. A process's cache trusts the first version of a path it heard, so a
//! read may answer any version current since the process was built (`doc/03-cache.md`,
//! decision 11); a path's absence is never remembered, so only a read that ran while the file was
//! gone may fail.

use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use object_store::GetRange;

/// Each byte a hash of the id and offset, so no two contents agree for long.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Content {
    id: u64,
    size: u64,
}

impl Content {
    pub(super) fn size(self) -> u64 {
        self.size
    }

    pub(super) fn bytes(self) -> Bytes {
        (0..self.size).map(|offset| self.byte(offset)).collect()
    }

    fn byte(self, offset: u64) -> u8 {
        let word = ((offset / 8) ^ self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .wrapping_mul(0xD1B5_4A32_D192_ED03);
        word.to_le_bytes()[(offset % 8) as usize]
    }

    fn holds(self, range: &Range<u64>, bytes: &[u8]) -> bool {
        range.end - range.start == bytes.len() as u64
            && (range.start..range.end)
                .zip(bytes)
                .all(|(offset, &byte)| self.byte(offset) == byte)
    }
}

#[derive(PartialEq, Eq)]
pub(super) enum Answer {
    Bytes { range: Range<u64>, bytes: Bytes },
    Failed,
}

impl Answer {
    /// Whether `content` read over `asked` answers this.
    fn is_of(&self, content: Option<Content>, asked: Option<&GetRange>) -> bool {
        let range = match (content, asked) {
            (None, _) => return *self == Self::Failed,
            (Some(content), None) => Ok(0..content.size),
            (Some(content), Some(asked)) => asked.as_range(content.size),
        };
        match (self, range) {
            (Self::Bytes { range, bytes }, Ok(expected)) => {
                *range == expected && content.is_some_and(|content| content.holds(range, bytes))
            }
            (Self::Failed, Err(_)) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes { range, bytes } => f
                .debug_struct("Bytes")
                .field("range", range)
                .field("head", &&bytes[..bytes.len().min(16)])
                .finish(),
            Self::Failed => f.write_str("Failed"),
        }
    }
}

/// Each file's versions, by the step that wrote them; step 0 is the start, before any.
#[derive(Debug, Default)]
pub(super) struct Model {
    files: [Vec<Written>; Model::FILES],
    next_id: u64,
}

#[derive(Clone, Copy, Debug)]
struct Written {
    step: usize,
    content: Option<Content>,
}

impl Model {
    pub(super) const FILES: usize = 3;

    pub(super) fn write(&mut self, file: usize, step: usize, size: Option<u64>) -> Option<Content> {
        let content = size.map(|size| {
            self.next_id += 1;
            Content {
                id: self.next_id,
                size,
            }
        });
        self.files[file].push(Written { step, content });
        content
    }

    pub(super) fn current(&self, file: usize) -> Option<Content> {
        self.files[file].last().and_then(|written| written.content)
    }

    /// What a read that started at `started`, by a process built at `built`, may answer.
    pub(super) fn allows(
        &self,
        file: usize,
        built: usize,
        started: usize,
        asked: Option<&GetRange>,
        answer: &Answer,
    ) -> bool {
        let versions = &self.files[file];
        // The version current at a step is the last written by then: none before the first.
        let current_at = |step: usize| versions.iter().rposition(|written| written.step <= step);
        let since_built = current_at(built).unwrap_or(0);
        let known = versions[since_built..]
            .iter()
            .filter_map(|written| written.content)
            .any(|content| answer.is_of(Some(content), asked));
        let gone = match current_at(started) {
            None => true,
            Some(at) => versions[at..]
                .iter()
                .any(|written| written.content.is_none()),
        };
        known || (gone && answer.is_of(None, asked))
    }

    /// When `content` was written, if its file was written that once: the owner holds a path's
    /// one version, which a read of another version of the file replaces.
    pub(super) fn written_once(&self, content: Content) -> Option<usize> {
        let file = (self.files.iter())
            .find(|file| file.iter().any(|written| written.content == Some(content)))?;
        match file.as_slice() {
            [written] => Some(written.step),
            _ => None,
        }
    }
}
