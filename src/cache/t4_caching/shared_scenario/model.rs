//! What a read may answer. A process's cache trusts the first version of a path it heard, so a
//! read may answer any version current since the process was built (`doc/03-cache.md`,
//! decision 11); a path's absence is never remembered, so only a read that ran while the file was
//! gone may fail.

use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use object_store::GetRange;

use crate::cache::BLOCK_SIZE_U64;
use crate::cache::seeded_bytes::SeededBytes;

/// Its bytes are seeded by its id, so no two contents agree for long.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Content {
    id: u64,
    size: u64,
}

impl Content {
    pub(super) fn size(self) -> u64 {
        self.size
    }
}

/// A content and its bytes, made once as it is written: a read is checked against a slice of them.
#[derive(Clone)]
pub(super) struct Version {
    content: Content,
    bytes: Bytes,
}

impl Version {
    fn of(content: Content) -> Self {
        let bytes = SeededBytes::new(content.id).over(0..content.size).into();
        Self { content, bytes }
    }

    pub(super) fn content(&self) -> Content {
        self.content
    }

    pub(super) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.content.fmt(f)
    }
}

#[derive(PartialEq, Eq)]
pub(super) enum Answer {
    Bytes { range: Range<u64>, bytes: Bytes },
    Failed,
}

impl Answer {
    /// Whether `version` read over `asked` answers this.
    fn is_of(&self, version: Option<&Version>, asked: Option<&GetRange>) -> bool {
        let Some(version) = version else {
            return *self == Self::Failed;
        };
        let size = version.content.size;
        let range = match asked {
            None => Ok(0..size),
            Some(asked) => asked.as_range(size),
        };
        match (self, range) {
            (Self::Bytes { range, bytes }, Ok(expected)) => {
                *range == expected
                    && version.bytes[range.start as usize..range.end as usize] == bytes[..]
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

#[derive(Clone, Debug)]
struct Written {
    step: usize,
    version: Option<Version>,
}

impl Model {
    pub(super) const FILES: usize = 3;

    pub(super) fn write(&mut self, file: usize, step: usize, size: Option<u64>) -> Option<Version> {
        let version = size.map(|size| {
            self.next_id += 1;
            Version::of(Content {
                id: self.next_id,
                size,
            })
        });
        let written = Written {
            step,
            version: version.clone(),
        };
        self.files[file].push(written);
        version
    }

    pub(super) fn current(&self, file: usize) -> Option<Content> {
        self.files[file].last()?.content()
    }

    /// The blocks of the largest version of `file` written so far.
    pub(super) fn most_blocks(&self, file: usize) -> u64 {
        (self.files[file].iter())
            .filter_map(Written::content)
            .map(|content| content.size.div_ceil(BLOCK_SIZE_U64))
            .max()
            .unwrap_or(0)
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
            .filter_map(|written| written.version.as_ref())
            .any(|version| answer.is_of(Some(version), asked));
        let gone = match current_at(started) {
            None => true,
            Some(at) => versions[at..]
                .iter()
                .any(|written| written.version.is_none()),
        };
        known || (gone && answer.is_of(None, asked))
    }

    /// When `content` was written, if its file was written that once: the owner holds a path's
    /// one version, which a read of another version of the file replaces.
    pub(super) fn written_once(&self, content: Content) -> Option<usize> {
        let file = (self.files.iter()).find(|file| {
            file.iter()
                .any(|written| written.content() == Some(content))
        })?;
        match file.as_slice() {
            [written] => Some(written.step),
            _ => None,
        }
    }
}

impl Written {
    fn content(&self) -> Option<Content> {
        self.version.as_ref().map(Version::content)
    }
}
