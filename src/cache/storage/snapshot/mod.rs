//! The disk tier kept across a restart (`doc/10-spiral-integration.md`,
//! decision 7): as the cache drops, it writes what its disk chunks hold to
//! a file beside its first cache file; the next cache on those files reads
//! it back as it opens, and removes it before it writes a chunk.
//!
//! - [`DiskSnapshot`]: the disk half of the index at one moment, and its
//!   bytes, checked whole as they are read back.
//! - `snapshot_file`: where a snapshot is kept, what it must match, and how
//!   it is written and taken back ([`SnapshotFile`]).

mod snapshot_file;

use std::collections::HashSet;

use snapshot_file::CacheLayout;
pub(in crate::cache) use snapshot_file::SnapshotFile;

use super::index::{FileVersion, PathTable};
use crate::cache::{BLOCK_SIZE, BLOCK_SIZE_U64, ChunkId, FileVersionId};

/// What the disk tier holds at one moment: each file version with blocks
/// on disk, and the chunk each of those blocks is in; and the id the next
/// new version takes, so that no id is given twice across a restart
/// (`doc/03-cache.md`, decision 5).
#[derive(Debug, PartialEq, Eq)]
pub(in crate::cache) struct DiskSnapshot {
    next_id: u64,
    versions: Vec<VersionOnDisk>,
}

/// A file version with blocks on disk: its path and e_tag, as the path
/// table holds them, and its blocks, each by its number, in order, with the
/// chunk that holds it.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::cache) struct VersionOnDisk {
    path: Box<[u8]>,
    e_tag: Box<str>,
    version: FileVersion,
    blocks: Vec<(u32, ChunkId)>,
}

impl VersionOnDisk {
    pub(in crate::cache) fn new(
        path: Box<[u8]>,
        e_tag: Box<str>,
        version: FileVersion,
        blocks: Vec<(u32, ChunkId)>,
    ) -> Self {
        Self {
            path,
            e_tag,
            version,
            blocks,
        }
    }

    pub(in crate::cache) fn path(&self) -> &[u8] {
        &self.path
    }

    pub(in crate::cache) fn e_tag(&self) -> &str {
        &self.e_tag
    }

    pub(in crate::cache) fn version(&self) -> FileVersion {
        self.version
    }

    pub(in crate::cache) fn blocks(&self) -> &[(u32, ChunkId)] {
        &self.blocks
    }
}

impl DiskSnapshot {
    /// What a snapshot's bytes start with.
    const MAGIC: [u8; 8] = *b"t4 disk\n";
    /// The layout of its bytes: a snapshot of another is never taken back.
    const FORMAT: u32 = 1;

    /// `versions`, in path order, each with a block on disk at least, and
    /// `next_id`, above each of their ids.
    pub(in crate::cache) fn new(next_id: u64, versions: Vec<VersionOnDisk>) -> Self {
        Self { next_id, versions }
    }

    pub(in crate::cache) fn next_id(&self) -> u64 {
        self.next_id
    }

    pub(in crate::cache) fn versions(&self) -> &[VersionOnDisk] {
        &self.versions
    }

    /// Its bytes, little-endian: a header that names `layout`, then the next
    /// id, then each version: its id, size, path and e_tag, and its blocks.
    fn encode(&self, layout: &CacheLayout) -> Vec<u8> {
        let mut out = SnapshotWriter::default();
        out.raw(&Self::MAGIC);
        out.u32(Self::FORMAT);
        out.u32(BLOCK_SIZE as u32);
        layout.encode(&mut out);
        out.u64(self.next_id);
        out.u64(self.versions.len() as u64);
        for version in &self.versions {
            out.u64(version.version.id().get());
            out.u64(version.version.size());
            out.bytes(&version.path);
            out.bytes(version.e_tag.as_bytes());
            out.u32(version.blocks.len() as u32);
            for &(index, chunk) in &version.blocks {
                out.u32(index);
                out.u32(chunk.index());
            }
        }
        out.0
    }

    /// The snapshot `bytes` hold, if they are one of a cache of `layout`,
    /// whole, and could be: every id below the next, no path or id twice,
    /// each block within its file and in a chunk of the tier, and no chunk
    /// twice. `None` for anything else, whatever made it: the cache then
    /// starts cold, which costs refills, never wrong bytes.
    fn decode(bytes: &[u8], layout: &CacheLayout) -> Option<Self> {
        let mut input = SnapshotReader { bytes };
        let header_matches = input.take(Self::MAGIC.len())? == Self::MAGIC
            && input.u32()? == Self::FORMAT
            && input.u32()? == BLOCK_SIZE as u32
            && CacheLayout::decode(&mut input)? == *layout;
        if !header_matches {
            return None;
        }
        let next_id = input.u64()?;
        if next_id > FileVersionId::LIMIT {
            return None;
        }
        let mut ids = HashSet::new();
        let mut taken = vec![false; layout.chunk_count()? as usize];
        let mut versions: Vec<VersionOnDisk> = Vec::new();
        for _ in 0..input.u64()? {
            let (id, size) = (input.u64()?, input.u64()?);
            let path: Box<[u8]> = input.bytes()?.into();
            let e_tag: Box<str> = std::str::from_utf8(input.bytes()?).ok()?.into();
            let in_order = versions.last().is_none_or(|last| last.path < path);
            if id >= next_id || !ids.insert(id) || !in_order {
                return None;
            }
            if path.len() > PathTable::MAX_PATH_LEN {
                return None;
            }
            let version = FileVersion::named(FileVersionId::new(id), size)?;
            let mut blocks: Vec<(u32, ChunkId)> = Vec::new();
            for _ in 0..input.u32()? {
                let (index, chunk) = (input.u32()?, input.u32()?);
                let after_last = blocks.last().is_none_or(|&(last, _)| last < index);
                let in_file = u64::from(index) * BLOCK_SIZE_U64 < size;
                let free = taken.get_mut(chunk as usize).filter(|taken| !**taken)?;
                if !after_last || !in_file {
                    return None;
                }
                *free = true;
                blocks.push((index, ChunkId::new(chunk)));
            }
            if blocks.is_empty() {
                return None;
            }
            versions.push(VersionOnDisk::new(path, e_tag, version, blocks));
        }
        input.bytes.is_empty().then_some(Self { next_id, versions })
    }
}

/// A snapshot's bytes being written, each field appended in order.
#[derive(Default)]
struct SnapshotWriter(Vec<u8>);

impl SnapshotWriter {
    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    /// `bytes`, their length first.
    fn bytes(&mut self, bytes: &[u8]) {
        self.u32(bytes.len() as u32);
        self.raw(bytes);
    }
}

/// A snapshot's bytes being read, each field in order; `None` once a field
/// runs past their end.
struct SnapshotReader<'b> {
    bytes: &'b [u8],
}

impl<'b> SnapshotReader<'b> {
    fn take(&mut self, len: usize) -> Option<&'b [u8]> {
        let (taken, rest) = self.bytes.split_at_checked(len)?;
        self.bytes = rest;
        Some(taken)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// Bytes, their length first.
    fn bytes(&mut self) -> Option<&'b [u8]> {
        let len = self.u32()?;
        self.take(len as usize)
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
