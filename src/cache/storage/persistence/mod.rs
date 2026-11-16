mod log;
mod log_file;

use std::collections::HashSet;

use log_file::CacheLayout;
pub(in crate::cache) use log_file::PersistenceLog;

use super::index::{FileVersion, PathTable};
use crate::cache::{BLOCK_SIZE, BLOCK_SIZE_U64, ChunkId, FileVersionId};

/// Keeps `next_id` so that no id is given twice across a restart.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::cache) struct LogBase {
    next_id: u64,
    versions: Vec<VersionOnDisk>,
}

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

impl LogBase {
    const MAGIC: [u8; 8] = *b"t4 disk\n";
    const FORMAT: u32 = 1;

    pub(in crate::cache) fn new(next_id: u64, versions: Vec<VersionOnDisk>) -> Self {
        Self { next_id, versions }
    }

    pub(in crate::cache) fn next_id(&self) -> u64 {
        self.next_id
    }

    pub(in crate::cache) fn versions(&self) -> &[VersionOnDisk] {
        &self.versions
    }

    fn encode(&self, layout: &CacheLayout) -> Vec<u8> {
        let mut out = LogWriter::default();
        Self::encode_header(layout, &mut out);
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

    fn encode_header(layout: &CacheLayout, out: &mut LogWriter) {
        out.raw(&Self::MAGIC);
        out.u32(Self::FORMAT);
        out.u32(BLOCK_SIZE as u32);
        layout.encode(out);
    }

    /// Rejects anything doubtful: starting cold costs refills, never wrong bytes.
    fn decode(bytes: &[u8], layout: &CacheLayout) -> Option<Self> {
        let mut input = LogReader { bytes };
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

#[cfg(feature = "__fuzz")]
impl LogBase {
    pub(in crate::cache) fn fuzzed(body: &[u8], chunks: u32) -> Option<Self> {
        let layout = CacheLayout::fuzzed(chunks);
        let mut bytes = LogWriter::default();
        Self::encode_header(&layout, &mut bytes);
        bytes.raw(body);
        let base = Self::decode(&bytes.0, &layout)?;
        assert_eq!(
            base.encode(&layout),
            bytes.0,
            "{base:?} encodes to other bytes"
        );
        Some(base)
    }

    pub(in crate::cache) fn fuzzed_body(&self, chunks: u32) -> Vec<u8> {
        let layout = CacheLayout::fuzzed(chunks);
        let mut header = LogWriter::default();
        Self::encode_header(&layout, &mut header);
        self.encode(&layout).split_off(header.0.len())
    }
}

#[derive(Default)]
struct LogWriter(Vec<u8>);

impl LogWriter {
    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.u32(bytes.len() as u32);
        self.raw(bytes);
    }
}

struct LogReader<'b> {
    bytes: &'b [u8],
}

impl<'b> LogReader<'b> {
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

    fn bytes(&mut self) -> Option<&'b [u8]> {
        let len = self.u32()?;
        self.take(len as usize)
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
