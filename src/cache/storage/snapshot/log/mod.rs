mod boot_id;

use std::collections::{BTreeMap, HashSet};

pub(super) use boot_id::BootId;

use super::{CacheLayout, DiskSnapshot, SnapshotReader, SnapshotWriter, VersionOnDisk};
use crate::cache::storage::index::{FileVersion, PathTable};
use crate::cache::{BLOCK_SIZE_U64, BlockKey, ChunkId, FileVersionId};

pub(super) struct DiskLog;

impl DiskLog {
    const MAGIC: [u8; 8] = *b"t4 log\n\0";
    const FORMAT: u32 = 1;

    pub(super) fn base(snapshot: &DiskSnapshot, layout: &CacheLayout, boot: BootId) -> Vec<u8> {
        let mut out = SnapshotWriter::default();
        out.raw(&Self::MAGIC);
        out.u32(Self::FORMAT);
        out.raw(boot.as_bytes());
        let base = snapshot.encode(layout);
        out.u64(base.len() as u64);
        out.u32(RecordChecksum::of(&base));
        out.raw(&base);
        out.0
    }

    pub(super) fn record(entries: &[LogEntry]) -> Vec<u8> {
        let mut body = SnapshotWriter::default();
        for entry in entries {
            entry.encode(&mut body);
        }
        let mut out = SnapshotWriter::default();
        out.u32(u32::try_from(body.0.len()).expect("a log record fits in 4 GiB"));
        out.u32(RecordChecksum::of(&body.0));
        out.raw(&body.0);
        out.0
    }

    pub(super) fn replay(bytes: &[u8], layout: &CacheLayout, boot: BootId) -> Option<DiskSnapshot> {
        let mut input = SnapshotReader { bytes };
        if input.take(8)? != Self::MAGIC || input.u32()? != Self::FORMAT {
            return None;
        }
        let same_boot = input.take(16)? == boot.as_bytes();
        let base_len = usize::try_from(input.u64()?).ok()?;
        let checksum = input.u32()?;
        let base = input.take(base_len)?;
        if RecordChecksum::of(base) != checksum {
            return None;
        }
        let mut replay = LogReplay::new(DiskSnapshot::decode(base, layout)?, layout.chunk_count()?);
        while !input.bytes.is_empty() {
            let Some(body) = Self::take_record(&mut input) else {
                break;
            };
            let mut record = SnapshotReader { bytes: body };
            while !record.bytes.is_empty() {
                replay.apply(LogEntry::decode(&mut record)?)?;
            }
        }
        (same_boot || replay.clean).then(|| replay.snapshot())
    }

    fn take_record<'b>(input: &mut SnapshotReader<'b>) -> Option<&'b [u8]> {
        let len = input.u32()? as usize;
        let checksum = input.u32()?;
        let body = input.take(len)?;
        (RecordChecksum::of(body) == checksum).then_some(body)
    }
}

pub(super) enum LogEntry {
    Path(VersionOnDisk),
    Add {
        version: u64,
        index: u32,
        chunk: u32,
    },
    Free(u32),
    Clean(u64),
}

impl LogEntry {
    fn encode(&self, out: &mut SnapshotWriter) {
        match self {
            Self::Path(version) => {
                out.raw(&[0]);
                out.u64(version.version.id().get());
                out.u64(version.version.size());
                out.bytes(&version.path);
                out.bytes(version.e_tag.as_bytes());
            }
            Self::Add {
                version,
                index,
                chunk,
            } => {
                out.raw(&[1]);
                out.u64(*version);
                out.u32(*index);
                out.u32(*chunk);
            }
            Self::Free(chunk) => {
                out.raw(&[2]);
                out.u32(*chunk);
            }
            Self::Clean(next_id) => {
                out.raw(&[3]);
                out.u64(*next_id);
            }
        }
    }

    fn decode(input: &mut SnapshotReader<'_>) -> Option<Self> {
        match input.take(1)?[0] {
            0 => {
                let (id, size) = (input.u64()?, input.u64()?);
                let path = input.bytes()?;
                let e_tag = std::str::from_utf8(input.bytes()?).ok()?;
                if id >= FileVersionId::LIMIT || path.len() > PathTable::MAX_PATH_LEN {
                    return None;
                }
                Some(Self::Path(VersionOnDisk::new(
                    path.into(),
                    e_tag.into(),
                    FileVersion::named(FileVersionId::new(id), size)?,
                    Vec::new(),
                )))
            }
            1 => Some(Self::Add {
                version: input.u64()?,
                index: input.u32()?,
                chunk: input.u32()?,
            }),
            2 => Some(Self::Free(input.u32()?)),
            3 => Some(Self::Clean(input.u64()?)),
            _ => None,
        }
    }
}

struct LogReplay {
    minimum_new_id: u64,
    next_id: u64,
    versions: BTreeMap<u64, VersionOnDisk>,
    paths: BTreeMap<Box<[u8]>, u64>,
    chunks: Vec<Option<BlockKey>>,
    blocks: HashSet<BlockKey>,
    clean: bool,
}

impl LogReplay {
    fn new(base: DiskSnapshot, chunks: u32) -> Self {
        let mut replay = Self {
            minimum_new_id: base.next_id,
            next_id: base.next_id,
            versions: BTreeMap::new(),
            paths: BTreeMap::new(),
            chunks: vec![None; chunks as usize],
            blocks: HashSet::new(),
            clean: false,
        };
        for mut version in base.versions {
            for (index, chunk) in version.blocks.drain(..) {
                let key = BlockKey::new(version.version.id(), index);
                replay.chunks[chunk.index() as usize] = Some(key);
                replay.blocks.insert(key);
            }
            replay
                .paths
                .insert(version.path.clone(), version.version.id().get());
            replay.versions.insert(version.version.id().get(), version);
        }
        replay
    }

    fn apply(&mut self, entry: LogEntry) -> Option<()> {
        self.clean = matches!(entry, LogEntry::Clean(_));
        match entry {
            LogEntry::Path(version) => {
                let id = version.version.id().get();
                if id < self.minimum_new_id || self.versions.contains_key(&id) {
                    return None;
                }
                self.next_id = self.next_id.max(id + 1);
                self.paths.insert(version.path.clone(), id);
                self.versions.insert(id, version);
            }
            LogEntry::Add {
                version,
                index,
                chunk,
            } => {
                let version = self.versions.get(&version)?.version;
                if index >= BlockKey::INDEX_LIMIT
                    || u64::from(index) * BLOCK_SIZE_U64 >= version.size()
                {
                    return None;
                }
                let held = self.chunks.get_mut(chunk as usize)?;
                let key = BlockKey::new(version.id(), index);
                if held.is_some() || !self.blocks.insert(key) {
                    return None;
                }
                *held = Some(key);
            }
            LogEntry::Free(chunk) => {
                let key = self.chunks.get_mut(chunk as usize)?.take()?;
                self.blocks.remove(&key);
            }
            LogEntry::Clean(next_id) => {
                if next_id < self.next_id || next_id > FileVersionId::LIMIT {
                    return None;
                }
                self.next_id = next_id;
                self.minimum_new_id = next_id;
            }
        }
        Some(())
    }

    fn snapshot(mut self) -> DiskSnapshot {
        for (chunk, key) in self.chunks.into_iter().enumerate() {
            if let Some(key) = key {
                self.versions
                    .get_mut(&key.file_version().get())
                    .unwrap()
                    .blocks
                    .push((key.index(), ChunkId::new(chunk as u32)));
            }
        }
        let versions = self
            .paths
            .into_values()
            .filter_map(|id| {
                let mut version = self.versions.remove(&id).unwrap();
                version.blocks.sort_unstable_by_key(|&(index, _)| index);
                (!version.blocks.is_empty()).then_some(version)
            })
            .collect();
        DiskSnapshot::new(self.next_id, versions)
    }
}

struct RecordChecksum;

impl RecordChecksum {
    fn of(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
