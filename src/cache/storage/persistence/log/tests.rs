use super::*;
use crate::cache::storage::persistence::PersistenceLog;
use crate::io::file_system::FileSystem;

struct LogFixture {
    _directory: tempfile::TempDir,
    file: PersistenceLog,
    boot: BootId,
}

impl LogFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache");
        let file_system = FileSystem::default();
        let opened = file_system.open_cache_file(&path, false).unwrap();
        Self {
            file: PersistenceLog::beside(&file_system, &path, &[opened], 4)
                .unwrap()
                .unwrap(),
            boot: BootId::current(&file_system).unwrap(),
            _directory: directory,
        }
    }

    fn base(&self) -> Vec<u8> {
        DiskLog::base(&Self::sample_base(), self.file.layout(), self.boot)
    }

    fn sample_base() -> LogBase {
        LogBase::new(
            3,
            vec![VersionOnDisk::new(
                b"a".as_slice().into(),
                "v1".into(),
                FileVersion::named(FileVersionId::new(1), 2 * BLOCK_SIZE_U64).unwrap(),
                vec![(0, ChunkId::new(0))],
            )],
        )
    }

    fn path(id: u64, path: &[u8]) -> LogEntry {
        LogEntry::Path(VersionOnDisk::new(
            path.into(),
            "v2".into(),
            FileVersion::named(FileVersionId::new(id), BLOCK_SIZE_U64).unwrap(),
            Vec::new(),
        ))
    }

    fn add(version: u64, index: u32, chunk: u32) -> LogEntry {
        LogEntry::Add {
            version,
            index,
            chunk,
        }
    }

    fn replay(&self, bytes: &[u8]) -> Option<LogBase> {
        DiskLog::replay(bytes, self.file.layout(), self.boot)
    }

    fn with_entries(&self, entries: &[LogEntry]) -> Vec<u8> {
        let mut bytes = self.base();
        bytes.extend(DiskLog::record(entries));
        bytes
    }

    fn raw_record(body: &[u8]) -> Vec<u8> {
        let mut out = LogWriter::default();
        out.u32(body.len() as u32);
        out.u32(crc32c::crc32c(body));
        out.raw(body);
        out.0
    }
}

#[test]
fn replay_applies_paths_adds_and_frees_in_order() {
    let fixture = LogFixture::new();
    assert_eq!(
        fixture.replay(&fixture.base()),
        Some(LogFixture::sample_base())
    );
    let bytes = fixture.with_entries(&[
        LogEntry::Free(0),
        LogFixture::path(7, b"b"),
        LogFixture::add(7, 0, 0),
        LogFixture::add(1, 1, 2),
    ]);
    let replayed = fixture.replay(&bytes).unwrap();
    assert_eq!(replayed.next_id(), 8);
    assert_eq!(replayed.versions()[0].blocks(), &[(1, ChunkId::new(2))]);
    assert_eq!(replayed.versions()[1].path(), b"b");
    assert_eq!(replayed.versions()[1].blocks(), &[(0, ChunkId::new(0))]);
    assert_eq!(
        LogBase::decode(
            &replayed.encode(fixture.file.layout()),
            fixture.file.layout()
        ),
        Some(replayed)
    );
}

#[test]
fn a_last_record_cut_short_in_its_header_or_body_replays_the_prefix() {
    let fixture = LogFixture::new();
    let prefix = fixture.with_entries(&[LogFixture::add(1, 1, 2)]);
    let expected = fixture.replay(&prefix);
    assert_ne!(expected, Some(LogFixture::sample_base()));
    let record = DiskLog::record(&[LogEntry::Free(0), LogFixture::add(1, 0, 1)]);
    for cut in 0..record.len() {
        let bytes = [&prefix[..], &record[..cut]].concat();
        assert_eq!(fixture.replay(&bytes), expected, "cut {cut}");
    }
}

#[test]
fn a_complete_record_with_a_bad_crc_starts_cold_in_the_middle_or_at_the_end() {
    let fixture = LogFixture::new();
    let prefix = fixture.with_entries(&[LogFixture::add(1, 1, 2)]);
    let record = DiskLog::record(&[LogEntry::Free(0), LogFixture::add(1, 0, 1)]);
    let following = DiskLog::record(&[LogEntry::Free(2)]);
    for at in 4..record.len() {
        let mut corrupt = record.clone();
        corrupt[at] ^= 1;
        let last = [&prefix[..], &corrupt].concat();
        assert_eq!(fixture.replay(&last), None, "last record, byte {at}");
        let middle = [&last[..], &following].concat();
        assert_eq!(fixture.replay(&middle), None, "middle record, byte {at}");
    }
}

#[test]
fn another_boot_needs_a_final_clean_mark() {
    let fixture = LogFixture::new();
    let mut other_boot = fixture.base();
    other_boot[12] ^= 1;
    assert_eq!(fixture.replay(&other_boot), None);
    other_boot.extend(DiskLog::record(&[LogEntry::Clean(3)]));
    assert_eq!(fixture.replay(&other_boot), Some(LogFixture::sample_base()));
    let clean = other_boot.clone();
    other_boot.extend(DiskLog::record(&[LogEntry::Free(0)]));
    assert_eq!(fixture.replay(&other_boot), None);
    assert_eq!(
        fixture.replay(&[&clean[..], &[9, 0, 0]].concat()),
        Some(LogFixture::sample_base())
    );
}

#[test]
fn replay_rejects_impossible_ownership_and_reused_version_ids() {
    let fixture = LogFixture::new();
    let impossible = [
        vec![LogEntry::Clean(2)],
        vec![LogEntry::Clean(FileVersionId::LIMIT + 1)],
        vec![LogEntry::Clean(8), LogFixture::path(7, b"b")],
        vec![LogFixture::add(1, 1, 0)],
        vec![LogFixture::add(1, 0, 1)],
        vec![LogFixture::add(2, 0, 1)],
        vec![LogFixture::add(1, 2, 1)],
        vec![LogFixture::add(1, BlockKey::INDEX_LIMIT, 1)],
        vec![LogFixture::add(1, 1, 4)],
        vec![LogFixture::add(u64::MAX, 0, 1)],
        vec![LogEntry::Free(1)],
        vec![LogEntry::Free(4)],
        vec![LogEntry::Free(0), LogEntry::Free(0)],
        vec![LogFixture::path(1, b"b")],
        vec![LogFixture::path(2, b"b")],
        vec![LogFixture::path(3, b"b"), LogFixture::path(3, b"c")],
        vec![LogFixture::path(
            3,
            &vec![b'p'; PathTable::MAX_PATH_LEN + 1],
        )],
    ];
    for entries in impossible {
        assert_eq!(fixture.replay(&fixture.with_entries(&entries)), None);
    }
    let largest = fixture.with_entries(&[LogFixture::path(FileVersionId::LIMIT - 1, b"b")]);
    assert_eq!(
        fixture.replay(&largest).unwrap().next_id(),
        FileVersionId::LIMIT
    );
}

#[test]
fn each_path_field_and_every_entry_tag_is_checked() {
    let fixture = LogFixture::new();
    let path_record = DiskLog::record(&[LogFixture::path(3, b"b")]);
    let body = &path_record[8..];
    for cut in 1..body.len() {
        let bytes = [&fixture.base()[..], &LogFixture::raw_record(&body[..cut])].concat();
        assert_eq!(fixture.replay(&bytes), None, "path cut {cut}");
    }
    for (offset, replacement) in [
        (1, FileVersionId::LIMIT.to_le_bytes()),
        (9, (FileVersion::MAX_SIZE + 1).to_le_bytes()),
    ] {
        let mut malformed = body.to_vec();
        malformed[offset..offset + 8].copy_from_slice(&replacement);
        let bytes = [&fixture.base()[..], &LogFixture::raw_record(&malformed)].concat();
        assert_eq!(fixture.replay(&bytes), None);
    }
    let mut invalid_utf8 = body.to_vec();
    *invalid_utf8.last_mut().unwrap() = 255;
    for malformed in [vec![255], vec![1], vec![2], invalid_utf8] {
        let bytes = [&fixture.base()[..], &LogFixture::raw_record(&malformed)].concat();
        assert_eq!(fixture.replay(&bytes), None);
    }
}

#[test]
fn a_bad_header_or_base_starts_cold() {
    let fixture = LogFixture::new();
    let bytes = fixture.base();
    for cut in 0..bytes.len() {
        assert_eq!(fixture.replay(&bytes[..cut]), None);
    }
    for at in (0..12).chain(28..bytes.len()) {
        let mut corrupt = bytes.clone();
        corrupt[at] ^= 1;
        assert_eq!(fixture.replay(&corrupt), None, "byte {at}");
    }
    let mut wrong_layout = bytes;
    // Keep the base CRC valid: the base's decoder must reject the wrong block size.
    wrong_layout[40 + 12] ^= 1;
    let checksum = crc32c::crc32c(&wrong_layout[40..]);
    wrong_layout[36..40].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(fixture.replay(&wrong_layout), None);
}

#[test]
fn paths_can_arrive_out_of_id_order_but_ids_never_repeat() {
    let fixture = LogFixture::new();
    let bytes = fixture.with_entries(&[
        LogFixture::path(8, b"a"),
        LogFixture::path(7, b"a"),
        LogFixture::add(7, 0, 1),
        LogFixture::add(8, 0, 2),
    ]);
    let replayed = fixture.replay(&bytes).unwrap();
    assert_eq!(replayed.next_id(), 9);
    assert_eq!(replayed.versions().len(), 1);
    assert_eq!(replayed.versions()[0].version().id().get(), 7);
    assert_eq!(replayed.versions()[0].blocks(), &[(0, ChunkId::new(1))]);
}
