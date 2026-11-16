use std::path::Path;

use super::*;
use crate::io::file_system::{FileSystem, OpenFile};

const MIB: u64 = 1 << 20;

fn opened(dir: &Path, names: &[&str]) -> Vec<OpenFile> {
    let file_system = FileSystem::default();
    names
        .iter()
        .map(|name| file_system.open_cache_file(&dir.join(name), false).unwrap())
        .collect()
}

fn on_disk(path: &[u8], id: u64, size: u64, blocks: &[(u32, u32)]) -> VersionOnDisk {
    VersionOnDisk::new(
        path.into(),
        format!("e{id}").into(),
        FileVersion::named(FileVersionId::new(id), size).unwrap(),
        blocks
            .iter()
            .map(|&(index, chunk)| (index, ChunkId::new(chunk)))
            .collect(),
    )
}

fn sample_base() -> LogBase {
    LogBase::new(
        9,
        vec![
            on_disk(b"digest/a.vortex", 3, 2 * MIB, &[(0, 5), (1, 0)]),
            on_disk(b"digest/b", 8, 3 * MIB + 7, &[(3, 2)]),
        ],
    )
}

#[test]
fn a_clean_log_is_replayed_and_rebased_on_each_build() {
    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let place = PersistenceLog::beside(
        &FileSystem::default(),
        &dir.path().join("cache0"),
        &files,
        4,
    )
    .unwrap()
    .unwrap();
    assert_eq!(place.path(), dir.path().join("cache0.log"));

    place.seed(&sample_base()).unwrap();
    assert_eq!(place.take().unwrap(), Some(sample_base()));
    assert!(place.path().exists(), "replaced with an unclean base");
    assert_eq!(place.take().unwrap(), Some(sample_base()));
}

#[test]
fn a_log_of_other_cache_files_is_rebased_cold() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("cache0");
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let keep = |files: &[OpenFile]| {
        let place = PersistenceLog::beside(&FileSystem::default(), &first, files, 4)
            .unwrap()
            .unwrap();
        place.seed(&sample_base()).unwrap();
    };
    let reordered = opened(dir.path(), &["cache1", "cache0"]);
    let others = [
        PersistenceLog::beside(&FileSystem::default(), &first, &files, 5),
        PersistenceLog::beside(&FileSystem::default(), &first, &files[..1], 4),
        PersistenceLog::beside(&FileSystem::default(), &first, &reordered, 4),
    ];
    for other in others {
        keep(&files);
        let other = other.unwrap().unwrap();
        assert_eq!(other.take().unwrap(), None);
        assert!(other.path().exists());
    }

    // The first file made again, at the same path: another file.
    keep(&files);
    drop((files, reordered));
    std::fs::remove_file(&first).unwrap();
    let remade = opened(dir.path(), &["cache0", "cache1"]);
    let other = PersistenceLog::beside(&FileSystem::default(), &first, &remade, 4)
        .unwrap()
        .unwrap();
    assert_eq!(other.take().unwrap(), None);
    assert!(other.path().exists());
}

#[test]
fn bytes_no_cache_could_have_kept_are_no_base() {
    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let place = PersistenceLog::beside(
        &FileSystem::default(),
        &dir.path().join("cache0"),
        &files,
        4,
    )
    .unwrap()
    .unwrap();
    let layout = place.layout();
    let whole = sample_base().encode(layout);
    assert_eq!(LogBase::decode(&whole, layout), Some(sample_base()));
    for cut in 0..whole.len() {
        assert_eq!(LogBase::decode(&whole[..cut], layout), None);
    }
    let longer = [&whole[..], &[0]].concat();
    assert_eq!(LogBase::decode(&longer, layout), None);

    let a = |id, blocks: &[(u32, u32)]| on_disk(b"a", id, 2 * MIB, blocks);
    let b = |id, blocks: &[(u32, u32)]| on_disk(b"b", id, 2 * MIB, blocks);
    let long_path = vec![b'p'; PathTable::MAX_PATH_LEN + 1];
    let impossible = [
        ("an id not below the next", vec![a(9, &[(0, 0)])]),
        ("an id twice", vec![a(1, &[(0, 0)]), b(1, &[(0, 1)])]),
        ("paths out of order", vec![b(1, &[(0, 0)]), a(2, &[(0, 1)])]),
        ("a path twice", vec![a(1, &[(0, 0)]), a(2, &[(0, 1)])]),
        (
            "a path too long",
            vec![on_disk(&long_path, 1, MIB, &[(0, 0)])],
        ),
        ("a chunk twice", vec![a(1, &[(0, 3)]), b(2, &[(0, 3)])]),
        ("a chunk past the tier", vec![a(1, &[(0, 8)])]),
        ("a block past the file", vec![a(1, &[(2, 0)])]),
        ("blocks out of order", vec![a(1, &[(1, 0), (0, 1)])]),
        ("a version with no block", vec![a(1, &[])]),
    ];
    for (what, versions) in impossible {
        let bytes = LogBase::new(9, versions).encode(layout);
        assert_eq!(LogBase::decode(&bytes, layout), None, "{what}");
    }

    std::fs::write(place.path(), b"no log").unwrap();
    assert_eq!(place.take().unwrap(), None);
    assert!(place.path().exists());
}

#[test]
fn a_log_only_grows_until_its_next_build_rebases_it() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let log = PersistenceLog::beside(
        &FileSystem::default(),
        &dir.path().join("cache0"),
        &files,
        4,
    )
    .unwrap()
    .unwrap();
    log.seed(&sample_base()).unwrap();
    assert_eq!(log.take().unwrap(), Some(sample_base()));
    let base = std::fs::read(log.path()).unwrap();
    let inode = std::fs::metadata(log.path()).unwrap().ino();
    let version = FileVersion::named(FileVersionId::new(9), MIB).unwrap();
    {
        let mut changes = log.changes();
        changes.path(b"digest/c", "e-9", version);
        changes.flush().unwrap();
        changes.add(
            crate::cache::BlockKey::new(version.id(), 0),
            ChunkId::new(1),
        );
        changes.free(ChunkId::new(5));
        changes.flush().unwrap();
    }
    log.finish(10).unwrap();
    let appended = std::fs::read(log.path()).unwrap();
    assert!(appended.starts_with(&base));
    assert_eq!(std::fs::metadata(log.path()).unwrap().ino(), inode);
    let replayed = log.inspect().unwrap();
    assert_eq!(replayed.next_id(), 10);
    assert_eq!(replayed.versions()[0].blocks(), &[(1, ChunkId::new(0))]);
    assert_eq!(replayed.versions()[2].blocks(), &[(0, ChunkId::new(1))]);
    assert_eq!(log.take().unwrap(), Some(replayed));
    assert!(std::fs::metadata(log.path()).unwrap().len() < appended.len() as u64);
    assert_ne!(std::fs::metadata(log.path()).unwrap().ino(), inode);
}

#[test]
fn with_no_boot_id_no_log_is_kept_and_an_old_one_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let first = dir.path().join("cache0");
    let log = PersistenceLog::beside(&FileSystem::default(), &first, &files, 4)
        .unwrap()
        .unwrap();
    log.seed(&sample_base()).unwrap();
    let path = log.path().to_owned();
    drop(log);
    assert!(path.exists());
    assert!(
        PersistenceLog::beside_in_boot(&FileSystem::default(), &first, &files, 4, None)
            .unwrap()
            .is_none()
    );
    assert!(!path.exists(), "an old log could outlive a machine crash");
}
