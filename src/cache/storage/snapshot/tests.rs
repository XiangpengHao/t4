use std::fs::File;
use std::path::Path;

use super::*;

const MIB: u64 = 1 << 20;

/// The cache files `names` in `dir`, opened, each made if missing.
fn opened(dir: &Path, names: &[&str]) -> Vec<File> {
    names
        .iter()
        .map(|name| {
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.join(name))
                .unwrap()
        })
        .collect()
}

/// Version `id` of the file at `path`, of `size` bytes, with `blocks` on
/// disk.
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

/// Two versions: one with both its blocks on disk, the other its last.
fn snapshot() -> DiskSnapshot {
    DiskSnapshot::new(
        9,
        vec![
            on_disk(b"digest/a.vortex", 3, 2 * MIB, &[(0, 5), (1, 0)]),
            on_disk(b"digest/b", 8, 3 * MIB + 7, &[(3, 2)]),
        ],
    )
}

#[test]
fn a_kept_snapshot_is_taken_back_once() {
    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let place = SnapshotFile::beside(&dir.path().join("cache0"), &files, 4).unwrap();
    assert_eq!(place.path(), dir.path().join("cache0.snapshot"));

    place.keep(&snapshot()).unwrap();
    assert_eq!(place.take().unwrap(), Some(snapshot()));
    assert!(!place.path().exists(), "removed as it is taken");
    assert_eq!(place.take().unwrap(), None);
}

/// A snapshot of other cache files, or of these in another layout, is
/// removed, and not taken for theirs.
#[test]
fn a_snapshot_of_other_cache_files_is_removed_not_taken() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("cache0");
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let keep = |files: &[File]| {
        let place = SnapshotFile::beside(&first, files, 4).unwrap();
        place.keep(&snapshot()).unwrap();
    };
    let reordered = opened(dir.path(), &["cache1", "cache0"]);
    let others = [
        SnapshotFile::beside(&first, &files, 5),
        SnapshotFile::beside(&first, &files[..1], 4),
        SnapshotFile::beside(&first, &reordered, 4),
    ];
    for other in others {
        keep(&files);
        let other = other.unwrap();
        assert_eq!(other.take().unwrap(), None);
        assert!(!other.path().exists());
    }

    // The first file made again, at the same path: another file.
    keep(&files);
    drop((files, reordered));
    std::fs::remove_file(&first).unwrap();
    let remade = opened(dir.path(), &["cache0", "cache1"]);
    let other = SnapshotFile::beside(&first, &remade, 4).unwrap();
    assert_eq!(other.take().unwrap(), None);
    assert!(!other.path().exists());
}

/// Bytes that are no whole snapshot, or a snapshot no cache could have
/// kept, are no snapshot: the cache starts cold.
#[test]
fn bytes_no_cache_could_have_kept_are_no_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let files = opened(dir.path(), &["cache0", "cache1"]);
    let place = SnapshotFile::beside(&dir.path().join("cache0"), &files, 4).unwrap();
    let layout = place.layout();
    let whole = snapshot().encode(layout);
    assert_eq!(DiskSnapshot::decode(&whole, layout), Some(snapshot()));
    for cut in 0..whole.len() {
        assert_eq!(DiskSnapshot::decode(&whole[..cut], layout), None);
    }
    let longer = [&whole[..], &[0]].concat();
    assert_eq!(DiskSnapshot::decode(&longer, layout), None);

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
        let bytes = DiskSnapshot::new(9, versions).encode(layout);
        assert_eq!(DiskSnapshot::decode(&bytes, layout), None, "{what}");
    }

    std::fs::write(place.path(), b"no snapshot").unwrap();
    assert_eq!(place.take().unwrap(), None);
    assert!(!place.path().exists());
}
