//! Both caches live in this process: the second one built on the owner's file is its client.
//! The file leaves the store, so a read the cache didn't serve fails rather than times the store.

use std::path::Path as FsPath;
use std::sync::Arc;

use bytes::Bytes;
use divan::Bencher;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use pollster::block_on;
use t4::{FileHandle, T4Caching};

fn main() {
    divan::main();
}

/// Within memory's chunks, so every block of the file stays there.
const FILE_BYTES: u64 = 4 << 20;

const READ_BYTES: [u64; 2] = [4 << 10, 1 << 20];

struct SharedHotFile {
    _dir: tempfile::TempDir,
    _owner: T4Caching,
    _client: T4Caching,
    through_owner: FileHandle,
    through_client: FileHandle,
}

impl SharedHotFile {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let path = Path::from("hot");
        let data: Bytes = (0..FILE_BYTES).map(|byte| (byte % 251) as u8).collect();
        block_on(store.put(&path, data.into())).unwrap();
        let meta = block_on(store.head(&path)).unwrap();

        let owner = Self::shared_cache(&store, dir.path());
        let client = Self::shared_cache(&store, dir.path());
        let through_owner = owner.open_listed(meta.clone());
        block_on(through_owner.read(0..FILE_BYTES, None)).unwrap();
        block_on(store.delete(&path)).unwrap();
        Self {
            through_client: client.open_listed(meta),
            through_owner,
            _dir: dir,
            _owner: owner,
            _client: client,
        }
    }

    fn shared_cache(store: &Arc<InMemory>, dir: &FsPath) -> T4Caching {
        T4Caching::builder(Arc::clone(store) as Arc<dyn ObjectStore>)
            .with_memory_capacity(64 << 20)
            .with_disk_file(dir.join("t4.cache"), 64 << 20)
            .with_multiprocess_sharing()
            .build()
            .unwrap()
    }

    /// Drops the bytes inside the timed call: a client's read ends then, with a frame to its owner.
    fn read(file: &FileHandle, len: u64) {
        let bytes: Bytes = block_on(file.read(0..len, None)).unwrap();
        assert_eq!(bytes.len() as u64, len);
    }
}

#[divan::bench(args = READ_BYTES)]
fn owner_reads_hot(bencher: Bencher, len: u64) {
    let hot = SharedHotFile::new();
    bencher.bench_local(|| SharedHotFile::read(&hot.through_owner, len));
}

#[divan::bench(args = READ_BYTES)]
fn client_reads_hot(bencher: Bencher, len: u64) {
    let hot = SharedHotFile::new();
    bencher.bench_local(|| SharedHotFile::read(&hot.through_client, len));
}
