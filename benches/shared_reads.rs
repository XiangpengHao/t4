//! A hot read's time through a shared cache (`doc/11-shared-mode.md`,
//! Cost): the owner's own read of bytes its memory holds, against its
//! client's read of the same bytes, which takes round trips over the
//! socket. Both caches are in this process, as the second one built for
//! sharing on the owner's files is its client: the socket, its threads and
//! the region are a client's in another process.
//!
//! The file leaves the store once the owner holds it: a read the cache
//! didn't serve fails, rather than time the store.

use std::path::Path as FsPath;
use std::sync::Arc;

use bytes::Bytes;
use divan::Bencher;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use pollster::block_on;
use t4::{CachedFile, T4Caching};

fn main() {
    divan::main();
}

/// The bytes the file holds: within memory's chunks, so every block of it
/// stays there.
const FILE_BYTES: u64 = 4 << 20;

/// The read sizes timed: a small read, and a block's.
const READ_BYTES: [u64; 2] = [4 << 10, 1 << 20];

/// An owner and its client, the file's every block in the owner's memory,
/// the file gone from the store.
struct SharedHotFile {
    _dir: tempfile::TempDir,
    _owner: T4Caching,
    _client: T4Caching,
    through_owner: CachedFile,
    through_client: CachedFile,
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

    /// A cache built for sharing on the cache file in `dir`: its owner if
    /// it is the first, else the first's client.
    fn shared_cache(store: &Arc<InMemory>, dir: &FsPath) -> T4Caching {
        T4Caching::builder(Arc::clone(store) as Arc<dyn ObjectStore>)
            .with_memory_capacity(64 << 20)
            .with_disk_file(dir.join("t4.cache"), 64 << 20)
            .with_sharing()
            .build()
            .unwrap()
    }

    /// The file's first `len` bytes, read through `file`.
    fn read(file: &CachedFile, len: u64) -> Bytes {
        block_on(file.read(0..len, None)).unwrap()
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
