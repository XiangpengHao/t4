use std::ops::Range;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use shuttle::future::block_on;

use crate::PAGE_SIZE_U64;
use crate::cache::BLOCK_SIZE_U64;
use crate::io::sync::Arc;

/// Each byte is a function of its path and offset, so that a client lands a fetch's true bytes
/// knowing only its range, whichever version it named.
pub(super) struct SharedFiles {
    store: Arc<InMemory>,
    metas: Vec<ObjectMeta>,
}

impl SharedFiles {
    /// The first `READ_BY_BOTH` are the well-behaved client's: the misbehaving one lands only
    /// their true bytes, as it may commit any bytes under any name (`doc/11-shared-mode.md`,
    /// decision 8). Each ends past a block's start, in a page's middle.
    const FILES: [(&str, u64); 3] = [
        ("g0", BLOCK_SIZE_U64 + 3000),
        ("g1", 2 * BLOCK_SIZE_U64 - PAGE_SIZE_U64 - 7),
        ("e0", 3 * PAGE_SIZE_U64 + 500),
    ];
    const READ_BY_BOTH: usize = 2;

    pub(super) fn put() -> Self {
        let store = Arc::new(InMemory::new());
        let metas = Self::FILES
            .iter()
            .map(|&(name, size)| {
                let path = Path::from(name);
                let bytes = Self::bytes(path.as_ref().as_bytes(), 0..size);
                block_on(store.put(&path, Bytes::from(bytes).into())).expect("an in-memory put");
                block_on(store.head(&path)).expect("the file just put")
            })
            .collect();
        Self { store, metas }
    }

    pub(super) fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }

    /// `pick` past the files names a path the store doesn't hold.
    pub(super) fn path(pick: u8) -> Bytes {
        match Self::FILES.get(usize::from(pick)) {
            Some((name, _)) => Bytes::from_static(name.as_bytes()),
            None => Bytes::from_static(b"absent"),
        }
    }

    pub(super) fn meta(&self, path: &[u8]) -> Option<&ObjectMeta> {
        self.metas
            .iter()
            .find(|meta| meta.location.as_ref().as_bytes() == path)
    }

    /// A path the store doesn't hold has no e_tag, and no bytes.
    pub(super) fn version_of(&self, path: &[u8]) -> (String, u64) {
        match self.meta(path) {
            Some(meta) => (meta.e_tag.clone().unwrap_or_default(), meta.size),
            None => (String::new(), 0),
        }
    }

    pub(super) fn read_by_both(&self) -> &[ObjectMeta] {
        &self.metas[..Self::READ_BY_BOTH]
    }

    pub(super) fn is_read_by_both(&self, path: &[u8]) -> bool {
        self.read_by_both()
            .iter()
            .any(|meta| meta.location.as_ref().as_bytes() == path)
    }

    pub(super) fn bytes(path: &[u8], range: Range<u64>) -> Vec<u8> {
        let salt = path.iter().fold(0x51_7cc1_b727_220a_u64, |salt, &byte| {
            (salt ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
        });
        range
            .map(|at| {
                let mixed = (at ^ salt).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                (mixed >> 56) as u8
            })
            .collect()
    }
}
