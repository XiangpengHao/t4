use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::ops::Range;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use clap::ValueEnum;
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};
use kismet_cache::{CacheBuilder, CacheHitAction};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use t4::T4Caching;
use t4::policy::{Admit, FixedPlacementPolicy};

use crate::Error;
use crate::synthetic_objects::SyntheticObjects;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DiskCacheKind {
    /// t4, every block on disk: direct I/O through io_uring.
    T4,
    /// foyer as shipped: through the page cache, pread on tokio's blocking pool.
    Foyer,
    /// foyer with `O_DIRECT` and its io_uring engine. Linux only.
    FoyerTuned,
    /// Kismet as spiraldb runs it: a file per block in 64 shard folders.
    Kismet,
    /// Reference: a plain file in the page cache, read with pread. Reads no drive.
    PageCache,
}

pub struct Blocks {
    objects: Arc<SyntheticObjects>,
    path: Path,
    count: u64,
}

impl Blocks {
    /// t4's block size, and spiraldb's Kismet's.
    pub const SIZE: u64 = 1 << 20;

    pub fn new(count: u64) -> Self {
        let path = Path::from("cold_reads/blocks");
        let objects = Arc::new(SyntheticObjects::new([(path.clone(), count * Self::SIZE)]));
        Self {
            objects,
            path,
            count,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.objects) as Arc<dyn ObjectStore>
    }

    pub fn bytes(&self, index: u64) -> Bytes {
        self.objects.bytes(&self.path, Self::range(index))
    }

    fn range(index: u64) -> Range<u64> {
        index * Self::SIZE..(index + 1) * Self::SIZE
    }
}

pub enum DiskCache {
    T4(T4Blocks),
    Foyer(FoyerBlocks),
    Kismet(KismetBlocks),
    PageCache(PageCacheBlocks),
}

impl DiskCache {
    pub async fn build(
        kind: DiskCacheKind,
        dir: &FsPath,
        blocks: &Blocks,
        source: Arc<dyn ObjectStore>,
        io_depth: Option<u32>,
    ) -> Result<Self, Error> {
        let capacity = (blocks.count() + blocks.count() / 8 + 64) * Blocks::SIZE;
        Ok(match kind {
            DiskCacheKind::T4 => Self::T4(T4Blocks::new(dir, capacity, blocks, source, io_depth)?),
            DiskCacheKind::Foyer | DiskCacheKind::FoyerTuned => {
                let tuned = kind == DiskCacheKind::FoyerTuned;
                Self::Foyer(FoyerBlocks::open(dir.join("foyer"), capacity, tuned, io_depth).await?)
            }
            // Random keys overfill some of Kismet's even shards: twice the room keeps every block.
            DiskCacheKind::Kismet => {
                Self::Kismet(KismetBlocks::new(dir.join("kismet"), 2 * capacity)?)
            }
            DiskCacheKind::PageCache => Self::PageCache(PageCacheBlocks::new(dir, blocks)?),
        })
    }

    pub fn reads_memory(&self) -> bool {
        matches!(self, Self::PageCache(_))
    }

    pub async fn fill(self: &Arc<Self>, index: u64, blocks: &Blocks) -> Result<(), Error> {
        match &**self {
            // A read fetches the block and lands it on disk, as the policy says.
            Self::T4(cache) => cache.read(index).await.map(drop),
            Self::Foyer(cache) => cache.fill(index, blocks.bytes(index)).await,
            Self::Kismet(_) | Self::PageCache(_) => {
                let cache = Arc::clone(self);
                let value = blocks.bytes(index);
                tokio::task::spawn_blocking(move || match &*cache {
                    Self::Kismet(kismet) => kismet.put(index, &value),
                    Self::PageCache(file) => file.write(index, &value),
                    _ => unreachable!("matched as blocking above"),
                })
                .await?
            }
        }
    }

    /// foyer reopens to empty its memory tier; the reference syncs so its pages stay cached clean.
    pub async fn settled(self) -> Result<Self, Error> {
        match self {
            Self::Foyer(cache) => Ok(Self::Foyer(cache.reopened().await?)),
            Self::PageCache(file) => {
                file.file.sync_all()?;
                Ok(Self::PageCache(file))
            }
            other => Ok(other),
        }
    }

    pub async fn read(self: &Arc<Self>, index: u64) -> Result<Option<Bytes>, Error> {
        match &**self {
            Self::T4(cache) => cache.read(index).await.map(Some),
            Self::Foyer(cache) => cache.read(index).await,
            Self::Kismet(_) | Self::PageCache(_) => {
                let cache = Arc::clone(self);
                tokio::task::spawn_blocking(move || match &*cache {
                    Self::Kismet(kismet) => kismet.get(index),
                    Self::PageCache(file) => file.read(index).map(Some),
                    _ => unreachable!("matched as blocking above"),
                })
                .await?
            }
        }
    }
}

pub struct T4Blocks {
    store: T4Caching,
    path: Path,
}

impl T4Blocks {
    /// t4 needs some memory; the policy puts nothing there.
    const MEMORY: u64 = 64 << 20;

    fn new(
        dir: &FsPath,
        capacity: u64,
        blocks: &Blocks,
        source: Arc<dyn ObjectStore>,
        io_depth: Option<u32>,
    ) -> Result<Self, Error> {
        let mut builder = T4Caching::builder(source)
            .with_memory_capacity(Self::MEMORY)
            .with_disk_file(dir.join("t4.cache"), capacity)
            .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk));
        if let Some(depth) = io_depth {
            builder = builder.with_queue_depth(depth);
        }
        let store = builder.build()?;
        Ok(Self {
            store,
            path: blocks.path.clone(),
        })
    }

    async fn read(&self, index: u64) -> Result<Bytes, Error> {
        Ok(self
            .store
            .get_range(&self.path, Blocks::range(index))
            .await?)
    }
}

pub struct FoyerBlocks {
    cache: HybridCache<u64, Bytes>,
    dir: PathBuf,
    capacity: u64,
    tuned: bool,
    io_depth: Option<u32>,
}

impl FoyerBlocks {
    const MEMORY: usize = 64 << 20;
    /// Under foyer's 16 MiB flush buffer, so no insert is dropped for want of room.
    const FILL_BATCH: u64 = 12;

    async fn open(
        dir: PathBuf,
        capacity: u64,
        tuned: bool,
        io_depth: Option<u32>,
    ) -> Result<Self, Error> {
        std::fs::create_dir_all(&dir)?;
        let device = Self::device(&dir, capacity, tuned)?;
        let storage = HybridCacheBuilder::new()
            .memory(Self::MEMORY)
            .with_weighter(|_key: &u64, value: &Bytes| value.len())
            .storage();
        let storage = Self::with_io_engine(storage, tuned, io_depth)?;
        let cache = storage
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await?;
        Ok(Self {
            cache,
            dir,
            capacity,
            tuned,
            io_depth,
        })
    }

    #[cfg(target_os = "linux")]
    fn device(dir: &FsPath, capacity: u64, tuned: bool) -> Result<Arc<dyn foyer::Device>, Error> {
        FsDeviceBuilder::new(dir)
            .with_capacity(usize::try_from(capacity).expect("the capacity fits a usize"))
            .with_direct(tuned)
            .build()
            .map_err(Error::from)
    }

    #[cfg(not(target_os = "linux"))]
    fn device(dir: &FsPath, capacity: u64, tuned: bool) -> Result<Arc<dyn foyer::Device>, Error> {
        if tuned {
            return Err("foyer's tuned setup (O_DIRECT, io_uring) is Linux only".into());
        }
        FsDeviceBuilder::new(dir)
            .with_capacity(usize::try_from(capacity).expect("the capacity fits a usize"))
            .build()
            .map_err(Error::from)
    }

    #[cfg(target_os = "linux")]
    fn with_io_engine(
        storage: foyer::HybridCacheBuilderPhaseStorage<u64, Bytes, foyer::DefaultHasher>,
        tuned: bool,
        io_depth: Option<u32>,
    ) -> Result<foyer::HybridCacheBuilderPhaseStorage<u64, Bytes, foyer::DefaultHasher>, Error>
    {
        Ok(if tuned {
            let mut uring = foyer::UringIoEngineConfig::new();
            if let Some(depth) = io_depth {
                uring = uring.with_io_depth(depth as usize);
            }
            storage.with_io_engine_config(Box::new(uring) as Box<dyn foyer::IoEngineConfig>)
        } else {
            storage
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn with_io_engine(
        storage: foyer::HybridCacheBuilderPhaseStorage<u64, Bytes, foyer::DefaultHasher>,
        _tuned: bool,
        _io_depth: Option<u32>,
    ) -> Result<foyer::HybridCacheBuilderPhaseStorage<u64, Bytes, foyer::DefaultHasher>, Error>
    {
        Ok(storage)
    }

    async fn fill(&self, index: u64, value: Bytes) -> Result<(), Error> {
        self.cache.storage_writer(index).force().insert(value);
        if (index + 1).is_multiple_of(Self::FILL_BATCH) {
            self.cache.storage().wait().await;
        }
        Ok(())
    }

    async fn reopened(self) -> Result<Self, Error> {
        self.cache.storage().wait().await;
        self.cache.close().await?;
        drop(self.cache);
        Self::open(self.dir, self.capacity, self.tuned, self.io_depth).await
    }

    async fn read(&self, index: u64) -> Result<Option<Bytes>, Error> {
        let entry = self.cache.get(&index).await?;
        Ok(entry.map(|entry| entry.value().clone()))
    }
}

pub struct KismetBlocks {
    cache: kismet_cache::Cache,
}

impl KismetBlocks {
    /// spiraldb's shard count.
    const SHARDS: usize = 64;
    /// spiraldb's envelope: magic, key digest, payload CRC32, payload length.
    const MAGIC: &'static [u8] = b"SPFSKISMET3\0";
    const HEADER_LEN: usize = Self::MAGIC.len() + 32 + 4 + 8;

    fn new(root: PathBuf, capacity: u64) -> Result<Self, Error> {
        std::fs::create_dir_all(&root)?;
        let files = usize::try_from(capacity / Blocks::SIZE).expect("the file count fits a usize");
        let mut builder = CacheBuilder::new();
        builder.writer(&root, Self::SHARDS, files);
        // As spiraldb: entries are disposable and checked, so no fsync.
        builder.auto_sync(false);
        Ok(Self {
            cache: builder.build(),
        })
    }

    /// As spiraldb derives a key from canonical bytes.
    fn key(index: u64) -> ([u8; 32], String, u64, u64) {
        let digest: [u8; 32] = Sha256::digest(format!("cold_reads/blocks/{index}")).into();
        let hash = u64::from_le_bytes(digest[0..8].try_into().expect("8 bytes"));
        let secondary = u64::from_le_bytes(digest[8..16].try_into().expect("8 bytes"));
        (
            digest,
            format!("v1-{}", hex::encode(digest)),
            hash,
            secondary,
        )
    }

    fn put(&self, index: u64, payload: &[u8]) -> Result<(), Error> {
        let (digest, name, hash, secondary) = Self::key(index);
        self.cache.get_or_update(
            kismet_cache::Key::new(&name, hash, secondary),
            |_| CacheHitAction::Replace,
            |file, _old| Self::write_envelope(file, &digest, payload),
        )?;
        Ok(())
    }

    fn write_envelope(file: &mut File, digest: &[u8; 32], payload: &[u8]) -> std::io::Result<()> {
        file.write_all(Self::MAGIC)?;
        file.write_all(digest)?;
        file.write_all(&crc32fast::hash(payload).to_le_bytes())?;
        file.write_all(&(payload.len() as u64).to_le_bytes())?;
        file.write_all(payload)
    }

    /// Read as spiraldb's `read_envelope` reads it.
    fn get(&self, index: u64) -> Result<Option<Bytes>, Error> {
        let (digest, name, hash, secondary) = Self::key(index);
        let Some(mut file) = self
            .cache
            .get(kismet_cache::Key::new(&name, hash, secondary))?
        else {
            return Ok(None);
        };
        let file_len = file.metadata()?.len();
        let mut header = [0; Self::HEADER_LEN];
        if file_len < Self::HEADER_LEN as u64 || !Self::read_exact_or_miss(&mut file, &mut header)?
        {
            return Ok(None);
        }
        let (magic, rest) = header.split_at(Self::MAGIC.len());
        let (key_digest, rest) = rest.split_at(32);
        let (checksum, len) = rest.split_at(4);
        let len = u64::from_le_bytes(len.try_into().expect("8 bytes"));
        if magic != Self::MAGIC || key_digest != digest || file_len != Self::HEADER_LEN as u64 + len
        {
            return Ok(None);
        }
        let len = usize::try_from(len).expect("a block fits a usize");
        let Some(payload) = FreshRead::exact_at(&file, len, Self::HEADER_LEN as u64)? else {
            return Ok(None);
        };
        if crc32fast::hash(&payload).to_le_bytes() != checksum {
            return Ok(None);
        }
        Ok(Some(Bytes::from(payload)))
    }

    fn read_exact_or_miss(file: &mut File, buffer: &mut [u8]) -> std::io::Result<bool> {
        match file.read_exact(buffer) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
            Err(error) => Err(error),
        }
    }
}

pub struct PageCacheBlocks {
    file: File,
}

impl PageCacheBlocks {
    fn new(dir: &FsPath, blocks: &Blocks) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(dir.join("blocks"))?;
        file.set_len(blocks.count() * Blocks::SIZE)?;
        Ok(Self { file })
    }

    fn write(&self, index: u64, value: &[u8]) -> Result<(), Error> {
        Ok(self.file.write_all_at(value, index * Blocks::SIZE)?)
    }

    fn read(&self, index: u64) -> Result<Bytes, Error> {
        let len = usize::try_from(Blocks::SIZE).expect("a block fits a usize");
        let bytes = FreshRead::exact_at(&self.file, len, index * Blocks::SIZE)?
            .ok_or("the reference's file ended inside a block")?;
        Ok(Bytes::from(bytes))
    }
}

/// Into unzeroed memory, as spiraldb's Kismet reads payloads (`read_exact_uninit`).
struct FreshRead;

impl FreshRead {
    fn exact_at(file: &File, len: usize, offset: u64) -> std::io::Result<Option<Vec<u8>>> {
        let mut buffer = Vec::with_capacity(len);
        while buffer.len() < len {
            let filled = buffer.len();
            let at =
                libc::off_t::try_from(offset + filled as u64).map_err(std::io::Error::other)?;
            let spare = &mut buffer.spare_capacity_mut()[..len - filled];
            // SAFETY: pread writes at most `spare.len()` bytes into memory the vector owns.
            let read = unsafe {
                libc::pread(file.as_raw_fd(), spare.as_mut_ptr().cast(), spare.len(), at)
            };
            match read {
                0 => return Ok(None),
                read if read < 0 => {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
                read => {
                    let read = usize::try_from(read).expect("a positive count fits a usize");
                    // SAFETY: pread initialized these `read` bytes, within the vector's capacity.
                    unsafe { buffer.set_len(buffer.len() + read) };
                }
            }
        }
        Ok(Some(buffer))
    }
}
