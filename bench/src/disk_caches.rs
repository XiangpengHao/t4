//! The disk caches `cold_reads` compares, each holding the same 1 MiB blocks
//! of one synthetic object on the same drives: t4 with every block on disk,
//! foyer's hybrid cache as shipped and tuned, and Kismet as spiraldb runs it.
//!
//! Each cache keeps what a deployment would key it by. t4 caches the object
//! itself, so a read is a byte range of it. foyer and Kismet cache values by
//! key, so each block is a value under its index: the layer an app would
//! build to cache files in them (`doc/08-spiral-presentation`, the foyer
//! slides). Kismet goes through the same key and envelope as spiraldb's
//! `shared/spiral-kismet-cache`: a SHA-256 name, a header, the payload, and
//! a CRC32 checked on every read.
//!
//! Beside them, a reference that reads no drive: the blocks as a plain file
//! whose every page is in the page cache, read with pread. That is reading
//! from memory, as Vortex reads a file the OS has cached, and it prices what
//! a byte from memory costs the CPU.

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

/// Which cache holds the blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DiskCacheKind {
    /// t4, every block on disk ([`FixedPlacementPolicy`]), as shipped:
    /// direct I/O through io_uring.
    T4,
    /// foyer as shipped: its directory device through the page cache, read
    /// with pread on tokio's blocking pool.
    Foyer,
    /// foyer tuned: the same device opened with `O_DIRECT`, read through its
    /// io_uring engine. Linux only.
    FoyerTuned,
    /// Kismet as spiraldb runs it: a file per block in 64 shard folders.
    Kismet,
    /// The reference: a plain file of the blocks, every page already in the
    /// page cache, read with pread into a fresh buffer. Reads memory, not the
    /// drives.
    PageCache,
}

/// The blocks every cache holds: `count` 1 MiB blocks of one synthetic
/// object, whose bytes [`SyntheticObjects`] computes and can check.
pub struct Blocks {
    objects: Arc<SyntheticObjects>,
    path: Path,
    count: u64,
}

impl Blocks {
    /// A block's bytes: 1 MiB, the block size of t4, of spiraldb's Kismet,
    /// and of every value here.
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

    /// The store t4 fetches the object from as it fills.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.objects) as Arc<dyn ObjectStore>
    }

    /// Block `index`'s bytes, as every cache must return them.
    pub fn bytes(&self, index: u64) -> Bytes {
        self.objects.bytes(&self.path, Self::range(index))
    }

    fn range(index: u64) -> Range<u64> {
        index * Self::SIZE..(index + 1) * Self::SIZE
    }
}

/// A cache holding [`Blocks`], filled once and then read.
pub enum DiskCache {
    T4(T4Blocks),
    Foyer(FoyerBlocks),
    Kismet(KismetBlocks),
    PageCache(PageCacheBlocks),
}

impl DiskCache {
    /// A cache of `kind` with its files in `dir`, room for `blocks` and some
    /// spare, reading `source` for what it doesn't hold (t4 alone does).
    /// `io_depth`, if given, is the I/Os its io_uring engine keeps in flight:
    /// t4's queue depth, tuned foyer's; each's default (64) otherwise.
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
            // Kismet splits its capacity evenly over its shards, and random
            // keys fill some shards past their share, which then evict: twice
            // the room keeps every block, so a pass measures reads alone.
            DiskCacheKind::Kismet => {
                Self::Kismet(KismetBlocks::new(dir.join("kismet"), 2 * capacity)?)
            }
            DiskCacheKind::PageCache => Self::PageCache(PageCacheBlocks::new(dir, blocks)?),
        })
    }

    /// Whether its reads come from memory, so the page cache must stay
    /// filled between passes: the reference alone.
    pub fn reads_memory(&self) -> bool {
        matches!(self, Self::PageCache(_))
    }

    /// Puts block `index` on disk.
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

    /// Done filling: whatever is still in memory goes. foyer flushes, then
    /// reopens on its files with an empty memory tier, its disk index
    /// rebuilt from them; t4 and Kismet hold nothing in memory to drop. The
    /// reference writes its pages back, so that they stay cached clean.
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

    /// Block `index`, from disk, or `None` if the cache doesn't hold it.
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

/// t4 over the synthetic object, admitting every fetched block to disk and
/// keeping it there, its cache file in the benchmark's directory.
pub struct T4Blocks {
    store: T4Caching,
    path: Path,
}

impl T4Blocks {
    /// Memory for cached bytes: a few blocks, as t4 needs some; the policy
    /// puts none there.
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

/// foyer's hybrid cache, each block a value under its index. Its memory tier
/// is small, so reads go to disk; its disk tier is the block engine on a
/// directory device, foyer's default.
pub struct FoyerBlocks {
    cache: HybridCache<u64, Bytes>,
    dir: PathBuf,
    capacity: u64,
    tuned: bool,
    io_depth: Option<u32>,
}

impl FoyerBlocks {
    /// The memory tier's bytes: a few blocks.
    const MEMORY: usize = 64 << 20;
    /// Blocks inserted between waits for the flushers: under foyer's 16 MiB
    /// flush buffer, so none is dropped for want of room.
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

    /// Writes block `index` to disk, past the memory tier and the admission
    /// filter, waiting for the flushers once a batch is in.
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

/// Kismet as spiraldb runs it (`shared/spiral-kismet-cache`): a file per
/// block in 64 shard folders, unsynced writes, each entry an envelope whose
/// CRC32 a read checks.
pub struct KismetBlocks {
    cache: kismet_cache::Cache,
}

impl KismetBlocks {
    /// spiraldb's shard folders (`doc/10-spiral-integration.md`).
    const SHARDS: usize = 64;
    /// spiraldb's envelope: magic, the key's digest, the payload's CRC32,
    /// the payload's length.
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

    /// Block `index`'s key, as spiraldb derives one from canonical bytes:
    /// its SHA-256 names the file and gives the two hashes.
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

    /// The block, read as spiraldb's `read_envelope` does: the file's
    /// length, the header, the payload, and its CRC32 checked.
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

/// The reference: the blocks as one plain file on the same drives, whose
/// pages the fill leaves in the page cache, read as a file the OS has cached
/// is read: pread into a fresh buffer.
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

/// A read into a fresh buffer that nothing zeroes first: pread straight into
/// a vector's spare capacity, as spiraldb's Kismet reads its payloads
/// (`read_exact_uninit`), and as a cached file's read costs at its least.
struct FreshRead;

impl FreshRead {
    /// `len` bytes of `file` from `offset`, or `None` if it ends first.
    fn exact_at(file: &File, len: usize, offset: u64) -> std::io::Result<Option<Vec<u8>>> {
        let mut buffer = Vec::with_capacity(len);
        while buffer.len() < len {
            let filled = buffer.len();
            let at =
                libc::off_t::try_from(offset + filled as u64).map_err(std::io::Error::other)?;
            let spare = &mut buffer.spare_capacity_mut()[..len - filled];
            // SAFETY: pread writes at most `spare.len()` bytes into `spare`,
            // memory the vector owns and nothing else reads meanwhile.
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
                    // SAFETY: pread initialized those `read` bytes, just past
                    // the vector's length, within its capacity.
                    unsafe { buffer.set_len(buffer.len() + read) };
                }
            }
        }
        Ok(Some(buffer))
    }
}
