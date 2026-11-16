#![no_main]

//! A read may answer a version written over behind the cache's back: the cache may still trust
//! it (`doc/03-cache.md`, decision 11).

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU8;
use std::ops::Range;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use arbitrary::{Arbitrary, Unstructured};
use bytes::Bytes;
use futures_util::future::join_all;
use libfuzzer_sys::fuzz_target;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};
use pollster::block_on;
use t4::T4Caching;
use t4::policy::{
    Admit, BlockKey, BlockRead, ChunkId, DiskHit, IoEvent, LocatedRead, MemoryVictim,
    PlacementPolicy, PlannedGet, ReadPlan, ReadPolicy, Tier, TierSizes, VictimAction,
};
use tempfile::TempDir;

const BLOCK: u64 = 1 << 20;
const PAGE: u64 = 4096;

fuzz_target!(|scenario: Scenario| {
    if let Some(mut harness) = Harness::new(&scenario) {
        block_on(harness.run(&scenario.operations));
    }
});

#[derive(Debug)]
struct Scenario {
    settings: CacheSettings,
    operations: Vec<Operation>,
}

#[derive(Clone, Debug)]
struct CacheSettings {
    memory_blocks: u64,
    /// The smallest cache file's; each other is a block larger, room the tier leaves unused.
    disk_blocks: u64,
    disk_files: u64,
    queue_depth: u32,
    policy: PolicyChoice,
    reads: ReadsChoice,
    on_demand: bool,
}

impl Scenario {
    const MAX_OPERATIONS: usize = 32;
}

impl<'a> Arbitrary<'a> for Scenario {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let memory_blocks = u.int_in_range(1..=4)?;
        let disk_blocks = u.int_in_range(1..=6)?;
        let disk_files = u.int_in_range(1..=3)?;
        let queue_depth = u.int_in_range(1..=8)?;
        let policy = if u.arbitrary()? {
            PolicyChoice::Clock
        } else {
            PolicyChoice::Scripted(u.arbitrary()?)
        };
        let reads = if u.arbitrary()? {
            ReadsChoice::Default
        } else {
            ReadsChoice::Scripted(u.arbitrary()?)
        };
        let on_demand = u.arbitrary()?;
        let mut operations = Vec::new();
        while !u.is_empty() && operations.len() < Self::MAX_OPERATIONS {
            operations.push(u.arbitrary()?);
        }
        Ok(Self {
            settings: CacheSettings {
                memory_blocks,
                disk_blocks,
                disk_files,
                queue_depth,
                policy,
                reads,
                on_demand,
            },
            operations,
        })
    }
}

#[derive(Clone, Debug)]
enum PolicyChoice {
    Clock,
    Scripted(Vec<u8>),
}

#[derive(Clone, Debug)]
enum ReadsChoice {
    /// The cache's default.
    Default,
    /// [`ScriptedReads`] on these bytes.
    Scripted(Vec<u8>),
}

#[derive(Debug)]
enum Operation {
    Put { file: FileName, size: FileSize },
    PutMultipart { file: FileName, size: FileSize },
    PutBehind { file: FileName, size: FileSize },
    Delete { file: FileName },
    DeleteBehind { file: FileName },
    Copy { from: FileName, to: FileName },
    Rename { from: FileName, to: FileName },
    Head { file: FileName },
    Get(Read),
    GetAtOnce([Read; 3]),
    GiveUp { read: Read, polls: u8 },
    Restart,
    ShutDown,
}

impl<'a> Arbitrary<'a> for Operation {
    /// Reads two times in five, as they fill, evict and promote.
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(match u.choose_index(19)? {
            0 | 1 => Self::Put {
                file: u.arbitrary()?,
                size: u.arbitrary()?,
            },
            2 => Self::PutMultipart {
                file: u.arbitrary()?,
                size: u.arbitrary()?,
            },
            3 | 4 => Self::PutBehind {
                file: u.arbitrary()?,
                size: u.arbitrary()?,
            },
            5 => Self::Delete {
                file: u.arbitrary()?,
            },
            6 => Self::DeleteBehind {
                file: u.arbitrary()?,
            },
            7 => Self::Copy {
                from: u.arbitrary()?,
                to: u.arbitrary()?,
            },
            8 => Self::Rename {
                from: u.arbitrary()?,
                to: u.arbitrary()?,
            },
            9 => Self::Head {
                file: u.arbitrary()?,
            },
            10..=12 => Self::Get(u.arbitrary()?),
            13 | 14 => Self::GetAtOnce(u.arbitrary()?),
            17 => Self::Restart,
            18 => Self::ShutDown,
            _ => Self::GiveUp {
                read: u.arbitrary()?,
                polls: u.int_in_range(1..=4)?,
            },
        })
    }
}

/// One of a few files, so that operations meet.
#[derive(Clone, Copy, Debug)]
struct FileName(usize);

impl FileName {
    const COUNT: usize = 3;

    fn path(self) -> Path {
        Path::from(format!("file-{}", self.0))
    }
}

impl<'a> Arbitrary<'a> for FileName {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.choose_index(Self::COUNT)?))
    }
}

#[derive(Clone, Copy, Debug)]
struct FileSize(u64);

impl<'a> Arbitrary<'a> for FileSize {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let blocks: u64 = u.int_in_range(0..=3)?;
        let tail = match u.int_in_range(0..=3)? {
            0 => 0,
            1 => u.int_in_range(1..=2 * PAGE)?,
            2 => u.int_in_range(BLOCK - 2 * PAGE..=BLOCK - 1)?,
            _ => u.int_in_range(1..=BLOCK - 1)?,
        };
        Ok(Self(blocks * BLOCK + tail))
    }
}

#[derive(Clone, Copy, Debug)]
struct Offset(u64);

impl<'a> Arbitrary<'a> for Offset {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let block: u64 = u.int_in_range(0..=4)?;
        let within = match u.int_in_range(0..=3)? {
            0 => 0,
            1 => u.int_in_range(1..=2 * PAGE)?,
            2 => u.int_in_range(BLOCK - 2 * PAGE..=BLOCK - 1)?,
            _ => u.int_in_range(0..=BLOCK - 1)?,
        };
        Ok(Self(block * BLOCK + within))
    }
}

#[derive(Clone, Debug)]
struct Read {
    file: FileName,
    range: Option<GetRange>,
}

impl Read {
    async fn answer(&self, store: &dyn ObjectStore) -> Answer {
        let options = GetOptions {
            range: self.range.clone(),
            ..GetOptions::default()
        };
        let Ok(got) = store.get_opts(&self.file.path(), options).await else {
            return Answer::Failed;
        };
        let range = got.range.clone();
        match got.bytes().await {
            Ok(bytes) => Answer::Bytes { range, bytes },
            Err(_) => Answer::Failed,
        }
    }
}

impl<'a> Arbitrary<'a> for Read {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let file = u.arbitrary()?;
        let range = match u.int_in_range(0..=3)? {
            0 => None,
            1 => {
                let (Offset(start), Offset(len)) = (u.arbitrary()?, u.arbitrary()?);
                Some(GetRange::Bounded(start..start + len))
            }
            2 => Some(GetRange::Offset(u.arbitrary::<Offset>()?.0)),
            _ => Some(GetRange::Suffix(u.arbitrary::<Offset>()?.0)),
        };
        Ok(Self { file, range })
    }
}

/// Each byte is a hash of the id and offset, so no two contents agree for long.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Content {
    id: u64,
    size: u64,
}

impl Content {
    fn bytes(self, range: Range<u64>) -> Bytes {
        range.map(|offset| self.byte(offset)).collect()
    }

    fn byte(self, offset: u64) -> u8 {
        let word = ((offset / 8) ^ self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .wrapping_mul(0xD1B5_4A32_D192_ED03);
        word.to_le_bytes()[(offset % 8) as usize]
    }

    fn payload(self) -> PutPayload {
        PutPayload::from(self.bytes(0..self.size))
    }
}

#[derive(PartialEq, Eq)]
enum Answer {
    Bytes { range: Range<u64>, bytes: Bytes },
    Failed,
}

impl Answer {
    fn of(content: Option<Content>, range: Option<&GetRange>) -> Self {
        let Some(content) = content else {
            return Self::Failed;
        };
        let range = match range.map(|range| range.as_range(content.size)) {
            None => 0..content.size,
            Some(Ok(range)) => range,
            Some(Err(_)) => return Self::Failed,
        };
        Self::Bytes {
            bytes: content.bytes(range.clone()),
            range,
        }
    }
}

impl fmt::Debug for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes { range, bytes } => f
                .debug_struct("Bytes")
                .field("range", range)
                .field("len", &bytes.len())
                .field("head", &&bytes[..bytes.len().min(16)])
                .finish(),
            Self::Failed => f.write_str("Failed"),
        }
    }
}

#[derive(Debug, Default)]
struct ModelFile {
    current: Option<Content>,
    /// Written over behind the cache's back since it last forgot the path: it may still trust them.
    stale: Vec<Content>,
}

impl ModelFile {
    fn readable(&self) -> impl Iterator<Item = Option<Content>> + '_ {
        std::iter::once(self.current).chain(self.stale.iter().copied().map(Some))
    }
}

#[derive(Debug, Default)]
struct Model {
    files: [ModelFile; FileName::COUNT],
    next_content: u64,
}

impl Model {
    fn new_content(&mut self, FileSize(size): FileSize) -> Content {
        self.next_content += 1;
        Content {
            id: self.next_content,
            size,
        }
    }

    fn file(&self, file: FileName) -> &ModelFile {
        &self.files[file.0]
    }

    /// A write through the cache makes it forget the path.
    fn write_through(&mut self, file: FileName, content: Option<Content>) {
        let file = &mut self.files[file.0];
        file.current = content;
        file.stale.clear();
    }

    /// A rebuilt cache asks the store afresh, so it trusts no version written over.
    fn restart(&mut self) {
        for file in &mut self.files {
            file.stale.clear();
        }
    }

    fn write_behind(&mut self, file: FileName, content: Option<Content>) {
        let file = &mut self.files[file.0];
        let overwritten = std::mem::replace(&mut file.current, content);
        file.stale.extend(overwritten);
    }
}

struct Harness {
    dir: TempDir,
    settings: CacheSettings,
    store: Arc<InMemory>,
    cache: Option<T4Caching>,
    model: Model,
    pickable: Option<Arc<PickableChunks>>,
}

impl Harness {
    /// `None` where io_uring isn't allowed.
    fn new(scenario: &Scenario) -> Option<Self> {
        let mut harness = Self {
            dir: tempfile::tempdir().unwrap(),
            settings: scenario.settings.clone(),
            store: Arc::new(InMemory::new()),
            cache: None,
            model: Model::default(),
            pickable: None,
        };
        match harness.build() {
            Ok(()) => Some(harness),
            Err(t4::Error::Io(error))
                if matches!(error.raw_os_error(), Some(libc::EPERM | libc::ENOSYS)) =>
            {
                None
            }
            Err(error) => panic!("building the cache failed: {error}"),
        }
    }

    fn build(&mut self) -> t4::Result<()> {
        let settings = &self.settings;
        let builder = (0..settings.disk_files)
            .fold(
                T4Caching::builder(Arc::clone(&self.store) as Arc<dyn ObjectStore>),
                |builder, file| {
                    let path = self.dir.path().join(format!("cache{file}"));
                    builder.with_disk_file(path, (settings.disk_blocks + file) * BLOCK)
                },
            )
            .with_memory_capacity(settings.memory_blocks * BLOCK)
            .with_queue_depth(settings.queue_depth)
            // So the cache files can be on tmpfs (`fuzz/README.md`).
            .with_direct_io(false);
        let builder = match &settings.reads {
            ReadsChoice::Default => builder,
            ReadsChoice::Scripted(script) => builder.with_read_policy(ScriptedReads {
                script: Script::new(script.clone()),
            }),
        };
        let builder = match settings.on_demand {
            true => builder.with_on_demand_disk_allocation(),
            false => builder,
        };
        let (builder, pickable) = match &settings.policy {
            PolicyChoice::Clock => (builder, None),
            PolicyChoice::Scripted(script) => {
                let script = script.clone();
                let pickable = Arc::new(PickableChunks::default());
                let shared = Arc::clone(&pickable);
                let builder = builder
                    .with_placement_policy(move |sizes| ScriptedPolicy::new(sizes, script, shared));
                (builder, Some(pickable))
            }
        };
        self.cache = Some(builder.build()?);
        self.pickable = pickable;
        Ok(())
    }

    fn cache(&self) -> &T4Caching {
        self.cache
            .as_ref()
            .expect("a cache, but while it is built again")
    }

    async fn run(&mut self, operations: &[Operation]) {
        for operation in operations {
            self.apply(operation).await;
            self.check_model().await;
            let in_use = self.at_rest(operation);
            self.check_policy_can_pick(&in_use, operation);
        }
    }

    /// Retries: a read given up on may leave I/O in flight, holding its chunks until it's done.
    fn at_rest(&self, after: &Operation) -> Vec<(Tier, ChunkId)> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.cache().check_at_rest() {
                Ok(in_use) => return in_use,
                Err(error) if Instant::now() > deadline => {
                    panic!("after {after:?}, the cache is out of step: {error}")
                }
                Err(_) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    }

    /// The cache gives each victim it doesn't free back to the policy.
    fn check_policy_can_pick(&self, in_use: &[(Tier, ChunkId)], after: &Operation) {
        let Some(pickable) = &self.pickable else {
            return;
        };
        for &(tier, chunk) in in_use {
            assert!(
                pickable.of(tier).contains(&chunk.index()),
                "after {after:?}, {chunk:?} in {tier:?} holds a block the policy can't pick"
            );
        }
    }

    async fn apply(&mut self, operation: &Operation) {
        match *operation {
            Operation::Put { file, size } => {
                let content = self.model.new_content(size);
                self.cache()
                    .put(&file.path(), content.payload())
                    .await
                    .unwrap();
                self.model.write_through(file, Some(content));
            }
            Operation::PutMultipart { file, size } => {
                let content = self.model.new_content(size);
                let bytes = content.bytes(0..content.size);
                let half = bytes.len() / 2;
                let mut upload = self.cache().put_multipart(&file.path()).await.unwrap();
                upload.put_part(bytes.slice(..half).into()).await.unwrap();
                upload.put_part(bytes.slice(half..).into()).await.unwrap();
                upload.complete().await.unwrap();
                self.model.write_through(file, Some(content));
            }
            Operation::PutBehind { file, size } => {
                let content = self.model.new_content(size);
                self.store
                    .put(&file.path(), content.payload())
                    .await
                    .unwrap();
                self.model.write_behind(file, Some(content));
            }
            Operation::Delete { file } => {
                self.cache().delete(&file.path()).await.unwrap();
                self.model.write_through(file, None);
            }
            Operation::DeleteBehind { file } => {
                self.store.delete(&file.path()).await.unwrap();
                self.model.write_behind(file, None);
            }
            Operation::Copy { from, to } => {
                let copied = self.cache().copy(&from.path(), &to.path()).await;
                let source = self.model.file(from).current;
                assert_eq!(copied.is_ok(), source.is_some(), "{copied:?}");
                // The cache forgets the target, copied or not.
                let target = source.or(self.model.file(to).current);
                self.model.write_through(to, target);
            }
            Operation::Rename { from, to } => {
                let renamed = self.cache().rename(&from.path(), &to.path()).await;
                let source = self.model.file(from).current;
                assert_eq!(renamed.is_ok(), source.is_some(), "{renamed:?}");
                // A copy then a delete of the source, which may be the target; the cache forgets both.
                let target = source.or(self.model.file(to).current);
                self.model.write_through(to, target);
                let source = if source.is_some() {
                    None
                } else {
                    self.model.file(from).current
                };
                self.model.write_through(from, source);
            }
            Operation::Head { file } => {
                let size = self
                    .cache()
                    .head(&file.path())
                    .await
                    .ok()
                    .map(|meta| meta.size);
                let readable = self.model.file(file).readable();
                assert!(
                    readable
                        .map(|content| content.map(|content| content.size))
                        .any(|known| known == size),
                    "{file:?}: head says {size:?}, the model {:?}",
                    self.model.file(file)
                );
            }
            Operation::Get(ref read) => {
                let answer = read.answer(self.cache()).await;
                self.check_answer(read, &answer);
            }
            Operation::GiveUp { ref read, polls } => {
                let mut answer = pin!(read.answer(self.cache()));
                let mut cx = Context::from_waker(Waker::noop());
                for _ in 0..polls {
                    if let Poll::Ready(answer) = answer.as_mut().poll(&mut cx) {
                        self.check_answer(read, &answer);
                        break;
                    }
                }
            }
            Operation::ShutDown => self.cache().shut_down().unwrap(),
            Operation::Restart => {
                // Dropped first, so its files' lock is free for the rebuild.
                self.cache = None;
                self.build().unwrap();
                self.model.restart();
            }
            Operation::GetAtOnce(ref reads) => {
                let answers = join_all(reads.iter().map(|read| read.answer(self.cache()))).await;
                for (read, answer) in reads.iter().zip(&answers) {
                    self.check_answer(read, answer);
                }
            }
        }
    }

    fn check_answer(&self, read: &Read, answer: &Answer) {
        let file = self.model.file(read.file);
        let allowed = file
            .readable()
            .any(|content| Answer::of(content, read.range.as_ref()) == *answer);
        assert!(allowed, "{read:?} answered {answer:?}; the model: {file:?}");
    }

    /// The harness's own check, so any other failure is the cache's.
    async fn check_model(&self) {
        for (index, file) in self.model.files.iter().enumerate() {
            let path = FileName(index).path();
            let size = self.store.head(&path).await.ok().map(|meta| meta.size);
            assert_eq!(size, file.current.map(|content| content.size), "{path}");
        }
    }
}

struct ScriptedPolicy {
    sizes: TierSizes,
    script: Script,
    pickable: Arc<PickableChunks>,
}

#[derive(Default)]
struct PickableChunks {
    memory: Mutex<BTreeSet<u32>>,
    disk: Mutex<BTreeSet<u32>>,
}

impl PickableChunks {
    fn of(&self, tier: Tier) -> MutexGuard<'_, BTreeSet<u32>> {
        match tier {
            Tier::Memory => self.memory.lock().unwrap(),
            Tier::Disk => self.disk.lock().unwrap(),
        }
    }
}

impl ScriptedPolicy {
    fn new(sizes: TierSizes, script: Vec<u8>, pickable: Arc<PickableChunks>) -> Self {
        Self {
            sizes,
            script: Script::new(script),
            pickable,
        }
    }

    fn next_byte(&self) -> u8 {
        self.script.next_byte()
    }

    fn chunk_count(&self, tier: Tier) -> u32 {
        match tier {
            Tier::Memory => self.sizes.memory_chunks().get(),
            Tier::Disk => self.sizes.disk_chunks().get(),
        }
    }

    fn heard(&self, tier: Tier, chunk: ChunkId) {
        assert!(
            chunk.index() < self.chunk_count(tier),
            "the cache named {chunk:?}, past the {tier:?} tier's {} chunks",
            self.chunk_count(tier)
        );
    }

    fn pick(&self, tier: Tier, count: usize) -> Vec<ChunkId> {
        let mut admitted = self.pickable.of(tier);
        let wanted = match self.next_byte() {
            0..32 => 0,
            byte => usize::from(byte)
                .checked_rem(count)
                .map_or(0, |extra| 1 + extra),
        };
        (0..wanted)
            .map(|_| {
                let byte = self.next_byte();
                if byte < 0xC0 && !admitted.is_empty() {
                    let picked = *admitted
                        .iter()
                        .nth(usize::from(byte) % admitted.len())
                        .unwrap();
                    admitted.remove(&picked);
                    ChunkId::new(picked)
                } else {
                    ChunkId::new(u32::from(byte) % self.chunk_count(tier))
                }
            })
            .collect()
    }
}

impl PlacementPolicy for ScriptedPolicy {
    fn on_memory_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) {
        self.heard(Tier::Memory, chunk);
    }

    fn on_disk_hit(&self, chunk: ChunkId, _read: &BlockRead<'_>) -> DiskHit {
        self.heard(Tier::Disk, chunk);
        if self.next_byte().is_multiple_of(2) {
            DiskHit::Stay
        } else {
            DiskHit::Promote
        }
    }

    fn on_remote_read(&self, _read: &BlockRead<'_>) -> Admit {
        match self.next_byte() % 3 {
            0 => Admit::Memory,
            1 => Admit::Disk,
            _ => Admit::Discard,
        }
    }

    fn on_admit(&self, tier: Tier, chunk: ChunkId, _block: BlockKey) {
        self.heard(tier, chunk);
        self.pickable.of(tier).insert(chunk.index());
    }

    fn pick_memory_victims(&self, count: usize) -> Vec<MemoryVictim> {
        self.pick(Tier::Memory, count)
            .into_iter()
            .map(|chunk| {
                let action = if self.next_byte().is_multiple_of(2) {
                    VictimAction::ToDisk
                } else {
                    VictimAction::Drop
                };
                MemoryVictim::new(chunk, action)
            })
            .collect()
    }

    fn pick_disk_victims(&self, count: usize) -> Vec<ChunkId> {
        self.pick(Tier::Disk, count)
    }
}

/// Bytes of the input a policy decides by.
struct Script {
    bytes: Vec<u8>,
    next: AtomicUsize,
}

impl Script {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            next: AtomicUsize::new(0),
        }
    }

    /// The next byte, round and round; 0 if there are none.
    fn next_byte(&self) -> u8 {
        if self.bytes.is_empty() {
            return 0;
        }
        let at = self.next.fetch_add(1, Ordering::Relaxed) % self.bytes.len();
        self.bytes[at]
    }
}

/// A read policy whose plans the input makes: up to three GETs around the
/// read's blocks, racing one to three requests. Many are plans the cache
/// must refuse, and plan by its default instead.
struct ScriptedReads {
    script: Script,
}

impl ReadPolicy for ScriptedReads {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        let blocks = read.blocks();
        let starts = blocks.len() as u32 + 2;
        let gets = (0..self.script.next_byte() % 4)
            .map(|_| {
                let start =
                    blocks.start.saturating_sub(1) + u32::from(self.script.next_byte()) % starts;
                let end = start + u32::from(self.script.next_byte()) % 4;
                let race = NonZeroU8::new(self.script.next_byte() % 3 + 1).expect("1 to 3");
                PlannedGet::new(start..end, race)
            })
            .collect();
        ReadPlan::new(gets)
    }

    fn on_io(&self, _event: &IoEvent<'_>) {}
}
