use std::collections::BTreeMap;
use std::fmt;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::future::join_all;
use object_store::ObjectStore;
use shuttle::future::block_on;

use super::model::{Answer, Content, Model};
use super::remote::{ProcessStore, Remote};
use super::ticker::Ticker;
use super::{ScenarioRead, SharedOperation, SharedScenario, SharedSettings};
use crate::cache::BLOCK_SIZE_U64;
use crate::cache::cache_stores::TestTransport;
use crate::cache::policy::{Admit, FixedPlacementPolicy};
use crate::cache::t4_caching::T4Caching;
use crate::io::file_system::FileSystem;
use crate::io::sync::Arc;
use crate::io::sync::thread::{self, JoinHandle};

pub(super) struct ScenarioRun<'s> {
    settings: &'s SharedSettings,
    file_system: FileSystem,
    transport: TestTransport,
    remote: Arc<Remote>,
    model: Model,
    slots: Vec<Slot>,
    spawned: Vec<SpawnedRead>,
    /// Steps that may make a block fetched again: an owner gone takes its tiers, a client gone
    /// or a read given up abandons its claims.
    disruptions: Vec<usize>,
    /// Chunks the reads given up may hold: a read's writes go on after it, each holding its
    /// chunk till done, and a tier with no other room caches nothing meanwhile.
    given_up_chunks: u64,
    ticker: Ticker,
}

/// A process's place in the script: its store, and its cache while it lives.
struct Slot {
    store: Arc<ProcessStore>,
    process: Option<Process>,
}

struct Process {
    cache: T4Caching,
    built: usize,
}

struct SpawnedRead {
    read: ScenarioRead,
    built: usize,
    started: usize,
    thread: JoinHandle<Answer>,
}

impl<'s> ScenarioRun<'s> {
    /// Rounds that settle the processes' threads; a wait past them hangs.
    const SETTLE_ROUNDS: usize = 10_000;
    /// PCT runs the others while the script's thread yields, as it lowers the yielder's
    /// priority below theirs.
    const SETTLE_STEPS: usize = 32;

    /// Every process's cache file is `cache`.
    const CACHE_FILE: &'static str = "cache";

    pub(super) fn start(settings: &'s SharedSettings) -> Self {
        let transport = match settings.loses_wakeups {
            true => TestTransport::default().losing_shutdown_wakeups(),
            false => TestTransport::default(),
        };
        let remote = Arc::new(Remote::default());
        let slots = (0..settings.processes)
            .map(|_| Slot {
                store: Arc::new(ProcessStore::new(&remote)),
                process: None,
            })
            .collect();
        let mut run = Self {
            settings,
            file_system: FileSystem::default(),
            ticker: Ticker::start(transport.clock().clone()),
            transport,
            remote,
            model: Model::default(),
            slots,
            spawned: Vec::new(),
            disruptions: Vec::new(),
            given_up_chunks: 0,
        };
        for slot in 0..run.slots.len() {
            run.build(slot, 0);
        }
        run
    }

    pub(super) fn apply(&mut self, step: usize, operation: &SharedOperation) {
        self.ticker.progressed();
        self.carry_out(step, operation);
        if self.spawned.is_empty() {
            self.check_quiet(step, operation);
        }
    }

    pub(super) fn finish(mut self, step: usize) {
        self.join_spawned();
        self.check_quiet(step, &SharedOperation::Join);
        // Shuttle fails the schedule if a thread is left blocked once these drop.
        for slot in &mut self.slots {
            slot.process = None;
        }
    }

    fn build(&mut self, slot: usize, step: usize) {
        let settings = self.settings;
        let store = Arc::clone(&self.slots[slot].store) as Arc<dyn ObjectStore>;
        let builder = T4Caching::builder(store)
            .with_memory_capacity(settings.memory_blocks * BLOCK_SIZE_U64)
            .with_disk_file(Self::CACHE_FILE, settings.disk_blocks * BLOCK_SIZE_U64)
            .with_network_rate(
                settings.network_blocks * BLOCK_SIZE_U64,
                Duration::from_secs(1),
            )
            .with_multiprocess_sharing()
            .with_test_transport(self.transport.clone())
            .with_test_file_system(self.file_system.clone());
        let builder = match settings.on_disk {
            true => {
                builder.with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
            }
            false => builder,
        };
        let cache = builder
            .build()
            .unwrap_or_else(|error| panic!("process {slot} built at step {step}: {error}"));
        self.slots[slot].process = Some(Process { cache, built: step });
    }

    fn slot(&self, process: usize) -> usize {
        process % self.slots.len()
    }

    fn process(&self, process: usize) -> Option<&Process> {
        self.slots[self.slot(process)].process.as_ref()
    }

    fn carry_out(&mut self, step: usize, operation: &SharedOperation) {
        match *operation {
            SharedOperation::Read { process, ref read } => {
                self.release_gets();
                let Some(process) = self.process(process) else {
                    return;
                };
                let answer = block_on(read.answer(&process.cache));
                self.check_answer(read, process.built, step, &answer);
            }
            SharedOperation::ReadAtOnce { process, ref reads } => {
                self.release_gets();
                let Some(process) = self.process(process) else {
                    return;
                };
                let answers = block_on(join_all(
                    reads.iter().map(|read| read.answer(&process.cache)),
                ));
                for (read, answer) in reads.iter().zip(&answers) {
                    self.check_answer(read, process.built, step, answer);
                }
            }
            SharedOperation::Spawn { process, ref read } => {
                let Some(process) = self.process(process) else {
                    return;
                };
                let (cache, built, asked) = (process.cache.clone(), process.built, read.clone());
                let thread = thread::spawn(move || block_on(asked.answer(&cache)));
                self.spawned.push(SpawnedRead {
                    read: read.clone(),
                    built,
                    started: step,
                    thread,
                });
            }
            SharedOperation::Join => self.join_spawned(),
            SharedOperation::GiveUp {
                process,
                ref read,
                polls,
            } => {
                let Some(process) = self.process(process) else {
                    return;
                };
                self.give_up(&process.cache, read, polls, process.built, step);
                self.note_given_up(read, step);
            }
            SharedOperation::WriteBehind { file, size } => {
                let file = file % Model::FILES;
                let size = size.map(|size| size.min(SharedScenario::MOST_FILE_BYTES));
                let version = self.model.write(file, step, size);
                block_on(self.remote.write(file, version));
            }
            SharedOperation::Drop { process } => self.drop_process(process, step),
            SharedOperation::Kill { process } => {
                if let Some(process) = self.process(process)
                    && let Some(owner) = process.cache.cache_store().owned()
                {
                    owner.abandon_persistence();
                }
                self.drop_process(process, step);
            }
            SharedOperation::DropMidRead {
                process,
                ref read,
                polls,
            } => {
                let slot = self.slot(process);
                let Some(process) = self.slots[slot].process.take() else {
                    return;
                };
                self.give_up(&process.cache, read, polls, process.built, step);
                drop(process);
                self.note_given_up(read, step);
            }
            SharedOperation::Restart { process } => {
                self.drop_process(process, step);
                self.build(self.slot(process), step);
            }
            SharedOperation::HoldGets { process } => self.slots[self.slot(process)].store.hold(),
            SharedOperation::ReleaseGets => self.release_gets(),
            SharedOperation::Tick => self.transport.clock().advance(Ticker::TICK),
            SharedOperation::Settle => (0..Self::SETTLE_STEPS).for_each(|_| thread::yield_now()),
        }
    }

    fn drop_process(&mut self, process: usize, step: usize) {
        let slot = self.slot(process);
        if self.slots[slot].process.take().is_some() {
            self.disruptions.push(step);
        }
    }

    fn give_up(
        &self,
        cache: &T4Caching,
        read: &ScenarioRead,
        polls: u8,
        built: usize,
        step: usize,
    ) {
        let mut answer = pin!(read.answer(cache));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..polls {
            if let Poll::Ready(answer) = answer.as_mut().poll(&mut context) {
                self.check_answer(read, built, step, &answer);
                return;
            }
        }
    }

    /// A read given up abandons its claims, and leaves its writes in flight.
    fn note_given_up(&mut self, read: &ScenarioRead, step: usize) {
        self.disruptions.push(step);
        self.given_up_chunks += self.model.most_blocks(read.file());
    }

    /// A read the script waits on must not wait on a GET it holds.
    fn release_gets(&self) {
        for slot in &self.slots {
            slot.store.release();
        }
    }

    fn join_spawned(&mut self) {
        self.release_gets();
        for spawned in std::mem::take(&mut self.spawned) {
            let answer = spawned.thread.join().expect("a spawned read panicked");
            self.check_answer(&spawned.read, spawned.built, spawned.started, &answer);
        }
    }

    fn check_answer(&self, read: &ScenarioRead, built: usize, started: usize, answer: &Answer) {
        assert!(
            (self.model).allows(read.file(), built, started, read.range(), answer),
            "{read:?} by a process built at step {built}, started at step {started}, \
             answered {answer:?}; the model: {:?}",
            self.model
        );
    }

    /// Once no read is in flight: every claim and call ended, every span free, a session for
    /// each client, and each block fetched no more often than the script explains.
    fn check_quiet(&self, step: usize, after: &SharedOperation) {
        self.check_model();
        let mut unsettled = None;
        for _ in 0..Self::SETTLE_ROUNDS {
            unsettled = self.unsettled();
            if unsettled.is_none() {
                break;
            }
            self.ticker.progressed();
            thread::yield_now();
        }
        if let Some(unsettled) = unsettled {
            panic!("after step {step}, {after:?}, the processes never settle: {unsettled}");
        }
        self.check_fetches(step, after);
    }

    /// The harness's own check, so any other failure is the cache's.
    fn check_model(&self) {
        for file in 0..Model::FILES {
            let size = block_on(self.remote.size(file));
            let expected = self.model.current(file).map(Content::size);
            assert_eq!(size, expected, "file {file}");
        }
    }

    fn unsettled(&self) -> Option<String> {
        let live = || {
            (self.slots.iter().enumerate())
                .filter_map(|(slot, held)| Some((slot, held.process.as_ref()?)))
        };
        let mut clients = 0;
        for (slot, process) in live() {
            let store = &process.cache.cache_store;
            if let Some(cache) = store.owned()
                && let Err(error) = cache.check_at_rest()
            {
                return Some(format!("the owner, process {slot}: {error}"));
            }
            let Some(owner) = store.owner_connection() else {
                continue;
            };
            let calls = owner.calls_waiting();
            if !calls.is_empty() {
                return Some(format!("process {slot}'s calls {calls:?} wait"));
            }
            if !owner.region_all_free() {
                return Some(format!("process {slot} has spans lent or held"));
            }
            clients += usize::from(!owner.owner_gone());
        }
        let owner = live().find(|(_, process)| process.cache.cache_store.owned().is_some());
        let serving = owner.map(|(_, process)| process.cache.cache_store.live_sessions());
        match serving {
            Some(serving) if serving != clients => Some(format!(
                "the owner serves {serving} sessions, for {clients} clients"
            )),
            _ => None,
        }
    }

    /// A block of a file written once is fetched once, and once more for each step since that
    /// could drop it or abandon its fetch; unless more blocks were fetched than the tier holds
    /// beside the chunks reads given up may still write, when eviction or want of room explains
    /// any count.
    fn check_fetches(&self, step: usize, after: &SharedOperation) {
        let fetched = self.remote.fetched();
        let evicted = fetched.len() as u64 + self.given_up_chunks > self.settings.tier_blocks();
        if evicted {
            return;
        }
        for (&(content, block), &count) in &fetched {
            let Some(written) = self.model.written_once(content) else {
                continue;
            };
            let disruptions = self.disruptions.iter().filter(|&&at| at > written).count();
            assert!(
                count as usize <= 1 + disruptions,
                "after step {step}, {after:?}: block {block} of {content:?} fetched {count} \
                 times, {disruptions} disruptions since it was written at step {written}; \
                 every block fetched: {}",
                FetchedBlocks(&fetched)
            );
        }
    }
}

struct FetchedBlocks<'f>(&'f BTreeMap<(Content, u64), u32>);

impl fmt::Display for FetchedBlocks<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.0.iter()).finish()
    }
}
