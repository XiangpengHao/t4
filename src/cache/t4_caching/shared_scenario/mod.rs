//! An owner and its clients in one process, driven by a script, on the simulated socket and
//! clock; shuttle's PCT picks the interleaving. The multi-process counterpart of the
//! `cache_equivalence` fuzz target (`fuzz/README.md`).

mod model;
mod remote;
mod run;
#[cfg(test)]
mod tests;
mod ticker;

use object_store::{GetOptions, GetRange, ObjectStore};

use crate::cache::BLOCK_SIZE_U64;
use model::{Answer, Model};
use remote::Remote;
use run::ScenarioRun;

/// A script of what each process does; a schedule's seed picks how their threads interleave.
#[derive(Clone, Debug)]
pub struct SharedScenario {
    settings: SharedSettings,
    operations: Vec<SharedOperation>,
}

/// Every process's: the owner's rule, and a client's count once it takes over.
#[derive(Clone, Debug)]
pub struct SharedSettings {
    processes: usize,
    memory_blocks: u64,
    disk_blocks: u64,
    /// The GETs' bytes in flight, so that more wait in the queue.
    network_blocks: u64,
    /// Every fetched block to disk, so the owner serves its clients from disk.
    on_disk: bool,
    loses_wakeups: bool,
}

/// Each aimed at a process by its index, of a file by its index, both taken modulo their count; a
/// process dropped does nothing till restarted.
#[derive(Clone, Debug)]
pub enum SharedOperation {
    Read {
        process: usize,
        read: ScenarioRead,
    },
    /// Joined on the script's thread, so they interleave at each await.
    ReadAtOnce {
        process: usize,
        reads: Vec<ScenarioRead>,
    },
    /// On a thread of its own, answered by the next `Join`.
    Spawn {
        process: usize,
        read: ScenarioRead,
    },
    Join,
    /// Polled a few times, then dropped.
    GiveUp {
        process: usize,
        read: ScenarioRead,
        polls: u8,
    },
    /// Straight to the store, behind every cache's back; `None` deletes.
    WriteBehind {
        file: usize,
        size: Option<u64>,
    },
    /// The owner's files go to the next to try them.
    Drop {
        process: usize,
    },
    /// Gone as a kill goes: its log keeps what it appended, less the last batch's adds.
    Kill {
        process: usize,
    },
    DropMidRead {
        process: usize,
        read: ScenarioRead,
        polls: u8,
    },
    /// Dropped, and built again on the same files.
    Restart {
        process: usize,
    },
    /// The process's GETs wait until `ReleaseGets`, or until the script waits on a read.
    HoldGets {
        process: usize,
    },
    ReleaseGets,
    /// A second passes: a client tries for the cache again, a waiting thread looks again.
    Tick,
    /// The script's thread steps aside, so the others run till they wait.
    Settle,
}

#[derive(Clone, Debug)]
pub struct ScenarioRead {
    file: usize,
    range: Option<GetRange>,
}

impl SharedScenario {
    /// A file is four blocks at most.
    const MOST_FILE_BYTES: u64 = 4 * BLOCK_SIZE_U64;

    pub fn new(settings: SharedSettings, operations: Vec<SharedOperation>) -> Self {
        Self {
            settings,
            operations,
        }
    }

    /// Runs the one schedule `seed` picks: an input replays exactly.
    #[cfg(t4_simulated)]
    pub fn run(self, seed: u64) {
        crate::io::test_schedule::TestSchedule::fuzzed(seed, move || self.play());
    }

    /// Under the checked-in schedule corpus.
    #[cfg(test)]
    fn check(self) {
        crate::io::test_schedule::TestSchedule::check(move || self.play());
    }

    /// Each schedule on a disk of its own: one schedule's log would warm the next.
    fn play(&self) {
        let mut run = ScenarioRun::start(&self.settings);
        for (index, operation) in self.operations.iter().enumerate() {
            run.apply(index + 1, operation);
        }
        run.finish(self.operations.len() + 1);
    }
}

impl SharedSettings {
    /// One to four processes, the first the owner; tiers of a block each, one block's GETs in
    /// flight, blocks placed by CLOCK.
    pub fn new(processes: usize) -> Self {
        Self {
            processes: processes.clamp(1, 4),
            memory_blocks: 1,
            disk_blocks: 1,
            network_blocks: 1,
            on_disk: false,
            loses_wakeups: false,
        }
    }

    pub fn with_tiers(mut self, memory_blocks: u64, disk_blocks: u64) -> Self {
        self.memory_blocks = memory_blocks.max(1);
        self.disk_blocks = disk_blocks.max(1);
        self
    }

    pub fn with_network_blocks(mut self, blocks: u64) -> Self {
        self.network_blocks = blocks.max(1);
        self
    }

    pub fn on_disk(mut self) -> Self {
        self.on_disk = true;
        self
    }

    /// A socket's shutdown wakes no thread waiting to read its own end, as on macOS.
    pub fn losing_wakeups(mut self) -> Self {
        self.loses_wakeups = true;
        self
    }

    /// Blocks the placement policy keeps in one tier: fetched no more than that, none was evicted.
    fn tier_blocks(&self) -> u64 {
        match self.on_disk {
            true => self.disk_blocks,
            false => self.memory_blocks,
        }
    }
}

impl ScenarioRead {
    pub fn new(file: usize, range: Option<GetRange>) -> Self {
        Self { file, range }
    }

    fn file(&self) -> usize {
        self.file % Model::FILES
    }

    fn range(&self) -> Option<&GetRange> {
        self.range.as_ref()
    }

    async fn answer(&self, store: &dyn ObjectStore) -> Answer {
        let options = GetOptions {
            range: self.range.clone(),
            ..GetOptions::default()
        };
        let Ok(got) = store.get_opts(&Remote::path(self.file()), options).await else {
            return Answer::Failed;
        };
        let range = got.range.clone();
        match got.bytes().await {
            Ok(bytes) => Answer::Bytes { range, bytes },
            Err(_) => Answer::Failed,
        }
    }
}
