//! The store on simulated disks under shuttle: thread interleavings, I/O
//! completing in any order, and power cuts.

use shuttle::rand::{Rng, thread_rng};
use shuttle::scheduler::{PctScheduler, RandomScheduler, Scheduler};

use super::{MountOptions, T4Store};
use crate::Error;
use crate::input_kv::{T4Key, T4KeyRef, T4Value};
use crate::io::simulated::SimDisk;
use crate::io::sync::Arc;

fn options() -> MountOptions {
    MountOptions {
        queue_depth: 4,
        direct_io: false,
        ..MountOptions::default()
    }
}

/// Runs `test` under each schedule `scheduler` explores.
fn explore(scheduler: impl Scheduler + 'static, test: impl Fn() + Send + Sync + 'static) {
    let mut config = shuttle::Config::new();
    // The default 60 KiB stack is too small for a debug build of a put,
    // whose index insert runs bbtree's descent; release fits.
    config.stack_size = 256 * 1024;
    shuttle::Runner::new(scheduler, config).run(test);
}

async fn put(store: &T4Store, key: &[u8], value: Vec<u8>) -> crate::Result<()> {
    let key = T4Key::try_from_slice(key).unwrap();
    store.put(key, T4Value::try_from_vec(value).unwrap()).await
}

async fn get(store: &T4Store, key: &[u8]) -> crate::Result<Vec<u8>> {
    store.get(T4KeyRef::try_from_slice(key).unwrap()).await
}

// bug from: https://github.com/XiangpengHao/t4/issues/10
#[test]
fn concurrent_put_same_key_index_matches_replay() {
    explore(RandomScheduler::new(500), || {
        shuttle::future::block_on(async {
            let disk = SimDisk::new();
            let store = Arc::new(T4Store::mount_simulated(&disk, options()).await.unwrap());

            let (s_a, s_b) = (store.clone(), store.clone());
            let t_a = shuttle::future::spawn(async move {
                put(&s_a, b"k", vec![b'a'; 16]).await.unwrap();
            });
            let t_b = shuttle::future::spawn(async move {
                put(&s_b, b"k", vec![b'b'; 16]).await.unwrap();
            });
            t_a.await.unwrap();
            t_b.await.unwrap();

            let live = get(&store, b"k").await.unwrap();

            // Drop the live store, remount, and re-read. With the bug,
            // replay picks the *highest LSN* WAL record, which can disagree
            // with the value-ref the in-memory index ended up holding.
            drop(store);
            let store = T4Store::mount_simulated(&disk, options()).await.unwrap();
            let replayed = get(&store, b"k").await.unwrap();

            assert_eq!(
                live, replayed,
                "live in-memory view disagrees with what replay reconstructs",
            );
        });
    });
}

/// A get racing an overwrite of its key, and a put that could reuse the
/// overwritten value's slot, returns a value the key held.
#[test]
fn get_never_reads_a_reused_extent() {
    // PCT finds this race reliably; random schedules mostly miss it.
    explore(PctScheduler::new(3, 2000), || {
        shuttle::future::block_on(async {
            let disk = SimDisk::new();
            let store = Arc::new(T4Store::mount_simulated(&disk, options()).await.unwrap());
            put(&store, b"k", vec![b'a'; 16]).await.unwrap();

            let reader = store.clone();
            let get = shuttle::future::spawn(async move { get(&reader, b"k").await.unwrap() });
            let writer = store.clone();
            let write = shuttle::future::spawn(async move {
                put(&writer, b"k", vec![b'b'; 16]).await.unwrap();
                put(&writer, b"other", vec![b'c'; 16]).await.unwrap();
            });
            let got = get.await.unwrap();
            write.await.unwrap();
            assert!(
                got == vec![b'a'; 16] || got == vec![b'b'; 16],
                "get(k) returned {:?}, which k never held",
                got[0] as char
            );
        });
    });
}

// ---------------------------------------------------------------------------
// Power cuts
//
// Writers change their own keys one operation at a time, so each key's
// history is a sequence of states. The power goes at a random completion,
// or at the end; the test then mounts what a reboot finds. The mount must
// succeed, and each key must hold a state from its history: at least the
// last one made durable, at most the one its writer was waiting on.
// ---------------------------------------------------------------------------

const WRITERS: u8 = 2;
const KEYS_PER_WRITER: u8 = 2;
const OPS_PER_WRITER: usize = 12;
/// Long keys fill a WAL page in 17 entries, so most runs start new pages.
const KEY_LEN: usize = 200;
/// Empty, within a page, and across pages.
const VALUE_LENS: [usize; 3] = [0, 100, 5000];
/// A run takes about 40 completions: the power goes during most runs, and
/// at their end otherwise.
const CUT_WITHIN: u32 = 50;

/// One key's history in a power-cut run.
struct KeyHistory {
    key: Vec<u8>,
    /// The states the key went through, from absent: one per operation
    /// issued, acknowledged or not.
    states: Vec<Option<Vec<u8>>>,
    /// How many of those operations were acknowledged.
    acked: usize,
}

impl KeyHistory {
    fn new(writer: u8, index: u8) -> Self {
        let mut key = vec![b'a' + writer; KEY_LEN];
        key[0] = b'0' + index;
        Self {
            key,
            states: vec![None],
            acked: 0,
        }
    }

    /// The states a reboot may find: from the last one acknowledged, which
    /// `O_DSYNC` made durable, to the last one issued.
    fn recoverable(&self) -> &[Option<Vec<u8>>] {
        &self.states[self.acked..]
    }

    fn describe(&self) -> String {
        format!("key {}{}", self.key[1] as char, self.key[0] as char)
    }
}

fn describe(state: &Option<Vec<u8>>) -> String {
    match state {
        None => "absent".to_string(),
        Some(value) => format!(
            "{:?} ({} bytes)",
            String::from_utf8_lossy(&value[..value.len().min(10)]),
            value.len()
        ),
    }
}

/// Changes `writer`'s keys at random until its operations are done or the
/// power goes, and returns their histories.
async fn write_until_power_cut(store: Arc<T4Store>, writer: u8) -> Vec<KeyHistory> {
    let mut keys: Vec<_> = (0..KEYS_PER_WRITER)
        .map(|index| KeyHistory::new(writer, index))
        .collect();
    let mut rng = thread_rng();
    for op in 0..OPS_PER_WRITER {
        let index = rng.gen_range(0..keys.len());
        let history = &mut keys[index];
        let state = if rng.gen_ratio(1, 4) {
            None
        } else {
            let mut value = format!("w{writer}op{op}-").into_bytes();
            value.resize(VALUE_LENS[rng.gen_range(0..VALUE_LENS.len())], b'.');
            Some(value)
        };
        history.states.push(state.clone());
        let done = match state {
            Some(value) => put(&store, &history.key, value).await,
            None => {
                let key = T4Key::try_from_slice(&history.key).unwrap();
                store.remove(key).await.map(|_| ())
            }
        };
        if done.is_err() {
            return keys;
        }
        history.acked += 1;
    }
    keys
}

/// Runs writers into a power cut on a disk persisting `sector_len` bytes
/// atomically, and checks that every acknowledged put and remove survives.
fn power_cut_keeps_acknowledged_writes_on(sector_len: usize) {
    explore(RandomScheduler::new(1000), move || {
        shuttle::future::block_on(async move {
            let disk = SimDisk::with_sector_len(sector_len);
            let store = Arc::new(T4Store::mount_simulated(&disk, options()).await.unwrap());
            disk.cut_power_within(CUT_WITHIN);

            let writers: Vec<_> = (0..WRITERS)
                .map(|writer| shuttle::future::spawn(write_until_power_cut(store.clone(), writer)))
                .collect();
            let mut histories = Vec::new();
            for writer in writers {
                histories.extend(writer.await.unwrap());
            }

            let rebooted = disk.power_cut();
            drop(store);
            let store = T4Store::mount_simulated(&rebooted, options())
                .await
                .unwrap_or_else(|err| panic!("mount after a power cut failed: {err}"));
            for history in &histories {
                let found = match get(&store, &history.key).await {
                    Ok(value) => Some(value),
                    Err(Error::NotFound) => None,
                    Err(err) => panic!(
                        "{}: get after a power cut failed: {err}",
                        history.describe()
                    ),
                };
                let recoverable = history.recoverable();
                assert!(
                    recoverable.contains(&found),
                    "{}: a reboot found {}, not one of {:?}",
                    history.describe(),
                    describe(&found),
                    recoverable.iter().map(describe).collect::<Vec<_>>(),
                );
            }
        });
    });
}

/// Every acknowledged put and remove survives a power cut, on a drive that
/// persists only 512-byte sectors atomically.
#[test]
fn power_cut_keeps_acknowledged_writes() {
    power_cut_keeps_acknowledged_writes_on(512);
}

/// As above, on a drive that persists whole pages atomically: no WAL page
/// can tear, so the WAL's pages must still link up after the cut.
#[test]
fn power_cut_keeps_acknowledged_writes_on_atomic_pages() {
    power_cut_keeps_acknowledged_writes_on(crate::PAGE_SIZE);
}
