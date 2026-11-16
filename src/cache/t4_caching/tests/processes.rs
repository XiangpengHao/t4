use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;

use super::*;

struct ChildProcess(Child);

impl ChildProcess {
    const ROLE: &str = "T4_SHARED_TEST_ROLE";
    const DIR: &str = "T4_SHARED_TEST_DIR";

    fn spawn(role: &str, dir: &std::path::Path) -> Self {
        Self(Self::command(role, dir).spawn().unwrap())
    }

    fn spawn_with_temp_dir(role: &str, dir: &std::path::Path, temp_dir: &std::path::Path) -> Self {
        let mut command = Self::command(role, dir);
        Self(command.env("TMPDIR", temp_dir).spawn().unwrap())
    }

    fn command(role: &str, dir: &std::path::Path) -> Command {
        let (_crate, module) = module_path!().split_once("::").unwrap();
        let test = format!("{module}::a_child_process_plays_its_role");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([test.as_str(), "--exact", "--nocapture", "--test-threads=1"])
            .env(Self::ROLE, role)
            .env(Self::DIR, dir)
            .stdout(Stdio::null())
            .stdin(Stdio::piped());
        command
    }

    fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }

    fn exited_ok(&mut self) -> bool {
        drop(self.0.stdin.take());
        self.0.wait().unwrap().success()
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Written aside, then renamed, so no process reads a marker half written.
struct Markers {
    dir: PathBuf,
}

impl Markers {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            dir: dir.to_owned(),
        }
    }

    fn set(&self, name: &str, what: &str) {
        let aside = self.dir.join(format!("{name}.writing"));
        std::fs::write(&aside, what).unwrap();
        std::fs::rename(aside, self.dir.join(name)).unwrap();
    }

    fn wait_for(&self, name: &str) -> String {
        loop {
            if let Ok(what) = std::fs::read_to_string(self.dir.join(name)) {
                return what;
            }
            std::thread::yield_now();
        }
    }

    fn said(&self, name: &str) -> String {
        self.wait_for(name)
    }
}

struct SharedProcess {
    cache: T4Caching,
    store: Arc<CountingStore>,
    meta: ObjectMeta,
}

impl SharedProcess {
    const SIZE: u64 = 2 * MIB;

    fn data() -> Bytes {
        file_bytes(Self::SIZE, 31)
    }

    fn start(dir: &std::path::Path) -> Self {
        Self::start_with(dir, |builder| builder)
    }

    fn start_on_disk(dir: &std::path::Path) -> Self {
        Self::start_with(dir, |builder| {
            builder.with_placement_policy(|sizes| {
                crate::cache::policy::FixedPlacementPolicy::new(sizes, Admit::Disk)
            })
        })
    }

    fn start_with(
        dir: &std::path::Path,
        customize: impl FnOnce(T4CachingBuilder) -> T4CachingBuilder,
    ) -> Self {
        let store = Arc::new(CountingStore::default());
        let path = Path::from("shared");
        put(store.as_ref(), &path, &Self::data());
        let cache = customize(builder(&store, dir).with_multiprocess_sharing())
            .build()
            .unwrap();
        let meta = block_on(store.head(&path)).unwrap();
        store.take_asked();
        Self { cache, store, meta }
    }

    fn read(&self, range: Range<u64>) -> Bytes {
        let file = self.cache.open_listed(self.meta.clone());
        block_on(file.read(range, None)).unwrap()
    }

    fn read_on_thread(&self, range: Range<u64>) -> mpsc::Receiver<Bytes> {
        let (done, read) = mpsc::channel();
        let file = self.cache.open_listed(self.meta.clone());
        std::thread::spawn(move || {
            let _ = done.send(block_on(file.read(range, None)).unwrap());
        });
        read
    }

    fn owns(&self) -> bool {
        self.cache.cache_store().owned().is_some()
    }

    fn fetched_bytes(&self) -> u64 {
        self.store
            .take_asked()
            .iter()
            .map(|asked| match asked {
                Asked::Get(Some(GetRange::Bounded(range))) => range.end - range.start,
                asked => panic!("a read asks its store for a range: {asked:?}"),
            })
            .sum()
    }
}

/// A no-op unless the environment names a role, as in every normal test run.
#[test]
fn a_child_process_plays_its_role() {
    let Ok(role) = std::env::var(ChildProcess::ROLE) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(ChildProcess::DIR).unwrap());
    let markers = Markers::new(&dir);
    let process = match role.as_str() {
        "shut-down-owner" => SharedProcess::start_on_disk(&dir),
        _ => SharedProcess::start(&dir),
    };
    // Held till the process ends, so the gated GET is never answered.
    let mut held = None;
    match role.as_str() {
        "owner" => {
            assert!(process.owns());
            markers.set("owning", "");
        }
        "shut-down-owner" => {
            assert!(process.owns());
            assert_eq!(process.read(0..SharedProcess::SIZE), SharedProcess::data());
            process.cache.shut_down().unwrap();
            markers.set("shut-down", "");
        }
        "stalled-client" => {
            assert!(!process.owns());
            held = Some(process.store.gate_next_get());
            let _read = process.read_on_thread(0..MIB);
            wait_until(|| process.store.gets_out_now() == 1, "its GET goes out");
            markers.set("get-out", "");
        }
        reader => {
            let index = reader.strip_prefix("reader-").unwrap();
            let owns = if process.owns() { "owner" } else { "client" };
            markers.set(&format!("role-{index}"), owns);
            let data = SharedProcess::data();
            for range in [0..MIB, MIB / 2..3 * MIB / 2, 0..2 * MIB] {
                let (start, end) = (range.start as usize, range.end as usize);
                assert_eq!(process.read(range), data.slice(start..end));
            }
            let fetched = process.fetched_bytes().to_string();
            markers.set(&format!("fetched-{index}"), &fetched);
        }
    }
    // EOF means the parent finished, or died: no wall-clock deadline or orphan.
    let _ = std::io::stdin().read(&mut [0]);
    drop(held);
}

#[test]
fn an_owner_killed_mid_read_leaves_its_client_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let mut owner = ChildProcess::spawn("owner", dir.path());
    markers.said("owning");
    let client = SharedProcess::start(dir.path());
    assert!(!client.owns());
    let open = client.store.gate_next_get();
    let read = client.read_on_thread(0..SharedProcess::SIZE);
    wait_until(
        || client.store.gets_out_now() == 1,
        "the client's GET goes out",
    );

    owner.kill();
    drop(open);
    assert_eq!(read.recv().unwrap(), SharedProcess::data());
    wait_until(
        || {
            assert_eq!(client.read(0..SharedProcess::SIZE), SharedProcess::data());
            client.owns()
        },
        "a read makes the client the owner",
    );
    client.store.take_asked();
    assert_eq!(client.read(0..SharedProcess::SIZE), SharedProcess::data());
    assert_eq!(client.fetched_bytes(), 0, "the read that took over cached");
}

#[test]
fn an_owner_shut_down_then_killed_leaves_its_disk_tier() {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let mut owner = ChildProcess::spawn("shut-down-owner", dir.path());
    markers.said("shut-down");
    owner.kill();

    let next = SharedProcess::start_on_disk(dir.path());
    wait_until(
        || next.owns() || (next.read(0..1) == SharedProcess::data().slice(..1) && next.owns()),
        "the next cache owns the files",
    );
    next.fetched_bytes();
    assert_eq!(next.read(0..SharedProcess::SIZE), SharedProcess::data());
    assert_eq!(next.fetched_bytes(), 0, "the killed owner's disk tier");
}

/// As a terminal's shell that sets its own `TMPDIR` and a desktop app's differ.
#[test]
fn a_client_finds_an_owner_of_another_tmpdir() {
    let dir = tempfile::tempdir().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let _owner = ChildProcess::spawn_with_temp_dir("owner", dir.path(), temp_dir.path());
    markers.said("owning");

    let client = SharedProcess::start(dir.path());
    assert!(!client.owns());
    assert_eq!(client.read(0..SharedProcess::SIZE), SharedProcess::data());
}

#[test]
fn a_client_killed_holding_a_claim_hands_it_to_its_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let owner = SharedProcess::start(dir.path());
    assert!(owner.owns());
    let mut client = ChildProcess::spawn("stalled-client", dir.path());
    markers.said("get-out");

    let read = owner.read_on_thread(0..MIB);
    wait_until(
        || owner.cache.cache_store().owned().unwrap().waiting_reads() == 1,
        "the owner's read registered on the client's claim",
    );
    assert!(read.try_recv().is_err());
    client.kill();
    assert_eq!(
        read.recv().unwrap(),
        SharedProcess::data().slice(..MIB as usize)
    );
    assert_eq!(owner.fetched_bytes(), MIB, "the owner's own GET");
}

/// The default read policy races nothing here (no footer, no timer).
#[test]
fn eight_processes_starting_at_once_fetch_each_block_once() {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let mut readers: Vec<ChildProcess> = (0..8)
        .map(|index| ChildProcess::spawn(&format!("reader-{index}"), dir.path()))
        .collect();
    let roles: Vec<String> = (0..8)
        .map(|index| markers.said(&format!("role-{index}")))
        .collect();
    let fetched: u64 = (0..8)
        .map(|index| {
            markers
                .said(&format!("fetched-{index}"))
                .parse::<u64>()
                .unwrap()
        })
        .sum();

    for reader in &mut readers {
        assert!(reader.exited_ok(), "each read its bytes right");
    }
    let owners = roles.iter().filter(|role| *role == "owner").count();
    assert_eq!(owners, 1, "{roles:?}");
    assert_eq!(fetched, SharedProcess::SIZE, "the file fetched once");
}
