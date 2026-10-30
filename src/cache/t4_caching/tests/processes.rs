//! A cache shared by real processes (`doc/11-shared-mode.md`, build step
//! 3): the test binary runs itself as children ([`ChildProcess`]), each
//! playing the role its environment names, and each process tells the
//! others what it did through marker files in a directory they share
//! ([`Markers`]). What only processes show: the kernel releasing a killed
//! owner's lock, a killed client's connection closing, and many processes
//! starting at once.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::*;

/// The most a test waits for a child's marker, its read, or its exit.
const PATIENCE: Duration = Duration::from_secs(10);

/// The most a child waits for its parent, which may have died, before it
/// exits: no test leaves a process behind for long.
const CHILD_PATIENCE: Duration = Duration::from_secs(30);

/// A child process of the test binary, playing `role` over a directory
/// its parent shares with it ([`Self::spawn`]): killed, and reaped, as it
/// drops, so that a failed test leaves none behind.
struct ChildProcess(Child);

impl ChildProcess {
    /// The environment variables that name a child's role and directory.
    const ROLE: &str = "T4_SHARED_TEST_ROLE";
    const DIR: &str = "T4_SHARED_TEST_DIR";

    /// The test binary run as a child that plays `role` over `dir`: only
    /// [`a_child_process_plays_its_role`] runs in it.
    fn spawn(role: &str, dir: &std::path::Path) -> Self {
        Self(Self::command(role, dir).spawn().unwrap())
    }

    /// As [`Self::spawn`], with `temp_dir` the child's `TMPDIR`, as a shell
    /// that sets its own gives it.
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
            .stdout(Stdio::null());
        command
    }

    /// Kills the child, as `SIGKILL` does, and waits for it to end.
    fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }

    /// Whether the child exited successfully, once it exits.
    fn exited_ok(&mut self) -> bool {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status.success();
            }
            assert!(Instant::now() < deadline, "a child process exits");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// What processes say to each other: files in the directory they share,
/// each made whole at once (written aside, then renamed), and waited for.
struct Markers {
    dir: PathBuf,
}

impl Markers {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            dir: dir.to_owned(),
        }
    }

    /// Says `what`, as marker `name`.
    fn set(&self, name: &str, what: &str) {
        let aside = self.dir.join(format!("{name}.writing"));
        std::fs::write(&aside, what).unwrap();
        std::fs::rename(aside, self.dir.join(name)).unwrap();
    }

    /// What marker `name` says, once set; `None` if it isn't within
    /// `patience`.
    fn wait_for(&self, name: &str, patience: Duration) -> Option<String> {
        let deadline = Instant::now() + patience;
        loop {
            if let Ok(what) = std::fs::read_to_string(self.dir.join(name)) {
                return Some(what);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// What marker `name` says, which a test waits [`PATIENCE`] for.
    fn said(&self, name: &str) -> String {
        self.wait_for(name, PATIENCE)
            .unwrap_or_else(|| panic!("marker {name} is set"))
    }
}

/// A process's part in a shared cache: a cache built for sharing on the
/// directory's cache file, over a store of its own that holds the file
/// every process reads, under the same e_tag.
struct SharedProcess {
    cache: T4Caching,
    store: Arc<CountingStore>,
    meta: ObjectMeta,
}

impl SharedProcess {
    const SIZE: u64 = 2 * MIB;

    /// The bytes of the file every process reads.
    fn data() -> Bytes {
        file_bytes(Self::SIZE, 31)
    }

    fn start(dir: &std::path::Path) -> Self {
        let store = Arc::new(CountingStore::default());
        let path = Path::from("shared");
        put(store.as_ref(), &path, &Self::data());
        let cache = builder(&store, dir).with_sharing().build().unwrap();
        let meta = block_on(store.head(&path)).unwrap();
        store.take_asked();
        Self { cache, store, meta }
    }

    fn read(&self, range: Range<u64>) -> Bytes {
        let file = self.cache.open_listed(self.meta.clone());
        block_on(file.read(range, None)).unwrap()
    }

    /// `range` read on a thread of its own: its bytes come through the
    /// receiver returned.
    fn read_on_thread(&self, range: Range<u64>) -> mpsc::Receiver<Bytes> {
        let (done, read) = mpsc::channel();
        let file = self.cache.open_listed(self.meta.clone());
        std::thread::spawn(move || {
            let _ = done.send(block_on(file.read(range, None)).unwrap());
        });
        read
    }

    fn owns(&self) -> bool {
        matches!(*self.cache.link(), StorageLink::Owner { .. })
    }

    /// The bytes the process's store sent since last asked.
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

/// A child process's part, when the environment names one; nothing
/// otherwise, as in every run of the tests.
#[test]
fn a_child_process_plays_its_role() {
    let Ok(role) = std::env::var(ChildProcess::ROLE) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(ChildProcess::DIR).unwrap());
    let markers = Markers::new(&dir);
    let process = SharedProcess::start(&dir);
    // A gate held till the process ends: the GET it holds is never
    // answered.
    let mut held = None;
    match role.as_str() {
        // Owns the cache, until it is killed.
        "owner" => {
            assert!(process.owns());
            markers.set("owning", "");
        }
        // Reads through its owner, its GET never answered, until killed.
        "stalled-client" => {
            assert!(!process.owns());
            held = Some(process.store.gate_next_get());
            let _read = process.read_on_thread(0..MIB);
            wait_until(|| process.store.gets_out_now() == 1, "its GET goes out");
            markers.set("get-out", "");
        }
        // Says whether it owns, reads, says what its store sent, and
        // stays till its parent says the test is done.
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
    markers.wait_for("done", CHILD_PATIENCE);
    drop(held);
}

/// The owner killed while its client's read is in flight: the read
/// returns the right bytes, from the client's store, and a later read
/// makes the client the owner, as the kernel released the dead owner's
/// lock.
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
    assert_eq!(read.recv_timeout(PATIENCE).unwrap(), SharedProcess::data());
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

/// An owner whose `TMPDIR` isn't its client's, as a terminal's in a shell
/// that sets its own isn't a desktop app's: the client still finds it, and
/// reads through it (decision 6).
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

/// A client killed while it holds a claim, its GET out: its connection
/// closes, the owner abandons its claim, and the owner's read waiting on
/// it fetches the block itself.
#[test]
fn a_client_killed_holding_a_claim_hands_it_to_its_waiter() {
    let dir = tempfile::tempdir().unwrap();
    let markers = Markers::new(dir.path());
    let owner = SharedProcess::start(dir.path());
    assert!(owner.owns());
    let mut client = ChildProcess::spawn("stalled-client", dir.path());
    markers.said("get-out");

    let read = owner.read_on_thread(0..MIB);
    let early = read.recv_timeout(Duration::from_millis(200));
    assert!(
        early.is_err(),
        "the owner's read waits on the client's claim"
    );
    client.kill();
    assert_eq!(
        read.recv_timeout(PATIENCE).unwrap(),
        SharedProcess::data().slice(..MIB as usize)
    );
    assert_eq!(owner.fetched_bytes(), MIB, "the owner's own GET");
}

/// Eight processes started at once on fresh cache files: one owns, the
/// others read through it, each reads the file right, and together their
/// stores sent the file once, no block fetched twice across processes.
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
    markers.set("done", "");

    for reader in &mut readers {
        assert!(reader.exited_ok(), "each read its bytes right");
    }
    let owners = roles.iter().filter(|role| *role == "owner").count();
    assert_eq!(owners, 1, "{roles:?}");
    assert_eq!(fetched, SharedProcess::SIZE, "the file fetched once");
}
