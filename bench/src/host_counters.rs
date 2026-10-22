//! [`HostProbe`]: the bytes a drive and the network moved, read from
//! Linux's `/proc`, so that a run can be set against either's bandwidth;
//! and the memory of the process's cgroup, so that a run held to a memory
//! budget can show what it cached.

use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// Reads the [`HostCounters`] of drives, the block devices under some
/// directories, summed, and of every network interface but loopback. Where
/// `/proc` has no such counters (not Linux), it reads none.
pub struct HostProbe {
    /// The drives' major and minor device numbers, each once.
    drives: Vec<(u64, u64)>,
    /// The `memory.stat` of the cgroup v2 this process runs in, if any.
    memory_stat: Option<PathBuf>,
}

impl HostProbe {
    /// A probe of the drives that hold `dirs`, and of the network.
    ///
    /// # Errors
    ///
    /// If a directory can't be read.
    pub fn new(dirs: &[PathBuf]) -> io::Result<Self> {
        let mut drives = dirs
            .iter()
            .map(|dir| Ok(Self::device_numbers(fs::metadata(dir)?.dev())))
            .collect::<io::Result<Vec<_>>>()?;
        drives.sort_unstable();
        drives.dedup();
        Ok(Self {
            drives,
            memory_stat: Self::memory_stat(),
        })
    }

    /// The memory of this process's cgroup now, or `None` outside a cgroup
    /// v2 with the memory controller.
    pub fn cgroup_memory(&self) -> Option<CgroupMemory> {
        let stat = fs::read_to_string(self.memory_stat.as_ref()?).ok()?;
        let field = |name: &str| {
            stat.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        };
        Some(CgroupMemory {
            page_cache: field("file")?,
            anon: field("anon")?,
        })
    }

    /// Where this process's cgroup keeps its `memory.stat`: cgroup v2 names
    /// the cgroup in `/proc/self/cgroup` as `0::/path`.
    fn memory_stat() -> Option<PathBuf> {
        let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
        let stat = Path::new("/sys/fs/cgroup")
            .join(path.trim_start_matches('/'))
            .join("memory.stat");
        stat.exists().then_some(stat)
    }

    /// The counters now, or `None` where `/proc` has none.
    pub fn sample(&self) -> Option<HostCounters> {
        let network_received = Self::network_received()?;
        let (mut drive_read, mut drive_written) = (0, 0);
        for &numbers in &self.drives {
            let (read, written) = Self::drive_bytes(numbers)?;
            drive_read += read;
            drive_written += written;
        }
        Some(HostCounters {
            drive_read,
            drive_written,
            network_received,
        })
    }

    /// Linux's major and minor numbers of the device `dev` names, as glibc's
    /// `major` and `minor` take them apart.
    fn device_numbers(dev: u64) -> (u64, u64) {
        let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
        let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
        (major, minor)
    }

    /// The bytes read and written on the device `(major, minor)` since boot,
    /// from `/proc/diskstats`: sectors read is its 6th field, sectors
    /// written its 10th, each sector 512 bytes.
    fn drive_bytes((major, minor): (u64, u64)) -> Option<(u64, u64)> {
        let stats = fs::read_to_string("/proc/diskstats").ok()?;
        stats.lines().find_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let number = |index: usize| fields.get(index)?.parse::<u64>().ok();
            if number(0)? != major || number(1)? != minor {
                return None;
            }
            Some((number(5)? * 512, number(9)? * 512))
        })
    }

    /// The bytes every interface but loopback received since boot, from
    /// `/proc/net/dev`: the first field after an interface's name.
    fn network_received() -> Option<u64> {
        let stats = fs::read_to_string("/proc/net/dev").ok()?;
        let received = stats
            .lines()
            .filter_map(|line| line.split_once(':'))
            .filter(|(interface, _)| interface.trim() != "lo")
            .filter_map(|(_, counters)| counters.split_whitespace().next()?.parse::<u64>().ok())
            .sum();
        Some(received)
    }
}

/// Bytes moved: by the probed drive, read and written, and received by the
/// network.
#[derive(Clone, Copy, Debug)]
pub struct HostCounters {
    drive_read: u64,
    drive_written: u64,
    network_received: u64,
}

impl HostCounters {
    /// The CSV columns [`Self::csv_fields`] fills.
    pub const CSV_COLUMNS: &str = "drive_read_bytes,drive_written_bytes,network_received_bytes";

    /// `counters` as the fields of [`Self::CSV_COLUMNS`], empty where none
    /// were counted.
    pub fn csv_fields(counters: Option<&Self>) -> String {
        counters.map_or_else(
            || ",,".to_owned(),
            |moved| {
                format!(
                    "{},{},{}",
                    moved.drive_read, moved.drive_written, moved.network_received
                )
            },
        )
    }

    /// What moved between `earlier` and `self`.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            drive_read: self.drive_read.saturating_sub(earlier.drive_read),
            drive_written: self.drive_written.saturating_sub(earlier.drive_written),
            network_received: self
                .network_received
                .saturating_sub(earlier.network_received),
        }
    }

    pub fn drive_read(&self) -> u64 {
        self.drive_read
    }

    pub fn drive_written(&self) -> u64 {
        self.drive_written
    }

    pub fn network_received(&self) -> u64 {
        self.network_received
    }
}

impl fmt::Display for HostCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mib = |bytes: u64| bytes as f64 / f64::from(1 << 20);
        write!(
            f,
            "drive read {:.0} MiB, written {:.0} MiB, network {:.0} MiB",
            mib(self.drive_read),
            mib(self.drive_written),
            mib(self.network_received)
        )
    }
}

/// A cgroup's memory at a moment: its page cache, and its anonymous memory,
/// the process's own (t4's memory tier included).
#[derive(Clone, Copy, Debug)]
pub struct CgroupMemory {
    page_cache: u64,
    anon: u64,
}

impl CgroupMemory {
    /// The CSV columns [`Self::csv_fields`] fills.
    pub const CSV_COLUMNS: &str = "page_cache_bytes,anon_bytes";

    /// `memory` as the fields of [`Self::CSV_COLUMNS`], empty where it
    /// wasn't read.
    pub fn csv_fields(memory: Option<&Self>) -> String {
        memory.map_or_else(
            || ",".to_owned(),
            |memory| format!("{},{}", memory.page_cache, memory.anon),
        )
    }

    pub fn page_cache(&self) -> u64 {
        self.page_cache
    }

    pub fn anon(&self) -> u64 {
        self.anon
    }
}

impl fmt::Display for CgroupMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mib = |bytes: u64| bytes as f64 / f64::from(1 << 20);
        write!(
            f,
            "page cache {:.0} MiB, anon {:.0} MiB",
            mib(self.page_cache),
            mib(self.anon)
        )
    }
}
