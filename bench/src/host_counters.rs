//! [`HostProbe`]: the bytes a drive and the network moved, read from
//! Linux's `/proc`, so that a run can be set against either's bandwidth,
//! and the CPU time spent moving them, the process's and the machine's (the
//! kernel takes in packets wherever their interrupts land, not always on
//! the process's time); and the memory of the process's cgroup, so that a
//! run held to a memory budget can show what it cached.
//!
//! [`NicProbe`]: what the network interfaces' drivers count past bytes, as
//! `ethtool -S` prints it: on AWS's ENA, the packets the instance's limits
//! held back, and each receive queue's bytes, so that a run shows whether
//! AWS shaped its traffic and whether its connections spread over the
//! queues.

use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

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
        let process = Self::process_stat()?;
        let machine = Self::machine_cpu()?;
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
            minor_faults: process.minor_faults,
            user_cpu_ms: process.user_cpu_ms,
            system_cpu_ms: process.system_cpu_ms,
            machine_busy_ms: machine.busy_ms,
            machine_softirq_ms: machine.softirq_ms,
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

    /// This process's page faults and CPU time since it started, from
    /// `/proc/self/stat`, whose fields after the command's name (which ends
    /// in the last `)`) start with the 3rd: `minflt`, the 10th, counts the
    /// faults that found the page in memory or made it there, each a fresh
    /// page faulted in; `utime` and `stime`, the 14th and 15th, the CPU time
    /// in user space and in the kernel, in clock ticks, which Linux fixes at
    /// 100 a second for `/proc`.
    fn process_stat() -> Option<ProcessStat> {
        let stat = fs::read_to_string("/proc/self/stat").ok()?;
        let (_, after_name) = stat.rsplit_once(')')?;
        let fields: Vec<&str> = after_name.split_whitespace().collect();
        let field = |number: usize| fields.get(number - 3)?.parse::<u64>().ok();
        Some(ProcessStat {
            minor_faults: field(10)?,
            user_cpu_ms: field(14)? * MS_PER_TICK,
            system_cpu_ms: field(15)? * MS_PER_TICK,
        })
    }

    /// Every CPU's time since boot, summed, from `/proc/stat`'s first line,
    /// `cpu user nice system idle iowait irq softirq steal ...` in clock
    /// ticks: busy is all but idle and iowait.
    fn machine_cpu() -> Option<MachineCpu> {
        let stat = fs::read_to_string("/proc/stat").ok()?;
        let ticks: Vec<u64> = stat
            .lines()
            .next()?
            .strip_prefix("cpu ")?
            .split_whitespace()
            .map(|field| field.parse().ok())
            .collect::<Option<_>>()?;
        let [user, nice, system, _idle, _iowait, irq, softirq, steal, ..] = ticks[..] else {
            return None;
        };
        Some(MachineCpu {
            busy_ms: (user + nice + system + irq + softirq + steal) * MS_PER_TICK,
            softirq_ms: softirq * MS_PER_TICK,
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

/// Linux's clock ticks in `/proc`, fixed at 100 a second, in milliseconds.
const MS_PER_TICK: u64 = 10;

/// What [`HostProbe::machine_cpu`] reads of every CPU.
struct MachineCpu {
    busy_ms: u64,
    softirq_ms: u64,
}

/// What [`HostProbe::process_stat`] reads of this process.
struct ProcessStat {
    minor_faults: u64,
    user_cpu_ms: u64,
    system_cpu_ms: u64,
}

/// Bytes moved: by the probed drive, read and written, and received by the
/// network; the page faults the process took, and its CPU time, in user
/// space and in the kernel; and every CPU's busy time, in all and in
/// softirqs, where the kernel takes in packets.
#[derive(Clone, Copy, Debug)]
pub struct HostCounters {
    drive_read: u64,
    drive_written: u64,
    network_received: u64,
    minor_faults: u64,
    user_cpu_ms: u64,
    system_cpu_ms: u64,
    machine_busy_ms: u64,
    machine_softirq_ms: u64,
}

impl HostCounters {
    /// The CSV columns [`Self::csv_fields`] fills.
    pub const CSV_COLUMNS: &str = "drive_read_bytes,drive_written_bytes,network_received_bytes,\
                                   minor_faults,user_cpu_ms,system_cpu_ms,\
                                   machine_busy_ms,machine_softirq_ms";

    /// `counters` as the fields of [`Self::CSV_COLUMNS`], empty where none
    /// were counted.
    pub fn csv_fields(counters: Option<&Self>) -> String {
        counters.map_or_else(
            || ",,,,,,,".to_owned(),
            |moved| {
                format!(
                    "{},{},{},{},{},{},{},{}",
                    moved.drive_read,
                    moved.drive_written,
                    moved.network_received,
                    moved.minor_faults,
                    moved.user_cpu_ms,
                    moved.system_cpu_ms,
                    moved.machine_busy_ms,
                    moved.machine_softirq_ms
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
            minor_faults: self.minor_faults.saturating_sub(earlier.minor_faults),
            user_cpu_ms: self.user_cpu_ms.saturating_sub(earlier.user_cpu_ms),
            system_cpu_ms: self.system_cpu_ms.saturating_sub(earlier.system_cpu_ms),
            machine_busy_ms: self.machine_busy_ms.saturating_sub(earlier.machine_busy_ms),
            machine_softirq_ms: self
                .machine_softirq_ms
                .saturating_sub(earlier.machine_softirq_ms),
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

    pub fn minor_faults(&self) -> u64 {
        self.minor_faults
    }

    /// The CPU time the process spent in user space, in milliseconds.
    pub fn user_cpu_ms(&self) -> u64 {
        self.user_cpu_ms
    }

    /// The CPU time the process spent in the kernel, in milliseconds.
    pub fn system_cpu_ms(&self) -> u64 {
        self.system_cpu_ms
    }

    /// Every CPU's busy time, summed, in milliseconds: the process's, the
    /// kernel's for it wherever it ran, and anything else running.
    pub fn machine_busy_ms(&self) -> u64 {
        self.machine_busy_ms
    }

    /// Every CPU's time in softirqs, summed, in milliseconds: where the
    /// kernel takes in packets.
    pub fn machine_softirq_ms(&self) -> u64 {
        self.machine_softirq_ms
    }
}

impl fmt::Display for HostCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mib = |bytes: u64| bytes as f64 / f64::from(1 << 20);
        write!(
            f,
            "drive read {:.0} MiB, written {:.0} MiB, network {:.0} MiB, {} page faults, \
             CPU {:.1} s user + {:.1} s kernel",
            mib(self.drive_read),
            mib(self.drive_written),
            mib(self.network_received),
            self.minor_faults,
            self.user_cpu_ms as f64 / 1e3,
            self.system_cpu_ms as f64 / 1e3
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

/// Reads the [`NicCounters`] of every network interface but loopback, by
/// running `ethtool -S` on each: its CPU time falls in the machine's, so
/// it is sampled outside the window the CPU counters cover.
pub struct NicProbe {
    interfaces: Vec<String>,
}

impl NicProbe {
    /// A probe of the interfaces `/proc/net/dev` names, but loopback; none
    /// where there is no such file.
    pub fn new() -> Self {
        let interfaces = fs::read_to_string("/proc/net/dev")
            .map(|stats| {
                stats
                    .lines()
                    .filter_map(|line| Some(line.split_once(':')?.0.trim().to_owned()))
                    .filter(|interface| interface != "lo")
                    .collect()
            })
            .unwrap_or_default();
        Self { interfaces }
    }

    /// The counters now, over every interface; `None` if there is none,
    /// ethtool fails on one, or one counts no allowances (not an ENA).
    pub fn sample(&self) -> Option<NicCounters> {
        if self.interfaces.is_empty() {
            return None;
        }
        let mut counters = NicCounters::default();
        for interface in &self.interfaces {
            let printed = Command::new("ethtool")
                .arg("-S")
                .arg(interface)
                .output()
                .ok()?;
            if !printed.status.success() {
                return None;
            }
            counters.add(&NicCounters::parse(&String::from_utf8_lossy(
                &printed.stdout,
            ))?);
        }
        Some(counters)
    }
}

impl Default for NicProbe {
    fn default() -> Self {
        Self::new()
    }
}

/// What ENA counts past bytes: the packets held back, queued or dropped,
/// for passing the instance's limits, inbound bandwidth, outbound, packets
/// a second, tracked connections and link-local traffic (DNS, the metadata
/// service, time); and each receive queue's bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NicCounters {
    bw_in_allowance_exceeded: u64,
    bw_out_allowance_exceeded: u64,
    pps_allowance_exceeded: u64,
    conntrack_allowance_exceeded: u64,
    linklocal_allowance_exceeded: u64,
    /// Each receive queue's bytes, interface after interface.
    rx_queue_bytes: Vec<u64>,
}

impl NicCounters {
    /// The CSV columns [`Self::csv_fields`] fills: the allowances, the
    /// receive queues that took bytes, and the busiest one's share of them.
    pub const CSV_COLUMNS: &str = "bw_in_allowance_exceeded,bw_out_allowance_exceeded,\
                                   pps_allowance_exceeded,conntrack_allowance_exceeded,\
                                   linklocal_allowance_exceeded,rx_queues_used,\
                                   rx_busiest_queue_share";

    /// `counters` as the fields of [`Self::CSV_COLUMNS`], empty where none
    /// were counted, and the share where no queue took bytes.
    pub fn csv_fields(counters: Option<&Self>) -> String {
        let Some(counted) = counters else {
            return ",,,,,,".to_owned();
        };
        let total: u64 = counted.rx_queue_bytes.iter().sum();
        let busiest = counted.rx_queue_bytes.iter().max().copied().unwrap_or(0);
        let share = match total {
            0 => String::new(),
            total => format!("{:.3}", busiest as f64 / total as f64),
        };
        format!(
            "{},{},{},{},{},{},{share}",
            counted.bw_in_allowance_exceeded,
            counted.bw_out_allowance_exceeded,
            counted.pps_allowance_exceeded,
            counted.conntrack_allowance_exceeded,
            counted.linklocal_allowance_exceeded,
            counted
                .rx_queue_bytes
                .iter()
                .filter(|&&bytes| bytes > 0)
                .count()
        )
    }

    /// What was counted between `earlier` and `self`.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            bw_in_allowance_exceeded: self
                .bw_in_allowance_exceeded
                .saturating_sub(earlier.bw_in_allowance_exceeded),
            bw_out_allowance_exceeded: self
                .bw_out_allowance_exceeded
                .saturating_sub(earlier.bw_out_allowance_exceeded),
            pps_allowance_exceeded: self
                .pps_allowance_exceeded
                .saturating_sub(earlier.pps_allowance_exceeded),
            conntrack_allowance_exceeded: self
                .conntrack_allowance_exceeded
                .saturating_sub(earlier.conntrack_allowance_exceeded),
            linklocal_allowance_exceeded: self
                .linklocal_allowance_exceeded
                .saturating_sub(earlier.linklocal_allowance_exceeded),
            rx_queue_bytes: self
                .rx_queue_bytes
                .iter()
                .zip(earlier.rx_queue_bytes.iter().chain(std::iter::repeat(&0)))
                .map(|(now, then)| now.saturating_sub(*then))
                .collect(),
        }
    }

    /// One interface's counters, from what `ethtool -S` printed for it:
    /// `name: value` a line, its queues' as `queue_<n>_rx_bytes`. `None`
    /// if it counts no allowances.
    fn parse(printed: &str) -> Option<Self> {
        let mut counters = Self::default();
        let mut allowances = false;
        let mut queues = Vec::new();
        for (name, value) in printed.lines().filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim(), value.trim().parse::<u64>().ok()?))
        }) {
            let allowance = match name {
                "bw_in_allowance_exceeded" => &mut counters.bw_in_allowance_exceeded,
                "bw_out_allowance_exceeded" => &mut counters.bw_out_allowance_exceeded,
                "pps_allowance_exceeded" => &mut counters.pps_allowance_exceeded,
                "conntrack_allowance_exceeded" => &mut counters.conntrack_allowance_exceeded,
                "linklocal_allowance_exceeded" => &mut counters.linklocal_allowance_exceeded,
                _ => {
                    let queue = name
                        .strip_prefix("queue_")
                        .and_then(|rest| rest.strip_suffix("_rx_bytes"))
                        .and_then(|number| number.parse::<usize>().ok());
                    if let Some(queue) = queue {
                        queues.push((queue, value));
                    }
                    continue;
                }
            };
            *allowance = value;
            allowances = true;
        }
        queues.sort_unstable();
        counters.rx_queue_bytes = queues.into_iter().map(|(_, bytes)| bytes).collect();
        allowances.then_some(counters)
    }

    /// Adds another interface's counters.
    fn add(&mut self, other: &Self) {
        self.bw_in_allowance_exceeded += other.bw_in_allowance_exceeded;
        self.bw_out_allowance_exceeded += other.bw_out_allowance_exceeded;
        self.pps_allowance_exceeded += other.pps_allowance_exceeded;
        self.conntrack_allowance_exceeded += other.conntrack_allowance_exceeded;
        self.linklocal_allowance_exceeded += other.linklocal_allowance_exceeded;
        self.rx_queue_bytes.extend(&other.rx_queue_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ENA's counters as `ethtool -S` prints them, the allowances and each
    /// queue's bytes among the rest.
    const PRINTED: &str = "NIC statistics:
     tx_timeout: 0
     bw_in_allowance_exceeded: 12
     bw_out_allowance_exceeded: 0
     pps_allowance_exceeded: 3
     conntrack_allowance_exceeded: 0
     linklocal_allowance_exceeded: 7
     conntrack_allowance_available: 100
     queue_0_rx_cnt: 5
     queue_0_rx_bytes: 600
     queue_1_rx_bytes: 0
     queue_10_rx_bytes: 300
     queue_2_rx_bytes: 100
";

    #[test]
    fn enas_counters_are_read_from_ethtool() {
        let counters = NicCounters::parse(PRINTED).unwrap();
        assert_eq!(counters.bw_in_allowance_exceeded, 12);
        assert_eq!(counters.pps_allowance_exceeded, 3);
        assert_eq!(counters.linklocal_allowance_exceeded, 7);
        assert_eq!(
            counters.rx_queue_bytes,
            [600, 0, 100, 300],
            "by queue number"
        );
        assert_eq!(
            NicCounters::csv_fields(Some(&counters)),
            "12,0,3,0,7,3,0.600"
        );
        assert_eq!(
            NicCounters::parse("NIC statistics:\n     rx_bytes: 9\n"),
            None,
            "not an ENA"
        );
    }

    #[test]
    fn enas_counters_move_between_samples() {
        let earlier = NicCounters::parse(PRINTED).unwrap();
        let later =
            NicCounters::parse(&PRINTED.replace("12", "15").replace(": 600", ": 900")).unwrap();
        let moved = later.since(&earlier);
        assert_eq!(moved.bw_in_allowance_exceeded, 3);
        assert_eq!(moved.rx_queue_bytes, [300, 0, 0, 0]);
        assert_eq!(NicCounters::csv_fields(Some(&moved)), "3,0,0,0,0,1,1.000");
        assert_eq!(NicCounters::csv_fields(None), ",,,,,,");
    }
}
