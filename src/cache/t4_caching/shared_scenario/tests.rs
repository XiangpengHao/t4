//! Fixed scripts for the PR suite, each a shape a past bug took; the fuzz target writes others.

use super::*;
use SharedOperation::*;

const MIB: u64 = BLOCK_SIZE_U64;

fn whole(file: usize) -> ScenarioRead {
    ScenarioRead::new(file, None)
}

fn bounded(file: usize, range: std::ops::Range<u64>) -> ScenarioRead {
    ScenarioRead::new(file, Some(GetRange::Bounded(range)))
}

/// Room for every block: each count of fetches the script can't explain fails.
fn roomy(processes: usize, network_blocks: u64) -> SharedSettings {
    SharedSettings::new(processes)
        .with_tiers(16, 4)
        .with_network_blocks(network_blocks)
}

/// The owner going hands one client's claim to another's waiting read, which must not fetch
/// for an owner that can't take it, then fetch again from its store (57bf283).
#[test]
fn a_claim_waited_on_as_the_owner_goes_is_fetched_once_more_at_most() {
    SharedScenario::new(
        roomy(3, 4),
        vec![
            WriteBehind {
                file: 0,
                size: Some(MIB),
            },
            HoldGets { process: 1 },
            Spawn {
                process: 1,
                read: whole(0),
            },
            Settle,
            Spawn {
                process: 2,
                read: whole(0),
            },
            Settle,
            Drop { process: 0 },
            Join,
            Tick,
            Read {
                process: 1,
                read: whole(0),
            },
            Read {
                process: 2,
                read: whole(0),
            },
        ],
    )
    .check();
}

/// A client's GETs under its killed owner still wait as it comes to own the cache, which gives
/// another file the id the owner gave theirs (a1c1f69).
#[test]
fn a_get_waiting_as_its_owner_goes_takes_no_bytes_of_the_next_caches_files() {
    SharedScenario::new(
        roomy(2, 2),
        vec![
            WriteBehind {
                file: 1,
                size: Some(3 * MIB),
            },
            WriteBehind {
                file: 2,
                size: Some(3 * MIB),
            },
            HoldGets { process: 1 },
            Spawn {
                process: 1,
                read: bounded(1, 0..2 * MIB),
            },
            Settle,
            Spawn {
                process: 1,
                read: bounded(1, 2 * MIB..3 * MIB),
            },
            Settle,
            Kill { process: 0 },
            Settle,
            Spawn {
                process: 1,
                read: bounded(2, MIB..2 * MIB),
            },
            Settle,
            Join,
        ],
    )
    .check();
}

#[test]
fn a_client_dropped_mid_read_leaves_its_claims_to_others() {
    SharedScenario::new(
        roomy(3, 4),
        vec![
            WriteBehind {
                file: 0,
                size: Some(2 * MIB + 5),
            },
            HoldGets { process: 1 },
            DropMidRead {
                process: 1,
                read: whole(0),
                polls: 3,
            },
            Spawn {
                process: 2,
                read: bounded(0, 4096..MIB + 1),
            },
            Read {
                process: 0,
                read: whole(0),
            },
            HoldGets { process: 2 },
            Spawn {
                process: 2,
                read: bounded(0, 2 * MIB..2 * MIB + 5),
            },
            Settle,
            ReleaseGets,
            Join,
            Restart { process: 1 },
            ReadAtOnce {
                process: 1,
                reads: vec![whole(0), bounded(0, MIB..2 * MIB)],
            },
        ],
    )
    .check();
}

/// Small tiers, every block to disk: the owner serves its clients from disk, and its disk tier
/// goes to the next owner.
#[test]
fn owners_come_and_go_over_a_disk_tier_while_files_change() {
    SharedScenario::new(
        SharedSettings::new(3)
            .with_tiers(2, 3)
            .with_network_blocks(2)
            .on_disk(),
        vec![
            WriteBehind {
                file: 0,
                size: Some(2 * MIB),
            },
            WriteBehind {
                file: 1,
                size: Some(MIB + 7),
            },
            Read {
                process: 0,
                read: whole(0),
            },
            Spawn {
                process: 1,
                read: whole(0),
            },
            Spawn {
                process: 2,
                read: whole(1),
            },
            WriteBehind {
                file: 0,
                size: Some(MIB),
            },
            ShutDown { process: 0 },
            Restart { process: 0 },
            Join,
            Tick,
            ReadAtOnce {
                process: 2,
                reads: vec![whole(0), whole(1), bounded(0, 10..MIB)],
            },
            GiveUp {
                process: 1,
                read: ScenarioRead::new(1, Some(GetRange::Suffix(5))),
                polls: 2,
            },
            WriteBehind {
                file: 1,
                size: None,
            },
            Read {
                process: 1,
                read: whole(1),
            },
        ],
    )
    .check();
}

/// A shutdown wakes no thread reading its own end, as on macOS: a client's drop waits on its
/// reader, which the end of its session wakes, and an owner's on its sessions, which look again
/// a second later (059f38c).
#[test]
fn a_shutdown_whose_wakeup_is_lost_hangs_no_process() {
    SharedScenario::new(
        roomy(3, 4).losing_wakeups(),
        vec![
            WriteBehind {
                file: 0,
                size: Some(MIB),
            },
            Read {
                process: 1,
                read: whole(0),
            },
            Settle,
            Drop { process: 1 },
            Spawn {
                process: 2,
                read: whole(0),
            },
            Drop { process: 0 },
            Join,
            Restart { process: 1 },
            Read {
                process: 1,
                read: whole(0),
            },
            Drop { process: 2 },
        ],
    )
    .check();
}

/// A read given up leaves its write to disk in flight, holding its chunk till done. With one
/// chunk, the reads at once find no room, so each runs to its end without waiting and fetches
/// the block; with two, they share one GET (fuzz run 43946954212, 16 Nov 2026).
#[test]
fn a_read_given_up_holds_a_disk_chunk_while_its_write_goes_on() {
    for disk_blocks in [1, 2] {
        SharedScenario::new(
            SharedSettings::new(2).with_tiers(1, disk_blocks).on_disk(),
            vec![
                WriteBehind {
                    file: 1,
                    size: Some(256),
                },
                // A read in flight till the end, so that no step waits for the processes to
                // settle, and the given-up write may outlast the reads.
                Spawn {
                    process: 1,
                    read: whole(0),
                },
                GiveUp {
                    process: 0,
                    read: whole(1),
                    polls: 1,
                },
                ReadAtOnce {
                    process: 0,
                    reads: vec![whole(1), whole(1), whole(1)],
                },
            ],
        )
        .check();
    }
}
