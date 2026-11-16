#![no_main]

//! Built with `--features shared-mode`: t4 then runs on shuttle's primitives, which work only
//! inside its runtime.

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use object_store::GetRange;
use t4::__fuzz::{ScenarioRead, SharedOperation, SharedScenario, SharedSettings};

const BLOCK: u64 = 1 << 20;
const PAGE: u64 = 4096;

fuzz_target!(|input: Input| {
    let dir = tempfile::tempdir().unwrap();
    input.scenario.run(input.seed, dir.path());
});

/// The script, and the seed of the schedule it runs in.
#[derive(Debug)]
struct Input {
    seed: u64,
    scenario: SharedScenario,
}

impl Input {
    const MOST_OPERATIONS: usize = 32;
    const FILES: usize = 3;
    const PROCESSES: usize = 4;
}

impl<'a> Arbitrary<'a> for Input {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let seed = u.arbitrary()?;
        let settings = Self::settings(u)?;
        let mut operations = (0..Self::FILES)
            .map(|file| {
                Ok(SharedOperation::WriteBehind {
                    file,
                    size: Some(Self::size(u)?),
                })
            })
            .collect::<arbitrary::Result<Vec<_>>>()?;
        while !u.is_empty() && operations.len() < Self::MOST_OPERATIONS {
            operations.push(Self::operation(u)?);
        }
        Ok(Self {
            seed,
            scenario: SharedScenario::new(settings, operations),
        })
    }
}

impl Input {
    /// Tiers that hold every block a script fetches, so its fetches are counted, or small ones,
    /// so that eviction, moves to disk and promotions start within a few reads.
    fn settings(u: &mut Unstructured<'_>) -> arbitrary::Result<SharedSettings> {
        let (memory_blocks, disk_blocks) = match u.arbitrary()? {
            true => (64, 64),
            false => (u.int_in_range(1..=4)?, u.int_in_range(1..=6)?),
        };
        let settings = SharedSettings::new(u.int_in_range(2..=Self::PROCESSES)?)
            .with_tiers(memory_blocks, disk_blocks)
            .with_network_blocks(u.int_in_range(1..=4)?);
        let settings = match u.ratio(1, 4)? {
            true => settings.on_disk(),
            false => settings,
        };
        Ok(match u.arbitrary()? {
            true => settings.losing_wakeups(),
            false => settings,
        })
    }

    /// Reads two times in five, many on threads of their own.
    fn operation(u: &mut Unstructured<'_>) -> arbitrary::Result<SharedOperation> {
        let process = u.choose_index(Self::PROCESSES)?;
        Ok(match u.choose_index(25)? {
            0..=3 => SharedOperation::Read {
                process,
                read: Self::read(u)?,
            },
            4 => SharedOperation::ReadAtOnce {
                process,
                reads: (0..u.int_in_range(2..=3)?)
                    .map(|_| Self::read(u))
                    .collect::<arbitrary::Result<_>>()?,
            },
            5..=8 => SharedOperation::Spawn {
                process,
                read: Self::read(u)?,
            },
            9 => SharedOperation::Join,
            10 => SharedOperation::GiveUp {
                process,
                read: Self::read(u)?,
                polls: u.int_in_range(1..=4)?,
            },
            11 | 12 => SharedOperation::WriteBehind {
                file: u.choose_index(Self::FILES)?,
                size: match u.ratio(1, 5)? {
                    true => None,
                    false => Some(Self::size(u)?),
                },
            },
            13 => SharedOperation::Drop { process },
            14 => SharedOperation::Kill { process },
            15 => SharedOperation::DropMidRead {
                process,
                read: Self::read(u)?,
                polls: u.int_in_range(1..=4)?,
            },
            16 => SharedOperation::Restart { process },
            17 => SharedOperation::ShutDown { process },
            18 | 19 => SharedOperation::HoldGets { process },
            20 => SharedOperation::ReleaseGets,
            21 => SharedOperation::Tick,
            _ => SharedOperation::Settle,
        })
    }

    fn read(u: &mut Unstructured<'_>) -> arbitrary::Result<ScenarioRead> {
        let file = u.choose_index(Self::FILES)?;
        let range = match u.int_in_range(0..=3)? {
            0 => None,
            1 => {
                let (start, len) = (Self::offset(u)?, Self::offset(u)?);
                Some(GetRange::Bounded(start..start + len))
            }
            2 => Some(GetRange::Offset(Self::offset(u)?)),
            _ => Some(GetRange::Suffix(Self::offset(u)?)),
        };
        Ok(ScenarioRead::new(file, range))
    }

    /// Up to four blocks, often a block's edge or a page off it.
    fn size(u: &mut Unstructured<'_>) -> arbitrary::Result<u64> {
        let blocks: u64 = u.int_in_range(0..=3)?;
        Ok(blocks * BLOCK + Self::within_block(u, 1)?)
    }

    fn offset(u: &mut Unstructured<'_>) -> arbitrary::Result<u64> {
        let block: u64 = u.int_in_range(0..=4)?;
        Ok(block * BLOCK + Self::within_block(u, 0)?)
    }

    fn within_block(u: &mut Unstructured<'_>, least: u64) -> arbitrary::Result<u64> {
        Ok(match u.int_in_range(0..=3)? {
            0 => 0,
            1 => u.int_in_range(1..=2 * PAGE)?,
            2 => u.int_in_range(BLOCK - 2 * PAGE..=BLOCK - 1)?,
            _ => u.int_in_range(least..=BLOCK - 1)?,
        })
    }
}
