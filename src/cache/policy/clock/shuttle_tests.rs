use std::num::NonZeroU32;

use super::ClockPolicy;
use crate::cache::policy::{BlockRead, DiskHit, PlacementPolicy, Tier};
use crate::cache::{BlockKey, ChunkId, FileVersionId};
use crate::io::sync::{Arc, thread};

fn block() -> BlockKey {
    BlockKey::new(FileVersionId::new(1), 0)
}

fn read() -> BlockRead<'static> {
    BlockRead::new(block(), 1, 0..1, b"a", None, true)
}

fn admit(policy: &ClockPolicy, tier: Tier, chunk: u32) {
    policy.on_admit(tier, ChunkId::new(chunk), block());
}

fn pick(policy: &ClockPolicy, count: usize) -> Vec<u32> {
    policy
        .pick_memory_victims(count)
        .iter()
        .map(|victim| victim.chunk().index())
        .collect()
}

/// Picker 0 reuses its first victim's chunk for another block, as the cache does.
#[test]
fn every_admission_is_picked_once() {
    crate::io::test_schedule::TestSchedule::iterations(500, || {
        let policy = Arc::new(ClockPolicy::new(
            NonZeroU32::new(66).unwrap(),
            NonZeroU32::MIN,
        ));
        let mut admitted = vec![0, 1, 64, 65];
        for &chunk in &admitted {
            admit(&policy, Tier::Memory, chunk);
        }
        let pickers: Vec<_> = (0..2)
            .map(|picker| {
                let policy = Arc::clone(&policy);
                thread::spawn(move || {
                    let victims = pick(&policy, 3);
                    let reused = victims.first().copied().filter(|_| picker == 0);
                    if let Some(chunk) = reused {
                        admit(&policy, Tier::Memory, chunk);
                    }
                    (victims, reused)
                })
            })
            .collect();
        let hitter = {
            let policy = Arc::clone(&policy);
            thread::spawn(move || {
                for chunk in [0, 2, 64] {
                    policy.on_memory_hit(ChunkId::new(chunk), &read());
                }
            })
        };
        let admitter = {
            let policy = Arc::clone(&policy);
            thread::spawn(move || admit(&policy, Tier::Memory, 2))
        };
        admitted.push(2);
        hitter.join().unwrap();
        admitter.join().unwrap();
        let mut picked = Vec::new();
        for picker in pickers {
            let (victims, reused) = picker.join().unwrap();
            picked.extend(victims);
            admitted.extend(reused);
        }
        picked.extend(pick(&policy, 10));
        picked.sort_unstable();
        admitted.sort_unstable();
        assert_eq!(picked, admitted);
    });
}

#[test]
fn of_two_racing_first_disk_hits_one_promotes() {
    crate::io::test_schedule::TestSchedule::iterations(500, || {
        let policy = Arc::new(ClockPolicy::new(NonZeroU32::MIN, NonZeroU32::MIN));
        admit(&policy, Tier::Disk, 0);
        let hits: Vec<_> = (0..2)
            .map(|_| {
                let policy = Arc::clone(&policy);
                thread::spawn(move || policy.on_disk_hit(ChunkId::new(0), &read()))
            })
            .collect();
        let promotions = hits
            .into_iter()
            .map(|hit| hit.join().unwrap())
            .filter(|&hit| hit == DiskHit::Promote)
            .count();
        assert_eq!(promotions, 1);
    });
}
