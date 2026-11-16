#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use t4::__fuzz::{FuzzedSnapshot, FuzzedVersion};

const MIB: u32 = 1 << 20;

fuzz_target!(|input: Input| {
    let snapshot = match input {
        Input::Body(body) => FuzzedSnapshot::new(&body),
        Input::Versions { next_id, versions } => FuzzedSnapshot::of_versions(
            next_id,
            versions.into_iter().map(Version::fuzzed).collect(),
        ),
    };
    let dir = tempfile::tempdir().unwrap();
    snapshot.check(dir.path());
});

#[derive(Debug)]
enum Input {
    Body(Vec<u8>),
    Versions {
        next_id: u64,
        versions: Vec<Version>,
    },
}

/// Small values, so that snapshots are often valid and often just off: a chunk past the tier's 8.
#[derive(Debug)]
struct Version {
    id: u64,
    size: u64,
    path: Vec<u8>,
    e_tag: String,
    blocks: Vec<(u32, u32)>,
}

impl Version {
    fn fuzzed(self) -> FuzzedVersion {
        FuzzedVersion::new(self.id, self.size, self.path, self.e_tag, self.blocks)
    }
}

impl<'a> Arbitrary<'a> for Input {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        if u.arbitrary()? {
            return Ok(Self::Body(u.arbitrary()?));
        }
        let next_id = u.int_in_range(0..=6)?;
        let mut versions = (0..u.int_in_range(0..=4)?)
            .map(|_| Version::arbitrary(u))
            .collect::<arbitrary::Result<Vec<_>>>()?;
        if u.arbitrary()? {
            versions.sort_by(|one, other| one.path.cmp(&other.path));
        }
        Ok(Self::Versions { next_id, versions })
    }
}

impl<'a> Arbitrary<'a> for Version {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let id = u.int_in_range(0..=6)?;
        let size = u64::from(u.int_in_range(0..=4 * MIB + 1)?);
        let path = (0..u.int_in_range(0..=2)?)
            .map(|_| u.int_in_range(b'a'..=b'c'))
            .collect::<arbitrary::Result<_>>()?;
        let e_tag = u.arbitrary()?;
        let mut blocks = (0..u.int_in_range(0..=4)?)
            .map(|_| Ok((u.int_in_range(0..=5)?, u.int_in_range(0..=8)?)))
            .collect::<arbitrary::Result<Vec<_>>>()?;
        if u.arbitrary()? {
            blocks.sort_unstable();
        }
        Ok(Self {
            id,
            size,
            path,
            e_tag,
            blocks,
        })
    }
}
