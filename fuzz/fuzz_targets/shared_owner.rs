#![no_main]

//! Shared mode's owner, serving a client whose frames the input picks while a well-behaved client
//! reads beside it. Its owner and clients run on shuttle: build it with
//! `RUSTFLAGS="--cfg t4_simulated"` (`fuzz/README.md`).

#[cfg(t4_simulated)]
mod scenario {
    use std::fmt;
    use std::num::NonZeroU8;
    use std::ops::Range;
    use std::time::Duration;

    use arbitrary::{Arbitrary, Unstructured};
    use libfuzzer_sys::fuzz_target;
    use t4::__fuzz::{FuzzedHello, FuzzedPath, FuzzedRead, FuzzedSession, FuzzedSpan, FuzzedStep};
    use t4::policy::{PlannedGet, ReadIntent, ReadPlan};
    use tempfile::TempDir;

    const MIB: u64 = 1 << 20;

    fuzz_target!(|scenario: Scenario| {
        let dir = TempDir::new().expect("a temporary directory");
        scenario.0.check(dir.path());
    });

    struct Scenario(FuzzedSession);

    impl fmt::Debug for Scenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    /// Few reads, calls and fetches, so that frames often name another's.
    impl<'a> Arbitrary<'a> for Scenario {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
            let seed = u.arbitrary()?;
            let hello = Self::hello(u)?;
            let reads = (0..u.int_in_range(0..=3)?)
                .map(|_| Ok(FuzzedRead::new(u.arbitrary()?, Self::range(u)?)))
                .collect::<arbitrary::Result<_>>()?;
            let mut steps = Vec::new();
            while !u.is_empty() && steps.len() < Self::MOST_STEPS {
                steps.push(Self::step(u)?);
            }
            Ok(Self(FuzzedSession::new(seed, hello, steps, reads)))
        }
    }

    impl Scenario {
        const MOST_STEPS: usize = 40;

        fn hello(u: &mut Unstructured<'_>) -> arbitrary::Result<FuzzedHello> {
            let region = u.arbitrary()?;
            Ok(match u.int_in_range(0..=9)? {
                0 => FuzzedHello::Versioned {
                    version: u.arbitrary()?,
                    region,
                },
                1 => FuzzedHello::ReadOnly { region },
                2 => FuzzedHello::Raw {
                    bytes: Self::bytes(u, 24)?,
                    region,
                },
                _ => FuzzedHello::Admitted { region },
            })
        }

        fn step(u: &mut Unstructured<'_>) -> arbitrary::Result<FuzzedStep> {
            Ok(match u.int_in_range(0..=19)? {
                0 | 1 => FuzzedStep::StartRead {
                    call: Self::call(u)?,
                    read: Self::read(u)?,
                    path: Self::path(u)?,
                    e_tag: match u.int_in_range(0..=7)? {
                        0 => Some(u.arbitrary()?),
                        _ => None,
                    },
                    size: match u.int_in_range(0..=7)? {
                        0 => Some(u.int_in_range(0..=4 * MIB)?),
                        1 => Some(Self::any_size(u)?),
                        _ => None,
                    },
                    range: Self::range(u)?,
                    intent: match u.int_in_range(0..=5)? {
                        0 => Some(ReadIntent::Latency),
                        1 => Some(ReadIntent::Throughput),
                        2 => Some(ReadIntent::Warm),
                        _ => None,
                    },
                    output: match u.int_in_range(0..=3)? {
                        0 => None,
                        1 => Some(Self::named_span(u)?),
                        _ => Some(FuzzedSpan::Fresh),
                    },
                },
                2 => FuzzedStep::TakeGets {
                    call: Self::call(u)?,
                    read: Self::read(u)?,
                    plan: Self::plan(u)?,
                },
                3 => FuzzedStep::Land {
                    read: Self::read(u)?,
                    fetch: Self::fetch(u)?,
                    len: Self::len(u)?,
                },
                4 => FuzzedStep::LandStaged {
                    read: Self::read(u)?,
                    fetch: Self::fetch(u)?,
                    len: Self::len(u)?,
                    staged: match u.int_in_range(0..=2)? {
                        0 => Self::named_span(u)?,
                        _ => FuzzedSpan::Fresh,
                    },
                },
                5 => FuzzedStep::Commit {
                    call: Self::call(u)?,
                    read: Self::read(u)?,
                    fetch: Self::fetch(u)?,
                },
                6 => FuzzedStep::Abandon {
                    read: Self::read(u)?,
                    fetch: Self::fetch(u)?,
                },
                7 => FuzzedStep::Finish {
                    call: Self::call(u)?,
                    read: Self::read(u)?,
                },
                8 => FuzzedStep::EndRead {
                    read: Self::read(u)?,
                },
                9 => FuzzedStep::KnownSize {
                    call: Self::call(u)?,
                    path: Self::path(u)?,
                },
                10 => match u.int_in_range(0..=3)? {
                    0 => FuzzedStep::Hello {
                        version: u.arbitrary()?,
                        region: u.arbitrary()?,
                    },
                    1 => FuzzedStep::CutOff {
                        length: match u.arbitrary()? {
                            true => u.int_in_range(0..=MIB)?,
                            false => Self::any_size(u)?,
                        },
                        body: Self::bytes(u, 64)?,
                    },
                    _ => FuzzedStep::Frame {
                        body: Self::bytes(u, 64)?,
                    },
                },
                11 => FuzzedStep::Scribble {
                    offset: u.arbitrary()?,
                    len: u.int_in_range(0..=MIB as u32)?,
                    byte: u.arbitrary()?,
                },
                12 | 13 => FuzzedStep::Fence,
                _ => FuzzedStep::Proceed {
                    read: Self::read(u)?,
                    file: u.int_in_range(0..=3)?,
                    staged: u.arbitrary()?,
                },
            })
        }

        fn read(u: &mut Unstructured<'_>) -> arbitrary::Result<u8> {
            u.int_in_range(0..=3)
        }

        fn call(u: &mut Unstructured<'_>) -> arbitrary::Result<u8> {
            u.int_in_range(0..=7)
        }

        fn fetch(u: &mut Unstructured<'_>) -> arbitrary::Result<u8> {
            u.int_in_range(0..=2)
        }

        fn len(u: &mut Unstructured<'_>) -> arbitrary::Result<Option<u32>> {
            Ok(match u.arbitrary()? {
                true => None,
                false => Some(u.int_in_range(0..=2 * MIB as u32 + 4096)?),
            })
        }

        fn path(u: &mut Unstructured<'_>) -> arbitrary::Result<FuzzedPath> {
            Ok(match u.int_in_range(0..=7)? {
                0 => FuzzedPath::Named(Self::bytes(u, 16)?),
                _ => FuzzedPath::Picked(u.int_in_range(0..=3)?),
            })
        }

        /// Of any order of magnitude, as a uniform `u64` is nearly always past 2^60.
        fn any_size(u: &mut Unstructured<'_>) -> arbitrary::Result<u64> {
            let bits = u.int_in_range(0..=64)?;
            Ok(u.arbitrary::<u64>()?.checked_shr(64 - bits).unwrap_or(0))
        }

        /// Mostly within a few blocks; sometimes any, reversed or past any file.
        fn range(u: &mut Unstructured<'_>) -> arbitrary::Result<Range<u64>> {
            Ok(match u.int_in_range(0..=7)? {
                0 => Self::any_size(u)?..Self::any_size(u)?,
                _ => {
                    let start = u.int_in_range(0..=3 * MIB)?;
                    start..start + u.int_in_range(0..=3 * MIB)?
                }
            })
        }

        /// Mostly on a page, as an output's span must be, within the 4 MiB the harness names
        /// spans in.
        fn named_span(u: &mut Unstructured<'_>) -> arbitrary::Result<FuzzedSpan> {
            Ok(FuzzedSpan::Named {
                offset: match u.int_in_range(0..=2)? {
                    0 => Self::any_size(u)?,
                    _ => u.int_in_range(0..=1023_u64)? * 4096,
                },
                len: match u.arbitrary()? {
                    true => u.int_in_range(0..=4 * MIB)?,
                    false => Self::any_size(u)?,
                },
            })
        }

        fn plan(u: &mut Unstructured<'_>) -> arbitrary::Result<ReadPlan> {
            let gets = (0..u.int_in_range(0..=3)?)
                .map(|_| {
                    let blocks = match u.int_in_range(0..=7)? {
                        0 => u.arbitrary::<u32>()?..u.arbitrary()?,
                        _ => {
                            let start = u.int_in_range(0..=3)?;
                            start..start + u.int_in_range(0..=3)?
                        }
                    };
                    let race = NonZeroU8::new(u.int_in_range(1..=3)?).expect("one at least");
                    let get = PlannedGet::new(blocks, race);
                    Ok(match u.int_in_range(0..=3)? {
                        0 => get.with_late_race(Duration::from_millis(u.int_in_range(0..=100)?)),
                        _ => get,
                    })
                })
                .collect::<arbitrary::Result<_>>()?;
            Ok(ReadPlan::new(gets))
        }

        fn bytes(u: &mut Unstructured<'_>, most: usize) -> arbitrary::Result<Vec<u8>> {
            let len = u.int_in_range(0..=most)?;
            Ok(u.bytes(len)?.to_vec())
        }
    }
}

#[cfg(not(t4_simulated))]
libfuzzer_sys::fuzz_target!(|_input: &[u8]| {
    panic!("shared_owner runs on shuttle: build it with RUSTFLAGS=\"--cfg t4_simulated\"");
});
