//! An owner served one client whose frames the fuzzer picks, while a well-behaved client reads
//! beside it (`fuzz/README.md`, `shared_owner`).
use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use object_store::ObjectMeta;
use shuttle::future::block_on;

use super::protocol::Transport;
use crate::cache::policy::{ReadIntent, ReadPlan};
use crate::cache::{BLOCK_SIZE_U64, T4Caching};
use crate::io::sync::{Arc, thread};
use crate::io::test_schedule::TestSchedule;

mod misbehaving_client;
mod shared_files;

use misbehaving_client::MisbehavingClient;
use shared_files::SharedFiles;

#[derive(Debug)]
pub struct FuzzedSession {
    seed: u64,
    hello: FuzzedHello,
    steps: Vec<FuzzedStep>,
    reads: Vec<FuzzedRead>,
}

#[derive(Clone, Debug)]
pub enum FuzzedHello {
    Admitted {
        region: bool,
    },
    Versioned {
        version: u32,
        region: bool,
    },
    /// Its descriptor of the cache file can't write it.
    ReadOnly {
        region: bool,
    },
    /// A hello's bytes, as many as there are, with its descriptors.
    Raw {
        bytes: Vec<u8>,
        region: bool,
    },
}

/// Each frame is well formed; most aren't where the protocol allows them. A fetch's bytes are its
/// true bytes where the client knows the fetch, as a shared file's must be.
#[derive(Clone, Debug)]
pub enum FuzzedStep {
    StartRead {
        call: u8,
        read: u8,
        path: FuzzedPath,
        /// The file's own unless set.
        e_tag: Option<String>,
        size: Option<u64>,
        range: Range<u64>,
        intent: Option<ReadIntent>,
        output: Option<FuzzedSpan>,
    },
    TakeGets {
        call: u8,
        read: u8,
        plan: ReadPlan,
    },
    /// `len` bytes, or the rest of the fetch.
    Land {
        read: u8,
        fetch: u8,
        len: Option<u32>,
    },
    LandStaged {
        read: u8,
        fetch: u8,
        len: Option<u32>,
        staged: FuzzedSpan,
    },
    Commit {
        call: u8,
        read: u8,
        fetch: u8,
    },
    Abandon {
        read: u8,
        fetch: u8,
    },
    Finish {
        call: u8,
        read: u8,
    },
    EndRead {
        read: u8,
    },
    KnownSize {
        call: u8,
        path: FuzzedPath,
    },
    Hello {
        version: u32,
        region: bool,
    },
    /// Any body, after its length.
    Frame {
        body: Vec<u8>,
    },
    /// A length, and fewer bytes than it says, maybe: the client killed mid-frame.
    CutOff {
        length: u64,
        body: Vec<u8>,
    },
    /// The client writes its own region, lent or not.
    Scribble {
        offset: u64,
        len: u32,
        byte: u8,
    },
    /// Waits for the owner to answer what came before, or close.
    Fence,
    /// What a well-behaved client sends next for `read`, as far as the owner's answers say: a
    /// read of `file` if none, then its plans, its fetches (`staged` in the region, if any) and
    /// their commits, its finish, and its end.
    Proceed {
        read: u8,
        file: u8,
        staged: bool,
    },
}

#[derive(Clone, Debug)]
pub enum FuzzedPath {
    /// One of the store's files, or past them a path it doesn't hold.
    Picked(u8),
    Named(Vec<u8>),
}

#[derive(Clone, Debug)]
pub enum FuzzedSpan {
    /// Free in the region, and as long as the bytes need.
    Fresh,
    Named {
        offset: u64,
        len: u64,
    },
}

/// A read of the well-behaved client's, of the files it shares with the other.
#[derive(Clone, Debug)]
pub struct FuzzedRead {
    file: u8,
    range: Range<u64>,
}

impl FuzzedRead {
    pub fn new(file: u8, range: Range<u64>) -> Self {
        Self { file, range }
    }
}

impl FuzzedSession {
    const CACHE_FILE: &str = "cache";
    const MOST_WAITS: u32 = 10_000;

    /// `seed` picks the schedule: an input replays exactly.
    pub fn new(
        seed: u64,
        hello: FuzzedHello,
        steps: Vec<FuzzedStep>,
        reads: Vec<FuzzedRead>,
    ) -> Self {
        Self {
            seed,
            hello,
            steps,
            reads,
        }
    }

    /// Panics if the owner panics or hangs, if the well-behaved client reads a wrong byte, or
    /// if, once all is quiet, the owner holds anything of the session's or its cache isn't whole.
    pub fn check(self, dir: &Path) {
        let session = Arc::new(self);
        let dir = dir.to_path_buf();
        TestSchedule::fuzzed(session.seed, move || session.run(&dir));
    }

    fn run(&self, dir: &Path) {
        let files = Arc::new(SharedFiles::put());
        let transport = Transport::default();
        let owner = Self::cache(dir, &files, &transport).expect("the owner");
        let reader = Self::cache(dir, &files, &transport).expect("a client of the owner");
        let reading = {
            let (files, reads) = (Arc::clone(&files), self.reads.clone());
            thread::spawn(move || {
                for read in reads {
                    // An object store refuses an empty range.
                    let meta = &files.read_by_both()[usize::from(read.file) % 2];
                    let start = read.range.start.min(meta.size - 1);
                    let range = start..read.range.end.clamp(start + 1, meta.size);
                    Self::assert_reads(&reader, meta, range);
                }
            })
        };

        let mut client = MisbehavingClient::connect(dir, &transport, &self.hello);
        for step in &self.steps {
            client.take(step, &files);
        }
        // Closed or still served, the connection never leaves the owner stuck.
        client.fence();
        let region = client.close();
        reading.join().expect("the well-behaved client's reads");

        let cache_store = owner.cache_store();
        Self::wait_until("every session ended", || cache_store.live_sessions() == 0);
        if let Some(region) = region {
            Self::wait_until("the owner unmapped the region", || !region.lent_to_owner());
        }
        for meta in files.read_by_both() {
            Self::assert_reads(&owner, meta, 0..meta.size);
        }
        let mut waits = 0;
        while let Err(error) = owner.check_at_rest() {
            waits += 1;
            assert!(
                waits < Self::MOST_WAITS,
                "the owner's cache once quiet: {error}"
            );
            thread::yield_now();
        }
    }

    fn cache(dir: &Path, files: &SharedFiles, transport: &Transport) -> crate::Result<T4Caching> {
        T4Caching::builder(files.store())
            .with_memory_capacity(2 * BLOCK_SIZE_U64)
            .with_disk_file(dir.join(Self::CACHE_FILE), 3 * BLOCK_SIZE_U64)
            .with_multiprocess_sharing()
            .with_test_transport(transport.clone())
            .build()
    }

    fn assert_reads(cache: &T4Caching, meta: &ObjectMeta, range: Range<u64>) {
        let read = block_on(cache.open_listed(meta.clone()).read(range.clone(), None));
        let path = meta.location.as_ref().as_bytes();
        let expected = Bytes::from(SharedFiles::bytes(path, range.clone()));
        match read {
            Ok(bytes) => assert!(bytes == expected, "{} {range:?} read wrong", meta.location),
            Err(error) => panic!("{} {range:?} failed: {error}", meta.location),
        }
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let mut waits = 0;
        while !done() {
            waits += 1;
            assert!(waits < Self::MOST_WAITS, "{what}, never");
            thread::yield_now();
        }
    }
}
