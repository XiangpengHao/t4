#![no_main]

//! The disk tier's snapshot from arbitrary bytes after a valid header: each
//! is refused, or encodes back to the same bytes and is taken back by a
//! cache whose index and tiers then agree.

use libfuzzer_sys::fuzz_target;
use t4::__fuzz::FuzzedSnapshot;

fuzz_target!(|body: &[u8]| {
    let dir = tempfile::tempdir().unwrap();
    FuzzedSnapshot::new(body).check(dir.path());
});
