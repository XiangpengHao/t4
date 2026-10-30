use std::io::Write;
use std::os::fd::AsFd;

use super::*;

const PAGE: u64 = PAGE_SIZE_U64;

/// Spans are taken first fit, and given back merged with the free runs
/// beside them: the region's pages, all given back, are one run again.
#[test]
fn spans_are_taken_first_fit_and_merged_when_given_back() {
    let mut free = FreeSpans::new(10);
    assert_eq!(free.take(3), Some(0));
    assert_eq!(free.take(2), Some(3));
    assert_eq!(free.take(4), Some(5));
    assert_eq!(free.take(2), None, "one page is left");

    free.give_back(3, 2);
    assert_eq!(free.take(1), Some(3), "the first run that fits");
    free.give_back(0, 3);
    free.give_back(5, 4);
    free.give_back(3, 1);
    assert_eq!(free.runs, BTreeMap::from([(0, 10)]), "merged into one");
    assert_eq!(free.take(10), Some(0));
    assert_eq!(free.take(1), None);
}

/// A span its client writes is read by its owner, through the owner's own
/// mapping of the region; and a span dropped gives its pages back.
#[test]
fn a_span_its_client_writes_is_read_by_its_owner() {
    let region = ClientRegion::make().unwrap();
    let pages = region.free_pages();
    let owner = OwnerRegion::accept(region.descriptor().try_clone_to_owned().unwrap()).unwrap();

    let _before = region.span(1).unwrap();
    let mut span = region.span(3 * PAGE).unwrap();
    assert_eq!(
        span.name().offset(),
        PAGE,
        "first fit, after the first span"
    );
    assert_eq!(span.name().len(), 3 * PAGE);
    span.copy_in(PAGE as usize - 2, b"shared");
    let read = owner.with_bytes(span.name(), |bytes| {
        bytes[PAGE as usize - 2..][..6].to_vec()
    });
    assert_eq!(read.unwrap(), b"shared");
    assert_eq!(region.free_pages(), pages - 4);
    drop(span);
    assert_eq!(region.free_pages(), pages - 1);
}

/// The owner takes no span past its mapping of the region.
#[test]
fn the_owner_takes_no_span_past_the_region() {
    let region = ClientRegion::make().unwrap();
    let owner = OwnerRegion::accept(region.descriptor().try_clone_to_owned().unwrap()).unwrap();
    let bytes = ClientRegion::BYTES;
    for (offset, len) in [(bytes, 1), (bytes - PAGE, 2 * PAGE), (u64::MAX - 1, 4)] {
        let span = SpanName::new(offset, len);
        assert!(owner.with_bytes(span, |_| ()).is_err(), "{span:?}");
    }
    let last = SpanName::new(bytes - PAGE, PAGE);
    assert!(owner.with_bytes(last, |_| ()).is_ok());
}

/// The owner maps only shared memory that can't shrink under it: not a
/// regular file, which takes an `ftruncate`.
#[test]
fn the_owner_refuses_memory_that_could_shrink() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[7; PAGE as usize]).unwrap();
    let refused = OwnerRegion::accept(file.as_fd().try_clone_to_owned().unwrap());
    assert!(refused.is_err());
    assert_eq!(file.metadata().unwrap().len(), PAGE, "left as it was");
}

/// A memfd left unsealed could shrink: the owner refuses it.
#[cfg(target_os = "linux")]
#[test]
fn the_owner_refuses_an_unsealed_memfd() {
    use rustix::fs::{MemfdFlags, ftruncate, memfd_create};
    let memfd = memfd_create("t4-test", MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING).unwrap();
    ftruncate(&memfd, ClientRegion::BYTES).unwrap();
    assert!(OwnerRegion::accept(memfd).is_err());
}
