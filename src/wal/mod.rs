mod le_bytes;
pub(crate) mod page;
pub(crate) mod replay;

use vstd::prelude::*;

use crate::buffer::AlignedBuf;
use crate::io::error::Result;
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

use page::WalPage;

verus! {

/// Arithmetic model for advancing `next_lsn`.
pub fn allocate_next_lsn(next_lsn: u64) -> (result: Option<u64>)
    ensures
        result.is_some() ==> result.unwrap() > next_lsn,
        result.is_some() ==> result.unwrap() - next_lsn == 1u64,
        result.is_none() ==> next_lsn == u64::MAX,
{
    let next = next_lsn.checked_add(1)?;
    Some(next)
}

} // verus!

/// In-memory state of the WAL log: where the tail page lives, what its
/// contents are, and the next LSN to hand out.
///
/// Pure data + small helpers — no I/O, no synchronization. The owner
/// (`DiskData`) takes a single mutex that covers this together with the
/// [`FileAllocator`], because appending to the WAL may need to allocate
/// a fresh page from the same file tail that value records draw from.
#[derive(Debug)]
pub(crate) struct WalState {
    pub(crate) tail: WalPage,
    pub(crate) tail_offset: u64,
    pub(crate) next_lsn: u64,
}

impl WalState {
    /// State for a brand-new file: a single empty WAL page at offset 0.
    /// The caller is responsible for actually writing the initial page.
    pub(crate) fn fresh() -> Self {
        Self {
            tail: WalPage::empty(),
            tail_offset: 0,
            next_lsn: 0,
        }
    }
}

/// Encode a WAL page into a page-sized, aligned `PageWrite` at `offset`.
pub(crate) fn encode_page_write(offset: u64, page: &WalPage) -> Result<PageWrite> {
    let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
    buf.as_mut_slice().copy_from_slice(page.as_slice());
    Ok(PageWrite { buf, offset })
}

/// Read a single WAL page from disk at `offset`, up to its first torn entry.
pub(crate) async fn read_page(io: &IoWorker, offset: u64) -> Result<WalPage> {
    let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
    let buf = io.read_exact_at(buf, offset).await?;
    let boxed = buf
        .try_into_boxed_array()
        .expect("invalid aligned buffer layout");
    Ok(WalPage::recover(boxed)?)
}

/// Initial size of a newly-created file: one WAL page.
pub(crate) const INITIAL_FILE_TAIL: u64 = PAGE_SIZE_U64;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::PAGE_SIZE;
    use crate::input_kv::T4Key;
    use crate::wal::page::AppendEntry;

    use super::*;

    #[test]
    fn page_round_trip() {
        let mut page = WalPage::empty();
        page.set_next_page(8192);
        page.append(
            &AppendEntry::Live {
                key: T4Key::try_from_vec(b"alpha".to_vec()).unwrap(),
                offset: 4096,
                length: 123,
            },
            0,
        )
        .unwrap();
        page.append(
            &AppendEntry::Tombstone {
                key: T4Key::try_from_vec(b"beta".to_vec()).unwrap(),
            },
            1,
        )
        .unwrap();

        let boxed: Box<[u8; PAGE_SIZE]> = Box::new(page.as_slice().try_into().unwrap());
        let decoded = WalPage::recover(boxed).unwrap();
        assert_eq!(decoded.as_slice(), page.as_slice());
    }

    /// A page of live entries, one per key, at LSNs from 0.
    fn page_of(keys: &[Vec<u8>]) -> WalPage {
        let mut page = WalPage::empty();
        for (lsn, key) in keys.iter().enumerate() {
            let entry = AppendEntry::Live {
                key: T4Key::try_from_slice(key).unwrap(),
                offset: 4096,
                length: 1,
            };
            page.append(&entry, lsn as u64).unwrap();
        }
        page
    }

    /// However a power cut mixes the 512-byte sectors of an append's page
    /// write, the page recovers to the page before the append or after it.
    #[test]
    fn recover_after_torn_append() {
        const SECTOR: usize = 512;
        // Two 228-byte entries end at 472, so the third crosses a sector.
        let keys: Vec<Vec<u8>> = (0..3).map(|index| vec![b'a' + index; 200]).collect();
        let before = page_of(&keys[..2]);
        let after = page_of(&keys);
        for landed in 0..1_u32 << (PAGE_SIZE / SECTOR) {
            let mut torn = Box::new([0_u8; PAGE_SIZE]);
            for (index, sector) in torn.chunks_mut(SECTOR).enumerate() {
                let source = if landed & (1 << index) != 0 {
                    &after
                } else {
                    &before
                };
                sector.copy_from_slice(&source.as_slice()[index * SECTOR..][..SECTOR]);
            }
            let recovered = WalPage::recover(torn).unwrap();
            assert!(
                recovered.as_slice() == before.as_slice()
                    || recovered.as_slice() == after.as_slice(),
                "sectors landed: {landed:08b}",
            );
        }
    }

    #[test]
    fn recover_needs_the_header() {
        let mut bytes: Box<[u8; PAGE_SIZE]> = Box::new(page_of(&[]).as_slice().try_into().unwrap());
        bytes[0] = 0;
        assert!(WalPage::recover(bytes).is_err());
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = WalPage::empty();
        let mut i = 0_u64;
        while page
            .append(
                &AppendEntry::Live {
                    key: T4Key::try_from_vec(vec![b'k'; 64]).unwrap(),
                    offset: i * 4096,
                    length: 64,
                },
                i,
            )
            .is_ok()
        {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&AppendEntry::Live {
            key: T4Key::try_from_vec(vec![1; 128]).unwrap(),
            offset: 0,
            length: 1,
        }));
    }
}
