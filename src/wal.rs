use crate::buffer::AlignedBuf;
use crate::io::error::Result;
use crate::io::io_task::PageWrite;
use crate::io::io_worker::IoWorker;
use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

use verified::wal::WalPage;

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

/// Read a single WAL page from disk at `offset`.
pub(crate) async fn read_page(io: &IoWorker, offset: u64) -> Result<WalPage> {
    let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32)?;
    let buf = io.read_exact_at(buf, offset).await?;
    let boxed = buf
        .try_into_boxed_array()
        .expect("invalid aligned buffer layout");
    Ok(WalPage::from_bytes(boxed)?)
}

/// Initial size of a newly-created file: one WAL page.
pub(crate) const INITIAL_FILE_TAIL: u64 = PAGE_SIZE_U64;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use verified::PAGE_SIZE;
    use verified::input_kv::T4Key;
    use verified::wal::AppendEntry;

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
        let decoded = WalPage::from_bytes(boxed).unwrap();
        assert_eq!(decoded.as_slice(), page.as_slice());
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
            i = i + 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&AppendEntry::Live {
            key: T4Key::try_from_vec(vec![1; 128]).unwrap(),
            offset: 0,
            length: 1,
        }));
    }
}
