use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

use crate::error::{Error, Result};
use crate::format::{FLAG_TOMBSTONE, IndexEntry, IndexPage, PAGE_SIZE, PAGE_SIZE_U64};
use crate::io::{AlignedBuf, align_down_u64, align_up_u64, align_up_usize};
use crate::uring_worker::UringWorker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct MountOptions {
    pub queue_depth: u32,
    pub direct_io: bool,
    pub dsync: bool,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            direct_io: true,
            dsync: true,
        }
    }
}

#[derive(Debug)]
struct TailIndexPage {
    offset: u64,
    page: IndexPage,
}

impl TailIndexPage {
    fn new(offset: u64, page: IndexPage) -> Self {
        Self { offset, page }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn can_fit(&self, entry: &IndexEntry) -> bool {
        self.page.can_fit(entry)
    }

    fn append(&mut self, entry: IndexEntry) {
        let pushed = self.page.push(entry);
        debug_assert!(pushed, "caller must check page capacity before append");
    }

    fn set_next_page(&mut self, next_page_offset: u64) {
        self.page.next_page = next_page_offset;
    }

    fn snapshot(&self) -> IndexPage {
        self.page.clone()
    }

    fn advance_to(&mut self, offset: u64, page: IndexPage) {
        self.offset = offset;
        self.page = page;
    }
}

#[derive(Debug)]
pub struct Engine {
    uring: UringWorker,
    index: HashMap<Vec<u8>, ValueRef>,
    bump_pointer: u64,
    tail_index_page: TailIndexPage,
}

impl Engine {
    pub async fn mount_with_options(path: impl AsRef<Path>, options: MountOptions) -> Result<Self> {
        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true);

        #[cfg(target_os = "linux")]
        {
            let mut custom_flags = 0;
            if options.direct_io {
                custom_flags |= libc::O_DIRECT;
            }
            if options.dsync {
                custom_flags |= libc::O_DSYNC;
            }
            open.custom_flags(custom_flags);
        }

        let file = open.open(path)?;
        let len = file.metadata()?.len();
        let uring = UringWorker::new(options.queue_depth, file)?;
        let mut index = HashMap::new();

        let (bump_pointer, tail_index_page) = if len == 0 {
            let page = IndexPage::empty();
            let mut page_buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
            page_buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
            uring.write_all_at(page_buf, 0).await?;
            (PAGE_SIZE_U64, TailIndexPage::new(0, page))
        } else {
            if len < PAGE_SIZE_U64 {
                return Err(Error::Format(
                    "store file shorter than first index page".into(),
                ));
            }

            let mut offset = 0_u64;
            let (last_offset, last_page) = loop {
                let page = Self::read_index_page_inner(&uring, offset).await?;
                for entry in &page.entries {
                    if entry.flags == FLAG_TOMBSTONE {
                        index.remove(entry.key.as_slice());
                    } else {
                        index.insert(
                            entry.key.clone(),
                            ValueRef {
                                offset: entry.offset,
                                length: entry.length,
                            },
                        );
                    }
                }
                if page.next_page == 0 {
                    break (offset, page);
                }
                offset = page.next_page;
            };
            (
                align_up_u64(len, PAGE_SIZE_U64),
                TailIndexPage::new(last_offset, last_page),
            )
        };

        Ok(Self {
            uring,
            index,
            bump_pointer,
            tail_index_page,
        })
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }

        let value_offset = self.bump_pointer;
        let value_len = value.len() as u64;
        if !value.is_empty() {
            let buf = AlignedBuf::from_padded_slice(value)?;
            let padded_len = buf.len() as u64;
            self.uring.write_all_at(buf, self.bump_pointer).await?;
            self.bump_pointer += padded_len;
        }

        let entry = IndexEntry::live(key.to_vec(), value_offset, value_len);
        self.append_index_entry(entry).await?;
        self.index.insert(
            key.to_vec(),
            ValueRef {
                offset: value_offset,
                length: value_len,
            },
        );
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let value = *self.index.get(key).ok_or(Error::NotFound)?;
        if value.length == 0 {
            return Ok(Vec::new());
        }
        let padded = align_up_usize(value.length as usize, PAGE_SIZE);
        let buf = AlignedBuf::new_zeroed(padded)?;
        let buf = self.uring.read_exact_at(buf, value.offset).await?;
        Ok(buf.as_slice()[..value.length as usize].to_vec())
    }

    pub async fn get_range(&self, key: &[u8], range_start: u64, range_len: u64) -> Result<Vec<u8>> {
        let value = *self.index.get(key).ok_or(Error::NotFound)?;
        let range_end = range_start
            .checked_add(range_len)
            .ok_or(Error::RangeOutOfBounds)?;
        if range_end > value.length {
            return Err(Error::RangeOutOfBounds);
        }
        if range_len == 0 {
            return Ok(Vec::new());
        }

        let abs_start = value
            .offset
            .checked_add(range_start)
            .ok_or(Error::RangeOutOfBounds)?;
        let abs_end = abs_start
            .checked_add(range_len)
            .ok_or(Error::RangeOutOfBounds)?;

        let aligned_start = align_down_u64(abs_start, PAGE_SIZE_U64);
        let aligned_end = align_up_u64(abs_end, PAGE_SIZE_U64);
        let read_len = (aligned_end - aligned_start) as usize;
        let buf = AlignedBuf::new_zeroed(read_len)?;
        let buf = self.uring.read_exact_at(buf, aligned_start).await?;

        let slice_start = (abs_start - aligned_start) as usize;
        let slice_end = slice_start + range_len as usize;
        Ok(buf.as_slice()[slice_start..slice_end].to_vec())
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }
        let existed = self.index.remove(key).is_some();
        self.append_index_entry(IndexEntry::tombstone(key.to_vec()))
            .await?;
        Ok(existed)
    }

    pub async fn sync(&self) -> Result<()> {
        self.uring.fsync().await
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    async fn append_index_entry(&mut self, entry: IndexEntry) -> Result<()> {
        if self.tail_index_page.can_fit(&entry) {
            self.tail_index_page.append(entry);
            let page = self.tail_index_page.snapshot();
            let offset = self.tail_index_page.offset();
            self.write_index_page(offset, &page).await?;
            return Ok(());
        }

        let new_page_offset = self.bump_pointer;
        self.bump_pointer = self
            .bump_pointer
            .checked_add(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Format("bump pointer overflow".into()))?;

        self.tail_index_page.set_next_page(new_page_offset);
        let prev_page = self.tail_index_page.snapshot();
        let prev_offset = self.tail_index_page.offset();
        self.write_index_page(prev_offset, &prev_page).await?;

        let mut new_page = IndexPage::empty();
        if !new_page.push(entry) {
            return Err(Error::Format(
                "entry does not fit in empty index page".into(),
            ));
        }
        self.write_index_page(new_page_offset, &new_page).await?;
        self.tail_index_page.advance_to(new_page_offset, new_page);
        Ok(())
    }

    async fn read_index_page_inner(uring: &UringWorker, offset: u64) -> Result<IndexPage> {
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        let buf = uring.read_exact_at(buf, offset).await?;
        IndexPage::from_bytes(buf.as_slice())
    }

    async fn write_index_page(&mut self, offset: u64, page: &IndexPage) -> Result<()> {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE)?;
        buf.as_mut_slice().copy_from_slice(&page.to_bytes()?);
        self.uring.write_all_at(buf, offset).await
    }
}
