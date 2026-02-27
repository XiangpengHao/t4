use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{Error, Result};
use crate::format::{PAGE_SIZE, PAGE_SIZE_U64};
use crate::io::{AlignedBuf, align_down_u64, align_up_u64, align_up_usize};
use crate::io_worker::IoWorker;
use crate::wal::{ValueRef, Wal};

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
pub struct Engine {
    io: IoWorker,
    wal: Wal,
    index: RwLock<HashMap<Vec<u8>, ValueRef>>,
}

impl Engine {
    pub async fn mount_with_options(path: impl AsRef<Path>, options: MountOptions) -> Result<Self> {
        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true);

        let mut custom_flags = 0;
        if options.direct_io {
            custom_flags |= libc::O_DIRECT;
        }
        if options.dsync {
            custom_flags |= libc::O_DSYNC;
        }
        open.custom_flags(custom_flags);

        let file = open.open(path)?;
        let len = file.metadata()?.len();
        let io = IoWorker::new(options.queue_depth, file)?;

        let (wal, index) = if len == 0 {
            let wal = Wal::create(io.clone()).await?;
            (wal, HashMap::new())
        } else {
            Wal::replay(io.clone(), len).await?
        };

        Ok(Self {
            io,
            wal,
            index: RwLock::new(index),
        })
    }

    fn read_index(&self) -> Result<RwLockReadGuard<'_, HashMap<Vec<u8>, ValueRef>>> {
        self.index.read().map_err(|_| Error::LockPoisoned)
    }

    fn write_index(&self) -> Result<RwLockWriteGuard<'_, HashMap<Vec<u8>, ValueRef>>> {
        self.index.write().map_err(|_| Error::LockPoisoned)
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }

        let value_len = value.len() as u64;
        let value_offset = self.wal.reserve_value_space(value_len)?;
        let buf = AlignedBuf::from_padded_slice(value)?;
        self.io.write_all_at(buf, value_offset).await?;

        self.wal
            .append_put(key.to_vec(), value_offset, value_len)
            .await?;
        self.write_index()?.insert(
            key.to_vec(),
            ValueRef {
                offset: value_offset,
                length: value_len,
            },
        );
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let value = {
            let index = self.read_index()?;
            *index.get(key).ok_or(Error::NotFound)?
        };
        if value.length == 0 {
            return Ok(Vec::new());
        }
        let padded = align_up_usize(value.length as usize, PAGE_SIZE);
        let buf = AlignedBuf::new_zeroed(padded)?;
        let buf = self.io.read_exact_at(buf, value.offset).await?;
        Ok(buf.as_slice()[..value.length as usize].to_vec())
    }

    pub async fn get_range(&self, key: &[u8], range_start: u64, range_len: u64) -> Result<Vec<u8>> {
        let value = {
            let index = self.read_index()?;
            *index.get(key).ok_or(Error::NotFound)?
        };
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
        let buf = self.io.read_exact_at(buf, aligned_start).await?;

        let slice_start = (abs_start - aligned_start) as usize;
        let slice_end = slice_start + range_len as usize;
        Ok(buf.as_slice()[slice_start..slice_end].to_vec())
    }

    pub async fn remove(&self, key: &[u8]) -> Result<bool> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }
        self.wal.append_tombstone(key.to_vec()).await?;
        let existed = self.write_index()?.remove(key).is_some();
        Ok(existed)
    }

    pub async fn sync(&self) -> Result<()> {
        self.io.fsync().await
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.read_index()?.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.read_index()?.is_empty())
    }
}
