use std::collections::HashMap;
use std::fs::OpenOptions;
use std::num::NonZeroU32;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{Error, Result};
use crate::format::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};
use crate::io::{AlignedBuf, align_down_u64, align_up_u32, align_up_u64};
use crate::io_worker::IoWorker;
use crate::types::{RangeRequest, T4Key, T4KeyRef, T4Value};
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
    index: RwLock<HashMap<T4Key, ValueRef>>,
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
        let queue_depth = NonZeroU32::new(options.queue_depth)
            .ok_or(Error::InvalidArgument("queue_depth must be > 0"))?;
        let io = IoWorker::new(queue_depth, file)?;

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

    fn read_index(&self) -> Result<RwLockReadGuard<'_, HashMap<T4Key, ValueRef>>> {
        self.index.read().map_err(|_| Error::LockPoisoned)
    }

    fn write_index(&self) -> Result<RwLockWriteGuard<'_, HashMap<T4Key, ValueRef>>> {
        self.index.write().map_err(|_| Error::LockPoisoned)
    }

    pub async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        let value_ref = self.wal.put(key.clone(), &value).await?;
        self.write_index()?.insert(key, value_ref);
        Ok(())
    }

    pub async fn get(&self, key: T4KeyRef<'_>) -> Result<Vec<u8>> {
        let value = {
            let index = self.read_index()?;
            *index.get(key.as_bytes()).ok_or(Error::NotFound)?
        };
        let Some(value_len_u32) = NonZeroU32::new(value.length) else {
            return Ok(Vec::new());
        };
        let padded_u32 = align_up_u32(value_len_u32, PAGE_SIZE_NZ_U32)
            .map_err(|_| Error::Format("value length exceeds io buffer limit".into()))?;
        let buf = AlignedBuf::new_zeroed(padded_u32)?;
        let buf = self.io.read_exact_at(buf, value.offset).await?;
        let value_len = value_len_u32.get() as usize;
        Ok(buf.as_slice()[..value_len].to_vec())
    }

    pub async fn get_range(&self, key: T4KeyRef<'_>, range: RangeRequest) -> Result<Vec<u8>> {
        let value = {
            let index = self.read_index()?;
            *index.get(key.as_bytes()).ok_or(Error::NotFound)?
        };

        let range = range.checked_against(value.length)?;
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let abs_start = value
            .offset
            .checked_add(u64::from(range.start()))
            .ok_or(Error::RangeOutOfBounds)?;
        let abs_end = value
            .offset
            .checked_add(u64::from(range.end()))
            .ok_or(Error::RangeOutOfBounds)?;

        let aligned_start = align_down_u64(abs_start, PAGE_SIZE_U64);
        let aligned_end = align_up_u64(abs_end, PAGE_SIZE_U64);
        let read_len_u64 = aligned_end
            .checked_sub(aligned_start)
            .ok_or(Error::RangeOutOfBounds)?;
        let read_len_u32: u32 = read_len_u64
            .try_into()
            .map_err(|_| Error::RangeOutOfBounds)?;
        let read_len_u32 = NonZeroU32::new(read_len_u32).ok_or(Error::RangeOutOfBounds)?;
        let buf = AlignedBuf::new_zeroed(read_len_u32)?;
        let buf = self.io.read_exact_at(buf, aligned_start).await?;

        let slice_start_u64 = abs_start
            .checked_sub(aligned_start)
            .ok_or(Error::RangeOutOfBounds)?;
        let slice_start_u32: u32 = slice_start_u64
            .try_into()
            .map_err(|_| Error::RangeOutOfBounds)?;
        let slice_start = slice_start_u32 as usize;
        let slice_len = range.len() as usize;
        let slice_end = slice_start
            .checked_add(slice_len)
            .ok_or(Error::RangeOutOfBounds)?;
        Ok(buf.as_slice()[slice_start..slice_end].to_vec())
    }

    pub async fn remove(&self, key: T4Key) -> Result<bool> {
        self.wal.tombstone(key.clone()).await?;
        let existed = self.write_index()?.remove(&key).is_some();
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
