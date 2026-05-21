use std::collections::HashMap;
use std::fs::OpenOptions;
use std::num::NonZeroU32;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use verified::input_kv::{T4Key, T4KeyRef, T4Value, ValueRef};
use verified::{CheckedRangeU32, RangeRequestU32};

use crate::buffer::{AlignedBuf, align_down_u64, align_up_u32, align_up_u64};
use crate::io::error::{Error, Result};
use crate::io::io_worker::IoWorker;
use crate::io::sync::RwLock;
use crate::wal::{Wal, WalCommit, WalTombstoneCommit};
use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

// ---------------------------------------------------------------------------
// Index
//
// The in-memory view over the WAL. Each entry carries the LSN of the WAL
// record that produced it, so `apply_put` / `apply_remove` can enforce the
// store's core invariant:
//
//     for every key K, index[K] reflects the WAL record for K with the
//     highest LSN
//
// The WAL's LSN order is authoritative. If the index lock is acquired in
// a different order than LSNs were assigned, the LSN check rejects the
// out-of-order writer; the "loser" releases its own value space. Both
// orderings converge to the same final state, matching what `Wal::replay`
// would reconstruct after a remount.
//
// Tombstones live in the index (not just on disk) so that a put whose
// LSN is older than a concurrent remove's LSN can be rejected even when
// the key wasn't present beforehand. Their cost is one entry per
// deleted key; they're dropped on remount because replay produces only
// live entries.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum IndexEntry {
    Live { lsn: u64, vref: ValueRef },
    Tomb { lsn: u64 },
}

impl IndexEntry {
    fn lsn(&self) -> u64 {
        match self {
            Self::Live { lsn, .. } | Self::Tomb { lsn } => *lsn,
        }
    }

    fn live_vref(&self) -> Option<ValueRef> {
        match self {
            Self::Live { vref, .. } => Some(*vref),
            Self::Tomb { .. } => None,
        }
    }
}

pub(crate) enum ApplyPutOutcome {
    /// Our LSN was the newest; the index now points at our vref.
    /// `displaced` is the previous vref if the key was Live before.
    Inserted { displaced: Option<ValueRef> },
    /// A newer LSN already won the race. Our vref is shadowed by the
    /// newer WAL record — release it so the hole list stays consistent
    /// with what replay would derive.
    Rejected { our_vref: ValueRef },
}

pub(crate) enum ApplyRemoveOutcome {
    /// Our tombstone won and the key was Live; release the old vref.
    Removed { displaced: ValueRef },
    /// Our tombstone won but the key wasn't Live (absent, or already a
    /// tombstone with a smaller LSN).
    MarkedAbsent,
    /// A newer LSN already won the race; nothing to release.
    Rejected,
}

#[derive(Debug)]
pub(crate) struct Index {
    map: RwLock<HashMap<T4Key, IndexEntry>>,
}

impl Index {
    fn from_replay(initial: HashMap<T4Key, ValueRef>) -> Self {
        // Replay produces only Live entries; they all predate any
        // live operation, so lsn=0 is a safe lower bound (the WAL's
        // post-replay `next_lsn` is strictly greater than any LSN we
        // can newly allocate, so live ops always displace replayed
        // entries — unless a fresh remove for that key gets to them
        // first, which is also correct).
        let map = initial
            .into_iter()
            .map(|(k, v)| (k, IndexEntry::Live { lsn: 0, vref: v }))
            .collect();
        Self {
            map: RwLock::new(map),
        }
    }

    fn new_empty() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<ValueRef>> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map.get(key).and_then(IndexEntry::live_vref))
    }

    pub(crate) fn apply_put(
        &self,
        key: T4Key,
        commit: WalCommit<'_>,
    ) -> Result<ApplyPutOutcome> {
        let (lsn, vref) = commit.into_parts();
        let mut map = self.map.write().map_err(|_| Error::LockPoisoned)?;
        if let Some(existing) = map.get(&key)
            && existing.lsn() >= lsn
        {
            return Ok(ApplyPutOutcome::Rejected { our_vref: vref });
        }
        let displaced = map
            .insert(key, IndexEntry::Live { lsn, vref })
            .and_then(|e| e.live_vref());
        Ok(ApplyPutOutcome::Inserted { displaced })
    }

    pub(crate) fn apply_remove(
        &self,
        key: T4Key,
        commit: WalTombstoneCommit,
    ) -> Result<ApplyRemoveOutcome> {
        let lsn = commit.lsn();
        let mut map = self.map.write().map_err(|_| Error::LockPoisoned)?;
        if let Some(existing) = map.get(&key)
            && existing.lsn() >= lsn
        {
            return Ok(ApplyRemoveOutcome::Rejected);
        }
        let displaced = map
            .insert(key, IndexEntry::Tomb { lsn })
            .and_then(|e| e.live_vref());
        match displaced {
            Some(vref) => Ok(ApplyRemoveOutcome::Removed { displaced: vref }),
            None => Ok(ApplyRemoveOutcome::MarkedAbsent),
        }
    }

    fn len(&self) -> Result<usize> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map
            .values()
            .filter(|e| matches!(e, IndexEntry::Live { .. }))
            .count())
    }

    fn is_empty(&self) -> Result<bool> {
        let map = self.map.read().map_err(|_| Error::LockPoisoned)?;
        Ok(map
            .values()
            .all(|e| !matches!(e, IndexEntry::Live { .. })))
    }
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
pub(crate) struct T4Store {
    io: IoWorker,
    wal: Wal,
    index: Index,
}

impl T4Store {
    pub async fn mount_with_options(path: impl AsRef<Path>, options: MountOptions) -> Result<Self> {
        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true);

        #[cfg(target_os = "linux")]
        {
            let mut custom_flags: i32 = 0;
            if options.direct_io {
                custom_flags |= libc::O_DIRECT;
            }
            if options.dsync {
                custom_flags |= libc::O_DSYNC;
            }
            open.custom_flags(custom_flags);
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            if options.direct_io {
                return Err(Error::InvalidArgument(
                    "direct_io not supported on target_os",
                ));
            }
            let mut custom_flags: i32 = 0;
            if options.dsync {
                custom_flags |= libc::O_DSYNC;
            }
            open.custom_flags(custom_flags);
        }

        #[cfg(windows)]
        {
            if options.direct_io {
                return Err(Error::InvalidArgument(
                    "direct_io not supported on target_os",
                ));
            }
            let mut custom_flags: u32 = 0;
            if options.dsync {
                // FILE_FLAG_WRITE_THROUGH — Windows analogue of O_DSYNC.
                custom_flags |= 0x8000_0000;
            }
            open.custom_flags(custom_flags);
        }

        let file = open.open(path)?;
        let len = file.metadata()?.len();
        let queue_depth = NonZeroU32::new(options.queue_depth)
            .ok_or(Error::InvalidArgument("queue_depth must be > 0"))?;
        let io = IoWorker::new(queue_depth, file)?;

        let (wal, index) = if len == 0 {
            let wal = Wal::create(io.clone()).await?;
            (wal, Index::new_empty())
        } else {
            let (wal, replay_map) = Wal::replay(io.clone(), len).await?;
            (wal, Index::from_replay(replay_map))
        };

        Ok(Self { io, wal, index })
    }

    pub async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        let commit = self.wal.put(key.clone(), &value).await?;
        match self.index.apply_put(key, commit)? {
            ApplyPutOutcome::Inserted {
                displaced: Some(old),
            } => self.wal.release_value_space(old)?,
            ApplyPutOutcome::Inserted { displaced: None } => {}
            ApplyPutOutcome::Rejected { our_vref } => {
                // A concurrent put or remove with a later LSN beat us
                // to the index. Our WAL record still exists but will be
                // shadowed on replay; free its space so the in-memory
                // hole list stays consistent with what replay derives.
                self.wal.release_value_space(our_vref)?;
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: T4KeyRef<'_>) -> Result<Vec<u8>> {
        let value = self.index.get(key.as_bytes())?.ok_or(Error::NotFound)?;
        let Some(value_len_u32) = NonZeroU32::new(value.length()) else {
            return Ok(Vec::new());
        };
        let padded_u32 = align_up_u32(value_len_u32, PAGE_SIZE_NZ_U32)
            .map_err(|_| Error::Format("value length exceeds io buffer limit".into()))?;
        let buf = AlignedBuf::new_zeroed(padded_u32)?;
        let buf = self.io.read_exact_at(buf, value.offset()).await?;
        let value_len = value_len_u32.get() as usize;
        Ok(buf.as_slice()[..value_len].to_vec())
    }

    pub async fn get_range(&self, key: T4KeyRef<'_>, range: RangeRequestU32) -> Result<Vec<u8>> {
        let value = self.index.get(key.as_bytes())?.ok_or(Error::NotFound)?;

        let range: CheckedRangeU32 = range
            .checked_against(value.length())
            .ok_or(Error::RangeOutOfBounds)?;
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let abs_start = value
            .offset()
            .checked_add(u64::from(range.start()))
            .ok_or(Error::RangeOutOfBounds)?;
        let abs_end = value
            .offset()
            .checked_add(u64::from(range.end()))
            .ok_or(Error::RangeOutOfBounds)?;

        let aligned_start = align_down_u64(abs_start, PAGE_SIZE_U64);
        let aligned_end = align_up_u64(abs_end, PAGE_SIZE_U64).ok_or(Error::RangeOutOfBounds)?;
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
        let commit = self.wal.tombstone(key.clone()).await?;
        match self.index.apply_remove(key, commit)? {
            ApplyRemoveOutcome::Removed { displaced } => {
                self.wal.release_value_space(displaced)?;
                Ok(true)
            }
            ApplyRemoveOutcome::MarkedAbsent => Ok(false),
            ApplyRemoveOutcome::Rejected => Ok(false),
        }
    }

    pub async fn sync(&self) -> Result<()> {
        self.io.fsync()?.await
    }

    pub fn len(&self) -> Result<usize> {
        self.index.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.index.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_options() -> MountOptions {
        MountOptions {
            queue_depth: 8,
            direct_io: false,
            dsync: true,
        }
    }

    #[test]
    fn reuses_deleted_hole_after_remount() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("reuse-after-remount.t4");
            let value_a = vec![b'a'; 1000];
            let value_b = vec![b'b'; 1000];
            let value_c = vec![b'c'; 1000];

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"a").unwrap(),
                        T4Value::try_from_vec(value_a).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
                let len_after_first_put = std::fs::metadata(&path).unwrap().len();
                assert!(
                    store
                        .remove(T4Key::try_from_slice(b"a").unwrap())
                        .await
                        .unwrap()
                );
                store
                    .put(
                        T4Key::try_from_slice(b"b").unwrap(),
                        T4Value::try_from_vec(value_b.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
                assert_eq!(std::fs::metadata(&path).unwrap().len(), len_after_first_put);
            }

            let len_after_reuse = std::fs::metadata(&path).unwrap().len();

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"c").unwrap(),
                        T4Value::try_from_vec(value_c).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
                assert_eq!(
                    store
                        .get(T4KeyRef::try_from_slice(b"b").unwrap())
                        .await
                        .unwrap(),
                    value_b
                );
            }

            assert!(std::fs::metadata(&path).unwrap().len() > len_after_reuse);
        });
    }

    #[test]
    fn oversized_hole_split_survives_remount() {
        // A put whose padded length is within 2× of a freed hole reuses it,
        // splitting off the remainder as a smaller hole. Both the split reuse
        // and the leftover survive a remount.
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("oversized-hole-split.t4");
            let value_a = vec![b'a'; 5000]; // padded 8192
            let value_b = vec![b'b'; 1000]; // padded 4096 — fits in a's 8192 hole (within 2×)
            let value_c = vec![b'c'; 1000]; // padded 4096 — should reuse the 4096 remainder

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"a").unwrap(),
                        T4Value::try_from_vec(value_a).unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(
                    store
                        .remove(T4Key::try_from_slice(b"a").unwrap())
                        .await
                        .unwrap()
                );
                store.sync().await.unwrap();
            }

            let len_after_free = std::fs::metadata(&path).unwrap().len();

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"b").unwrap(),
                        T4Value::try_from_vec(value_b.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
            }

            // b split a's 8192 hole into a used 4096 slot and a freed 4096
            // remainder — file size unchanged.
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                len_after_free,
                "b should reuse a's 8192 hole via split"
            );

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"c").unwrap(),
                        T4Value::try_from_vec(value_c.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
                assert_eq!(
                    store
                        .get(T4KeyRef::try_from_slice(b"b").unwrap())
                        .await
                        .unwrap(),
                    value_b
                );
                assert_eq!(
                    store
                        .get(T4KeyRef::try_from_slice(b"c").unwrap())
                        .await
                        .unwrap(),
                    value_c
                );
            }

            // c reused the 4096 remainder — still no growth.
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                len_after_free,
                "c should reuse the split remainder across remount"
            );
        });
    }

    #[test]
    fn overwrite_hole_survives_remount() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("overwrite-hole-remount.t4");
            let old_value = vec![b'a'; 1000];
            let new_value = vec![b'b'; 1000];
            let other_value = vec![b'c'; 1000];

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"a").unwrap(),
                        T4Value::try_from_vec(old_value).unwrap(),
                    )
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"a").unwrap(),
                        T4Value::try_from_vec(new_value.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
            }

            let len_after_overwrite = std::fs::metadata(&path).unwrap().len();

            {
                let store = T4Store::mount_with_options(&path, test_options())
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"b").unwrap(),
                        T4Value::try_from_vec(other_value).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();
                assert_eq!(
                    store
                        .get(T4KeyRef::try_from_slice(b"a").unwrap())
                        .await
                        .unwrap(),
                    new_value
                );
            }

            assert_eq!(std::fs::metadata(&path).unwrap().len(), len_after_overwrite);
        });
    }

    // bug from: https://github.com/XiangpengHao/t4/issues/10
    #[cfg(feature = "shuttle")]
    #[test]
    fn shuttle_concurrent_put_same_key_index_matches_replay() {
        use super::*;
        use crate::io::sync::Arc;
        use verified::input_kv::{T4Key, T4KeyRef, T4Value};

        fn test_options() -> MountOptions {
            MountOptions {
                queue_depth: 4,
                direct_io: false,
                dsync: false,
            }
        }

        shuttle::check_random(
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("shuttle-put-race.t4");

                shuttle::future::block_on(async move {
                    let store = Arc::new(
                        T4Store::mount_with_options(&path, test_options())
                            .await
                            .unwrap(),
                    );

                    let value_a = vec![b'a'; 16];
                    let value_b = vec![b'b'; 16];

                    let s_a = store.clone();
                    let s_b = store.clone();
                    let t_a = shuttle::future::spawn(async move {
                        s_a.put(
                            T4Key::try_from_slice(b"k").unwrap(),
                            T4Value::try_from_vec(value_a).unwrap(),
                        )
                        .await
                        .unwrap();
                    });
                    let t_b = shuttle::future::spawn(async move {
                        s_b.put(
                            T4Key::try_from_slice(b"k").unwrap(),
                            T4Value::try_from_vec(value_b).unwrap(),
                        )
                        .await
                        .unwrap();
                    });
                    t_a.await.unwrap();
                    t_b.await.unwrap();

                    let live = store
                        .get(T4KeyRef::try_from_slice(b"k").unwrap())
                        .await
                        .unwrap();

                    // Drop the live store (flushes IoWorker, closes file),
                    // remount, and re-read. With the bug, replay picks the
                    // *highest LSN* WAL record, which can disagree with the
                    // value-ref the in-memory index ended up holding.
                    drop(store);

                    let store2 = T4Store::mount_with_options(&path, test_options())
                        .await
                        .unwrap();
                    let replayed = store2
                        .get(T4KeyRef::try_from_slice(b"k").unwrap())
                        .await
                        .unwrap();

                    assert_eq!(
                        live, replayed,
                        "live in-memory view disagrees with what replay reconstructs",
                    );
                });
            },
            500,
        );
    }
}
