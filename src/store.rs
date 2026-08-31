use std::fs::OpenOptions;
use std::num::NonZeroU32;
use std::path::Path;

use futures_util::{StreamExt, TryStreamExt, stream};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use verified::RangeRequestU32;
use verified::input_kv::{T4Key, T4KeyRef, T4Value};

use crate::disk::DiskData;
use crate::index::{ApplyPutOutcome, ApplyRemoveOutcome, Index};
use crate::io::error::{Error, Result};
use crate::io::io_worker::IoWorker;

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
    index: Index,
    disk_data: DiskData,
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

        // macOS has no O_DIRECT; page-cache bypass is requested per-fd via
        // F_NOCACHE after open (see below), which has no alignment requirements.
        #[cfg(target_os = "macos")]
        {
            let mut custom_flags: i32 = 0;
            if options.dsync {
                custom_flags |= libc::O_DSYNC;
            }
            open.custom_flags(custom_flags);
        }

        #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
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

        #[cfg(target_os = "macos")]
        if options.direct_io {
            use std::os::fd::AsRawFd;
            // SAFETY: `file` was just opened, so its fd is valid.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
        }

        let len = file.metadata()?.len();
        let queue_depth = NonZeroU32::new(options.queue_depth)
            .ok_or(Error::InvalidArgument("queue_depth must be > 0"))?;
        let io = IoWorker::new(queue_depth, file)?;

        let (disk_data, replay_map) = DiskData::mount(io, len).await?;
        let index = Index::from_replay(replay_map);

        Ok(Self { index, disk_data })
    }

    pub async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        let commit = self.disk_data.append_put(key.clone(), &value).await?;
        match self.index.apply_put(key, commit)? {
            ApplyPutOutcome::Inserted {
                displaced: Some(old),
            } => self.disk_data.release_value_space(old)?,
            ApplyPutOutcome::Inserted { displaced: None } => {}
            ApplyPutOutcome::Rejected { our_vref } => {
                // A concurrent put or remove with a later LSN beat us
                // to the index. Our WAL record still exists but will be
                // shadowed on replay; free its space so the in-memory
                // hole list stays consistent with what replay derives.
                self.disk_data.release_value_space(our_vref)?;
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: T4KeyRef<'_>) -> Result<Vec<u8>> {
        let value = self.index.get(key.as_bytes())?.ok_or(Error::NotFound)?;
        self.disk_data.read_value(value).await
    }

    pub async fn get_range(&self, key: T4KeyRef<'_>, range: RangeRequestU32) -> Result<Vec<u8>> {
        let value = self.index.get(key.as_bytes())?.ok_or(Error::NotFound)?;
        self.disk_data.read_value_range(value, range).await
    }

    pub async fn remove(&self, key: T4Key) -> Result<bool> {
        let commit = self.disk_data.append_tombstone(key.clone()).await?;
        match self.index.apply_remove(key, commit)? {
            ApplyRemoveOutcome::Removed { displaced } => {
                self.disk_data.release_value_space(displaced)?;
                Ok(true)
            }
            ApplyRemoveOutcome::MarkedAbsent => Ok(false),
            ApplyRemoveOutcome::Rejected => Ok(false),
        }
    }

    pub async fn sync(&self) -> Result<()> {
        self.disk_data.sync().await
    }

    /// Stop-the-world snapshot: build a fresh store at `path` containing
    /// only the current live entries. Concurrent puts/removes block until
    /// snapshot finishes; gets continue to work. `path` must not already
    /// exist (otherwise the freshly mounted store would replay stale data
    /// before we write into it).
    pub async fn snapshot(&self, path: impl AsRef<Path>, options: MountOptions) -> Result<()> {
        const PARALLELISM: usize = 8;

        let path = path.as_ref();
        if path.exists() {
            return Err(Error::InvalidArgument("compact target path already exists"));
        }
        let view = self.index.read_locked()?;
        let new_store = T4Store::mount_with_options(path, options).await?;

        let disk = &self.disk_data;
        let new_store_ref = &new_store;
        stream::iter(view.iter_live().map(|(k, v)| (k.clone(), v)))
            .map(|(key, vref)| async move {
                let value_bytes = disk.read_value(vref).await?;
                let value = T4Value::try_from_vec(value_bytes)?;
                new_store_ref.put(key, value).await
            })
            .buffer_unordered(PARALLELISM)
            .try_collect::<()>()
            .await?;

        new_store.sync().await?;
        Ok(())
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
    fn mounts_with_default_options() {
        // Default options enable direct I/O: O_DIRECT on Linux, F_NOCACHE on
        // macOS. This must work on every platform CI runs on.
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("default-options.t4");
            let store = T4Store::mount_with_options(&path, MountOptions::default())
                .await
                .unwrap();
            store
                .put(
                    T4Key::try_from_slice(b"k").unwrap(),
                    T4Value::try_from_vec(vec![b'v'; 100]).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .get(T4KeyRef::try_from_slice(b"k").unwrap())
                    .await
                    .unwrap(),
                vec![b'v'; 100]
            );
        });
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

    #[test]
    fn compact_yields_smaller_file_with_only_live_entries() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let src = dir.path().join("source.t4");
            let dst = dir.path().join("compacted.t4");

            let value_a = vec![b'a'; 4000];
            let value_b = vec![b'b'; 4000];
            let value_c_old = vec![b'c'; 4000];
            let value_c_new = vec![b'C'; 4000];

            {
                let store = T4Store::mount_with_options(&src, test_options())
                    .await
                    .unwrap();
                // Live entry that survives.
                store
                    .put(
                        T4Key::try_from_slice(b"a").unwrap(),
                        T4Value::try_from_vec(value_a.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                // Inserted then removed — should not appear in the compacted store.
                store
                    .put(
                        T4Key::try_from_slice(b"b").unwrap(),
                        T4Value::try_from_vec(value_b).unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(
                    store
                        .remove(T4Key::try_from_slice(b"b").unwrap())
                        .await
                        .unwrap()
                );
                // Overwritten — only the latest value should appear.
                store
                    .put(
                        T4Key::try_from_slice(b"c").unwrap(),
                        T4Value::try_from_vec(value_c_old).unwrap(),
                    )
                    .await
                    .unwrap();
                store
                    .put(
                        T4Key::try_from_slice(b"c").unwrap(),
                        T4Value::try_from_vec(value_c_new.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                store.sync().await.unwrap();

                let src_len = std::fs::metadata(&src).unwrap().len();
                store.snapshot(&dst, test_options()).await.unwrap();
                let dst_len = std::fs::metadata(&dst).unwrap().len();
                assert!(
                    dst_len < src_len,
                    "compacted file ({dst_len}) should be smaller than source ({src_len})"
                );
            }

            // Re-mount the compacted file and verify contents.
            let compacted = T4Store::mount_with_options(&dst, test_options())
                .await
                .unwrap();
            assert_eq!(compacted.len().unwrap(), 2);
            assert_eq!(
                compacted
                    .get(T4KeyRef::try_from_slice(b"a").unwrap())
                    .await
                    .unwrap(),
                value_a
            );
            assert_eq!(
                compacted
                    .get(T4KeyRef::try_from_slice(b"c").unwrap())
                    .await
                    .unwrap(),
                value_c_new
            );
            assert!(matches!(
                compacted.get(T4KeyRef::try_from_slice(b"b").unwrap()).await,
                Err(Error::NotFound)
            ));
        });
    }

    #[test]
    fn compact_handles_more_entries_than_parallelism() {
        // 32 live entries — well above the in-function PARALLELISM=8 cap —
        // exercises the refill-as-they-complete path.
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let src = dir.path().join("many.t4");
            let dst = dir.path().join("many-compacted.t4");

            let store = T4Store::mount_with_options(&src, test_options())
                .await
                .unwrap();
            let mut expected = Vec::new();
            for i in 0..32u32 {
                let key = format!("key-{i:04}");
                let value = vec![b'v'; 200 + (i as usize) * 10];
                store
                    .put(
                        T4Key::try_from_slice(key.as_bytes()).unwrap(),
                        T4Value::try_from_vec(value.clone()).unwrap(),
                    )
                    .await
                    .unwrap();
                expected.push((key, value));
            }
            store.sync().await.unwrap();
            store.snapshot(&dst, test_options()).await.unwrap();

            let compacted = T4Store::mount_with_options(&dst, test_options())
                .await
                .unwrap();
            assert_eq!(compacted.len().unwrap(), expected.len());
            for (key, value) in &expected {
                let got = compacted
                    .get(T4KeyRef::try_from_slice(key.as_bytes()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(&got, value);
            }
        });
    }

    #[test]
    fn compact_rejects_existing_target_path() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let src = dir.path().join("source.t4");
            let dst = dir.path().join("already-there.t4");
            std::fs::write(&dst, b"existing").unwrap();

            let store = T4Store::mount_with_options(&src, test_options())
                .await
                .unwrap();
            let err = store.snapshot(&dst, test_options()).await.unwrap_err();
            assert!(matches!(err, Error::InvalidArgument(_)));
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
