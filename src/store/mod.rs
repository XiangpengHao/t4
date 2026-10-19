use std::fs::OpenOptions;
use std::num::NonZeroU32;
use std::path::Path;

use futures_util::{StreamExt, TryStreamExt, stream};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::disk::{DiskData, FreedExtents};
use crate::index::Index;
use crate::input_kv::{T4Key, T4KeyRef, T4Value};
use crate::io::error::{Error, Result};
use crate::io::io_worker::IoWorker;
#[cfg(all(feature = "shuttle", test))]
use crate::io::simulated::SimDisk;
use crate::range::RangeRequestU32;

/// How a store opens its file. Writes always go to it with `O_DSYNC`
/// (`FILE_FLAG_WRITE_THROUGH` on Windows): every crash-safety rule of the
/// store needs a write durable once it completes.
#[derive(Debug, Clone, Copy)]
pub struct MountOptions {
    pub queue_depth: u32,
    pub direct_io: bool,
}

impl MountOptions {
    fn queue_depth(&self) -> Result<NonZeroU32> {
        NonZeroU32::new(self.queue_depth).ok_or(Error::InvalidArgument("queue_depth must be > 0"))
    }
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            queue_depth: 256,
            direct_io: true,
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
            let mut custom_flags = libc::O_DSYNC;
            if options.direct_io {
                custom_flags |= libc::O_DIRECT;
            }
            open.custom_flags(custom_flags);
        }

        // macOS has no O_DIRECT; page-cache bypass is requested per-fd via
        // F_NOCACHE after open (see below), which has no alignment requirements.
        #[cfg(target_os = "macos")]
        open.custom_flags(libc::O_DSYNC);

        #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
        {
            if options.direct_io {
                return Err(Error::InvalidArgument(
                    "direct_io not supported on target_os",
                ));
            }
            open.custom_flags(libc::O_DSYNC);
        }

        #[cfg(windows)]
        {
            if options.direct_io {
                return Err(Error::InvalidArgument(
                    "direct_io not supported on target_os",
                ));
            }
            // FILE_FLAG_WRITE_THROUGH — Windows analogue of O_DSYNC.
            open.custom_flags(0x8000_0000);
        }

        let path = path.as_ref();
        let file = open.open(path)?;
        #[cfg(unix)]
        Self::sync_parent_dir(path)?;

        #[cfg(target_os = "macos")]
        if options.direct_io {
            use std::os::fd::AsRawFd;
            // SAFETY: `file` was just opened, so its fd is valid.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
        }

        let len = file.metadata()?.len();
        let io = IoWorker::new(options.queue_depth()?, file)?;
        Self::mount_on(io, len).await
    }

    /// Makes the store file's name durable, which `O_DSYNC` writes to the
    /// file itself don't: a power cut soon after creating a store could
    /// otherwise lose the whole file. Windows has no fsync for a directory;
    /// NTFS journals directory changes.
    #[cfg(unix)]
    fn sync_parent_dir(path: &Path) -> Result<()> {
        // A bare file name's parent is the empty path: the working directory.
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// Mounts on a simulated disk, for shuttle tests.
    #[cfg(all(feature = "shuttle", test))]
    pub(crate) async fn mount_simulated(disk: &SimDisk, options: MountOptions) -> Result<Self> {
        let io = IoWorker::simulated(options.queue_depth()?, disk.clone())?;
        Self::mount_on(io, disk.len()).await
    }

    /// Mounts on the file behind `io`, `len` bytes long.
    async fn mount_on(io: IoWorker, len: u64) -> Result<Self> {
        let freed = FreedExtents::new();
        let index = Index::new();
        let disk_data = DiskData::mount(io, len, freed.clone(), &mut index.replay(&freed)).await?;

        Ok(Self { index, disk_data })
    }

    pub async fn put(&self, key: T4Key, value: T4Value) -> Result<()> {
        let commit = self.disk_data.append_put(key.clone(), &value).await?;
        self.index.apply_put(key, commit);
        Ok(())
    }

    pub async fn get(&self, key: T4KeyRef<'_>) -> Result<Vec<u8>> {
        let extent = self.index.get(key.as_bytes()).ok_or(Error::NotFound)?;
        self.disk_data.read_value(&extent).await
    }

    pub async fn get_range(&self, key: T4KeyRef<'_>, range: RangeRequestU32) -> Result<Vec<u8>> {
        let extent = self.index.get(key.as_bytes()).ok_or(Error::NotFound)?;
        self.disk_data.read_value_range(&extent, range).await
    }

    pub async fn remove(&self, key: T4Key) -> Result<bool> {
        let commit = self.disk_data.append_tombstone(key.clone()).await?;
        Ok(self.index.apply_remove(key, commit))
    }

    /// Stop-the-world snapshot: build a fresh store at `path` containing
    /// only the current live entries. Puts/removes not yet in the WAL
    /// block until snapshot finishes; ones already logged overlap it, and
    /// may or may not be in it. Gets continue to work. `path` must not
    /// already exist (otherwise the freshly mounted store would replay
    /// stale data before we write into it).
    pub async fn snapshot(&self, path: impl AsRef<Path>, options: MountOptions) -> Result<()> {
        const PARALLELISM: usize = 8;

        let path = path.as_ref();
        if path.exists() {
            return Err(Error::InvalidArgument("compact target path already exists"));
        }
        // Writers not yet in the WAL wait until the copy is done.
        let _paused = self.disk_data.pause_writes()?;
        let new_store = T4Store::mount_with_options(path, options).await?;

        let disk = &self.disk_data;
        let new_store_ref = &new_store;
        stream::iter(self.index.iter_live())
            .map(|(key, extent)| async move {
                let key = T4Key::try_from_vec(key.into_vec())?;
                let value_bytes = disk.read_value(&extent).await?;
                let value = T4Value::try_from_vec(value_bytes)?;
                new_store_ref.put(key, value).await
            })
            .buffer_unordered(PARALLELISM)
            .try_collect::<()>()
            .await?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_options() -> MountOptions {
        MountOptions {
            queue_depth: 8,
            direct_io: false,
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
            assert_eq!(compacted.len(), 2);
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
            store.snapshot(&dst, test_options()).await.unwrap();

            let compacted = T4Store::mount_with_options(&dst, test_options())
                .await
                .unwrap();
            assert_eq!(compacted.len(), expected.len());
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

    /// What a get holds while its read is in flight: overwriting the key
    /// meanwhile can't hand the extent to another put.
    #[test]
    fn held_extent_is_not_reused() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = T4Store::mount_with_options(dir.path().join("held.t4"), test_options())
                .await
                .unwrap();
            let put = |key: &'static [u8], byte: u8| {
                store.put(
                    T4Key::try_from_slice(key).unwrap(),
                    T4Value::try_from_vec(vec![byte; 16]).unwrap(),
                )
            };
            put(b"k", b'a').await.unwrap();

            let extent = store.index.get(b"k").unwrap();
            put(b"k", b'b').await.unwrap(); // releases the index's handle
            put(b"other", b'c').await.unwrap(); // same size: would take a's slot

            assert_eq!(
                store.disk_data.read_value(&extent).await.unwrap(),
                vec![b'a'; 16]
            );
        });
    }
}
