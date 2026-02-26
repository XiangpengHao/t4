pub mod engine;
pub mod error;
pub mod format;
pub mod future;
pub mod io;
pub mod uring;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use engine::{Engine, MountOptions, ValueRef};
pub use error::{Error, Result};

#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Mutex<Engine>>,
}

impl Store {
    pub fn mount(path: impl AsRef<Path>) -> impl std::future::Future<Output = Result<Self>> {
        let path = path.as_ref().to_path_buf();
        future::leaf(move || Self::mount_blocking(path))
    }

    pub fn mount_with_options(
        path: impl AsRef<Path>,
        options: MountOptions,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let path = path.as_ref().to_path_buf();
        future::leaf(move || Self::mount_blocking_with_options(path, options))
    }

    pub fn mount_blocking(path: impl AsRef<Path>) -> Result<Self> {
        Self::mount_blocking_with_options(path, MountOptions::default())
    }

    pub fn mount_blocking_with_options(
        path: impl AsRef<Path>,
        options: MountOptions,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Engine::mount_with_options(path, options)?)),
        })
    }

    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> {
        let inner = Arc::clone(&self.inner);
        future::leaf(move || {
            let mut engine = inner.lock().map_err(|_| Error::LockPoisoned)?;
            engine.put(&key, &value)
        })
    }

    pub fn get(&self, key: Vec<u8>) -> impl std::future::Future<Output = Result<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        future::leaf(move || {
            let mut engine = inner.lock().map_err(|_| Error::LockPoisoned)?;
            engine.get(&key)
        })
    }

    pub fn get_range(
        &self,
        key: Vec<u8>,
        range_start: u64,
        range_len: u64,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        future::leaf(move || {
            let mut engine = inner.lock().map_err(|_| Error::LockPoisoned)?;
            engine.get_range(&key, range_start, range_len)
        })
    }

    pub fn remove(&self, key: Vec<u8>) -> impl std::future::Future<Output = Result<bool>> {
        let inner = Arc::clone(&self.inner);
        future::leaf(move || {
            let mut engine = inner.lock().map_err(|_| Error::LockPoisoned)?;
            engine.remove(&key)
        })
    }

    pub fn sync(&self) -> impl std::future::Future<Output = Result<()>> {
        let inner = Arc::clone(&self.inner);
        future::leaf(move || {
            let mut engine = inner.lock().map_err(|_| Error::LockPoisoned)?;
            engine.sync()
        })
    }

    pub fn pathless_debug_snapshot_len(&self) -> Result<usize> {
        let engine = self.inner.lock().map_err(|_| Error::LockPoisoned)?;
        Ok(engine.len())
    }
}

#[allow(dead_code)]
fn _owned_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}
