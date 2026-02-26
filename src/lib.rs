mod engine;
mod error;
mod format;
mod future;
mod io;
mod uring;

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::engine::Engine;

pub use engine::MountOptions;
pub use error::{Error, Result};

#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Mutex<Engine>>,
}

pub fn mount(path: impl AsRef<Path>) -> impl std::future::Future<Output = Result<Store>> {
    Store::mount(path)
}

pub fn mount_with_options(
    path: impl AsRef<Path>,
    options: MountOptions,
) -> impl std::future::Future<Output = Result<Store>> {
    Store::mount_with_options(path, options)
}

impl Store {
    fn mount(path: impl AsRef<Path>) -> impl std::future::Future<Output = Result<Self>> {
        Self::mount_with_options(path, MountOptions::default())
    }

    fn mount_with_options(
        path: impl AsRef<Path>,
        options: MountOptions,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let path = path.as_ref().to_path_buf();
        future::leaf(move || {
            Ok(Self {
                inner: Arc::new(Mutex::new(Engine::mount_with_options(path, options)?)),
            })
        })
    }

    fn lock_engine(&self) -> Result<MutexGuard<'_, Engine>> {
        self.inner.lock().map_err(|_| Error::LockPoisoned)
    }

    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> {
        let this = self.clone();
        future::leaf(move || {
            let mut engine = this.lock_engine()?;
            engine.put(&key, &value)
        })
    }

    pub fn get(&self, key: Vec<u8>) -> impl std::future::Future<Output = Result<Vec<u8>>> {
        let this = self.clone();
        future::leaf(move || {
            let mut engine = this.lock_engine()?;
            engine.get(&key)
        })
    }

    pub fn get_range(
        &self,
        key: Vec<u8>,
        range_start: u64,
        range_len: u64,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> {
        let this = self.clone();
        future::leaf(move || {
            let mut engine = this.lock_engine()?;
            engine.get_range(&key, range_start, range_len)
        })
    }

    pub fn remove(&self, key: Vec<u8>) -> impl std::future::Future<Output = Result<bool>> {
        let this = self.clone();
        future::leaf(move || {
            let mut engine = this.lock_engine()?;
            engine.remove(&key)
        })
    }

    pub fn sync(&self) -> impl std::future::Future<Output = Result<()>> {
        let this = self.clone();
        future::leaf(move || {
            let mut engine = this.lock_engine()?;
            engine.sync()
        })
    }

    pub fn len(&self) -> impl std::future::Future<Output = Result<usize>> {
        let this = self.clone();
        future::leaf(move || {
            let engine = this.lock_engine()?;
            Ok(engine.len())
        })
    }

    pub fn is_empty(&self) -> impl std::future::Future<Output = Result<bool>> {
        let this = self.clone();
        future::leaf(move || {
            let engine = this.lock_engine()?;
            Ok(engine.is_empty())
        })
    }
}
