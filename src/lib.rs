mod engine;
mod error;
mod format;
mod io;
mod io_task;
mod io_worker;
mod sync;
mod thread;
mod types;
mod wal;

use std::path::Path;

use crate::engine::Engine;
use crate::sync::Arc;

pub use engine::MountOptions;
pub use error::{Error, Result};
pub use types::{T4Key, T4KeyRef, T4Value};

#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Engine>,
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
        async move {
            Ok(Self {
                inner: Arc::new(Engine::mount_with_options(path, options).await?),
            })
        }
    }

    pub fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> impl std::future::Future<Output = Result<()>> {
        let this = self.clone();
        let key = key.into();
        let value = value.into();
        async move {
            let key = crate::types::T4Key::try_from(key)?;
            let value = crate::types::T4Value::try_from(value)?;
            this.inner.put(key, value).await
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + 'a {
        let this = self.clone();
        async move {
            let key = crate::types::T4KeyRef::try_from(key)?;
            this.inner.get(key).await
        }
    }

    pub fn get_range<'a>(
        &'a self,
        key: &'a [u8],
        range_start: u64,
        range_len: u64,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + 'a {
        let this = self.clone();
        async move {
            let key = crate::types::T4KeyRef::try_from(key)?;
            let range = crate::types::RangeRequest::from_u64(range_start, range_len)
                .ok_or(Error::RangeOutOfBounds)?;
            this.inner.get_range(key, range).await
        }
    }

    pub fn remove<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl std::future::Future<Output = Result<bool>> + 'a {
        let this = self.clone();
        async move {
            let key = crate::types::T4Key::try_from(key)?;
            this.inner.remove(key).await
        }
    }

    pub fn sync(&self) -> impl std::future::Future<Output = Result<()>> {
        let this = self.clone();
        async move { this.inner.sync().await }
    }

    pub fn len(&self) -> impl std::future::Future<Output = Result<usize>> {
        let this = self.clone();
        async move { this.inner.len() }
    }

    pub fn is_empty(&self) -> impl std::future::Future<Output = Result<bool>> {
        let this = self.clone();
        async move { this.inner.is_empty() }
    }
}
