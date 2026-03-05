mod buffer;
mod error;
mod io_task;
mod io_worker;
mod store;
mod sync;
mod wal;

use std::{num::NonZeroU32, path::Path};

use crate::store::T4Store;
use crate::sync::Arc;

pub use error::{Error, Result};
pub use store::MountOptions;
use verified::input_kv::{T4Key, T4KeyRef, T4Value};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_U32: u32 = PAGE_SIZE as u32;
pub const PAGE_SIZE_NZ_U32: NonZeroU32 = match NonZeroU32::new(PAGE_SIZE_U32) {
    Some(value) => value,
    None => panic!("PAGE_SIZE must be non-zero"),
};
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<T4Store>,
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
                inner: Arc::new(T4Store::mount_with_options(path, options).await?),
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
            let key = T4Key::try_from_vec(key)?;
            let value = T4Value::try_from_vec(value)?;
            this.inner.put(key, value).await
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + 'a {
        let this = self.clone();
        async move {
            let key = T4KeyRef::try_from_slice(key)?;
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
            let key = T4KeyRef::try_from_slice(key)?;
            let range = verified::RangeRequestU32::from_u64(range_start, range_len)
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
            let key = T4Key::try_from_slice(key)?;
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
