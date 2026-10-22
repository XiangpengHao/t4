mod buffer;
// Not wired into a front end yet (`doc/03-cache.md`). Linux and macOS:
// its memory pool is an anonymous mapping.
#[cfg(unix)]
#[allow(dead_code)]
mod cache;
mod disk;
mod index;
mod input_kv;
mod io;
mod range;
#[cfg(unix)]
mod shared;
mod store;
mod wal;

use std::{num::NonZeroU32, path::Path};

use vstd::prelude::*;

use crate::input_kv::{T4Key, T4KeyRef, T4Value};
use crate::io::sync::Arc;
use crate::range::RangeRequestU32;
#[cfg(unix)]
use crate::shared::SharedStore;
use crate::store::T4Store;

pub use input_kv::InputError;
pub use io::error::{Error, Result};
pub use store::{MountMode, MountOptions, ServerOptions};

/// `t4-server`'s entry point. Not part of the public API.
#[doc(hidden)]
pub mod __server {
    #[cfg(unix)]
    pub fn main() -> std::process::ExitCode {
        crate::shared::Server::main()
    }

    #[cfg(not(unix))]
    pub fn main() -> std::process::ExitCode {
        eprintln!("t4-server needs Linux or macOS");
        std::process::ExitCode::FAILURE
    }
}

#[cfg(feature = "__bench")]
#[doc(hidden)]
pub mod __bench {
    pub use crate::buffer::AlignedBuf;
    pub use crate::input_kv::T4Key;
    pub use crate::io::io_task::PageWrite;
    pub use crate::io::io_worker::IoWorker;
    pub use crate::wal::page::{AppendEntry, WalPage};
}

verus! {

pub const PAGE_SIZE: usize = 4096;

} // verus!

pub const PAGE_SIZE_U32: u32 = PAGE_SIZE as u32;
pub const PAGE_SIZE_NZ_U32: NonZeroU32 = match NonZeroU32::new(PAGE_SIZE_U32) {
    Some(value) => value,
    None => panic!("PAGE_SIZE must be non-zero"),
};
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

/// A mounted store. Clones share it.
#[derive(Clone, Debug)]
pub struct Store {
    owner: StoreOwner,
}

/// The process that runs the store's engine, as [`MountMode`] chose.
#[derive(Clone, Debug)]
enum StoreOwner {
    InProcess(Arc<T4Store>),
    #[cfg(unix)]
    Shared(Arc<SharedStore>),
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
            let owner = match options.mode.clone() {
                MountMode::InProcess => StoreOwner::InProcess(Arc::new(
                    T4Store::mount_with_options(path, options).await?,
                )),
                #[cfg(unix)]
                MountMode::Shared(server) => {
                    StoreOwner::Shared(Arc::new(SharedStore::mount(&path, options, server).await?))
                }
                #[cfg(not(unix))]
                MountMode::Shared(_) => {
                    return Err(Error::InvalidArgument("shared mode needs Linux or macOS"));
                }
            };
            Ok(Self { owner })
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
            match &this.owner {
                StoreOwner::InProcess(store) => store.put(key, value).await,
                #[cfg(unix)]
                StoreOwner::Shared(store) => store.put(key, value).await,
            }
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + 'a {
        let this = self.clone();
        async move {
            match &this.owner {
                StoreOwner::InProcess(store) => store.get(T4KeyRef::try_from_slice(key)?).await,
                #[cfg(unix)]
                StoreOwner::Shared(store) => store.get(T4Key::try_from_slice(key)?).await,
            }
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
            let range =
                RangeRequestU32::from_u64(range_start, range_len).ok_or(Error::RangeOutOfBounds)?;
            match &this.owner {
                StoreOwner::InProcess(store) => {
                    store.get_range(T4KeyRef::try_from_slice(key)?, range).await
                }
                #[cfg(unix)]
                StoreOwner::Shared(store) => {
                    store.get_range(T4Key::try_from_slice(key)?, range).await
                }
            }
        }
    }

    pub fn remove<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl std::future::Future<Output = Result<bool>> + 'a {
        let this = self.clone();
        async move {
            let key = T4Key::try_from_slice(key)?;
            match &this.owner {
                StoreOwner::InProcess(store) => store.remove(key).await,
                #[cfg(unix)]
                StoreOwner::Shared(store) => store.remove(key).await,
            }
        }
    }

    /// Stop-the-world snapshot. Creates a new store at `path` containing
    /// only the currently-live entries — no holes, WAL has only inserts.
    /// `path` must not already exist. Puts/removes on `self` that haven't
    /// reached the WAL block until snapshot finishes; ones already logged
    /// overlap it, and may or may not be in it.
    pub fn snapshot(
        &self,
        path: impl AsRef<Path>,
    ) -> impl std::future::Future<Output = Result<()>> {
        let this = self.clone();
        let path = path.as_ref().to_path_buf();
        async move {
            let options = MountOptions::default();
            // Created here in both modes: a server would resolve the path
            // against its own working directory, with its own permissions.
            let target = T4Store::create_snapshot_target(&path, options.direct_io)?;
            match &this.owner {
                StoreOwner::InProcess(store) => store.snapshot(target, &options).await,
                #[cfg(unix)]
                StoreOwner::Shared(store) => store.snapshot(target).await,
            }
        }
    }

    pub fn len(&self) -> impl std::future::Future<Output = Result<usize>> {
        let this = self.clone();
        async move {
            match &this.owner {
                StoreOwner::InProcess(store) => Ok(store.len()),
                #[cfg(unix)]
                StoreOwner::Shared(store) => store.len().await,
            }
        }
    }

    pub fn is_empty(&self) -> impl std::future::Future<Output = Result<bool>> {
        let this = self.clone();
        async move {
            match &this.owner {
                StoreOwner::InProcess(store) => Ok(store.is_empty()),
                #[cfg(unix)]
                StoreOwner::Shared(store) => Ok(store.len().await? == 0),
            }
        }
    }

    /// The process id of the server a shared store is connected to; `None`
    /// in-process. For tests; not part of the public API.
    #[doc(hidden)]
    pub fn __server_pid(&self) -> Option<u32> {
        match &self.owner {
            StoreOwner::InProcess(_) => None,
            #[cfg(unix)]
            StoreOwner::Shared(store) => Some(store.server_pid()),
        }
    }
}
