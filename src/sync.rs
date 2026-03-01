#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::mpsc;
#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::mpsc;
#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
