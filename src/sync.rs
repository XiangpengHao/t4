#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::mpsc;
#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::mpsc;
#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "shuttle")]
pub(crate) use shuttle::thread::spawn;

#[allow(unused)]
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::thread::spawn;

#[cfg(feature = "shuttle")]
#[inline]
pub(crate) fn cooperative_yield() {
    shuttle::thread::yield_now();
}

#[cfg(not(feature = "shuttle"))]
#[inline]
pub(crate) fn cooperative_yield() {}
