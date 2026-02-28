#[cfg(feature = "shuttle")]
pub(crate) use shuttle::thread::spawn;

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::thread::spawn;
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;

#[cfg(feature = "shuttle")]
#[inline]
pub(crate) fn cooperative_yield() {
    shuttle::thread::yield_now();
}

#[cfg(not(feature = "shuttle"))]
#[inline]
pub(crate) fn cooperative_yield() {}
