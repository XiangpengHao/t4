

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


#[cfg(all(feature = "shuttle", test))]
pub(crate) use shuttle::sync::*;

#[cfg(all(feature = "shuttle", test))]
pub(crate) use shuttle::thread;

#[cfg(not(all(feature = "shuttle", test)))]
pub(crate) use std::sync::*;

#[cfg(not(all(feature = "shuttle", test)))]
#[allow(unused_imports)]
pub(crate) use std::thread;