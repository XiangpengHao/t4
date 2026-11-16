#[allow(unused)]
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;

#[allow(unused_imports)]
#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
pub(crate) use shuttle::thread::spawn;

#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
#[inline]
pub(crate) fn cooperative_yield() {
    shuttle::thread::yield_now();
}

#[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
#[inline]
pub(crate) fn cooperative_yield() {}

#[allow(unused_imports)]
#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
pub(crate) use shuttle::sync::*;

#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
#[allow(unused_imports)]
pub(crate) use shuttle::thread;

#[cfg(not(all(feature = "shuttle", any(test, fuzzing))))]
#[allow(unused_imports)]
pub(crate) use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, atomic};

#[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
#[allow(unused_imports)]
pub(crate) use std::thread;

#[cfg(not(all(feature = "shuttle", any(test, fuzzing))))]
pub(crate) mod mpsc {
    #[allow(unused_imports)]
    pub use crossbeam_channel::{Receiver, SendError, Sender, TryRecvError, unbounded as channel};
}
