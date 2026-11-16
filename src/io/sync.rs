#[allow(unused)]
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;

#[allow(unused_imports)]
#[cfg(all(test, feature = "shuttle"))]
pub(crate) use shuttle::thread::spawn;

#[cfg(all(test, feature = "shuttle"))]
#[inline]
pub(crate) fn cooperative_yield() {
    shuttle::thread::yield_now();
}

#[cfg(not(all(test, feature = "shuttle")))]
#[inline]
pub(crate) fn cooperative_yield() {}

#[allow(unused_imports)]
#[cfg(all(test, feature = "shuttle"))]
pub(crate) use shuttle::sync::*;

#[cfg(all(test, feature = "shuttle"))]
#[allow(unused_imports)]
pub(crate) use shuttle::thread;

#[cfg(not(all(feature = "shuttle", test)))]
#[allow(unused_imports)]
pub(crate) use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, atomic};

#[cfg(not(all(test, feature = "shuttle")))]
#[allow(unused_imports)]
pub(crate) use std::thread;

#[cfg(not(all(feature = "shuttle", test)))]
pub(crate) mod mpsc {
    #[allow(unused_imports)]
    pub use crossbeam_channel::{Receiver, SendError, Sender, TryRecvError, unbounded as channel};
}
