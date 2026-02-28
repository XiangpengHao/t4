#[cfg(feature = "shuttle")]
pub(crate) use shuttle::thread::spawn;
#[cfg(all(feature = "shuttle", test))]
pub(crate) use shuttle::thread::JoinHandle;

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::thread::spawn;
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;
