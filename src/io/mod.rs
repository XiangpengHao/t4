mod common;
pub(crate) mod error;
pub(crate) mod io_task;
pub(crate) mod io_worker;
pub(crate) mod sync;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub(crate) mod io_uring;

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
pub(crate) mod generic;
