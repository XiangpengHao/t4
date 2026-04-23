mod common;
pub(crate) mod error;
pub(crate) mod io_task;
pub(crate) mod io_worker;
pub(crate) mod sync;

#[cfg(feature = "io-uring")]
#[cfg(target_os = "linux")]
pub(crate) mod io_uring;

#[cfg(not(feature = "io-uring"))]
pub(crate) mod generic;
