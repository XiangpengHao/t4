pub(crate) mod aligned_buf;
mod common;
pub(crate) mod error;
pub(crate) mod io_task;
pub(crate) mod io_worker;
pub(crate) mod sync;

cfg_if::cfg_if! {
    if #[cfg(all(feature = "shuttle", any(test, fuzzing)))] {
        pub(crate) mod simulated;
        pub(crate) use simulated::new_backend;
    } else if #[cfg(all(feature = "io-uring", target_os = "linux"))] {
        mod io_uring;
        pub(crate) use io_uring::new_backend;
    } else {
        mod blocking;
        mod generic;
        pub(crate) use generic::new_backend;
    }
}

#[cfg(any(test, all(fuzzing, feature = "shuttle")))]
pub(crate) mod test_schedule;

pub(crate) mod time;
