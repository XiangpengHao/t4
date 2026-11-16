//! The checked-in schedule corpus. Never draw scheduler seeds from entropy.
#[cfg(feature = "shuttle")]
use shuttle::scheduler::{RandomScheduler, ReplayScheduler};

pub(crate) struct TestSchedule;

impl TestSchedule {
    pub(crate) fn check(test: impl Fn() + Send + Sync + 'static) {
        #[cfg(feature = "shuttle")]
        Self::iterations(8, test);
        #[cfg(not(feature = "shuttle"))]
        test();
    }
    #[cfg(feature = "shuttle")]
    pub(crate) fn iterations(iterations: usize, test: impl Fn() + Send + Sync + 'static) {
        if let Ok(schedule) = std::env::var("T4_SHUTTLE_REPLAY") {
            shuttle::Runner::new(ReplayScheduler::new_from_encoded(&schedule), Self::config())
                .run(test);
            return;
        }
        let test = std::sync::Arc::new(test);
        for seed in [0x57bf_0283, 0x0dc8_2a31] {
            eprintln!("shuttle: random seed={seed:#x}, iterations={iterations}");

            let test = std::sync::Arc::clone(&test);
            shuttle::Runner::new(
                RandomScheduler::new_from_seed(seed, iterations),
                Self::config(),
            )
            .run(move || test());
        }
    }
    #[cfg(feature = "shuttle")]
    fn config() -> shuttle::Config {
        let mut config = shuttle::Config::new();
        config.stack_size = 512 * 1024;
        config
    }
}
