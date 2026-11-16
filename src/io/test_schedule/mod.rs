//! The checked-in schedule corpus. Never draw scheduler seeds from entropy.
#[cfg(feature = "shuttle")]
use shuttle::scheduler::{PctScheduler, ReplayScheduler};

pub(crate) struct TestSchedule;

impl TestSchedule {
    #[cfg(feature = "shuttle")]
    const SEEDS: [u64; 2] = [0x57bf_0283, 0x0dc8_2a31];
    /// PCT finds every bug of this many ordering constraints with a known
    /// probability per iteration; random scheduling gives no such bound.
    #[cfg(feature = "shuttle")]
    const PCT_DEPTH: usize = 3;

    pub(crate) fn check(test: impl Fn() + Send + Sync + 'static) {
        #[cfg(feature = "shuttle")]
        Self::iterations(16, test);
        #[cfg(not(feature = "shuttle"))]
        test();
    }

    /// A test with no thread of its own: one run, inside shuttle's runtime
    /// because its primitives need one. PCT refuses to repeat a run with no
    /// concurrency, and repeating it would explore nothing.
    pub(crate) fn once(test: impl Fn() + Send + Sync + 'static) {
        #[cfg(feature = "shuttle")]
        shuttle::Runner::new(
            PctScheduler::new_from_seed(Self::SEEDS[0], Self::PCT_DEPTH, 1),
            Self::config(),
        )
        .run(test);
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
        for seed in Self::SEEDS {
            eprintln!(
                "shuttle: pct seed={seed:#x}, depth={}, iterations={iterations}",
                Self::PCT_DEPTH
            );
            let test = std::sync::Arc::clone(&test);
            shuttle::Runner::new(
                PctScheduler::new_from_seed(seed, Self::PCT_DEPTH, iterations),
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
