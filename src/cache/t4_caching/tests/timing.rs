use super::*;
use crate::io::time::Clock;

/// Move time at explicit I/O boundaries, and record exactly what the policy hears.
#[derive(Clone)]
struct TimedReads {
    clock: Clock,
    events: Arc<Mutex<Vec<IoPhase>>>,
}
impl ReadPolicy for TimedReads {
    fn plan(&self, read: &LocatedRead<'_>) -> ReadPlan {
        FetchRuns::default().plan(read)
    }
    fn on_io(&self, event: &IoEvent<'_>) {
        self.events.lock().unwrap().push(event.phase());
        match event.phase() {
            IoPhase::Started => self.clock.advance(Duration::from_millis(7)),
            IoPhase::Answered { .. } => self.clock.advance(Duration::from_millis(11)),
            _ => {}
        }
    }
}

#[test]
fn network_timings_come_from_the_builders_owned_clock() {
    crate::io::test_schedule::TestSchedule::check(|| {
        let clock = Clock::default();
        let reads = TimedReads {
            clock: clock.clone(),
            events: Arc::default(),
        };
        let inner = Arc::new(CountingStore::default());
        let dir = tempfile::tempdir().unwrap();
        let store = builder(&inner, &dir)
            .with_test_clock(clock)
            .with_read_policy(reads.clone())
            .build()
            .unwrap();
        let path = Path::from("a");
        let data = file_bytes(64, 9);
        put(inner.as_ref(), &path, &data);
        assert_eq!(get_range(&store, &path, 0..64), data);
        let events = reads.events.lock().unwrap();
        assert!(
            matches!(events[1], IoPhase::Answered { after } if after == Duration::from_millis(7))
        );
        assert!(
            matches!(events[2], IoPhase::Finished { after, .. } if after == Duration::from_millis(18))
        );
        assert!(
            matches!(events[3], IoPhase::Landed { received, landing, waited, .. }
            if received == Duration::from_millis(18) && landing == Duration::ZERO && waited == Duration::ZERO)
        );
    });
}
