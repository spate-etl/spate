//! The split-lease headroom histogram, read from the exporter while a real
//! coordinator renews on a frozen clock.
//!
//! Lives in its own binary because the exporter installs a process-global
//! recorder.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use spate_coordination::{CoordinationEvent, SplitCoordinator, StoreCoordinator};
use spate_core::clock::tokio::Clock;
use spate_core::metrics::{
    ComponentLabels, CoordinationMetrics, Exporter, MetricsHandle, MetricsSettings, install,
};
use spate_test::metric_sum;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as Wall};
use support::{DEADLINE, Held, LEASE, PhasedPlanner, TestClock, config, runtime};
use tokio::time::Instant;

const HEADROOM: &str = "spate_coordination_split_lease_headroom_seconds";
const SPLIT_KEY: &str = "split.r0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Won,
    Lost,
    LandedError,
    ErrorUnwritten,
    Error,
}

/// A fault armed for the next split-lease update.
enum Fault {
    None,
    /// Advance the clock by `pre`, apply the write, then report an error.
    LandThenFail {
        pre: Duration,
    },
    /// Report an error without calling the inner store.
    FailUnwritten,
}

type Log = Arc<Mutex<Vec<(String, Instant, Outcome)>>>;

/// [`MemoryStore`] with one-shot faults on split-lease updates, logging each
/// such update's clock time at entry and its outcome.
#[derive(Clone)]
struct FaultyStore {
    inner: MemoryStore,
    clock: Arc<TestClock>,
    fault: Arc<Mutex<Fault>>,
    /// Absolute time the next split-lease update moves the clock to first.
    jump_to: Arc<Mutex<Option<Instant>>>,
    log: Log,
}

impl CoordinationStore for FaultyStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    fn op_timeout(&self) -> Option<Duration> {
        self.inner.op_timeout()
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        self.inner.attach_metrics(metrics);
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        if !(ks == Keyspace::Ephemeral && key.starts_with("split.")) {
            return self.inner.update(ks, key, value, expected).await;
        }
        if let Some(target) = self.jump_to.lock().unwrap().take() {
            self.clock.advance(target - self.clock.now());
        }
        let at = self.clock.now();
        let fault = std::mem::replace(&mut *self.fault.lock().unwrap(), Fault::None);
        let (outcome, result) = match fault {
            Fault::FailUnwritten => (
                Outcome::ErrorUnwritten,
                Err(StoreError::Retryable("renewal failed unwritten".into())),
            ),
            Fault::LandThenFail { pre } => {
                self.clock.advance(pre);
                let landed = self.inner.update(ks, key, value, expected).await;
                assert!(
                    matches!(landed, Ok(CasOutcome::Won(_))),
                    "the landing write lost: {landed:?}"
                );
                (
                    Outcome::LandedError,
                    Err(StoreError::Retryable("renewal landed, reply lost".into())),
                )
            }
            Fault::None => {
                let result = self.inner.update(ks, key, value, expected).await;
                let outcome = match &result {
                    Ok(CasOutcome::Won(_)) => Outcome::Won,
                    Ok(CasOutcome::Lost) => Outcome::Lost,
                    Err(_) => Outcome::Error,
                };
                (outcome, result)
            }
        };
        self.log
            .lock()
            .unwrap()
            .push((key.to_string(), at, outcome));
        result
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
}

/// One solo worker holding `r0` on a frozen clock. Fields drop in order, so
/// the runtime goes last.
struct Rig {
    worker: StoreCoordinator<FaultyStore>,
    fleet: support::Fleet,
    held: Held,
    store: FaultyStore,
    clock: Arc<TestClock>,
    metrics: MetricsHandle,
    component: &'static str,
    _rt: tokio::runtime::Runtime,
}

impl Rig {
    fn new(component: &'static str) -> Rig {
        let metrics = install(&MetricsSettings {
            exporter: Exporter::Prometheus,
            ..MetricsSettings::default()
        })
        .expect("install the exporter");
        let rt = runtime();
        let clock = TestClock::frozen();
        let inner = MemoryStore::with_clock(LEASE, clock.clone());
        let store = FaultyStore {
            inner: inner.clone(),
            clock: clock.clone(),
            fault: Arc::new(Mutex::new(Fault::None)),
            jump_to: Arc::default(),
            log: Arc::default(),
        };
        let labels = ComponentLabels::new("lease-headroom", component.to_string(), "memory");
        let mut worker = StoreCoordinator::with_clock(
            store.clone(),
            config(Some(component)),
            rt.handle().clone(),
            Some(CoordinationMetrics::new(&labels)),
            clock.clone() as Arc<dyn Clock>,
        )
        .expect("coordinator");
        worker
            .start(Box::new(PhasedPlanner::one_final("headroom:v1", &["r0"])))
            .unwrap();
        let mut fleet = support::Fleet::new(&inner, rt.handle());
        fleet.join(&worker);
        let mut held = Held::default();
        support::drive(&mut worker, &mut held, "claiming the split", |h| {
            h.splits.len() == 1
        });
        Rig {
            worker,
            fleet,
            held,
            store,
            clock,
            metrics,
            component,
            _rt: rt,
        }
    }

    /// Advances the clock by `LEASE / 12`, settles, and folds the worker's
    /// events; panics if the split is lost.
    fn step(&mut self) {
        self.fleet.step(&self.clock, LEASE / 12);
        for event in self.worker.poll().expect("poll") {
            assert!(
                !matches!(
                    event,
                    CoordinationEvent::Lost { .. } | CoordinationEvent::Quarantined { .. }
                ),
                "the split was lost: {event:?}; log {:?}",
                self.store.log.lock().unwrap()
            );
            self.held.fold(vec![event]);
        }
    }

    /// The logged `split.r0` updates from index `from` of the log on.
    fn r0_since(&self, from: usize) -> Vec<(Instant, Outcome)> {
        self.store.log.lock().unwrap()[from..]
            .iter()
            .filter(|(key, _, _)| key == SPLIT_KEY)
            .map(|(_, at, outcome)| (*at, *outcome))
            .collect()
    }

    fn log_len(&self) -> usize {
        self.store.log.lock().unwrap().len()
    }

    /// Steps until `done` holds for the `split.r0` updates logged from `from`.
    fn step_until(
        &mut self,
        what: &str,
        from: usize,
        mut done: impl FnMut(&[(Instant, Outcome)]) -> bool,
    ) -> Vec<(Instant, Outcome)> {
        let deadline = Wall::now() + DEADLINE;
        loop {
            let entries = self.r0_since(from);
            if done(&entries) {
                return entries;
            }
            assert!(Wall::now() < deadline, "{what} never happened");
            self.step();
        }
    }

    /// Steps until the next won renewal of `r0` and returns its instant.
    fn next_win(&mut self) -> Instant {
        let from = self.log_len();
        let entries = self.step_until("a renewal", from, |e| {
            e.iter().any(|(_, o)| *o == Outcome::Won)
        });
        entries.iter().find(|(_, o)| *o == Outcome::Won).unwrap().0
    }

    fn render(&self) -> String {
        self.metrics.render()
    }

    /// One sample of this rig's headroom series; panics when the line is
    /// absent.
    #[track_caller]
    fn read(&self, text: &str, suffix: &str, le: Option<&str>) -> f64 {
        let name = format!("{HEADROOM}{suffix}");
        let mut labels = vec![("component", self.component)];
        if let Some(le) = le {
            labels.push(("le", le));
        }
        metric_sum(text, &name, &labels).unwrap_or_else(|| {
            panic!(
                "no `{name}` line for component {} at {labels:?}:\n{text}",
                self.component
            )
        })
    }

    /// The change in `_count`, `_sum` and `_bucket{le}` from `before` to now.
    #[track_caller]
    fn delta(&self, before: &str, le: &str) -> (f64, f64, f64) {
        let after = self.render();
        (
            self.read(&after, "_count", None) - self.read(before, "_count", None),
            self.read(&after, "_sum", None) - self.read(before, "_sum", None),
            self.read(&after, "_bucket", Some(le)) - self.read(before, "_bucket", Some(le)),
        )
    }
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

/// Pins that each direct renewal records `LEASE` less the time since the
/// previous confirmed write on the worker's clock, in the configured buckets.
#[test]
fn a_direct_renewal_records_the_lease_left_at_confirmation() {
    let mut rig = Rig::new("direct");
    let w0 = rig.next_win();
    let before = rig.render();
    let mut wins = vec![w0];
    for _ in 0..3 {
        wins.push(rig.next_win());
    }

    let expected: Vec<f64> = wins
        .windows(2)
        .map(|w| secs(LEASE) - secs(w[1].duration_since(w[0])))
        .collect();
    for sample in &expected {
        assert!(
            *sample > 0.0 && *sample <= 1.0,
            "expected sample {sample} outside (0, 1.0]"
        );
    }
    let (count, sum, le_1) = rig.delta(&before, "1");
    assert_eq!(count, 3.0);
    let want: f64 = expected.iter().sum();
    assert!((sum - want).abs() < 1e-6, "sum {sum}, want {want}");
    assert_eq!(le_1, 3.0);
}

/// Pins that a renewal adopted after a lost reply records `LEASE` less the
/// time since the failed attempt, the instant adoption writes into
/// `last_ok_write` before the re-renewal confirms.
#[test]
fn an_adopted_renewal_records_headroom_from_the_lost_write() {
    let mut rig = Rig::new("adopted");
    let t_p = rig.next_win();
    let before = rig.render();
    let from = rig.log_len();
    *rig.store.fault.lock().unwrap() = Fault::LandThenFail {
        pre: Duration::ZERO,
    };
    let entries = rig.step_until("a win after the landed failure", from, |e| {
        e.iter().any(|(_, o)| *o == Outcome::Won)
    });

    let outcomes: Vec<Outcome> = entries.iter().map(|(_, o)| *o).collect();
    assert_eq!(
        outcomes,
        [Outcome::LandedError, Outcome::Lost, Outcome::Won]
    );
    let (t_a, t_c) = (entries[0].0, entries[2].0);
    assert!(
        t_a.duration_since(t_p) >= LEASE / 3,
        "t_a - t_p = {:?}",
        t_a.duration_since(t_p)
    );
    let (count, sum, _) = rig.delta(&before, "1");
    assert_eq!(count, 1.0);
    let want = secs(LEASE) - secs(t_c.duration_since(t_a));
    assert!((sum - want).abs() < 1e-6, "sum {sum}, want {want}");
}

/// Pins that a direct win after a renewal that failed with nothing written
/// records `LEASE` less the time since the last confirmed write.
#[test]
fn a_direct_renewal_after_an_unwritten_failure_measures_from_the_last_confirmed_write() {
    let mut rig = Rig::new("unwritten");
    let t_p = rig.next_win();
    let before = rig.render();
    let from = rig.log_len();
    *rig.store.fault.lock().unwrap() = Fault::FailUnwritten;
    let entries = rig.step_until("a win after the unwritten failure", from, |e| {
        e.iter().any(|(_, o)| *o == Outcome::Won)
    });

    let outcomes: Vec<Outcome> = entries.iter().map(|(_, o)| *o).collect();
    assert_eq!(outcomes, [Outcome::ErrorUnwritten, Outcome::Won]);
    let (t_f, t_w) = (entries[0].0, entries[1].0);
    assert!(
        t_f.duration_since(t_p) >= LEASE / 3,
        "t_f - t_p = {:?}",
        t_f.duration_since(t_p)
    );
    let (count, sum, _) = rig.delta(&before, "1");
    assert_eq!(count, 1.0);
    let want = secs(LEASE) - secs(t_w.duration_since(t_p));
    assert!((sum - want).abs() < 1e-6, "sum {sum}, want {want}");
}

/// Pins that a renewal confirmed more than a full lease after the write the
/// self-fence counts from records exactly 0 and keeps the split.
#[test]
fn a_renewal_confirmed_after_a_full_lease_records_zero_headroom() {
    let mut rig = Rig::new("zero");
    rig.next_win();
    let before = rig.render();
    let from = rig.log_len();
    *rig.store.fault.lock().unwrap() = Fault::LandThenFail {
        pre: Duration::from_millis(1),
    };
    let landed = rig.step_until("the landed failure", from, |e| {
        e.iter().any(|(_, o)| *o == Outcome::LandedError)
    });
    let t_a = landed
        .iter()
        .find(|(_, o)| *o == Outcome::LandedError)
        .unwrap()
        .0;
    *rig.store.jump_to.lock().unwrap() = Some(t_a + LEASE + Duration::from_micros(500));
    // Stepping stops at this win: a further beat can let the presence and
    // leader keys lapse after the jump.
    let entries = rig.step_until("a win after the landed failure", from, |e| {
        e.iter().any(|(_, o)| *o == Outcome::Won)
    });

    let outcomes: Vec<Outcome> = entries.iter().map(|(_, o)| *o).collect();
    assert_eq!(
        outcomes,
        [Outcome::LandedError, Outcome::Lost, Outcome::Won]
    );
    assert!(entries[2].0.duration_since(t_a) > LEASE);
    let (count, sum, le_min) = rig.delta(&before, "0.001");
    assert_eq!(count, 1.0);
    assert_eq!(sum, 0.0);
    assert_eq!(le_min, 1.0);
    assert_eq!(rig.held.splits.len(), 1, "the split was dropped");
}
