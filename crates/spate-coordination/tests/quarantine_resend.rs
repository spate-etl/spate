//! Failure reports that quarantine a split, over a store that loses a reply
//! or rejects a request.

mod support;

use spate_coordination::store::memory::MemoryStore;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use spate_coordination::{
    CoordinationError, CoordinationErrorKind, CoordinationEvent, SplitCoordinator, SplitProgress,
    StoreCoordinator,
};
use spate_core::clock::tokio::Clock;
use spate_core::metrics::{ComponentLabels, CoordinationMetrics, MetricsHandle};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::polled::PolledStore;
use support::tap::TapStore;
use support::{Fleet, Held, LEASE, PhasedPlanner, TestClock, config, runtime, split_id};

/// Reconcile first runs at a point in this interval drawn per coordinator, so it
/// re-reads a split record during a test only if that point falls inside the test.
const NO_RECONCILE: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// The update applies, then fails Retryable.
    UpdateReply,
    /// The update fails Retryable without applying.
    UpdateRejected,
}

/// A store that fails the next durable update of one key as armed, and can
/// answer the next durable read of a key from a stale entry.
#[derive(Clone)]
struct FaultStore {
    inner: TapStore<MemoryStore>,
    armed: Arc<Mutex<Option<(Fault, String)>>>,
    stale: Arc<Mutex<Option<Entry>>>,
}

impl FaultStore {
    fn new(inner: TapStore<MemoryStore>) -> FaultStore {
        FaultStore {
            inner,
            armed: Arc::default(),
            stale: Arc::default(),
        }
    }

    fn arm(&self, fault: Fault, key: &str) {
        *self.armed.lock().unwrap() = Some((fault, key.to_string()));
    }

    fn armed(&self) -> bool {
        self.armed.lock().unwrap().is_some()
    }

    fn take(&self, fault: Fault, ks: Keyspace, key: &str) -> bool {
        let mut armed = self.armed.lock().unwrap();
        let hit =
            ks == Keyspace::Durable && armed.as_ref().is_some_and(|(f, k)| *f == fault && k == key);
        if hit {
            *armed = None;
        }
        hit
    }
}

impl CoordinationStore for FaultStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }
    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
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
        if self.take(Fault::UpdateRejected, ks, key) {
            return Err(StoreError::Retryable(
                "fixture: update rejected before apply".into(),
            ));
        }
        let result = self.inner.update(ks, key, value, expected).await?;
        if matches!(result, CasOutcome::Won(_)) && self.take(Fault::UpdateReply, ks, key) {
            return Err(StoreError::Retryable(
                "fixture: applied update reply lost".into(),
            ));
        }
        Ok(result)
    }
    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete(ks, key, expected).await
    }
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        if ks == Keyspace::Durable {
            let mut stale = self.stale.lock().unwrap();
            if stale.as_ref().is_some_and(|e| e.key == key) {
                return Ok(stale.take());
            }
        }
        self.inner.get(ks, key).await
    }
    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        self.inner.list(ks, prefix).await
    }
    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.inner.watch(ks, prefix).await
    }
}

/// Installs the Prometheus exporter once per process and returns its handle.
fn exporter() -> MetricsHandle {
    spate_core::metrics::install(&spate_core::metrics::MetricsSettings {
        exporter: spate_core::metrics::Exporter::Prometheus,
        ..spate_core::metrics::MetricsSettings::default()
    })
    .expect("install the exporter")
}

/// The sum of `name` for `pipeline` and `labels`, or 0 when no series matches.
fn metric(handle: &MetricsHandle, pipeline: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let mut filter = vec![("pipeline", pipeline)];
    filter.extend_from_slice(labels);
    spate_test::metric_sum(&handle.render(), name, &filter).unwrap_or(0.0)
}

fn lost(events: &[CoordinationEvent], id: &str) -> bool {
    events
        .iter()
        .any(|e| matches!(e, CoordinationEvent::Lost { split } if split.as_str() == id))
}

fn quarantined(events: &[CoordinationEvent], id: &str) -> usize {
    events
        .iter()
        .filter(
            |e| matches!(e, CoordinationEvent::Quarantined { split, .. } if split.as_str() == id),
        )
        .count()
}

fn kind(result: &Result<(), CoordinationError>) -> Option<CoordinationErrorKind> {
    result.as_ref().err().map(|e| e.kind)
}

/// One metered worker on a frozen clock holding split `x`, settled.
struct Rig {
    pipeline: &'static str,
    metrics: MetricsHandle,
    rt: tokio::runtime::Runtime,
    clock: Arc<TestClock>,
    memory: MemoryStore,
    tap: TapStore<MemoryStore>,
    store: FaultStore,
    worker: StoreCoordinator<FaultStore>,
    fleet: Fleet,
    held: Held,
}

impl Rig {
    fn new(pipeline: &'static str, max_attempts: u32) -> Rig {
        let metrics = exporter();
        let rt = runtime();
        let clock = TestClock::frozen();
        let memory = MemoryStore::with_clock(LEASE, clock.clone());
        let tap = TapStore::new(memory.clone());
        let store = FaultStore::new(tap.clone());
        let mut cfg = config(Some("resend"));
        cfg.max_attempts = max_attempts;
        let labels = ComponentLabels::new(pipeline, "coordination", "s3");
        let mut worker = StoreCoordinator::with_clock(
            store.clone(),
            cfg,
            rt.handle().clone(),
            Some(CoordinationMetrics::new(&labels)),
            clock.clone() as Arc<dyn Clock>,
        )
        .unwrap();
        worker
            .start(Box::new(PhasedPlanner::one_final("resend:v1", &["x"])))
            .unwrap();
        let mut fleet = Fleet::new(&memory, rt.handle());
        fleet.join(&worker);
        let mut held = Held::default();
        support::drive(&mut worker, &mut held, "initial acquisition", |h| {
            h.splits.contains_key("x")
        });
        fleet.settle(&clock);
        Rig {
            pipeline,
            metrics,
            rt,
            clock,
            memory,
            tap,
            store,
            worker,
            fleet,
            held,
        }
    }

    /// Stops the worker's view from seeing writes to the split record.
    fn hide(&self) {
        self.tap
            .hide(|ks, key| ks == Keyspace::Durable && key == "split.x");
    }

    fn fail(&mut self) -> Result<(), CoordinationError> {
        self.worker.fail(&split_id("x"), "poison")
    }

    /// Sends a report whose write applies and whose reply is lost.
    fn fail_applied_reply_lost(&mut self) {
        self.store.arm(Fault::UpdateReply, "split.x");
        let first = self.fail();
        assert_eq!(kind(&first), Some(CoordinationErrorKind::Retryable));
        assert!(!self.store.armed(), "the report took the fault");
    }

    /// Sends a report whose request fails before it applies.
    fn fail_rejected(&mut self) {
        self.store.arm(Fault::UpdateRejected, "split.x");
        let sent = self.fail();
        assert_eq!(kind(&sent), Some(CoordinationErrorKind::Retryable));
        assert!(!self.store.armed(), "the report took the fault");
    }

    fn entry(&self) -> Entry {
        self.rt
            .block_on(self.memory.get(Keyspace::Durable, "split.x"))
            .unwrap()
            .expect("the split record")
    }

    fn record(&self) -> serde_json::Value {
        serde_json::from_slice(&self.entry().value).unwrap()
    }

    fn lease(&self) -> bool {
        self.rt
            .block_on(self.memory.get(Keyspace::Ephemeral, "split.x"))
            .unwrap()
            .is_some()
    }

    fn events(&mut self) -> Vec<CoordinationEvent> {
        self.worker.poll().unwrap()
    }

    fn metric(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        metric(&self.metrics, self.pipeline, name, labels)
    }

    fn fenced(&self) -> f64 {
        self.metric(
            "spate_coordination_split_losses_total",
            &[("reason", "fenced")],
        )
    }

    fn quarantines(&self) -> f64 {
        self.metric("spate_coordination_quarantines_total", &[])
    }
}

/// A quarantining report applied with its reply lost, then sent again with
/// the first write either hidden from the view or folded into it.
fn resent(pipeline: &'static str, folded: bool) {
    let mut rig = Rig::new(pipeline, 1);
    if !folded {
        rig.hide();
    }
    rig.fail_applied_reply_lost();
    assert_eq!(rig.record()["status"], "quarantined");
    if folded {
        rig.fleet.settle(&rig.clock);
    }
    let second = rig.fail();
    let after = rig.record();
    let events = rig.events();
    let lost = lost(&events, "x");
    let lease = rig.lease();
    assert!(
        second.is_ok()
            && after["attempts"] == 1
            && after["status"] == "quarantined"
            && !lost
            && !lease,
        "second={second:?}, record={after}, lost={lost}, lease={lease}"
    );
    assert_eq!(
        (
            rig.quarantines(),
            rig.fenced(),
            rig.metric("spate_coordination_split_failures_total", &[]),
        ),
        (1.0, 0.0, 2.0),
        "quarantines, fenced losses, failure reports"
    );
}

/// A re-sent quarantining report whose first send applied unseen returns `Ok`,
/// emits no `Lost` and deletes the lease.
/// Regression for #941.
#[test]
fn quarantining_report_resent_unfolded() {
    resent("quarantining_report_resent_unfolded", false);
}

/// A re-sent quarantining report whose first send the view already folded
/// returns `Ok`, emits no `Lost` and deletes the lease.
/// Regression for #941.
#[test]
fn quarantining_report_resent_folded() {
    resent("quarantining_report_resent_folded", true);
}

/// A folded quarantining report whose reply was lost leaves the lease for the
/// next heartbeat to delete, with no `Lost`.
/// Regression for #941.
#[test]
fn folded_quarantining_report_owes_the_lease_to_the_next_heartbeat() {
    let mut rig = Rig::new(
        "folded_quarantining_report_owes_the_lease_to_the_next_heartbeat",
        1,
    );
    rig.fail_applied_reply_lost();
    rig.fleet.settle(&rig.clock);
    let mut events = rig.events();
    assert!(rig.lease(), "the lease outlives the fold");
    let mut steps = 0;
    while rig.lease() && steps < 7 {
        rig.fleet.step(&rig.clock, LEASE / 12);
        events.extend(rig.events());
        steps += 1;
    }
    let lease = rig.lease();
    assert!(
        !lease && !lost(&events, "x") && quarantined(&events, "x") == 1,
        "after {steps} steps: lease={lease}, events={events:?}"
    );
    assert_eq!((rig.quarantines(), rig.fenced()), (1.0, 0.0));
}

/// Writes a peer's quarantine of `x` over the stored record, as
/// `try_quarantine` writes it for a reclaim, stamped a minute ahead.
fn peer_quarantine(rig: &Rig) {
    let entry = rig.entry();
    let before: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    let mut peer = before.clone();
    peer["epoch"] = (before["epoch"].as_u64().unwrap() + 1).into();
    peer["status"] = "quarantined".into();
    peer["owner"] = serde_json::Value::Null;
    peer["attempts"] = (before["attempts"].as_u64().unwrap() + 1).into();
    let ahead = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        + Duration::from_secs(60);
    peer["written_at_ms"] = i64::try_from(ahead.as_millis()).unwrap().into();
    assert!(
        before["status"] == "runnable"
            && before["owner"] == "resend"
            && peer["epoch"].as_u64() == Some(before["epoch"].as_u64().unwrap() + 1)
            && peer["status"] == "quarantined"
            && peer["owner"].is_null()
            && peer["attempts"] == 1,
        "before={before}, peer={peer}"
    );
    let written = rig
        .rt
        .block_on(rig.memory.update(
            Keyspace::Durable,
            "split.x",
            serde_json::to_vec(&peer).unwrap(),
            entry.revision,
        ))
        .unwrap();
    assert!(matches!(written, CasOutcome::Won(_)));
}

/// A peer's quarantine read back after this worker's quarantining report
/// failed before applying is `Fenced` with `Lost`.
#[test]
fn a_peer_quarantine_after_an_unapplied_quarantining_report_is_still_lost() {
    let mut rig = Rig::new(
        "a_peer_quarantine_after_an_unapplied_quarantining_report_is_still_lost",
        1,
    );
    rig.hide();
    rig.fail_rejected();
    peer_quarantine(&rig);
    let resent = rig.fail();
    let events = rig.events();
    assert!(
        kind(&resent) == Some(CoordinationErrorKind::Fenced) && lost(&events, "x"),
        "resent={resent:?}, events={events:?}"
    );
    assert_eq!(rig.fenced(), 1.0);
}

/// A peer's quarantine folded after this worker's quarantining report failed
/// before applying emits `Lost`, and a re-send is `Fenced`.
#[test]
fn a_peer_quarantine_after_an_unapplied_quarantining_report_is_still_lost_folded() {
    let mut rig = Rig::new(
        "a_peer_quarantine_after_an_unapplied_quarantining_report_is_still_lost_folded",
        1,
    );
    rig.fail_rejected();
    peer_quarantine(&rig);
    rig.fleet.settle(&rig.clock);
    let events = rig.events();
    let resent = rig.fail();
    assert!(
        kind(&resent) == Some(CoordinationErrorKind::Fenced) && lost(&events, "x"),
        "resent={resent:?}, events={events:?}"
    );
    assert_eq!(rig.fenced(), 1.0);
}

/// A commit that reads back this worker's own applied quarantining report is
/// `Fenced` with `Lost`.
#[test]
fn a_commit_after_a_lost_quarantining_report_is_fenced_with_lost() {
    let mut rig = Rig::new(
        "a_commit_after_a_lost_quarantining_report_is_fenced_with_lost",
        1,
    );
    rig.hide();
    rig.fail_applied_reply_lost();
    let committed = rig
        .worker
        .commit(&split_id("x"), &SplitProgress::new(1, vec![]));
    let events = rig.events();
    assert!(
        kind(&committed) == Some(CoordinationErrorKind::Fenced) && lost(&events, "x"),
        "commit={committed:?}, events={events:?}"
    );
}

/// A quarantining report whose read-back lags is `Fenced` with `Lost`, and
/// forgets every record it sent, so a later re-send is `Fenced` too.
#[test]
fn a_quarantining_report_whose_read_back_lags_is_fenced_and_forgets_every_record() {
    let mut rig = Rig::new(
        "a_quarantining_report_whose_read_back_lags_is_fenced_and_forgets_every_record",
        1,
    );
    rig.hide();
    let e0 = rig.entry();
    rig.fail_applied_reply_lost();
    let e1 = rig.entry();
    std::thread::sleep(Duration::from_millis(2));
    rig.fail_rejected();

    *rig.store.stale.lock().unwrap() = Some(e0);
    let third = rig.fail();
    let events = rig.events();
    let record = rig.record();
    let lease = rig.lease();
    assert!(
        kind(&third) == Some(CoordinationErrorKind::Fenced)
            && lost(&events, "x")
            && lease
            && record["status"] == "quarantined"
            && record["attempts"] == 1,
        "third={third:?}, events={events:?}, lease={lease}, record={record}"
    );

    rig.tap.inject(Keyspace::Durable, WatchEvent::Put(e1));
    support::drive(
        &mut rig.worker,
        &mut rig.held,
        "folding the applied report",
        |h| h.quarantined.iter().any(|(id, _)| id == "x"),
    );
    let fourth = rig.fail();
    assert_eq!(
        kind(&fourth),
        Some(CoordinationErrorKind::Fenced),
        "fourth={fourth:?}"
    );
}

/// A re-sent quarantining report of a tenancy that began after earlier failure
/// reports returns `Ok` with no `Lost` and deletes the lease.
/// Regression for #941.
#[test]
fn quarantining_report_resent_after_earlier_failures() {
    let mut rig = Rig::new("quarantining_report_resent_after_earlier_failures", 3);
    for _ in 0..2 {
        let epoch = rig.held.splits["x"].0;
        rig.fail().expect("a report under the cap");
        let clock = rig.clock.clone();
        support::drive_clocked(
            &mut rig.worker,
            &clock,
            &mut rig.held,
            "re-claiming x",
            |h| h.splits.get("x").is_some_and(|(e, _)| *e > epoch),
        );
    }
    rig.fleet.settle(&rig.clock);
    assert_eq!(rig.record()["attempts"], 2);
    rig.hide();
    rig.fail_applied_reply_lost();
    let second = rig.fail();
    let after = rig.record();
    let events = rig.events();
    let lease = rig.lease();
    assert!(
        second.is_ok()
            && after["attempts"] == 3
            && after["status"] == "quarantined"
            && !lost(&events, "x")
            && !lease,
        "second={second:?}, record={after}, events={events:?}, lease={lease}"
    );
}

/// A re-sent quarantining report of a split under revocation, on a polled
/// store, settles the revocation `forced` with no loss and deletes the lease.
/// Regression for #941.
#[test]
fn quarantining_report_resent_during_a_revocation_settles_it_forced() {
    let pipeline = "quarantining_report_resent_during_a_revocation_settles_it_forced";
    let metrics = exporter();
    let rt = runtime();
    let memory = MemoryStore::new(LEASE);
    let fault = FaultStore::new(TapStore::new(memory.clone()));
    let ids = ["f0", "f1", "f2", "f3"];
    let mut a_config = config(Some("worker-a"));
    a_config.max_attempts = 1;
    a_config.reconcile_interval = NO_RECONCILE;
    a_config.drain_deadline = LEASE * 20;
    let labels = ComponentLabels::new(pipeline, "coordination", "s3");
    let mut a = StoreCoordinator::new(
        PolledStore::new(fault.clone(), LEASE / 10),
        a_config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&labels)),
    )
    .unwrap();
    a.start(Box::new(PhasedPlanner::one_final("resend:v1", &ids)))
        .unwrap();
    let mut held_a = Held::default();
    support::drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });

    let mut b = StoreCoordinator::new(
        PolledStore::new(fault.clone(), LEASE / 10),
        config(Some("worker-b")),
        rt.handle().clone(),
        None,
    )
    .unwrap();
    b.start(Box::new(PhasedPlanner::one_final("resend:v1", &ids)))
        .unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + support::DEADLINE;
    let asked = loop {
        assert!(Instant::now() < deadline, "no revocation was requested");
        let mut asked = None;
        for event in a.poll().unwrap() {
            if let CoordinationEvent::RevokeRequested { split } = &event {
                asked = Some(split.clone());
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(split) = asked {
            break split;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    };

    let key = format!("split.{}", asked.as_str());
    fault.arm(Fault::UpdateReply, &key);
    let first = a.fail(&asked, "poison");
    assert_eq!(kind(&first), Some(CoordinationErrorKind::Retryable));
    assert!(!fault.armed(), "the report took the fault");
    let second = a.fail(&asked, "poison");
    let events = a.poll().unwrap();
    let lost = lost(&events, asked.as_str());
    let lease = rt
        .block_on(memory.get(Keyspace::Ephemeral, &key))
        .unwrap()
        .is_some();
    assert!(
        second.is_ok() && !lost && !lease,
        "second={second:?}, lost={lost}, lease={lease}"
    );
    let count = |name, labels: &[(&str, &str)]| metric(&metrics, pipeline, name, labels);
    assert_eq!(
        (
            count(
                "spate_coordination_revocations_total",
                &[("outcome", "forced")]
            ),
            count(
                "spate_coordination_split_losses_total",
                &[("reason", "fenced")]
            ),
            count("spate_coordination_quarantines_total", &[]),
        ),
        (1.0, 0.0, 1.0),
        "forced revocations, fenced losses, quarantines"
    );
}

/// A quarantining report applied with its reply lost, then sent once more and
/// rejected before applying, is recognised by the next send's read-back.
/// Regression for #941.
#[test]
fn quarantining_report_resent_after_two_ambiguous_sends() {
    let mut rig = Rig::new("quarantining_report_resent_after_two_ambiguous_sends", 1);
    rig.hide();
    rig.fail_applied_reply_lost();
    std::thread::sleep(Duration::from_millis(2));
    rig.fail_rejected();
    let third = rig.fail();
    let after = rig.record();
    let events = rig.events();
    let lease = rig.lease();
    assert!(
        third.is_ok()
            && after["attempts"] == 1
            && after["status"] == "quarantined"
            && !lost(&events, "x")
            && !lease,
        "third={third:?}, record={after}, events={events:?}, lease={lease}"
    );
    assert_eq!(rig.fenced(), 0.0);
}
