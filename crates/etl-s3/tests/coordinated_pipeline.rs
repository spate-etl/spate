//! Two full pipelines sharing one coordinated backfill over an
//! in-process store — the issue's acceptance shape, infrastructure-free.
//!
//! Both instances run concurrently against the same prefix and the same
//! coordination store; the collective result must cover the input exactly
//! once (union), with the leader's single LIST as the fleet's only
//! enumeration.

mod support;

use etl_coordination::CoordinationConfig;
use etl_core::pipeline::ExitState;
use etl_test::{SinkScript, WriteOutcome, wait_until};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use support::spy::counting_local_store;
use support::{
    Launched, captured_rows, launch, launch_customized, launch_tuned, line_framer, lines_bytes,
    recs, shared_store, sorted, test_options, test_tuning,
};

fn config_yaml(data: &std::path::Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-coordinated-test, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

/// [`config_yaml`] with a small in-flight budget, so a paced sink (delay per
/// write) actually throttles the owner rather than letting many batches
/// overlap their delays — the handoff test needs the owner to hold its splits
/// open across several rebalance rounds.
fn throttled_config_yaml(data: &std::path::Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-coordinated-test, threads: 2 }}
checkpoint: {{ interval: 100ms }}
backpressure: {{ max_inflight_bytes: 128KiB }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

/// Launch one coordinated instance with a LIST-counting data store.
fn launch_instance(
    yaml: &str,
    store: &etl_coordination::store::memory::MemoryStore,
    instance: &str,
    pre: impl FnOnce(&SinkScript),
) -> (Launched, Arc<AtomicUsize>) {
    let (spy, lists) = counting_local_store();
    let store = store.clone();
    let tuning = CoordinationConfig {
        instance_id: Some(instance.to_string()),
        max_in_flight: 2,
        ..test_tuning()
    };
    let launched = launch_customized(yaml, test_options(), pre, move |source, io| {
        let coordinator = etl_coordination::StoreCoordinator::new(store, tuning, io, None)
            .expect("coordinator builds");
        line_framer(source)
            .with_coordinator(Box::new(coordinator))
            .with_store(spy)
    });
    (launched, lists)
}

#[test]
fn two_instances_complete_collectively_and_only_the_leader_lists() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    // 96 small objects → six splits at the 16-member cap: enough work for
    // two instances (max_in_flight: 2 each) to genuinely share.
    let mut expected: Vec<String> = Vec::new();
    for i in 0..96 {
        let lines = recs(&format!("o{i:02}"), 20);
        fs::write(data.join(format!("obj-{i:02}.ndjson")), lines_bytes(&lines)).unwrap();
        expected.extend(lines);
    }
    let store = shared_store();
    let yaml = config_yaml(&data);

    // Pace both sinks slightly so neither instance can inhale the whole
    // job before its peer has claimed anything.
    let pace = |script: &SinkScript| {
        for _ in 0..10 {
            script.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(100)));
        }
    };
    let (a, lists_a) = launch_instance(&yaml, &store, "instance-a", pace);
    let (b, lists_b) = launch_instance(&yaml, &store, "instance-b", pace);

    let ra = a.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    let rb = b.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    assert_eq!(ra.state, ExitState::Completed, "instance-a completes");
    assert_eq!(rb.state, ExitState::Completed, "instance-b completes");

    let rows_a = captured_rows(&a.script);
    let rows_b = captured_rows(&b.script);
    // At-least-once: the union covers the input exactly; overlap is fine.
    let mut union: Vec<String> = rows_a.iter().chain(rows_b.iter()).cloned().collect();
    union.sort();
    union.dedup();
    assert_eq!(union, sorted(expected), "the union must cover every record");
    assert!(
        !rows_a.is_empty() && !rows_b.is_empty(),
        "both instances share the work (a={}, b={})",
        rows_a.len(),
        rows_b.len()
    );

    // The acceptance criterion with teeth: one LIST for the whole fleet —
    // the elected leader's plan. Workers read descriptors, never listings.
    assert_eq!(
        lists_a.load(Ordering::Relaxed) + lists_b.load(Ordering::Relaxed),
        1,
        "exactly one LIST fleet-wide (a={}, b={})",
        lists_a.load(Ordering::Relaxed),
        lists_b.load(Ordering::Relaxed)
    );
}

#[test]
fn empty_and_recordless_objects_complete_via_the_sweep() {
    // Splits whose members frame zero records never produce a
    // watermark-carrying commit — only the completion sweep can finish
    // them. Zero-byte objects and whitespace-only objects both take that
    // path; the job must still self-terminate cleanly.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    for i in 0..3 {
        fs::write(data.join(format!("empty-{i}.ndjson")), b"").unwrap();
    }
    fs::write(data.join("blank.ndjson"), b"\n   \n\n").unwrap();

    let l = launch(&config_yaml(&data), test_options());
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(
        report.state,
        ExitState::Completed,
        "recordless splits complete via the sweep"
    );
    assert!(captured_rows(&l.script).is_empty());
}

#[test]
fn a_standby_instance_drains_when_the_job_completes() {
    // A third instance joining a nearly-done job may hold zero splits; it
    // must keep polling and still observe completion (the standby
    // contract) instead of hanging or exiting early.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    for (i, n) in [(0, 5), (1, 4)] {
        fs::write(
            data.join(format!("obj-{i}.ndjson")),
            lines_bytes(&recs(&format!("o{i}"), n)),
        )
        .unwrap();
    }
    let store = shared_store();
    let yaml = config_yaml(&data);

    let (worker, _) = launch_instance(&yaml, &store, "worker", |_| {});
    let rw = worker
        .run
        .wait_exit(Duration::from_secs(60))
        .unwrap()
        .unwrap();
    assert_eq!(rw.state, ExitState::Completed);

    // Joins after everything is complete: zero splits, must still drain.
    let (standby, standby_lists) = launch_instance(&yaml, &store, "standby", |_| {});
    let rs = standby
        .run
        .wait_exit(Duration::from_secs(60))
        .unwrap()
        .unwrap();
    assert_eq!(
        rs.state,
        ExitState::Completed,
        "standby observes AllComplete"
    );
    assert!(captured_rows(&standby.script).is_empty());
    assert_eq!(
        standby_lists.load(Ordering::Relaxed),
        0,
        "a standby never lists"
    );
}

/// Launch a coordinated instance with a chosen working-set bound, so one
/// instance can over-claim (hold every split) before its peer joins.
fn launch_handoff_instance(
    yaml: &str,
    store: &etl_coordination::store::memory::MemoryStore,
    instance: &str,
    max_in_flight: u32,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let tuning = CoordinationConfig {
        instance_id: Some(instance.to_string()),
        max_in_flight,
        // A paced drain takes several heartbeat rounds; the default
        // handoff_rounds would fall back to a mid-drain steal and
        // reintroduce exactly the duplicates this test refutes. The
        // fallback path is covered by the coordination-level tests.
        handoff_rounds: 1000,
        // Pinned rather than defaulted: the zero-duplicate claim below has
        // to hold while several drains overlap, which is the interesting
        // case. If the default ever moves, this test must keep testing it.
        handoff_max_grants: 2,
        ..test_tuning()
    };
    launch_tuned(yaml, test_options(), store, tuning, pre)
}

#[test]
fn a_cooperative_handoff_moves_splits_with_zero_duplicates() {
    // The teeth of issue #58. When a *live* owner gives up a split, the
    // cooperative handoff drains its intake at an object boundary, commits the
    // tail, and only then releases — so the peer resumes covering everything
    // the owner emitted, replay-free. The steal-era CAS transfer this replaces
    // moved ownership *before* the owner stopped reading, re-reading
    // `[committed watermark, fence]` and producing nonzero duplicates on every
    // move. The assertion that separates the two is `total == union`: exactly
    // zero duplicates. This test is written to PASS with handoffs active and
    // FAIL (duplicates > 0, or no movement) if transfers regress to steals.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    // 96 objects → six splits at the 16-member cap (as in the two-instance
    // test): with instance A holding all six when B joins, at least three must
    // move for the fleet to balance. Records are padded so each split spans
    // many sink batches — the paced sink then throttles the owner for seconds,
    // holding its splits open (committed but incomplete) long enough for the
    // peer to take them over rather than the owner racing to the finish.
    let big = |prefix: &str, n: usize| -> Vec<String> {
        let pad = "x".repeat(2000);
        (0..n)
            .map(|i| format!("{{\"k\":\"{prefix}-{i}\",\"pad\":\"{pad}\"}}"))
            .collect()
    };
    let mut expected: Vec<String> = Vec::new();
    for i in 0..96 {
        let lines = big(&format!("o{i:02}"), 20);
        fs::write(data.join(format!("obj-{i:02}.ndjson")), lines_bytes(&lines)).unwrap();
        expected.extend(lines);
    }
    let store = shared_store();
    let yaml = throttled_config_yaml(&data);

    // Pace both sinks so neither instance can inhale the whole job before the
    // other has moved splits: with the small in-flight budget above, each
    // delayed write serializes into real throughput throttling, so the owner
    // holds its splits open across several rebalance rounds (lease 1s →
    // heartbeat ~330ms) and the drain window stays open long enough for the
    // handoff to complete. Generous, for CI safety.
    let pace = |script: &SinkScript| {
        for _ in 0..2000 {
            script.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(300)));
        }
    };

    // Instance A starts alone with a working set large enough to claim every
    // split — it over-claims relative to a two-instance fleet.
    let a = launch_handoff_instance(&yaml, &store, "instance-a", 6, pace);
    // Give A a real head start: wait until it has captured (and therefore is
    // committing) real progress — several checkpoint intervals (100ms) at the
    // 250ms poll cadence — so B joins against a live owner with committed
    // watermarks, not a cold job.
    wait_until(
        Duration::from_secs(30),
        "instance-a commits real progress before B joins",
        || captured_rows(&a.script).len() >= 40,
    );

    let b = launch_handoff_instance(&yaml, &store, "instance-b", 6, pace);

    let ra = a.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    let rb = b.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    assert_eq!(ra.state, ExitState::Completed, "instance-a completes");
    assert_eq!(rb.state, ExitState::Completed, "instance-b completes");

    let rows_a = captured_rows(&a.script);
    let rows_b = captured_rows(&b.script);
    let total = rows_a.len() + rows_b.len();

    // Coverage: the union covers every expected record exactly.
    let mut union: Vec<String> = rows_a.iter().chain(rows_b.iter()).cloned().collect();
    union.sort();
    union.dedup();
    assert_eq!(
        union,
        sorted(expected),
        "the union must cover every record exactly"
    );

    // Movement: both instances captured data, so splits genuinely moved from a
    // live owner to its peer (not merely a solo run).
    assert!(
        !rows_a.is_empty() && !rows_b.is_empty(),
        "splits must move between live owners (a={}, b={})",
        rows_a.len(),
        rows_b.len()
    );

    // Zero duplicates — the point of the feature. `total == union` iff no
    // record was delivered twice; the steal-era transfer would make
    // `total > union`.
    assert_eq!(
        total,
        union.len(),
        "a cooperative handoff moves splits with ZERO duplicates \
         (total={}, union={}, duplicates={})",
        total,
        union.len(),
        total - union.len(),
    );
}
