//! Driver-level choreography over a scripted coordinator: ownership
//! events and commit outcomes are scripted, the source runs its real
//! `CoordinationDriver` + `SplitSource` machinery against real `file://`
//! objects, and the script observes every commit, failure report, and
//! release — deterministic, no store, no clock.

mod support;

use spate_core::coordination::{CoordinationErrorKind, SplitSpec};
use spate_core::pipeline::ExitState;
use spate_s3::{SplitDescriptor, split_id_for};
use spate_test::{WriteOutcome, scripted_coordinator, wait_until};
use std::fs;
use std::path::Path;
use std::time::Duration;
use support::{
    Launched, captured_rows, launch_customized, line_framer, lines_bytes, recs, test_options,
};

fn config_yaml(data: &Path) -> String {
    format!(
        r#"
pipeline: {{ name: s3-scripted-test, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

/// Build a real `SplitSpec` over staged files: sizes from the
/// filesystem, no ETag pins (the streaming read path), ids minted with
/// the crate's own public derivation.
fn spec_over(data: &Path, names: &[&str]) -> SplitSpec {
    let entries: Vec<spate_s3::DescriptorObject> = names
        .iter()
        .map(|name| spate_s3::DescriptorObject {
            key: data
                .join(name)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string(),
            size: fs::metadata(data.join(name)).unwrap().len(),
            etag: None,
            last_modified_ms: 1,
        })
        .collect();
    let id = split_id_for(entries.iter().map(|e| (e.key.as_str(), None))).unwrap();
    let descriptor = SplitDescriptor::new(entries);
    SplitSpec::new(id, descriptor.encode().unwrap())
}

fn launch_scripted_coordinator(
    yaml: &str,
    coordinator: spate_test::ScriptedCoordinator,
    pre: impl FnOnce(&spate_test::SinkScript),
) -> Launched {
    launch_customized(yaml, test_options(), pre, move |source, _io| {
        line_framer(source).with_coordinator(Box::new(coordinator))
    })
}

#[test]
fn gains_stream_commits_carry_completion_and_all_complete_drains() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 30))).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&recs("b", 20))).unwrap();
    let split = spec_over(&data, &["a.ndjson", "b.ndjson"]);
    let id = split.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(split, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data), coordinator, |_| {});

    // The driver commits acked watermarks; the final commit (or sweep)
    // must carry the terminal completion flag.
    wait_until(Duration::from_secs(30), "terminal commit", || {
        script
            .commits()
            .iter()
            .any(|(sid, p)| sid == &id && p.completed)
    });
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(captured_rows(&l.script).len(), 50);
    assert!(script.failed().is_empty(), "no split failed");

    // Watermarks never regress across the commit sequence.
    let watermarks: Vec<i64> = script
        .commits()
        .iter()
        .filter(|(sid, _)| sid == &id)
        .map(|(_, p)| p.watermark)
        .collect();
    assert!(
        watermarks.windows(2).all(|w| w[0] <= w[1]),
        "non-decreasing watermarks: {watermarks:?}"
    );
}

#[test]
fn a_mid_flow_gain_folds_the_drain_commit_and_completes() {
    // Regression (deep-review finding F1): gaining split B while split A
    // flowed used to revoke A's lane and drop its context state, so the
    // post-drain commit for A's acked watermark tripped the "unheld
    // split" fatal and killed the pipeline. Gains are additive now: the
    // commit must fold and both splits must complete. The 60s checkpoint
    // interval doubles as the eager-commit check — completion can only
    // arrive through the commit-ready chase, never the periodic tick.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("flow.ndjson"), lines_bytes(&recs("flow", 800))).unwrap();
    fs::write(data.join("extra.ndjson"), lines_bytes(&recs("extra", 10))).unwrap();
    let flow = spec_over(&data, &["flow.ndjson"]);
    let extra = spec_over(&data, &["extra.ndjson"]);
    let flow_id = flow.id.clone();
    let extra_id = extra.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(flow, 1, None);
    // Pace the sink so A still has acked-but-uncommitted progress in
    // flight when B arrives — the exact shape that crashed pre-fix.
    let yaml = config_yaml(&data).replace("interval: 100ms", "interval: 60s");
    let l = launch_scripted_coordinator(&yaml, coordinator, |sink| {
        for _ in 0..20 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(100)));
        }
    });
    wait_until(Duration::from_secs(30), "rows durably written", || {
        !captured_rows(&l.script).is_empty()
    });

    // The routine incremental gain: mid-flow, no loss anywhere.
    script.gain(extra, 1, None);

    wait_until(Duration::from_secs(60), "both splits complete", || {
        let commits = script.commits();
        [&flow_id, &extra_id]
            .iter()
            .all(|id| commits.iter().any(|(sid, p)| sid == *id && p.completed))
    });
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    // No revoke means no replay: a clean run is exactly-once.
    assert_eq!(captured_rows(&l.script).len(), 810);
    assert!(script.failed().is_empty(), "no split failed");
}

#[test]
fn losing_a_split_detaches_it_without_failing_the_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("lost.ndjson"), lines_bytes(&recs("lost", 500))).unwrap();
    fs::write(data.join("kept.ndjson"), lines_bytes(&recs("kept", 10))).unwrap();
    let lost = spec_over(&data, &["lost.ndjson"]);
    let kept = spec_over(&data, &["kept.ndjson"]);
    let (lost_id, kept_id) = (lost.id.clone(), kept.id.clone());

    let (coordinator, script) = scripted_coordinator();
    script.gain(lost, 1, None);
    // Pace the sink so the lost split is still mid-flight when the loss
    // arrives.
    let l = launch_scripted_coordinator(&config_yaml(&data), coordinator, |sink| {
        for _ in 0..4 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(30), "first rows flow", || {
        !captured_rows(&l.script).is_empty()
    });

    // Steal it away, hand over a different split: the lane retires
    // without aborting anything and the pipeline keeps running.
    script.lose(&lost_id);
    script.gain(kept, 2, None);
    wait_until(
        Duration::from_secs(30),
        "replacement split completes",
        || {
            script
                .commits()
                .iter()
                .any(|(sid, p)| sid == &kept_id && p.completed)
        },
    );
    let commits_at_drain = script.commits().len();

    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(
        report.state,
        ExitState::Completed,
        "a lost split is not an error"
    );
    assert_eq!(
        script.commits().len(),
        commits_at_drain,
        "no late commits after the loss was observed and the job drained"
    );
}

#[test]
fn a_missing_object_reports_the_split_as_failed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("real.ndjson"), lines_bytes(&recs("real", 5))).unwrap();
    // A descriptor naming an object that does not exist: the ranged path
    // needs an ETag pin, so hand-craft one to route it there — a 404 on a
    // pinned read is the deleted-after-planning shape.
    let mut ghost = spec_over(&data, &["real.ndjson"]);
    let ghost_descriptor = SplitDescriptor::new(vec![spate_s3::DescriptorObject {
        key: data
            .join("ghost.ndjson")
            .to_string_lossy()
            .trim_start_matches('/')
            .to_string(),
        size: 64,
        etag: Some("\"gone\"".into()),
        last_modified_ms: 1,
    }]);
    ghost.descriptor = ghost_descriptor.encode().unwrap();
    ghost.id = split_id_for([("ghost", Some("\"gone\""))]).unwrap();
    let ghost_id = ghost.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(ghost, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data), coordinator, |_| {});

    wait_until(Duration::from_secs(30), "failure reported", || {
        !script.failed().is_empty()
    });
    let failed = script.failed();
    assert_eq!(failed[0].0, ghost_id);
    assert!(
        failed[0].1.contains("ghost"),
        "the report names the object: {}",
        failed[0].1
    );

    // The pipeline survives; the coordinator decides what happens next.
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

#[test]
fn a_retryable_commit_is_recommitted_on_a_later_tick() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 25))).unwrap();
    let split = spec_over(&data, &["a.ndjson"]);
    let id = split.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.fail_next_commit(&id, CoordinationErrorKind::Retryable);
    script.gain(split, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data), coordinator, |_| {});

    // Despite the transient store failure, the driver recommits until the
    // terminal progress lands.
    wait_until(Duration::from_secs(30), "terminal commit lands", || {
        script
            .commits()
            .iter()
            .any(|(sid, p)| sid == &id && p.completed)
    });
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

#[test]
fn stalled_is_fatal_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 400))).unwrap();
    let split = spec_over(&data, &["a.ndjson"]);

    let (coordinator, script) = scripted_coordinator();
    script.gain(split, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data), coordinator, |sink| {
        for _ in 0..4 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(30), "rows flow", || {
        !captured_rows(&l.script).is_empty()
    });

    // A stalled job (quarantined splits block completion) is fatal by
    // default — never a silent partial "Completed".
    script.stalled(3, 1);
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!("stalled must be fatal, got {:?}", report.state);
    };
    assert!(
        failure.reason.contains("quarantined"),
        "actionable stall error: {}",
        failure.reason
    );
}

#[test]
fn shutdown_releases_splits_still_held() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 20_000))).unwrap();
    let split = spec_over(&data, &["a.ndjson"]);
    let id = split.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(split, 1, None);
    // A tight in-flight budget, a tiny read window, and a paced sink keep
    // most of the object unfetched: the drain can flush what is in
    // flight, but the split stays incomplete — the release path's
    // precondition.
    let yaml = config_yaml(&data).replace(
        "    url:",
        "    prefetch_bytes: 64KiB\n    chunk_bytes: 16KiB\n    url:",
    );
    let yaml = format!("{yaml}backpressure: {{ max_inflight_bytes: 4KiB }}\n");
    let l = launch_scripted_coordinator(&yaml, coordinator, |sink| {
        for _ in 0..30 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(30), "rows flow", || {
        !captured_rows(&l.script).is_empty()
    });

    // Graceful shutdown: the drain commits acked progress and the
    // source's teardown hands the still-held split back (Drop → release)
    // so peers claim it without waiting out a lease.
    l.shutdown.trigger();
    let report = l.run.join().expect("run exits");
    assert_eq!(report.state, ExitState::Completed, "drain completes");
    assert_eq!(script.released(), vec![id], "the held split was released");
}
