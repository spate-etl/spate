//! Driver-level choreography over a scripted coordinator: ownership
//! events and commit outcomes are scripted, the source runs its real
//! `CoordinationDriver` + `SplitSource` machinery against real `file://`
//! objects, and the script observes every commit, failure report, and
//! release. Deterministic, no store, no clock.

mod support;

use spate_core::coordination::{CoordinationErrorKind, LeaseEpoch, SplitProgress, SplitSpec};
use spate_core::framing::RecordFramer;
use spate_core::pipeline::ExitState;
use spate_s3::{SplitDescriptor, split_id_for};
use spate_test::{WriteOutcome, scripted_coordinator, wait_until};
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;
use support::{
    Launched, LineFramer, PipelineYaml, captured_rows, key_of, launch_customized, line_framer,
    line_starts, lines_bytes, ranged_spec, recs, sorted, test_options,
};

fn config_yaml(data: &Path) -> PipelineYaml {
    PipelineYaml::file("s3-scripted-test", data)
}

/// Build a real `SplitSpec` over staged files: sizes from the
/// filesystem, no ETag pins (the streaming read path), ids minted with
/// the crate's own public derivation.
fn spec_over(data: &Path, names: &[&str]) -> SplitSpec {
    let entries: Vec<spate_s3::DescriptorObject> = names
        .iter()
        .map(|name| spate_s3::DescriptorObject {
            key: key_of(data, name),
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
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

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
    assert!(
        script.departed(),
        "a finished worker departs holding nothing"
    );
    assert!(script.released().is_empty());

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
    // A gain is additive: taking split B while split A flows leaves A's
    // lane and context state in place, so the post-drain commit for A's
    // acked watermark folds and both splits complete. The 60s checkpoint
    // interval doubles as the eager-commit check, since completion can
    // only arrive through the commit-ready chase and never the periodic
    // tick.
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
    // flight when B arrives, the shape that crashed the earlier code.
    let yaml = config_yaml(&data).checkpoint("60s").build();
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
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |sink| {
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

/// A split whose descriptor names an object that does not exist. The ranged
/// path needs an ETag pin, so the descriptor is hand-crafted to route there:
/// a 404 on a pinned read is the deleted-after-planning shape.
fn ghost_spec(data: &Path) -> SplitSpec {
    let mut ghost = spec_over(data, &["real.ndjson"]);
    let descriptor = SplitDescriptor::new(vec![spate_s3::DescriptorObject {
        key: data
            .join("ghost.ndjson")
            .to_string_lossy()
            .trim_start_matches('/')
            .to_string(),
        size: 64,
        etag: Some("\"gone\"".into()),
        last_modified_ms: 1,
    }]);
    ghost.descriptor = descriptor.encode().unwrap();
    ghost.id = split_id_for([("ghost", Some("\"gone\""))]).unwrap();
    ghost
}

#[test]
fn a_missing_object_reports_the_split_as_failed() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("real.ndjson"), lines_bytes(&recs("real", 5))).unwrap();
    let ghost = ghost_spec(&data);
    let ghost_id = ghost.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(ghost, 4, None);
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

    wait_until(Duration::from_secs(30), "failure reported", || {
        !script.failed().is_empty()
    });
    let failed = script.failed();
    assert_eq!(failed[0].0, ghost_id);
    assert_eq!(failed[0].1, LeaseEpoch(4), "the report names the tenancy");
    assert!(
        failed[0].2.contains("ghost"),
        "the report names the object: {}",
        failed[0].2
    );

    // The pipeline survives; the coordinator decides what happens next.
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// A failure report the coordinator answers `Retryable` is offered again.
#[test]
fn a_retryable_failure_report_is_offered_again() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("real.ndjson"), lines_bytes(&recs("real", 5))).unwrap();
    let ghost = ghost_spec(&data);
    let ghost_id = ghost.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.fail_next_report(&ghost_id, CoordinationErrorKind::Retryable);
    script.gain(ghost, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

    wait_until(Duration::from_secs(30), "the report offered again", || {
        script.failed().len() >= 2
    });
    assert!(script.failed().iter().all(|(id, _, _)| id == &ghost_id));

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
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

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
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |sink| {
        for _ in 0..4 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(30), "rows flow", || {
        !captured_rows(&l.script).is_empty()
    });

    // A stalled job (quarantined splits block completion) is fatal by
    // default, never a silent partial "Completed".
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
    // flight, but the split stays incomplete, which is the release path's
    // precondition.
    let yaml = config_yaml(&data)
        .source("prefetch_bytes", "64KiB")
        .source("chunk_bytes", "16KiB")
        .section("backpressure: { max_inflight_bytes: 4KiB }")
        .build();
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

/// The source sends the final commit of a stop through
/// `SplitCoordinator::commit_final`, as one batch.
#[test]
fn the_final_commit_at_shutdown_goes_out_as_one_batch() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 20_000))).unwrap();
    let split = spec_over(&data, &["a.ndjson"]);
    let id = split.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(split, 1, None);
    let yaml = config_yaml(&data)
        .checkpoint("60s")
        .source("prefetch_bytes", "64KiB")
        .source("chunk_bytes", "16KiB")
        .section("backpressure: { max_inflight_bytes: 4KiB }")
        .build();
    let l = launch_scripted_coordinator(&yaml, coordinator, |sink| {
        for _ in 0..30 {
            sink.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(150)));
        }
    });
    wait_until(Duration::from_secs(30), "rows flow", || {
        !captured_rows(&l.script).is_empty()
    });

    l.shutdown.trigger();
    let report = l.run.join().expect("run exits");
    assert_eq!(report.state, ExitState::Completed, "drain completes");
    let batches = script.final_commits();
    assert_eq!(batches.len(), 1, "one final commit batch: {batches:?}");
    let splits: Vec<_> = batches[0].iter().map(|(s, _)| s.clone()).collect();
    assert_eq!(splits, vec![id], "the batch holds the held split");
}

/// A peer taking over a ranged split mid-range delivers exactly the owned
/// records past the carried watermark. One range starts right after a
/// delimiter, the other one byte past a delimiter, and both end inside a
/// record.
#[test]
fn a_ranged_split_taken_over_mid_range_delivers_the_rest_of_its_records() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let lines = recs("r", 20);
    fs::write(data.join("big.ndjson"), lines_bytes(&lines)).unwrap();
    let starts = line_starts(&lines);
    // Owns records 5 through 10: record 10 starts before `end`.
    let at_record = ranged_spec(&data, "big.ndjson", starts[5], starts[10] + 3, b'\n');
    // Owns records 13 through 16: record 12 starts before `start`.
    let past_record = ranged_spec(&data, "big.ndjson", starts[12] + 1, starts[16] + 3, b'\n');
    let (at_id, past_id) = (at_record.id.clone(), past_record.id.clone());

    let (coordinator, script) = scripted_coordinator();
    // The previous owners committed two and one records of the ranges.
    script.gain(at_record, 2, Some(SplitProgress::new(2, Vec::new())));
    script.gain(past_record, 2, Some(SplitProgress::new(1, Vec::new())));
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

    wait_until(Duration::from_secs(30), "both terminal commits", || {
        let commits = script.commits();
        [&at_id, &past_id]
            .iter()
            .all(|id| commits.iter().any(|(sid, p)| sid == *id && p.completed))
    });
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let expected: Vec<String> = lines[7..=10]
        .iter()
        .chain(&lines[14..=16])
        .cloned()
        .collect();
    assert_eq!(sorted(captured_rows(&l.script)), sorted(expected));
    assert_eq!(script.last_commit(&at_id).unwrap().watermark, 6);
    assert_eq!(script.last_commit(&past_id).unwrap().watermark, 4);
}

/// A range lying inside one record owns nothing, and its split completes at
/// watermark 0 without emitting.
#[test]
fn a_range_that_owns_no_record_completes_empty() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let lines = recs("r", 5);
    fs::write(data.join("big.ndjson"), lines_bytes(&lines)).unwrap();
    let starts = line_starts(&lines);
    let spec = ranged_spec(&data, "big.ndjson", starts[3] + 1, starts[3] + 4, b'\n');
    let id = spec.id.clone();

    let (coordinator, script) = scripted_coordinator();
    script.gain(spec, 1, None);
    let l = launch_scripted_coordinator(&config_yaml(&data).build(), coordinator, |_| {});

    wait_until(Duration::from_secs(30), "terminal commit", || {
        script
            .commits()
            .iter()
            .any(|(sid, p)| sid == &id && p.completed)
    });
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(captured_rows(&l.script).is_empty());
    assert_eq!(script.last_commit(&id).unwrap().watermark, 0);
    assert!(script.failed().is_empty(), "no split failed");
}

/// A record over the framer's cap inside one range fails that range's split
/// alone; the other range of the same object completes.
#[test]
fn an_oversized_record_fails_only_the_range_that_owns_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let mut lines = recs("r", 10);
    lines[2] = format!("{{\"k\":\"{}\"}}", "x".repeat(200));
    fs::write(data.join("big.ndjson"), lines_bytes(&lines)).unwrap();
    let starts = line_starts(&lines);
    let size = fs::metadata(data.join("big.ndjson")).unwrap().len();
    let oversized = ranged_spec(&data, "big.ndjson", 0, starts[5], b'\n');
    let healthy = ranged_spec(&data, "big.ndjson", starts[5], size, b'\n');
    let (oversized_id, healthy_id) = (oversized.id.clone(), healthy.id.clone());

    let (coordinator, script) = scripted_coordinator();
    script.gain(oversized, 1, None);
    script.gain(healthy, 1, None);
    let l = launch_customized(
        &config_yaml(&data).build(),
        test_options(),
        |_| {},
        move |source, _io| {
            source
                .with_framer(|| Box::new(spate_json::NdjsonFramer::new(64)))
                .with_coordinator(Box::new(coordinator))
        },
    );

    wait_until(
        Duration::from_secs(30),
        "one failure and one completion",
        || {
            !script.failed().is_empty()
                && script
                    .commits()
                    .iter()
                    .any(|(sid, p)| sid == &healthy_id && p.completed)
        },
    );
    script.all_complete();
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let failed = script.failed();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].0, oversized_id);
    assert!(failed[0].2.contains("max_record_bytes"), "{}", failed[0].2);
    assert_eq!(captured_rows(&l.script), lines[5..].to_vec());
}

/// A line framer that declares no resync delimiter.
#[derive(Default)]
struct NoResync(LineFramer);

impl RecordFramer for NoResync {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.push(bytes)
    }

    fn finish(&mut self) -> io::Result<()> {
        self.0.finish()
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        self.0.pop()
    }

    fn decoded_bytes(&self) -> u64 {
        self.0.decoded_bytes()
    }
}

/// A ranged split whose delimiter differs from the one the source's framer
/// declares, or whose framer declares none, fails the pipeline.
#[test]
fn a_ranged_split_planned_for_another_framer_is_fatal() {
    type MakeFramer = fn() -> Box<dyn RecordFramer>;
    let cases: [(u8, MakeFramer); 2] = [
        (b';', || Box::new(LineFramer::default())),
        (b'\n', || Box::new(NoResync::default())),
    ];
    for (delimiter, make_framer) in cases {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("big.ndjson"), lines_bytes(&recs("r", 5))).unwrap();
        let spec = ranged_spec(&data, "big.ndjson", 0, 10, delimiter);

        let (coordinator, script) = scripted_coordinator();
        script.gain(spec, 1, None);
        let l = launch_customized(
            &config_yaml(&data).build(),
            test_options(),
            |_| {},
            move |source, _io| {
                source
                    .with_framer(make_framer)
                    .with_coordinator(Box::new(coordinator))
            },
        );
        let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
        let ExitState::Failed(failure) = report.state else {
            panic!(
                "a framer mismatch must fail the pipeline, got {:?}",
                report.state
            );
        };
        assert!(
            failure.reason.contains("different framer"),
            "{}",
            failure.reason
        );
        assert!(captured_rows(&l.script).is_empty());
    }
}

/// A ranged split over an object the source decompresses fails the pipeline,
/// whether the key's extension or a forced `compression` selects the codec,
/// and delivers nothing.
#[test]
fn a_ranged_split_over_a_compressed_object_is_fatal() {
    use std::io::Write as _;
    let lines = recs("r", 2000);
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&lines_bytes(&lines)).unwrap();
    let gz = enc.finish().unwrap();
    // A 0x0a byte inside the compressed body, so the first range ends mid-stream.
    let cut = (10..gz.len() - 1)
        .find(|&i| gz[i] == b'\n')
        .expect("a 0x0a byte inside the compressed body") as u64
        + 1;
    for (name, compression) in [("big.ndjson.gz", "auto"), ("big.ndjson", "gzip")] {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join(name), &gz).unwrap();
        let size = gz.len() as u64;
        let head = ranged_spec(&data, name, 0, cut, b'\n');
        let tail = ranged_spec(&data, name, cut, size, b'\n');

        let (coordinator, script) = scripted_coordinator();
        script.gain(head, 1, None);
        script.gain(tail, 1, None);
        let yaml = config_yaml(&data)
            .source("compression", compression)
            .build();
        let l = launch_scripted_coordinator(&yaml, coordinator, |_| {});
        let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
        let ExitState::Failed(failure) = report.state else {
            panic!(
                "a ranged split over a compressed object must fail the pipeline, got {:?}",
                report.state
            );
        };
        assert!(failure.reason.contains(name), "{}", failure.reason);
        assert!(failure.reason.contains("gzip"), "{}", failure.reason);
        assert!(captured_rows(&l.script).is_empty());
        assert!(script.failed().is_empty(), "{:?}", script.failed());
    }
}
