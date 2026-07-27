//! Whole-pipeline backfill tests over a local filesystem store — the
//! infrastructure-free equivalent of spate-kafka's MockCluster suite.
//!
//! Each test assembles a real `Pipeline` (controller, drivers, capture
//! sink) around an [`S3Source`](spate_s3::S3Source) pointed at `file://`
//! URLs in a tempdir, and relies on the bounded-job contract: the
//! pipeline exits `Completed` on its own when every split is committed
//! complete, and `Completed` means every record was durably written and
//! committed.
//!
//! Progress lives in the coordination store. Plain `launch` runs solo
//! over the source's internal in-process store (ephemeral by design);
//! `launch_on_store` shares one in-process store across launches — the
//! infrastructure-free stand-in for a durable backend, which is what
//! makes resume-across-restart testable here.

mod support;

use spate_coordination::CoordinationConfig;
use spate_core::pipeline::{ExitState, RuntimeOptions};
use spate_test::{WriteOutcome, wait_until};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use support::{
    captured_rows, launch, launch_on_store, launch_scripted, launch_tuned, lines_bytes, recs,
    shared_store, sorted, test_options, test_tuning,
};

/// A tempdir holding the object prefix (`data/`).
struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("data")).unwrap();
        Fixture { dir }
    }

    fn object_path(&self, name: &str) -> PathBuf {
        self.dir.path().join("data").join(name)
    }

    fn write_plain(&self, name: &str, lines: &[String]) {
        fs::write(self.object_path(name), lines_bytes(lines)).unwrap();
    }

    fn write_gzip(&self, name: &str, lines: &[String]) {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&lines_bytes(lines)).unwrap();
        fs::write(self.object_path(name), enc.finish().unwrap()).unwrap();
    }

    fn write_zstd(&self, name: &str, lines: &[String]) {
        fs::write(
            self.object_path(name),
            zstd::encode_all(&lines_bytes(lines)[..], 3).unwrap(),
        )
        .unwrap();
    }

    /// Pipeline YAML. `split_target_bytes` stays at the 1MiB floor so the
    /// per-object cost floor is 64KiB: sixteen small objects per split,
    /// letting fixtures shape split counts deterministically.
    fn config_yaml(&self, extra_sections: &str) -> String {
        format!(
            r#"
pipeline: {{ name: s3-backfill-test, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
{extra_sections}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
sink: {{ capture: {{}} }}
"#,
            data = self.dir.path().join("data").display(),
        )
    }
}

#[test]
fn full_backfill_completes_itself() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 3));
    fx.write_gzip("b.ndjson.gz", &recs("b", 2));
    fx.write_zstd("c.ndjson.zst", &recs("c", 2));
    fx.write_plain("d.ndjson", &recs("d", 1));

    let l = launch(&fx.config_yaml(""), test_options());
    let report = l
        .run
        .wait_exit(Duration::from_secs(30))
        .expect("bounded job exits on its own")
        .expect("no start error");
    assert_eq!(report.state, ExitState::Completed);

    let mut expected: Vec<String> = Vec::new();
    for (p, n) in [("a", 3), ("b", 2), ("c", 2), ("d", 1)] {
        expected.extend(recs(p, n));
    }
    assert_eq!(sorted(captured_rows(&l.script)), sorted(expected));
    assert!(
        !report.final_watermarks.is_empty(),
        "the split tenancy committed: {:?}",
        report.final_watermarks
    );
}

#[test]
fn shutdown_handoff_resumes_from_committed_progress() {
    let fx = Fixture::new();
    // One split (8 objects < 16), big enough that run 1 is interrupted
    // mid-split: the resume is a genuine mid-split watermark carry.
    for i in 0..8 {
        fx.write_plain(&format!("part-{i:02}.ndjson"), &recs(&format!("p{i}"), 400));
    }
    let mut expected: Vec<String> = Vec::new();
    for i in 0..8 {
        expected.extend(recs(&format!("p{i}"), 400));
    }
    let store = shared_store();

    // Run 1: stop once data is flowing; the drain commits acked progress
    // and gracefully releases the split back to the store.
    let l1 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    wait_until(Duration::from_secs(30), "first rows captured", || {
        !captured_rows(&l1.script).is_empty()
    });
    l1.shutdown.trigger();
    let r1 = l1.run.join().expect("run 1 exits");
    assert_eq!(r1.state, ExitState::Completed, "signal shutdown drains");
    let rows1 = captured_rows(&l1.script);

    // Run 2: fresh pipeline over the same store — must finish the job.
    let l2 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r2 = l2
        .run
        .wait_exit(Duration::from_secs(60))
        .expect("run 2 completes on its own")
        .expect("no start error");
    assert_eq!(r2.state, ExitState::Completed);
    let rows2 = captured_rows(&l2.script);

    // At-least-once: the union covers everything; overlap (replay) is fine.
    let mut union: Vec<String> = rows1.iter().chain(rows2.iter()).cloned().collect();
    union.sort();
    union.dedup();
    assert_eq!(
        union,
        sorted(expected),
        "no record may be lost across a restart"
    );
}

#[test]
fn completed_job_reruns_to_zero_rows() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 5));
    fx.write_gzip("b.ndjson.gz", &recs("b", 4));
    let store = shared_store();

    let l1 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);
    assert_eq!(captured_rows(&l1.script).len(), 9);

    // Completed splits are never re-offered: the rerun observes
    // AllComplete and drains without reading anything.
    let l2 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r2.state, ExitState::Completed);
    assert_eq!(
        captured_rows(&l2.script),
        Vec::<String>::new(),
        "everything was already committed"
    );
}

#[test]
fn restart_replays_fully_on_an_ephemeral_store() {
    // The solo contract: no coordinator injected means an internal
    // in-process store, so a rerun replays the whole prefix (at-least-once
    // keeps it safe; the startup WARN says so).
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 5));

    for _ in 0..2 {
        let l = launch(&fx.config_yaml(""), test_options());
        let r = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
        assert_eq!(r.state, ExitState::Completed);
        assert_eq!(
            captured_rows(&l.script),
            recs("a", 5),
            "full replay each run"
        );
    }
}

/// Publish a plan over `fx` into `store`, without completing any of it.
///
/// The mutation tests need a *planned* prefix — split descriptors holding
/// each member's key and ETag — that they can then mutate underneath.
/// Doing that by racing a running pipeline (mutate while the first split
/// streams, hoping the victim is still unclaimed) is a timing assumption
/// that silently stops holding the moment split completion gets faster.
///
/// So: run a pipeline whose sink writes all fail. The plan is published
/// before any split can be claimed, so it always reaches the store, while
/// the failing writes mean no batch is ever acknowledged and therefore no
/// split can ever be committed complete. Stop it as soon as the plan is
/// durable. What is left behind is a planned, wholly incomplete job — and
/// the mutation then happens with no pipeline running at all.
fn plan_only_run(fx: &Fixture, store: &spate_coordination::store::memory::MemoryStore) {
    let l = launch_tuned(
        &fx.config_yaml(""),
        test_options(),
        store,
        test_tuning(),
        |script| {
            for _ in 0..64 {
                script.enqueue_global(WriteOutcome::fatal("hold the planning run"));
            }
        },
    );
    wait_until(
        Duration::from_secs(30),
        "the plan reaching the store",
        || !split_progress(store).is_empty(),
    );
    l.shutdown.trigger();
    let _ = l.run.join().expect("no start error");

    let splits = split_progress(store);
    assert!(!splits.is_empty(), "the plan reached the store");
    assert!(
        splits.iter().all(|s| s["status"] != "completed"),
        "no split may complete before the mutation: {splits:?}"
    );
}

/// Every split progress record in the store, decoded as JSON.
fn split_progress(
    store: &spate_coordination::store::memory::MemoryStore,
) -> Vec<serde_json::Value> {
    use spate_coordination::store::{CoordinationStore as _, Keyspace};
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(store.list(Keyspace::Durable, "split."))
        .expect("list")
        .iter()
        .map(|e| serde_json::from_slice(&e.value).expect("progress record"))
        .collect()
}

/// Run the planned job to its terminal state and assert it stalled.
fn assert_stalls(fx: &Fixture, store: &spate_coordination::store::memory::MemoryStore) {
    let l = launch_tuned(
        &fx.config_yaml(""),
        test_options(),
        store,
        CoordinationConfig {
            max_attempts: 2,
            ..test_tuning()
        },
        |_| {},
    );
    let report = l
        .run
        .wait_exit(Duration::from_secs(60))
        .expect("the job reaches a verdict")
        .expect("no start error");
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "a quarantined split must stall the bounded job, got {:?}",
            report.state
        );
    };
    assert!(
        failure.reason.contains("quarantined") || failure.reason.contains("stalled"),
        "actionable stall error: {}",
        failure.reason
    );
}

#[test]
fn deleting_a_planned_object_quarantines_its_split() {
    let fx = Fixture::new();
    for i in 0..4 {
        fx.write_plain(
            &format!("obj-{i:02}.ndjson"),
            &recs(&format!("o{i:02}"), 20),
        );
    }
    let store = shared_store();
    plan_only_run(&fx, &store);

    // Deleted after planning: the descriptor still names it, so the split
    // 404s on every delivery attempt until the budget quarantines it.
    fs::remove_file(fx.object_path("obj-00.ndjson")).unwrap();
    assert_stalls(&fx, &store);
}

#[test]
fn overwriting_a_planned_object_quarantines_its_split() {
    let fx = Fixture::new();
    for i in 0..4 {
        fx.write_plain(
            &format!("obj-{i:02}.ndjson"),
            &recs(&format!("o{i:02}"), 20),
        );
    }
    let store = shared_store();
    plan_only_run(&fx, &store);

    // Same key, different content (size changes, so the `file://` ETag
    // does too): the planned split's If-Match pin must trip rather than
    // splice the new content into the old descriptor's offsets.
    fx.write_plain("obj-00.ndjson", &recs("rewritten", 7));
    assert_stalls(&fx, &store);
}

#[test]
fn empty_listing_completes_with_zero_records() {
    let fx = Fixture::new();
    let l = launch(&fx.config_yaml(""), test_options());
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(
        report.state,
        ExitState::Completed,
        "an empty final plan completes immediately"
    );
    assert!(captured_rows(&l.script).is_empty());
}

#[test]
fn one_huge_object_spans_many_batches_in_order() {
    let fx = Fixture::new();
    let n = 3000;
    fx.write_zstd("big.ndjson.zst", &recs("big", n));

    let options = RuntimeOptions {
        handle_signals: false,
        max_records: 64, // force ~47 batches through one object
        ..RuntimeOptions::default()
    };
    let l = launch(&fx.config_yaml(""), options);
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let rows = captured_rows(&l.script);
    assert_eq!(
        rows,
        recs("big", n),
        "single split, single shard: exact order"
    );
}

#[test]
fn more_splits_than_threads_still_completes() {
    let fx = Fixture::new();
    // 48 objects → 3 splits at the 16-member cap, over 2 pipeline threads.
    for i in 0..48 {
        fx.write_plain(&format!("obj-{i:02}.ndjson"), &recs(&format!("o{i}"), 25));
    }
    let l = launch(&fx.config_yaml(""), test_options());
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(captured_rows(&l.script).len(), 48 * 25);
}

#[test]
fn slow_sink_backpressure_does_not_deadlock_the_backfill() {
    let fx = Fixture::new();
    for i in 0..4 {
        fx.write_plain(&format!("part-{i}.ndjson"), &recs(&format!("s{i}"), 500));
    }
    // A tight in-flight budget plus slow first writes engages the pause
    // controller while fetchers are mid-stream — the regression shape for
    // an async edge that blocks the shared I/O runtime.
    let yaml = fx.config_yaml("backpressure: { max_inflight_bytes: 64KiB }");
    let l = launch_scripted(&yaml, test_options(), |script| {
        for _ in 0..6 {
            script.enqueue_global(WriteOutcome::ok().after(Duration::from_millis(250)));
        }
    });
    let report = l.run.wait_exit(Duration::from_secs(90)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(captured_rows(&l.script).len(), 4 * 500);
}

#[test]
fn blank_line_heavy_objects_survive_a_rerun_consistently() {
    let fx = Fixture::new();
    // Records interleaved with blank and whitespace-only lines: indexes
    // count emitted records only, so the rerun's discard math must align.
    let mut lines = Vec::new();
    for i in 0..50 {
        lines.push(format!("{{\"k\":\"w-{i}\"}}"));
        lines.push(String::new());
        lines.push("   ".to_string());
    }
    fx.write_plain("weird.ndjson", &lines);
    fx.write_gzip("weird2.ndjson.gz", &lines);
    let store = shared_store();

    let l1 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);
    assert_eq!(captured_rows(&l1.script).len(), 100);

    let l2 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r2.state, ExitState::Completed);
    assert!(
        captured_rows(&l2.script).is_empty(),
        "blank lines must not desynchronize resume positions"
    );
}

#[test]
fn corrupt_object_quarantines_its_split_and_stalls() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 2));
    fs::write(fx.object_path("bad.ndjson.gz"), b"this is not gzip").unwrap();
    let store = shared_store();

    // Both objects share one split: the corrupt member poisons it on
    // every delivery attempt, so the bounded job must end stalled — a
    // loud failure, never a silently partial "Completed".
    let l = launch_tuned(
        &fx.config_yaml(""),
        test_options(),
        &store,
        CoordinationConfig {
            max_attempts: 2,
            ..test_tuning()
        },
        |_| {},
    );
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "a corrupt object must stall the bounded job, got {:?}",
            report.state
        );
    };
    assert!(
        failure.reason.contains("quarantined") || failure.reason.contains("stalled"),
        "actionable stall error: {}",
        failure.reason
    );
}

#[test]
fn mismatched_planner_config_is_rejected_at_startup() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 3));
    let store = shared_store();

    let l1 = launch_on_store(&fx.config_yaml(""), test_options(), &store, |_| {});
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);

    // A second instance whose planner config diverges (different split
    // target ⇒ different fingerprint) must be refused: divergent
    // configurations can never share a coordinated job.
    let divergent = fx
        .config_yaml("")
        .replace("split_target_bytes: 1MiB", "split_target_bytes: 2MiB");
    let l2 = launch_on_store(&divergent, test_options(), &store, |_| {});
    let report = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "a fingerprint mismatch must fail startup, got {:?}",
            report.state
        );
    };
    assert!(
        failure.reason.contains("fingerprint"),
        "actionable mismatch error: {}",
        failure.reason
    );
}
