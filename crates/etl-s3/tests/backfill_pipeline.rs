//! Whole-pipeline backfill tests over a local filesystem store — the
//! infrastructure-free equivalent of etl-kafka's MockCluster suite.
//!
//! Each test assembles a real `Pipeline` (controller, drivers, capture
//! sink) around an [`S3Source`] pointed at `file://` URLs in a tempdir,
//! and relies on the bounded-job contract: the pipeline exits
//! `Completed` on its own when the prefix is exhausted, and `Completed`
//! means every record was durably written and committed.

mod support;

use etl_core::pipeline::{ExitState, RuntimeOptions};
use etl_s3::Manifest;
/// Schema-1 watermark packing: `(ordinal: 23 bits) << 40 | record index`.
/// Deliberately inlined rather than imported: the manifest is a versioned
/// persisted document and this suite pins its concrete encoding — a layout
/// change must bump `MANIFEST_SCHEMA` and update this.
fn watermark(ordinal: u32, records: u64) -> i64 {
    ((ordinal as i64) << 40) | records as i64
}
use etl_test::{WriteOutcome, wait_until};
use std::fs;
use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;
use support::{captured_rows, launch, launch_scripted, lines_bytes, recs, sorted, test_options};

/// A tempdir holding the object prefix (`data/`) and the checkpoint
/// location (`state/`).
struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("data")).unwrap();
        fs::create_dir_all(dir.path().join("state")).unwrap();
        Fixture { dir }
    }

    fn object_path(&self, name: &str) -> PathBuf {
        self.dir.path().join("data").join(name)
    }

    fn write_plain(&self, name: &str, lines: &[String]) {
        fs::write(self.object_path(name), lines_bytes(lines)).unwrap();
    }

    fn write_gzip(&self, name: &str, lines: &[String]) {
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

    fn config_yaml(&self, lanes: u32, extra_sections: &str) -> String {
        format!(
            r#"
pipeline: {{ name: s3-backfill-test, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
{extra_sections}
source:
  s3:
    url: "file://{data}/"
    lanes: {lanes}
    checkpoint:
      url: "file://{state}/manifest.json"
sink: {{ capture: {{}} }}
"#,
            data = self.dir.path().join("data").display(),
            state = self.dir.path().join("state").display(),
        )
    }

    fn manifest(&self) -> Option<Manifest> {
        let bytes = fs::read(self.dir.path().join("state/manifest.json")).ok()?;
        Some(serde_json::from_slice(&bytes).expect("manifest parses"))
    }
}

#[test]
fn full_backfill_completes_itself_and_checkpoints_end_positions() {
    let fx = Fixture::new();
    // Sorted keys deal round-robin over 2 lanes:
    //   lane 0: a.ndjson (3 records), c.ndjson.zst (2)
    //   lane 1: b.ndjson.gz (2), d.ndjson (1)
    fx.write_plain("a.ndjson", &recs("a", 3));
    fx.write_gzip("b.ndjson.gz", &recs("b", 2));
    fx.write_zstd("c.ndjson.zst", &recs("c", 2));
    fx.write_plain("d.ndjson", &recs("d", 1));

    let l = launch(&fx.config_yaml(2, ""), test_options());
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

    // The manifest holds each lane's end position.
    let manifest = fx.manifest().expect("manifest written");
    assert_eq!(manifest.lanes, 2);
    assert_eq!(
        manifest.lane_states[&0].watermark,
        watermark(1, 2),
        "lane 0 ends after c"
    );
    assert_eq!(
        manifest.lane_states[&1].watermark,
        watermark(1, 1),
        "lane 1 ends after d"
    );
    assert_eq!(
        report.final_watermarks.len(),
        2,
        "both partitions committed: {:?}",
        report.final_watermarks
    );
}

#[test]
fn resume_after_shutdown_loses_nothing() {
    let fx = Fixture::new();
    // Enough records that the first run is usually interrupted mid-stream.
    for i in 0..8 {
        fx.write_plain(&format!("part-{i:02}.ndjson"), &recs(&format!("p{i}"), 400));
    }
    let mut expected: Vec<String> = Vec::new();
    for i in 0..8 {
        expected.extend(recs(&format!("p{i}"), 400));
    }

    // Run 1: stop as soon as the first commit has been persisted.
    let l1 = launch(&fx.config_yaml(3, ""), test_options());
    wait_until(Duration::from_secs(30), "first manifest commit", || {
        fx.manifest().is_some()
    });
    l1.shutdown.trigger();
    let r1 = l1.run.join().expect("run 1 exits");
    assert_eq!(r1.state, ExitState::Completed, "signal shutdown drains");
    let rows1 = captured_rows(&l1.script);

    // Run 2: fresh pipeline, same store and checkpoint — must finish the job.
    let l2 = launch(&fx.config_yaml(3, ""), test_options());
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
fn resume_of_a_finished_backfill_emits_nothing_and_completes() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 5));
    fx.write_gzip("b.ndjson.gz", &recs("b", 4));

    let l1 = launch(&fx.config_yaml(2, ""), test_options());
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);
    assert_eq!(captured_rows(&l1.script).len(), 9);

    // Watermarks sit exactly at each object's end: the rerun replays the
    // final object, discards exactly the committed count, and drains.
    let l2 = launch(&fx.config_yaml(2, ""), test_options());
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r2.state, ExitState::Completed);
    assert_eq!(
        captured_rows(&l2.script),
        Vec::<String>::new(),
        "everything was already committed"
    );
}

#[test]
fn listing_drift_below_the_watermark_fails_the_resume() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 3));
    fx.write_plain("b.ndjson", &recs("b", 3));
    fx.write_plain("c.ndjson", &recs("c", 3));

    let l1 = launch(&fx.config_yaml(2, ""), test_options());
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);

    // Deleting a key reshuffles ordinals below the committed positions.
    fs::remove_file(fx.object_path("b.ndjson")).unwrap();

    let l2 = launch(&fx.config_yaml(2, ""), test_options());
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    let ExitState::Failed(failure) = r2.state else {
        panic!(
            "a drifted listing must fail the pipeline, got {:?}",
            r2.state
        );
    };
    assert!(
        failure.reason.contains("frozen") || failure.reason.contains("listing changed"),
        "actionable drift error: {}",
        failure.reason
    );
}

#[test]
fn overwritten_object_at_the_watermark_fails_the_resume() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 3));
    fx.write_plain("b.ndjson", &recs("b", 3));

    let l1 = launch(&fx.config_yaml(1, ""), test_options());
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);

    // Same key, different content (size and mtime change → new ETag).
    std::thread::sleep(Duration::from_millis(20));
    fx.write_plain("b.ndjson", &recs("b-rewritten", 5));

    let l2 = launch(&fx.config_yaml(1, ""), test_options());
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    let ExitState::Failed(failure) = r2.state else {
        panic!(
            "an overwritten object must fail the resume, got {:?}",
            r2.state
        );
    };
    assert!(
        failure.reason.contains("overwritten") || failure.reason.contains("listing changed"),
        "actionable overwrite error: {}",
        failure.reason
    );
}

#[test]
fn empty_listing_completes_with_zero_records() {
    let fx = Fixture::new();
    let l = launch(&fx.config_yaml(4, ""), test_options());
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert!(captured_rows(&l.script).is_empty());
    let manifest = fx.manifest().expect("final flush writes a manifest");
    assert!(manifest.lane_states.is_empty(), "no lane made progress");
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
    let l = launch(&fx.config_yaml(1, ""), options);
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    let rows = captured_rows(&l.script);
    assert_eq!(
        rows,
        recs("big", n),
        "single lane, single shard: exact order"
    );
    let manifest = fx.manifest().unwrap();
    assert_eq!(manifest.lane_states[&0].watermark, watermark(0, n as u64));
}

#[test]
fn more_lanes_than_threads_still_completes() {
    let fx = Fixture::new();
    for i in 0..16 {
        fx.write_plain(&format!("obj-{i:02}.ndjson"), &recs(&format!("o{i}"), 25));
    }
    // 8 lanes over the config's 2 pipeline threads.
    let l = launch(&fx.config_yaml(8, ""), test_options());
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(captured_rows(&l.script).len(), 16 * 25);
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
    let yaml = fx.config_yaml(2, "backpressure: { max_inflight_bytes: 64KiB }");
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

    let l1 = launch(&fx.config_yaml(2, ""), test_options());
    let r1 = l1.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r1.state, ExitState::Completed);
    assert_eq!(captured_rows(&l1.script).len(), 100);

    let l2 = launch(&fx.config_yaml(2, ""), test_options());
    let r2 = l2.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(r2.state, ExitState::Completed);
    assert!(
        captured_rows(&l2.script).is_empty(),
        "blank lines must not desynchronize resume positions"
    );
}

#[test]
fn corrupt_compressed_object_fails_the_pipeline() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 2));
    fs::write(fx.object_path("bad.ndjson.gz"), b"this is not gzip").unwrap();

    let l = launch(&fx.config_yaml(1, ""), test_options());
    let report = l.run.wait_exit(Duration::from_secs(30)).unwrap().unwrap();
    let ExitState::Failed(failure) = report.state else {
        panic!(
            "a corrupt object must fail the pipeline, got {:?}",
            report.state
        );
    };
    assert!(
        failure.reason.contains("corrupt") || failure.reason.contains("decoding"),
        "actionable corruption error: {}",
        failure.reason
    );
}
