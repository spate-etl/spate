//! Whole-pipeline backfill tests over a local filesystem store, the
//! infrastructure-free equivalent of spate-kafka's MockCluster suite.
//!
//! Each test assembles a real `Pipeline` (controller, drivers, capture
//! sink) around an [`S3Source`](spate_s3::S3Source) pointed at `file://`
//! URLs in a tempdir, and relies on the bounded-job contract: the
//! pipeline exits `Completed` on its own when the in-process feed's lister has
//! finished and every unit is acknowledged, and `Completed` means every record
//! was durably written and committed.
//!

mod support;

use spate_core::pipeline::{ExitState, RuntimeOptions};
use spate_test::WriteOutcome;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use support::{
    PipelineYaml, captured_rows, launch, launch_scripted, lines_bytes, recs, sorted, test_options,
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

    /// Pipeline YAML. `split_target_bytes` stays at the 1MiB floor, so an
    /// uncompressed object above it is cut into ranges.
    fn config_yaml(&self, extra_sections: &str) -> String {
        PipelineYaml::file("s3-backfill-test", &self.dir.path().join("data"))
            .section(extra_sections)
            .build()
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
        report.final_watermarks.is_empty(),
        "every unit retired before the drained exit: {:?}",
        report.final_watermarks
    );
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
        "single unit, single shard: exact order"
    );
}

#[test]
fn more_units_than_threads_still_completes() {
    let fx = Fixture::new();
    // 48 objects are 48 units, over 2 pipeline threads.
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
    // controller while fetchers are mid-stream, the regression shape for
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
fn a_corrupt_object_dead_letters_its_unit_and_stalls() {
    let fx = Fixture::new();
    fx.write_plain("a.ndjson", &recs("a", 2));
    fs::write(fx.object_path("bad.ndjson.gz"), b"this is not gzip").unwrap();

    // The corrupt object fails its unit on every delivery, so the bounded job
    // must end stalled: a loud failure, never a silently partial "Completed".
    let l = launch(&fx.config_yaml(""), test_options());
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

/// A gzip object above the target whose key has no codec extension is read
/// whole under `compression: gzip`.
#[test]
fn forced_gzip_without_extension_is_read_whole() {
    use std::io::Write as _;
    let fx = Fixture::new();
    let lines: Vec<String> = (0..20_000)
        .map(|i| format!("{{\"k\":\"gz-{i:06}\",\"pad\":\"{}\"}}", "g".repeat(60)))
        .collect();
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::none());
    enc.write_all(&lines_bytes(&lines)).unwrap();
    fs::write(fx.object_path("big.ndjson"), enc.finish().unwrap()).unwrap();
    assert!(fs::metadata(fx.object_path("big.ndjson")).unwrap().len() > 1024 * 1024);

    let yaml = PipelineYaml::file("s3-forced-gzip", &fx.dir.path().join("data"))
        .source("compression", "gzip")
        .build();
    let l = launch(&yaml, test_options());
    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(sorted(captured_rows(&l.script)), sorted(lines));
}
