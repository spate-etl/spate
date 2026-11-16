//! The harness over a scripted feed: what a worker records and writes, how
//! it stops, and the stalled check on a real bounded job.

use spate_core::clock::tokio::TestClock;
use spate_core::config::PipelineConfig;
use spate_core::feed::{DepthOf, Feed, FeedErrorKind, Unit};
use spate_core::pipeline::{ExitReport, ExitState};
use spate_json::NdjsonFramer;
use spate_s3::{S3Source, SplitDescriptor};
use spate_test::{FeedScript, HANG, ScriptedFeed, WriteOutcome, scripted_feed, wait_until};
use spate_test_s3_feed::{
    CHECKPOINT, PipelineConfigFeed, Recording, STEP, TWO_RANGES, Worker, assert_stalled, data_dir,
    file_source, launch_customized, pipeline_yaml, publish, recs, sorted, stage, stage_with_etag,
    two_range_unit, written,
};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SCRIPTED: &str = "feed:\n  scripted: {}\n";

/// A worker over the files under `data` reading from `feed`.
fn launch(data: &Path, feed: ScriptedFeed, extra: &str) -> Worker<ScriptedFeed> {
    launch_customized(
        move |_, _| Ok(feed),
        &file_source(data, extra),
        SCRIPTED,
        |_| {},
        |source| source,
        CHECKPOINT,
        None,
    )
}

fn acknowledged(script: &FeedScript, n: usize) {
    wait_until(HANG, &format!("{n} units acknowledged"), || {
        script.acknowledged().len() == n
    });
}

#[test]
fn the_recording_passes_each_call_on_and_records_it() {
    let (feed, script) = scripted_feed();
    let recording = Recording::new(Arc::new(feed));
    assert_eq!(recording.backend(), "scripted");
    let first = script.deliver(Unit::new("a", b"a".to_vec()));
    let before = Instant::now();
    assert_eq!(recording.receive(8).unwrap().len(), 1);
    let second = script.redeliver(first);
    assert_eq!(recording.receive(8).unwrap().len(), 1);
    assert_eq!(recording.deliveries(), [1, 2]);
    assert_eq!(recording.receipts(), [first, second]);
    let (count, at) = recording.first_seen().unwrap();
    assert_eq!(count, 1);
    assert!(at >= before);

    recording.renew(second).unwrap();
    assert_eq!(script.renewed(), [second]);
    assert_eq!(
        recording.renew(first).unwrap_err().kind,
        FeedErrorKind::Gone
    );

    recording.read_depth(DepthOf::Feed);
    recording.read_depth(DepthOf::DeadLetters);
    assert_eq!(recording.depth_reads(), 1);
    assert_eq!(script.depth_reads(DepthOf::DeadLetters), 1);

    assert!(format!("{recording:?}").starts_with("Recording {"));
    assert_eq!(recording.stopped_receiving(), 0);
    assert!(!recording.hands_back());
    script.hand_back_on_stop();
    assert!(recording.hands_back());
    recording.stop_receiving();
    assert_eq!(recording.stopped_receiving(), 1);
    assert_eq!(script.stopped_receiving(), 1);
    recording.hand_back(second).unwrap();
    assert_eq!(recording.handed_back(), [second]);
    assert_eq!(script.handed_back(), [second]);
    recording.close(Duration::from_secs(3));
    assert_eq!(recording.closes(), [Duration::from_secs(3)]);
    assert_eq!(script.closes(), [Duration::from_secs(3)]);
}

#[test]
fn the_recording_passes_acknowledge_fail_and_publish_on() {
    let (feed, script) = scripted_feed();
    let recording = Recording::new(Arc::new(feed));
    let done = script.deliver(Unit::new("done", b"d".to_vec()));
    let bad = script.deliver(Unit::new("bad", b"b".to_vec()));
    recording.receive(8).unwrap();
    recording.acknowledge(done).unwrap();
    recording.fail(bad, "unreadable").unwrap();
    assert_eq!(script.acknowledged(), [done]);
    assert_eq!(script.failed(), [(bad, "unreadable".to_owned())]);
    publish(&recording, Unit::new("new", b"n".to_vec()));
    assert_eq!(script.published()[0].id, "new");
    assert!(Arc::ptr_eq(
        recording.inner(),
        &recording.clone().inner().clone()
    ));
}

/// `publish` returns once the feed has settled the unit, not before.
#[test]
fn publish_waits_for_the_confirmation() {
    let (feed, script) = scripted_feed();
    script.hold_publishes();
    std::thread::scope(|scope| {
        let publishing = scope.spawn(|| publish(&feed, Unit::new("held", b"h".to_vec())));
        wait_until(HANG, "the publish reaches the feed", || {
            script.published().len() == 1
        });
        script.confirm_held_publishes();
        publishing.join().expect("the publish settles");
    });
}

/// The `checkpoint` a test gives reaches the pipeline: a drain timeout
/// inside the stop reserve of a feed that hands back is refused at startup.
#[test]
fn the_checkpoint_section_reaches_the_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    script.hand_back_on_stop();
    let worker = launch_customized(
        move |_, _| Ok(feed),
        &file_source(&data, ""),
        SCRIPTED,
        |_| {},
        |source| source,
        "{ interval: 100ms, drain_timeout: 5s }",
        None,
    );
    let message = worker
        .run
        .wait_exit(HANG)
        .expect("the run exits")
        .unwrap_err()
        .to_string();
    assert!(message.contains("checkpoint.drain_timeout"), "{message}");
}

/// Two deliveries of one unit are read end to end: each row written, each
/// delivery acknowledged and recorded with its count.
#[test]
fn a_worker_reads_each_delivery_and_records_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("a", 2);
    let unit = stage(&data, "a.ndjson", &rows);
    let (feed, script) = scripted_feed();
    let first = script.deliver(unit.clone());
    let worker = launch(&data, feed, "");
    acknowledged(&script, 1);
    let second = script.deliver(unit);
    acknowledged(&script, 2);

    assert_eq!(worker.recording.deliveries(), [1, 2]);
    assert_eq!(worker.recording.receipts(), [first, second]);
    assert_eq!(
        written(&worker.script),
        sorted(rows.iter().chain(&rows).cloned().collect())
    );
    assert_eq!(worker.stop(), ExitState::Completed);
}

/// The stop triggers the shutdown, and the feed sees one stop and one close.
#[test]
fn stop_ends_the_run_and_the_feed_sees_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    script.hand_back_on_stop();
    script.deliver(stage(&data, "a.ndjson", &recs("a", 1)));
    let worker = launch(&data, feed, "");
    acknowledged(&script, 1);
    assert_eq!(worker.stop(), ExitState::Completed);
    assert_eq!(worker.recording.stopped_receiving(), 1);
    assert_eq!(worker.recording.closes().len(), 1);
}

/// `pre` scripts the sink before data flows, and `customize` is applied to
/// the source.
#[test]
fn pre_and_customize_reach_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    script.deliver(stage(&data, "held.ndjson", &recs("h", 1)));
    let worker = launch_customized(
        move |_, _| Ok(feed),
        &file_source(&data, ""),
        SCRIPTED,
        |sink| sink.enqueue_global(WriteOutcome::ok().held()),
        |source| source,
        CHECKPOINT,
        None,
    );
    wait_until(HANG, "the first write is held", || {
        worker.script.held_writes() == 1
    });
    worker.script.release_held();
    acknowledged(&script, 1);
    worker.stop();

    // A framer whose longest line is shorter than every record fails the unit.
    let (feed, script) = scripted_feed();
    let bad = script.deliver(stage(&data, "long.ndjson", &recs("l", 1)));
    let worker = launch_customized(
        move |_, _| Ok(feed),
        &file_source(&data, ""),
        SCRIPTED,
        |_| {},
        |source| source.with_framer(|| Box::new(NdjsonFramer::new(16))),
        CHECKPOINT,
        None,
    );
    wait_until(HANG, "the unit fails", || {
        script.failed().iter().any(|(r, _)| *r == bad)
    });
    assert!(written(&worker.script).is_empty());
    worker.stop();
}

/// A unit staged with its ETag is read in full under [`TWO_RANGES`].
#[test]
fn a_two_range_unit_is_read_in_full() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (unit, rows) = two_range_unit(&data, "big");
    let descriptor = SplitDescriptor::decode(&unit.body).unwrap();
    assert!(descriptor.objects[0].etag.is_some());
    assert_eq!(rows.len(), 68);
    // Each line is 16,386 bytes with its newline, so the object fills one
    // 1 MiB range and part of a second.
    assert_eq!(descriptor.objects[0].size, 68 * 16_386);
    let (feed, script) = scripted_feed();
    script.deliver(unit);
    let worker = launch(&data, feed, TWO_RANGES);
    acknowledged(&script, 1);
    assert_eq!(written(&worker.script), sorted(rows));
    worker.stop();
}

#[test]
fn stage_with_etag_records_the_store_s_etag() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let plain = stage(&data, "a.ndjson", &recs("a", 1));
    let tagged = stage_with_etag(&data, "a.ndjson", &recs("a", 1));
    let plain = SplitDescriptor::decode(&plain.body).unwrap();
    let tagged = SplitDescriptor::decode(&tagged.body).unwrap();
    assert_eq!(plain.objects[0].etag, None);
    assert_eq!(tagged.objects[0].key, plain.objects[0].key);
    assert_eq!(tagged.objects[0].size, plain.objects[0].size);
    assert!(tagged.objects[0].etag.is_some());
}

#[test]
fn the_pipeline_file_carries_each_section() {
    let yaml = pipeline_yaml(
        &file_source(Path::new("/d"), "    x: 1\n"),
        SCRIPTED,
        "{ interval: 7s }",
    );
    let config = PipelineConfig::from_str(&yaml).unwrap();
    assert_eq!(config.pipeline.name, "feed-worker");
    assert!(yaml.contains("checkpoint: { interval: 7s }"), "{yaml}");
    assert!(
        yaml.contains("  s3:\n    url: \"file:///d/\"\n    x: 1\n"),
        "{yaml}"
    );
    assert!(config.feed.is_some());
    let feed: PipelineConfigFeed = serde_yaml::from_str(SCRIPTED).unwrap();
    assert_eq!(feed.feed.backend().type_tag(), "scripted");
}

// ------------------------------------------------------------ stalled --

/// A bounded job whose one unit is on the dead-letter destination, run until
/// it ends.
fn stalled_report() -> ExitReport {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let jobs = dir.path().join("jobs");
    let section = format!("{SCRIPTED}  location: \"file://{}/\"\n", jobs.display());

    // The lister writes the job's fingerprint and end marker.
    let config = PipelineConfig::from_str(&pipeline_yaml(
        &file_source(&data, ""),
        &section,
        CHECKPOINT,
    ))
    .unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (lister, _) = scripted_feed();
    S3Source::from_component_config(&config.source, rt.handle().clone())
        .unwrap()
        .with_framer(|| Box::new(NdjsonFramer::new(1 << 20)))
        .list_once(config.feed.as_ref().unwrap(), Arc::new(lister))
        .unwrap();

    let (feed, script) = scripted_feed();
    let poison = script.deliver(Unit::new("poison", b"x".to_vec()));
    script.dead_letter(poison);
    let clock = TestClock::frozen();
    let worker = {
        let clock = Arc::clone(&clock);
        launch_customized(
            move |_, _| Ok(feed),
            &file_source(&data, ""),
            &section,
            |_| {},
            move |source| source.with_clock(clock),
            CHECKPOINT,
            None,
        )
    };
    let mut exit = None;
    for _ in 0..100 {
        let reads = worker.recording.depth_reads();
        clock.advance(STEP);
        wait_until(HANG, "the next depth read or the end", || {
            exit = worker.run.wait_exit(Duration::ZERO);
            exit.is_some() || worker.recording.depth_reads() > reads
        });
        if exit.is_some() {
            break;
        }
    }
    exit.expect("the job ends").expect("the run starts")
}

#[test]
fn a_stalled_job_passes_the_stalled_check() {
    assert_stalled(
        &stalled_report(),
        &["the scripted dead-letter destination", "1 visible"],
    );
}

#[test]
#[should_panic(expected = "no \"2 visible\" in")]
fn the_stalled_check_wants_every_needle() {
    assert_stalled(&stalled_report(), &["1 visible", "2 visible"]);
}

/// A failed run whose reason is not a stall fails the check, whatever the
/// needles.
#[test]
#[should_panic(expected = "refused by the feed script")]
fn the_stalled_check_wants_a_stall() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    script.refuse_next_receive(FeedErrorKind::Fatal);
    let worker = launch(&data, feed, "");
    let report = worker
        .run
        .wait_exit(HANG)
        .expect("the run exits")
        .expect("the run starts");
    assert_eq!(report.exit_code(), 1);
    assert_stalled(&report, &[]);
}

/// A run that completes fails the check on its exit code.
#[test]
#[should_panic(expected = "right: 1")]
fn the_stalled_check_wants_a_failed_exit() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, _script) = scripted_feed();
    let worker = launch(&data, feed, "");
    assert!(format!("{worker:?}").starts_with("Worker {"));
    worker.shutdown.trigger();
    let report = worker
        .run
        .wait_exit(HANG)
        .expect("the run exits")
        .expect("the run starts");
    assert_stalled(&report, &[]);
}
