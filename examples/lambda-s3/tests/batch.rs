//! The batch item failures `run_batch` reports, over a `file://` store and a
//! capture sink.
//!
//! On a `file://` source no notification names the source's bucket, so a
//! notification unit is dead-lettered, while a descriptor unit reads the
//! object it names and the S3 test event is acknowledged with nothing read.

use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use spate::clickhouse::ClickHouseSinkConfig;
use spate::clock::TestClock;
use spate::feed::Feed;
use spate::pipeline::ShutdownHandle;
use spate::prelude::*;
use spate::s3::{S3Source, S3SourceConfig};
use spate_example_lambda_s3::{
    BoxError, CONFIG, MissingId, RESPONSE_MARGIN, RUNTIME_SHUTDOWN, TooShort, handle_batch,
    messages, run_batch, run_seeded, stop_at,
};
use spate_test::{WriteOutcome, capture_sink};
use std::fs;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

mod support;
use support::*;

/// Pins that acknowledged units are not reported: a test event and a
/// readable object leave no failure, and the object's rows are written.
#[tokio::test]
async fn a_read_batch_reports_no_failure() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1", "a2", "a3"]);
    let (sink, script) = capture_sink(1, 1);
    let batch = vec![
        message("t", TEST_EVENT),
        message("a", descriptor(&data, "a.ndjson")),
    ];
    let failed = bounded(run_batch(
        batch,
        assemble(yaml(&data, "{ memory: {} }"), sink),
        pending(),
    ))
    .await;
    assert_eq!(failed, Vec::<String>::new());
    assert_eq!(rows(&script), ["a1", "a2", "a3"]);
}

/// Pins that a dead-lettered unit is reported: a notification for a bucket
/// the source does not read fails on its one delivery, and only its message
/// is returned.
#[tokio::test]
async fn a_dead_lettered_unit_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    let (sink, _script) = capture_sink(1, 1);
    let batch = vec![message("t", TEST_EVENT), message("n", FOREIGN)];
    let failed = bounded(run_batch(
        batch,
        assemble(yaml(&data, "{ memory: {} }"), sink),
        pending(),
    ))
    .await;
    assert_eq!(failed, ["n"]);
}

/// Pins that the units a stop leaves are reported, and only those. With one
/// unit held at a time, the test event is acknowledged, the next unit's write
/// is held for good, and the unit after it is never received. The stop passes
/// its drain deadline on the stop clock, so the run exits `Completed` with
/// both units unsettled.
#[tokio::test]
async fn the_units_a_stop_leaves_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    fs::write(data.join("b.ndjson"), "{\"id\":\"b1\"}\n").unwrap();
    let (sink, script) = capture_sink(1, 1);
    script.enqueue_global(WriteOutcome::ok().held());
    let batch = vec![
        message("t", TEST_EVENT),
        message("a", descriptor(&data, "a.ndjson")),
        message("b", descriptor(&data, "b.ndjson")),
    ];
    let clock = TestClock::frozen();
    let handle: Arc<Mutex<Option<ShutdownHandle>>> = Arc::default();
    let kept = Arc::clone(&handle);
    let stop_clock = Arc::clone(&clock);
    let assembled = assemble(yaml(&data, "{ memory: {}, units_held: 1 }"), sink);
    let start = move |feed: Arc<dyn Feed>| {
        let (pipeline, source) = assembled()?;
        let runtime = pipeline
            .seeded_feed(feed)
            .into_runtime(source)?
            .with_stop_clock(stop_clock);
        *kept.lock().unwrap() = Some(runtime.shutdown_handle());
        Ok::<_, BoxError>(runtime)
    };
    let held = script.clone();
    let stop = async move {
        while held.held_writes() == 0 {
            tokio::task::yield_now().await;
        }
        // The deadline counts from the first trigger, so the clock moves
        // past it only once the stop has begun.
        handle
            .lock()
            .unwrap()
            .as_ref()
            .expect("assembled")
            .trigger();
        clock.advance(DRAIN_TIMEOUT + Duration::from_secs(1));
    };
    let failed = bounded(run_seeded(batch, start, stop)).await;
    assert_eq!(failed, ["a", "b"]);
    assert_eq!(rows(&script), Vec::<String>::new());
}

/// A unit whose batch the sink abandons is reported without a stop: the
/// source fails it back, the feed dead-letters it, and the test event is
/// done.
#[tokio::test]
async fn a_unit_whose_batch_the_sink_abandons_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    let (sink, script) = capture_sink(1, 1);
    script.enqueue_global(WriteOutcome::fatal("refused"));
    let batch = vec![
        message("t", TEST_EVENT),
        message("a", descriptor(&data, "a.ndjson")),
    ];
    let failed = bounded(run_batch(
        batch,
        assemble(yaml(&data, "{ memory: {} }"), sink),
        pending(),
    ))
    .await;
    assert_eq!(failed, ["a"]);
}

/// Pins that a failed run reports every message, the acknowledged test
/// event too.
#[tokio::test]
async fn a_failed_run_reports_every_message() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1", POISON]);
    let (sink, _script) = capture_sink(1, 1);
    let batch = vec![
        message("t", TEST_EVENT),
        message("a", descriptor(&data, "a.ndjson")),
    ];
    let failed = bounded(run_batch(
        batch,
        assemble(yaml(&data, "{ memory: {} }"), sink),
        pending(),
    ))
    .await;
    assert_eq!(failed, ["t", "a"]);
}

/// Pins that a pipeline that cannot be built reports every message.
#[tokio::test]
async fn a_batch_whose_pipeline_does_not_build_reports_every_message() {
    let batch = vec![message("t", TEST_EVENT), message("u", TEST_EVENT)];
    let failed = bounded(run_batch(
        batch,
        || Err::<(Pipeline, S3Source), BoxError>("no pipeline".into()),
        pending(),
    ))
    .await;
    assert_eq!(failed, ["t", "u"]);
}

/// Pins that two batches run one after the other in one process, as in a
/// warm execution environment, each reporting its own failures.
#[tokio::test]
async fn a_second_batch_in_the_same_process_runs() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    for round in ["1", "2"] {
        let (sink, script) = capture_sink(1, 1);
        let batch = vec![
            message(&format!("a{round}"), descriptor(&data, "a.ndjson")),
            message(&format!("n{round}"), FOREIGN),
        ];
        let failed = bounded(run_batch(
            batch,
            assemble(yaml(&data, "{ memory: {} }"), sink),
            pending(),
        ))
        .await;
        assert_eq!(failed, [format!("n{round}")]);
        assert_eq!(rows(&script), ["a1"]);
    }
}

fn sqs(id: Option<&str>, body: Option<&str>) -> SqsMessage {
    let mut m = SqsMessage::default();
    m.message_id = id.map(str::to_owned);
    m.body = body.map(str::to_owned);
    m
}

/// Pins the batch's messages in order, with a missing body read as empty.
#[test]
fn a_batch_maps_to_messages_in_order() {
    let mut event = SqsEvent::default();
    event.records = vec![sqs(Some("x"), Some("{}")), sqs(Some("y"), None)];
    assert_eq!(
        messages(event),
        Ok(vec![message("x", "{}"), message("y", "")])
    );
}

/// Pins that a message with no id fails the whole batch.
#[test]
fn a_message_with_no_id_refuses_the_batch() {
    let mut event = SqsEvent::default();
    event.records = vec![sqs(Some("x"), Some("{}")), sqs(None, Some("{}"))];
    assert_eq!(messages(event), Err(MissingId { position: 1 }));
}

const DRAIN: Duration = Duration::from_secs(10);

/// The default `checkpoint.drain_timeout`, which the test pipelines keep.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

fn needed() -> Duration {
    DRAIN + RUNTIME_SHUTDOWN + RESPONSE_MARGIN
}

/// Pins the stop instant: the deadline less the drain, the runtime shutdown
/// and the response margin.
#[test]
fn the_stop_leaves_room_for_the_drain() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let deadline = now + Duration::from_secs(60);
    assert_eq!(stop_at(now, deadline, DRAIN), Ok(deadline - needed()));
}

/// Pins that an invocation with exactly the stop's time left is refused,
/// since it would stop at once with nothing read.
#[test]
fn an_invocation_with_only_the_stop_time_left_is_refused() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    assert_eq!(
        stop_at(now, now + needed(), DRAIN),
        Err(TooShort {
            left: needed(),
            needed: needed()
        })
    );
    let one_more = now + needed() + Duration::from_millis(1);
    assert_eq!(
        stop_at(now, one_more, DRAIN),
        Ok(now + Duration::from_millis(1))
    );
}

/// Pins that an invocation already past its deadline is refused with no
/// time left.
#[test]
fn an_invocation_past_its_deadline_is_refused() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let deadline = now - Duration::from_secs(1);
    assert_eq!(
        stop_at(now, deadline, DRAIN),
        Err(TooShort {
            left: Duration::ZERO,
            needed: needed()
        })
    );
}

/// Pins that the function's pipeline file parses, with its source and sink
/// sections, once the environment fills its `${VAR}` forms.
#[test]
fn the_function_pipeline_file_parses() {
    let text = CONFIG
        .replace("${SPATE_S3_URL}", "s3://events/page-views/")
        .replace("${CLICKHOUSE_URL}", "http://clickhouse:8123");
    let config = PipelineConfig::from_str(&text).unwrap();
    assert_eq!(config.checkpoint.drain_timeout, Duration::from_secs(10));
    let source: S3SourceConfig = config.source.deserialize_into().unwrap();
    assert_eq!(source.url, "s3://events/page-views/");
    let _: ClickHouseSinkConfig = config
        .sink_config("default")
        .unwrap()
        .deserialize_into()
        .unwrap();
}

/// Pins the error a message with no id gives, which Lambda logs.
#[test]
fn a_missing_id_names_its_position() {
    assert_eq!(
        MissingId { position: 3 }.to_string(),
        "message 3 of the batch has no message id; failing the whole batch"
    );
}

/// Pins the error an invocation with too little time gives, which names
/// both settings that fix it.
#[test]
fn too_little_time_names_both_settings() {
    let short = TooShort {
        left: Duration::from_secs(5),
        needed: needed(),
    };
    assert_eq!(
        short.to_string(),
        "the invocation has 5s left and a stop needs 13s (checkpoint.drain_timeout, 2s to \
         shut the runtime down and 1s to answer); raise the function timeout or lower \
         checkpoint.drain_timeout"
    );
}

fn event(messages: &[(Option<&str>, &str)]) -> SqsEvent {
    let mut event = SqsEvent::default();
    event.records = messages
        .iter()
        .map(|(id, body)| sqs(*id, Some(body)))
        .collect();
    event
}

fn far_deadline() -> SystemTime {
    SystemTime::now() + Duration::from_secs(3_600)
}

/// Pins the invocation's answer: one batch item failure per message to
/// deliver again, named by its message id.
#[tokio::test]
async fn an_invocation_answers_with_its_failures() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    let (sink, _script) = capture_sink(1, 1);
    let response = bounded(handle_batch(
        event(&[(Some("t"), TEST_EVENT), (Some("n"), FOREIGN)]),
        far_deadline(),
        DRAIN,
        assemble(yaml(&data, "{ memory: {} }"), sink),
    ))
    .await
    .unwrap();
    let ids: Vec<&str> = response
        .batch_item_failures
        .iter()
        .map(|f| f.item_identifier.as_str())
        .collect();
    assert_eq!(ids, ["n"]);
}

/// Pins that an invocation stops its run at the stop instant. The write is
/// held for good, so only the handler's stop ends the run, and the unit is
/// unsettled whether the stop comes before or after its read. The deadline is
/// an hour out and the drain budget leaves 200ms before the stop, so a handler
/// that waits for the deadline runs into the hang bound.
#[tokio::test]
async fn an_invocation_stops_its_run_before_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let data = stage(dir.path(), "a.ndjson", &["a1"]);
    let (sink, script) = capture_sink(1, 1);
    script.enqueue_global(WriteOutcome::ok().held());
    let pipeline = yaml(&data, "{ memory: {} }") + "checkpoint: { drain_timeout: 100ms }\n";
    let body = descriptor(&data, "a.ndjson");
    let hour = Duration::from_secs(3_600);
    let drain = hour - RUNTIME_SHUTDOWN - RESPONSE_MARGIN - Duration::from_millis(200);
    let response = bounded(handle_batch(
        event(&[(Some("a"), &body)]),
        SystemTime::now() + hour,
        drain,
        assemble(pipeline, sink),
    ))
    .await
    .unwrap();
    let ids: Vec<&str> = response
        .batch_item_failures
        .iter()
        .map(|f| f.item_identifier.as_str())
        .collect();
    assert_eq!(ids, ["a"]);
}

/// Pins that an invocation with too little time left fails before it builds
/// a pipeline.
#[tokio::test]
async fn an_invocation_with_too_little_time_builds_nothing() {
    let error = handle_batch(
        event(&[(Some("t"), TEST_EVENT)]),
        SystemTime::now(),
        DRAIN,
        || -> Result<(Pipeline, S3Source), BoxError> { panic!("the pipeline was built") },
    )
    .await
    .unwrap_err();
    assert!(error.downcast_ref::<TooShort>().is_some(), "{error}");
}

/// Pins that an invocation whose batch has a message with no id fails as a
/// whole.
#[tokio::test]
async fn an_invocation_with_a_nameless_message_fails() {
    let error = handle_batch(
        event(&[(Some("t"), TEST_EVENT), (None, TEST_EVENT)]),
        far_deadline(),
        DRAIN,
        || -> Result<(Pipeline, S3Source), BoxError> { panic!("the pipeline was built") },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<MissingId>(),
        Some(&MissingId { position: 1 })
    );
}
