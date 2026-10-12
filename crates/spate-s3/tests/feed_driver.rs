//! The feed read path: the source reads one-object work units from a
//! scripted feed or the in-process feed, over real `file://` objects, and
//! the feed observes every acknowledgment, failure, renewal and `Gone`
//! answer. Each test ends the run with a shutdown, since this path never
//! reports `Drained`.

mod support;

use spate_core::checkpoint::Checkpointer;
use spate_core::config::PipelineConfig;
use spate_core::error::{ErrorClass, SourceError};
use spate_core::feed::memory::{FeedCounts, MemoryFeed};
use spate_core::feed::{
    Confirmation, Delivery, Feed, FeedError, FeedErrorKind, FeedWaker, Receipt, Unit,
};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{ExitState, Pipeline, RuntimeOptions};
use spate_core::sink::{KeyHashRouter, SinkPoolConfig};
use spate_core::source::{Source, SourceCtx};
use spate_s3::{DescriptorObject, S3Source, SplitDescriptor};
use spate_test::{
    BytesPassthrough, FeedScript, PipelineRun, SinkScript, TestEncoder, WriteOutcome, capture_sink,
    scripted_feed, wait_until,
};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{
    Launched, PipelineYaml, captured_rows, key_of, launch_customized, line_framer, lines_bytes,
    recs, sorted, test_options,
};

const DEADLINE: Duration = Duration::from_secs(30);

/// A unit whose body is a one-member descriptor over `name` under `data`,
/// with no ETag pin.
fn unit_over(data: &Path, name: &str) -> Unit {
    let object = DescriptorObject {
        key: key_of(data, name),
        size: fs::metadata(data.join(name)).map_or(10, |m| m.len()),
        etag: None,
        last_modified_ms: 1,
    };
    let body = SplitDescriptor::new(vec![object]).encode().unwrap();
    Unit::new(name, body)
}

fn stage(data: &Path, name: &str, rows: &[String]) {
    fs::write(data.join(name), lines_bytes(rows)).unwrap();
}

fn data_dir(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    data
}

fn launch_on_feed(data: &Path, feed: Arc<dyn Feed>, pre: impl FnOnce(&SinkScript)) -> Launched {
    let yaml = PipelineYaml::file("s3-feed-test", data).build();
    launch_customized(&yaml, test_options(), pre, move |source, _io| {
        line_framer(source).with_feed(feed)
    })
}

fn stop(l: &Launched) {
    l.shutdown.trigger();
    let report = l.run.wait_exit(DEADLINE).expect("the run exits").unwrap();
    assert_eq!(report.state, ExitState::Completed);
}

/// Rows the sink wrote successfully, decoded to strings.
fn written_rows(script: &SinkScript) -> Vec<String> {
    script
        .writes()
        .iter()
        .filter(|w| w.result == spate_test::ScriptedResult::Ok)
        .flat_map(|w| spate_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8(r).unwrap())
        .collect()
}

fn renewals_of(script: &FeedScript, receipt: Receipt) -> usize {
    script.renewed().iter().filter(|r| **r == receipt).count()
}

/// Pins that a unit delivered again while its first delivery is still in
/// flight is read again from its first record under the new receipt, that
/// only the new receipt is acknowledged, and that the lapsed one is let go.
#[test]
fn a_redelivered_unit_is_read_again_from_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("r", 400);
    stage(&data, "r.ndjson", &rows);

    let (feed, script) = scripted_feed();
    let first = script.deliver(unit_over(&data, "r.ndjson"));
    // Hold the first write so the first delivery's records are still in
    // flight when the redelivery arrives.
    let l = launch_on_feed(&data, Arc::new(feed), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    wait_until(DEADLINE, "the first delivery's write is held", || {
        l.script.held_writes() == 1
    });
    assert!(
        script.acknowledged().is_empty(),
        "nothing is acknowledged yet"
    );

    let second = script.redeliver(first);
    l.script.release_held();
    wait_until(DEADLINE, "the redelivery is acknowledged", || {
        script.acknowledged() == vec![second]
    });
    wait_until(DEADLINE, "the lapsed receipt is answered Gone", || {
        script.gone().contains(&first)
    });
    stop(&l);

    assert_eq!(script.acknowledged(), vec![second], "only the new receipt");
    let got = captured_rows(&l.script);
    assert!(
        got.iter().filter(|r| **r == rows[0]).count() >= 2,
        "the redelivery is read from its first record"
    );
    let mut seen = sorted(got);
    seen.dedup();
    assert_eq!(seen, sorted(rows), "every record at least once");
}

/// Pins that two deliveries of one unit held at once are each read in full
/// and each acknowledged under its own receipt.
#[test]
fn a_duplicate_delivery_of_a_held_unit_is_read_twice() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("d", 50);
    stage(&data, "d.ndjson", &rows);

    let (feed, script) = scripted_feed();
    let first = script.deliver(unit_over(&data, "d.ndjson"));
    let second = script.deliver(unit_over(&data, "d.ndjson"));
    let l = launch_on_feed(&data, Arc::new(feed), |_| {});

    wait_until(DEADLINE, "both receipts acknowledged", || {
        script.acknowledged().len() == 2
    });
    stop(&l);

    assert_eq!(sorted_receipts(script.acknowledged()), vec![first, second]);
    assert!(script.gone().is_empty(), "neither receipt lapsed");
    let twice: Vec<String> = sorted(rows.iter().chain(&rows).cloned().collect());
    assert_eq!(
        sorted(captured_rows(&l.script)),
        twice,
        "every record twice"
    );
}

fn sorted_receipts(mut receipts: Vec<Receipt>) -> Vec<Receipt> {
    receipts.sort();
    receipts
}

/// Pins publish confirmation through `Arc<dyn Feed>` while the source reads
/// the same feed: a refused publish reads `Refused` and is never delivered,
/// and a held publish reads pending until the feed confirms it, after which
/// the source reads the unit and acknowledges it.
#[test]
fn a_publish_is_confirmed_through_the_feed_trait() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "early.ndjson", &recs("early", 5));
    stage(&data, "refused.ndjson", &recs("refused", 5));
    let held_rows = recs("held", 20);
    stage(&data, "held.ndjson", &held_rows);

    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    let early = script.deliver(unit_over(&data, "early.ndjson"));
    let l = launch_on_feed(&data, Arc::clone(&feed), |_| {});
    // The source has received and finished a unit before anything is
    // published, so the publishes below reach a reader already polling.
    wait_until(DEADLINE, "the scripted unit is acknowledged", || {
        script.acknowledged() == vec![early]
    });

    script.refuse_next_publish(FeedErrorKind::Refused);
    let mut refused = feed.publish(unit_over(&data, "refused.ndjson"));
    let outcome = refused.try_outcome().expect("a refusal settles at once");
    assert_eq!(outcome.unwrap_err().kind, FeedErrorKind::Refused);

    script.hold_publishes();
    let mut held = feed.publish(unit_over(&data, "held.ndjson"));
    assert!(held.try_outcome().is_none(), "a held publish is pending");
    script.confirm_held_publishes();
    assert!(matches!(held.try_outcome(), Some(Ok(()))));

    wait_until(DEADLINE, "the confirmed unit is acknowledged", || {
        script.acknowledged().len() == 2
    });
    stop(&l);

    let delivered: Vec<String> = script.delivered().into_iter().map(|d| d.unit.id).collect();
    assert_eq!(delivered, ["early.ndjson", "held.ndjson"]);
    let got = captured_rows(&l.script);
    assert_eq!(
        got.iter().filter(|r| r.contains("held-")).count(),
        held_rows.len(),
        "the confirmed unit is read once"
    );
    assert!(!got.iter().any(|r| r.contains("refused-")));
}

/// Pins INV-1 on the feed path: while the sink keeps failing the unit's
/// records, the source renews the unit and does not acknowledge it, though
/// its lane has read to the end; once a write succeeds, it acknowledges.
#[test]
fn a_unit_is_acknowledged_only_after_its_last_record_is_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("slow", 10);
    stage(&data, "slow.ndjson", &rows);

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "slow.ndjson"));
    // Three failures open the sink's breaker, so the first successful write
    // waits out its open window.
    let l = launch_on_feed(&data, Arc::new(feed), |sink| {
        for _ in 0..3 {
            sink.enqueue_global(WriteOutcome::retryable("sink down"));
        }
    });

    wait_until(DEADLINE, "the unit is acknowledged", || {
        !script.acknowledged().is_empty()
    });
    // Read at once: an acknowledgment that came before the sink's first
    // successful write finds no rows here.
    let written = written_rows(&l.script);
    assert_eq!(
        sorted(written),
        sorted(rows),
        "the unit was acknowledged before the sink acknowledged its records"
    );
    assert_eq!(script.acknowledged(), vec![receipt]);
    assert!(
        renewals_of(&script, receipt) >= 2,
        "the unit was renewed while its records were unacknowledged"
    );
    stop(&l);
}

/// Pins the in-process feed end to end: a unit with rows and a zero-byte
/// unit are acknowledged, and a unit over a missing object, a unit whose body
/// names two objects, a unit whose body is not a descriptor and a unit that
/// names a byte range are failed on every delivery until the feed
/// dead-letters them.
#[test]
fn in_process_units_are_acknowledged_or_dead_lettered_at_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("m", 30);
    stage(&data, "rows.ndjson", &rows);
    fs::write(data.join("empty.ndjson"), b"").unwrap();
    stage(&data, "other.ndjson", &recs("o", 3));

    let feed = MemoryFeed::new();
    let two_members = {
        let member = |name: &str| DescriptorObject {
            key: key_of(&data, name),
            size: fs::metadata(data.join(name)).unwrap().len(),
            etag: None,
            last_modified_ms: 1,
        };
        let body = SplitDescriptor::new(vec![member("rows.ndjson"), member("other.ndjson")]);
        Unit::new("two-members", body.encode().unwrap())
    };
    for unit in [
        unit_over(&data, "rows.ndjson"),
        unit_over(&data, "empty.ndjson"),
        unit_over(&data, "missing.ndjson"),
        two_members,
        Unit::new("not-a-descriptor", b"{ not a descriptor".to_vec()),
        Unit::new(
            "ranged",
            support::ranged_spec(&data, "other.ndjson", 0, 12, b'\n').descriptor,
        ),
    ] {
        drop(feed.publish(unit));
    }
    let l = launch_on_feed(&data, Arc::new(feed.clone()), |_| {});

    let counts = |c: FeedCounts| (c.visible, c.held, c.dead_lettered, c.acknowledged);
    wait_until(DEADLINE, "every unit settled", || {
        let c = feed.counts();
        c.visible == 0 && c.held == 0
    });
    stop(&l);
    assert_eq!(counts(feed.counts()), (0, 0, 4, 2));

    let mut dead: Vec<String> = feed.dead_letters().into_iter().map(|u| u.id).collect();
    dead.sort();
    assert_eq!(
        dead,
        [
            "missing.ndjson",
            "not-a-descriptor",
            "ranged",
            "two-members"
        ]
    );
    let got = captured_rows(&l.script);
    assert_eq!(sorted(got), sorted(rows), "the rows unit is read once");
}

/// Pins that a source reading a feed refuses a `coordination:` section when
/// the runtime offers it, and refuses a coordinator handed to it in code
/// when it opens.
#[test]
#[expect(deprecated, reason = "v0.4.0 removes this with spate-coordination")]
fn a_feed_refuses_coordination_on_the_same_source() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let yaml = PipelineYaml::file("s3-feed-conflict", &data)
        .section(&support::unreachable_nats("feed-conflict"))
        .build();
    let config = PipelineConfig::from_str(&yaml).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let source = || {
        let source = S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap();
        line_framer(source).with_feed(Arc::new(MemoryFeed::new()))
    };

    let section = config.coordination.as_ref().expect("the section parses");
    let err = source().configure_coordination(section).unwrap_err();
    assert!(err.to_string().contains("feed"), "{err}");

    let (coordinator, _script) = spate_test::scripted_coordinator();
    let mut both = source().with_coordinator(Box::new(coordinator));
    let err = both
        .open(SourceCtx::new(Checkpointer::new().handle()))
        .unwrap_err();
    let SourceError::Client { class, reason } = err else {
        panic!("a client error, got {err:?}");
    };
    assert_eq!(class, ErrorClass::Fatal);
    assert!(reason.contains("coordinator"), "{reason}");
}

/// Launch over `feed` with one record per source batch and one row per sink
/// write, so an object's records span many sink writes.
fn launch_one_row_writes(
    data: &Path,
    feed: Arc<dyn Feed>,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let yaml = PipelineYaml::file("s3-feed-row-writes", data)
        .threads(1)
        .build()
        .replace(
            "sink: { capture: {} }",
            "sink: { capture: { chunk: { target_bytes: 64 } } }",
        );
    let mut config = PipelineConfig::from_str(&yaml).unwrap();
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).unwrap();
    let io = pipeline.io_handle();
    let source =
        line_framer(S3Source::from_component_config(&pipeline.config().source, io).unwrap())
            .with_feed(feed);
    let mut pool = SinkPoolConfig::default();
    pool.batch.max_rows = 1;
    pool.batch.linger = Duration::from_millis(10);
    let (sink, script) = capture_sink(1, 1);
    pre(&script);
    let runtime = pipeline
        .sink(sink.with_pool_config(pool))
        .unwrap()
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            max_records: 4,
            ..test_options()
        })
        .into_runtime(source)
        .unwrap();
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    Launched {
        run,
        script,
        shutdown,
    }
}

/// Pins that a unit whose records span several sink writes, one of them
/// held and then released, is acknowledged with every row written.
#[test]
fn a_unit_over_several_writes_is_acknowledged_with_every_row_written() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("p", 40);
    stage(&data, "p.ndjson", &rows);

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "p.ndjson"));
    let l = launch_one_row_writes(&data, Arc::new(feed), |sink| {
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok());
        sink.enqueue_global(WriteOutcome::ok().held());
    });

    wait_until(DEADLINE, "the third write is held", || {
        l.script.held_writes() == 1
    });
    assert!(
        script.acknowledged().is_empty(),
        "the unit was acknowledged while one of its writes was held"
    );
    let writes = l.script.writes();
    assert!(
        writes[..2]
            .iter()
            .all(|w| w.result == spate_test::ScriptedResult::Ok),
        "the writes before the held one succeeded"
    );

    l.script.release_held();
    wait_until(DEADLINE, "the unit is acknowledged", || {
        !script.acknowledged().is_empty()
    });
    assert_eq!(script.acknowledged(), vec![receipt]);
    assert_eq!(sorted(written_rows(&l.script)), sorted(rows));
    stop(&l);
}

/// Pins that a receipt answered `Gone` on renewal is let go: the reader
/// neither renews nor acknowledges it again, and the redelivery is read and
/// acknowledged under its own receipt.
#[test]
fn a_receipt_answered_gone_on_renewal_is_let_go() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("g", 40);
    stage(&data, "g.ndjson", &rows);

    let (feed, script) = scripted_feed();
    let first = script.deliver(unit_over(&data, "g.ndjson"));
    // Holding the first write keeps the first delivery short of its terminal
    // watermark, so only a renewal can learn that its receipt lapsed.
    let l = launch_on_feed(&data, Arc::new(feed), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    wait_until(DEADLINE, "the first delivery's write is held", || {
        l.script.held_writes() == 1
    });

    let second = script.redeliver(first);
    wait_until(DEADLINE, "a renewal is answered Gone", || {
        script.gone().contains(&first)
    });
    l.script.release_held();
    wait_until(DEADLINE, "the redelivery is acknowledged", || {
        script.acknowledged() == vec![second]
    });
    stop(&l);

    assert_eq!(
        script.gone(),
        vec![first],
        "the lapsed receipt was used after Gone"
    );
    let mut seen = sorted(captured_rows(&l.script));
    seen.dedup();
    assert_eq!(seen, sorted(rows), "every record at least once");
}

/// A feed over [`MemoryFeed`] that answers the first `acknowledge` and the
/// first `fail` of each receipt with `Retryable`.
struct FirstCallRetryable {
    inner: MemoryFeed,
    seen: Mutex<HashSet<(bool, Receipt)>>,
}

impl FirstCallRetryable {
    fn first(&self, ack: bool, receipt: Receipt) -> bool {
        self.seen.lock().unwrap().insert((ack, receipt))
    }
}

impl Feed for FirstCallRetryable {
    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        self.inner.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        if self.first(true, receipt) {
            return Err(FeedError::new(FeedErrorKind::Retryable, "throttled"));
        }
        self.inner.acknowledge(receipt)
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        if self.first(false, receipt) {
            return Err(FeedError::new(FeedErrorKind::Retryable, "throttled"));
        }
        self.inner.fail(receipt, reason)
    }
    fn publish(&self, unit: Unit) -> Confirmation {
        self.inner.publish(unit)
    }
}

/// Pins that an `acknowledge` or `fail` answered `Retryable` is sent again:
/// a unit with rows is acknowledged, and a unit over a missing object and an
/// undecodable unit are failed until they dead-letter.
#[test]
fn a_retryable_answer_is_sent_again() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "rows.ndjson", &recs("y", 10));
    let inner = MemoryFeed::new();
    drop(inner.publish(unit_over(&data, "rows.ndjson")));
    drop(inner.publish(unit_over(&data, "missing.ndjson")));
    drop(inner.publish(Unit::new("not-a-descriptor", b"{".to_vec())));
    let feed = FirstCallRetryable {
        inner: inner.clone(),
        seen: Mutex::default(),
    };
    let l = launch_on_feed(&data, Arc::new(feed), |_| {});

    let counts = |c: FeedCounts| (c.visible, c.held, c.dead_lettered, c.acknowledged);
    wait_until(DEADLINE, "acknowledged and dead-lettered", || {
        counts(inner.counts()) == (0, 0, 2, 1)
    });
    stop(&l);
}

/// A feed over [`MemoryFeed`] that answers the first `receive` and the first
/// `renew` of each receipt with `Retryable`.
struct RetryableReceiveAndRenew {
    inner: MemoryFeed,
    receives: Mutex<usize>,
    refused: Mutex<HashSet<Receipt>>,
}

impl Feed for RetryableReceiveAndRenew {
    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        let mut receives = self.receives.lock().unwrap();
        *receives += 1;
        if *receives == 1 {
            return Err(FeedError::new(FeedErrorKind::Retryable, "throttled"));
        }
        drop(receives);
        self.inner.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        if self.refused.lock().unwrap().insert(receipt) {
            return Err(FeedError::new(FeedErrorKind::Retryable, "throttled"));
        }
        self.inner.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.acknowledge(receipt)
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.inner.fail(receipt, reason)
    }
    fn publish(&self, unit: Unit) -> Confirmation {
        self.inner.publish(unit)
    }
}

/// Pins that a `receive` or a renewal answered `Retryable` is tried again:
/// the run goes on and the unit it holds is acknowledged.
#[test]
fn a_retryable_receive_or_renewal_is_tried_again() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "rows.ndjson", &recs("k", 10));
    let inner = MemoryFeed::new();
    drop(inner.publish(unit_over(&data, "rows.ndjson")));
    let feed = Arc::new(RetryableReceiveAndRenew {
        inner: inner.clone(),
        receives: Mutex::default(),
        refused: Mutex::default(),
    });
    let l = launch_on_feed(&data, feed.clone(), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    wait_until(DEADLINE, "the write is held", || {
        l.script.held_writes() == 1
    });
    wait_until(DEADLINE, "a renewal is refused", || {
        !feed.refused.lock().unwrap().is_empty()
    });
    l.script.release_held();
    wait_until(DEADLINE, "the unit is acknowledged", || {
        inner.counts().acknowledged == 1
    });
    stop(&l);
}
