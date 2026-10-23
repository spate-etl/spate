//! The feed read path: the source reads work units naming one object or one
//! byte range, and storage notifications, from a scripted feed or the
//! in-process feed, over real files, and the feed observes every
//! acknowledgment, failure, renewal and `Gone` answer. Each run attaches its
//! feed with `with_feed`, a job with no end check, so each test ends it with a
//! shutdown.

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
use support::scripted_store::{HeadAnswer, ScriptedStore};
use support::spy::{SpyOptions, spying_local_store};
use support::{
    Launched, PipelineYaml, captured_rows, key_of, launch_customized, line_framer, lines_bytes,
    recs, sorted, stop, test_options, unit_over,
};

const DEADLINE: Duration = Duration::from_secs(30);

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

/// Pins the in-process feed end to end: a unit with rows, a zero-byte unit
/// and a unit naming a byte range cut on the framer's delimiter are
/// acknowledged, the range read alone. A unit over a missing object, a unit
/// whose body names two objects, a unit whose body is not a descriptor, a
/// range cut on another delimiter and a range of a compressed object are
/// failed on every delivery until the feed dead-letters them.
#[test]
fn in_process_units_are_acknowledged_or_dead_lettered_at_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("m", 30);
    stage(&data, "rows.ndjson", &rows);
    fs::write(data.join("empty.ndjson"), b"").unwrap();
    let other = recs("o", 3);
    stage(&data, "other.ndjson", &other);
    let gz = {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&lines_bytes(&recs("g", 3))).unwrap();
        enc.finish().unwrap()
    };
    fs::write(data.join("x.ndjson.gz"), &gz).unwrap();

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
        // `recs` rows are 12 bytes, so the range owns `o-0` alone.
        Unit::new(
            "ranged",
            support::ranged_spec(&data, "other.ndjson", 0, 12, b'\n').descriptor,
        ),
        Unit::new(
            "ranged-cr",
            support::ranged_spec(&data, "other.ndjson", 0, 12, b'\r').descriptor,
        ),
        Unit::new(
            "ranged-gz",
            support::ranged_spec(&data, "x.ndjson.gz", 0, gz.len() as u64, b'\n').descriptor,
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
    assert_eq!(counts(feed.counts()), (0, 0, 5, 3));

    let mut dead: Vec<String> = feed.dead_letters().into_iter().map(|u| u.id).collect();
    dead.sort();
    assert_eq!(
        dead,
        [
            "missing.ndjson",
            "not-a-descriptor",
            "ranged-cr",
            "ranged-gz",
            "two-members"
        ]
    );
    let got = captured_rows(&l.script);
    let mut want = rows;
    want.push(other[0].clone());
    assert_eq!(
        sorted(got),
        sorted(want),
        "the rows unit and the range are each read once"
    );
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

/// The ETag the file store reports for the staged object `name`.
fn etag_of(data: &Path, name: &str) -> String {
    use object_store::ObjectStoreExt as _;
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let key = key_of(data, name);
    let meta = rt
        .block_on(object_store::local::LocalFileSystem::new().head(&key.as_str().into()))
        .unwrap();
    meta.e_tag.expect("the file store reports an ETag")
}

/// Pins that the source over a feed reads a whole-object unit in the byte
/// ranges its `split_target_bytes` cuts: with a 1 MiB target, a unit over an
/// object of 1 MiB + 64 KiB with an ETag is read by two ranged lanes, the
/// second reading from the byte before its range, and every row is written
/// once.
#[test]
fn a_whole_unit_over_a_large_object_is_read_in_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    // 1,088 lines of 1 KiB: the ranges are [0, 557056) and [557056, 1114112).
    let pad = "p".repeat(1024 - 20);
    let rows: Vec<String> = (0..1088)
        .map(|i| format!("{{\"k\":\"{i:04}\",\"p\":\"{pad}\"}}"))
        .collect();
    stage(&data, "big.ndjson", &rows);
    let mut unit = unit_over(&data, "big.ndjson");
    let mut descriptor = SplitDescriptor::decode(&unit.body).unwrap();
    assert_eq!(descriptor.objects[0].size, 1088 * 1024);
    descriptor.objects[0].etag = Some(etag_of(&data, "big.ndjson"));
    unit.body = descriptor.encode().unwrap();

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit);
    let (store, spy) = spying_local_store(SpyOptions::default());
    let yaml = PipelineYaml::file("s3-feed-ranges", &data).build();
    let l = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _io| {
            line_framer(source)
                .with_store(store)
                .with_feed(Arc::new(feed))
        },
    );
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    stop(&l);

    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(rows),
        "each row is written once"
    );
    let starts: Vec<u64> = spy
        .gets()
        .iter()
        .filter_map(|g| match g.range {
            support::spy::RangeKind::Bounded(start, _) => Some(start),
            _ => None,
        })
        .collect();
    assert!(
        starts.contains(&0) && starts.contains(&(557_056 - 1)),
        "no GET opened the second range: {starts:?}"
    );
}

/// A framer that declares no resync delimiter.
#[derive(Default)]
struct NoResync(support::LineFramer);

impl spate_core::framing::RecordFramer for NoResync {
    fn push(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.push(bytes)
    }
    fn finish(&mut self) -> std::io::Result<()> {
        self.0.finish()
    }
    fn pop(&mut self) -> Option<Vec<u8>> {
        self.0.pop()
    }
    fn decoded_bytes(&self) -> u64 {
        self.0.decoded_bytes()
    }
}

/// Pins that a source opened over a feed with a framer that declares no
/// resync delimiter says that it reads large objects whole and fails ranged
/// units back, and that one whose framer declares a delimiter does not.
#[test]
fn a_framer_with_no_delimiter_is_reported_when_the_feed_path_opens() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let config =
        PipelineConfig::from_str(&PipelineYaml::file("s3-feed-resync", &data).build()).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let open_logs = |resync: bool| {
        let source = S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap();
        let source = if resync {
            line_framer(source)
        } else {
            source.with_framer(|| Box::new(NoResync::default()))
        };
        let mut source = source.with_feed(Arc::new(MemoryFeed::new()));
        spate_test::capture_logs(tracing::Level::INFO, || {
            source
                .open(SourceCtx::new(Checkpointer::new().handle()))
                .unwrap();
        })
    };
    let said = |lines: &[String]| {
        lines
            .iter()
            .any(|l| l.contains("units naming a byte range are failed back"))
    };
    let lines = open_logs(false);
    assert!(said(&lines), "{lines:#?}");
    let lines = open_logs(true);
    assert!(!said(&lines), "{lines:#?}");
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
    fn backend(&self) -> &str {
        self.inner.backend()
    }
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

    fn read_depth(&self, of: spate_core::feed::DepthOf) -> Option<spate_core::feed::DepthReading> {
        self.inner.read_depth(of)
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
    fn backend(&self) -> &str {
        self.inner.backend()
    }
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

    fn read_depth(&self, of: spate_core::feed::DepthOf) -> Option<spate_core::feed::DepthReading> {
        self.inner.read_depth(of)
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

/// Pins that a lane reaching the end of its unit wakes the control plane:
/// the unit is acknowledged long before the reader's 60 s park would time
/// out. The unit's GET is held until the reader has renewed it, so the
/// reader is parked when the lane ends.
#[test]
fn a_finished_unit_is_acknowledged_before_the_poll_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "w.ndjson", &recs("w", 30));

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "w.ndjson"));
    // A depth of two is never reached by one GET, so the gate holds it
    // until `open_gate`; the deadline outlasts every wait below.
    let (store, spy) = spying_local_store(SpyOptions {
        gate_depth: 2,
        gate_deadline: Duration::from_secs(300),
        ..SpyOptions::default()
    });

    // The checkpoint interval also bounds the chase that follows the lane's
    // signal. Past it, a sink write still in flight leaves the acknowledgment
    // to the 60 s park, so the interval outlasts any write under load.
    let yaml = PipelineYaml::file("s3-feed-wake", &data)
        .checkpoint("30s")
        .build();
    let mut config = PipelineConfig::from_str(&yaml).unwrap();
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).unwrap();
    let io = pipeline.io_handle();
    let source =
        line_framer(S3Source::from_component_config(&pipeline.config().source, io).unwrap())
            .with_store(store)
            .with_feed(Arc::new(feed));
    let mut pool = SinkPoolConfig::default();
    pool.batch.linger = Duration::from_millis(10);
    let (sink, sink_script) = capture_sink(1, 1);
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
            event_poll_timeout: Duration::from_secs(60),
            ..test_options()
        })
        .into_runtime(source)
        .unwrap();
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    wait_until(DEADLINE, "the reader renews the unit", || {
        renewals_of(&script, receipt) > 0
    });
    spy.open_gate();
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });

    shutdown.trigger();
    // The reader is parked for up to the poll timeout; a scripted call wakes
    // it to see the shutdown.
    script.hold_publishes();
    let report = run.wait_exit(DEADLINE).expect("the run exits").unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(sorted(written_rows(&sink_script)), sorted(recs("w", 30)));
}

/// A unit naming a zero-byte object whose key ends in `/` is acknowledged
/// without a read, as the planner leaves such an object out of a plan.
#[test]
fn a_zero_byte_folder_marker_unit_is_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let body = SplitDescriptor::new(vec![DescriptorObject {
        key: format!("{}/", key_of(&data, "folder")),
        size: 0,
        etag: None,
        last_modified_ms: 1,
    }])
    .encode()
    .unwrap();
    let feed = MemoryFeed::new();
    drop(feed.publish(Unit::new("folder-marker", body)));
    let l = launch_on_feed(&data, Arc::new(feed.clone()), |_| {});
    wait_until(DEADLINE, "the unit settled", || {
        let c = feed.counts();
        c.visible == 0 && c.held == 0
    });
    stop(&l);
    let c = feed.counts();
    assert_eq!((c.dead_lettered, c.acknowledged), (0, 1));
}

/// Pins that a source given a feed or a coordinator in code refuses a
/// `feed:` section when the runtime offers it.
#[test]
#[expect(deprecated, reason = "v0.4.0 removes this with spate-coordination")]
fn a_feed_section_is_refused_beside_a_feed_or_coordinator_given_in_code() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let yaml = PipelineYaml::file("s3-feed-section-conflict", &data)
        .section("feed:\n  memory: {}\n")
        .build();
    let config = PipelineConfig::from_str(&yaml).unwrap();
    let section = config.feed.as_ref().expect("the section parses");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let source = || {
        line_framer(S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap())
    };

    let err = source()
        .with_feed(Arc::new(MemoryFeed::new()))
        .configure_feed(
            section,
            Arc::new(MemoryFeed::new()),
            spate_core::feed::FeedOrigin::Section,
        )
        .unwrap_err();
    assert!(err.to_string().contains("with_feed"), "{err}");

    let (coordinator, _script) = spate_test::scripted_coordinator();
    let err = source()
        .with_coordinator(Box::new(coordinator))
        .configure_feed(
            section,
            Arc::new(MemoryFeed::new()),
            spate_core::feed::FeedOrigin::Section,
        )
        .unwrap_err();
    assert!(err.to_string().contains("with_coordinator"), "{err}");
}

/// A run over `<scheme>://b/data/` whose store is the directory `root`, so a
/// notification for bucket `b` names the file `root/<key>`.
fn launch_on_notifications(
    scheme: &str,
    root: &Path,
    feed: Arc<dyn Feed>,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let yaml = PipelineYaml::endpoint("s3-notification-test", "http://127.0.0.1:9")
        .build()
        .replace("\"s3://b/data/\"", &format!("\"{scheme}://b/data/\""));
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    launch_customized(&yaml, test_options(), pre, move |source, _io| {
        line_framer(source).with_store(store).with_feed(feed)
    })
}

/// A notification whose records each create the object of that size under
/// that key in bucket `b`, with no ETag, so a local file reads unpinned.
fn notification(objects: &[(&str, u64)]) -> Unit {
    let records: Vec<serde_json::Value> = objects
        .iter()
        .map(|(key, size)| {
            serde_json::json!({
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": { "bucket": { "name": "b" }, "object": { "key": key, "size": size } },
            })
        })
        .collect();
    Unit::new(
        "notification",
        serde_json::to_vec(&serde_json::json!({ "Records": records })).unwrap(),
    )
}

/// Pins that a notification naming two objects is acknowledged once, only
/// after the sink has acknowledged every record of both.
#[test]
fn a_message_with_two_records_is_acknowledged_after_both_units() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let first = recs("first", 4);
    let second = recs("second", 6);
    stage(&data, "first.ndjson", &first);
    stage(&data, "second.ndjson", &second);
    let size = |name: &str| fs::metadata(data.join(name)).unwrap().len();

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(notification(&[
        ("data/first.ndjson", size("first.ndjson")),
        ("data/second.ndjson", size("second.ndjson")),
    ]));
    // Three failures open the sink's breaker, so the first successful write
    // waits out its open window.
    let l = launch_on_notifications("s3", dir.path(), Arc::new(feed), |sink| {
        for _ in 0..3 {
            sink.enqueue_global(WriteOutcome::retryable("sink down"));
        }
    });

    wait_until(DEADLINE, "the notification is acknowledged", || {
        !script.acknowledged().is_empty()
    });
    // Read at once: an acknowledgment that came before either object's last
    // record was written finds rows missing here.
    let mut want = first.clone();
    want.extend(second.iter().cloned());
    assert_eq!(
        sorted(written_rows(&l.script)),
        sorted(want),
        "the notification was acknowledged before every record of both objects"
    );
    assert_eq!(script.acknowledged(), vec![receipt]);
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    stop(&l);
}

/// Pins that the S3 test event and a notification that creates nothing are
/// acknowledged without a read.
#[test]
fn a_removal_and_a_test_event_are_acknowledged_by_the_reader() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "kept.ndjson", &recs("kept", 3));

    let (feed, script) = scripted_feed();
    let test_event = script.deliver(Unit::new(
        "test-event",
        serde_json::to_vec(&serde_json::json!({
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Bucket": "b",
        }))
        .unwrap(),
    ));
    let removal = script.deliver(Unit::new(
        "removal",
        serde_json::to_vec(&serde_json::json!({ "Records": [{
            "eventName": "ObjectRemoved:Delete",
            "s3": { "bucket": { "name": "b" }, "object": { "key": "data/kept.ndjson" } },
        }] }))
        .unwrap(),
    ));
    let l = launch_on_notifications("s3", dir.path(), Arc::new(feed), |_| {});

    wait_until(DEADLINE, "both units are acknowledged", || {
        script.acknowledged().len() == 2
    });
    let mut acknowledged = script.acknowledged();
    acknowledged.sort();
    let mut want = vec![test_event, removal];
    want.sort();
    assert_eq!(acknowledged, want);
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    assert!(
        written_rows(&l.script).is_empty(),
        "a unit with nothing to read was read"
    );
    stop(&l);
}

/// Pins that a source given its store under an `s3a://` URL takes the URL's
/// host as the bucket a notification must name, as it does under `s3://`.
#[test]
fn a_notification_under_an_s3a_url_is_read_from_the_urls_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("s3a", 3);
    stage(&data, "a.ndjson", &rows);
    let size = fs::metadata(data.join("a.ndjson")).unwrap().len();

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(notification(&[("data/a.ndjson", size)]));
    let l = launch_on_notifications("s3a", dir.path(), Arc::new(feed), |_| {});

    wait_until(DEADLINE, "the notification is acknowledged", || {
        !script.acknowledged().is_empty()
    });
    assert_eq!(script.acknowledged(), vec![receipt]);
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    assert_eq!(sorted(written_rows(&l.script)), sorted(rows));
    stop(&l);
}

/// Pins that the source hands the reader its URL's prefix: a notification
/// for an object in the bucket but outside `data/` is failed back unread.
#[test]
fn a_notification_outside_the_urls_prefix_is_failed_back() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("outside", 3);
    let other = dir.path().join("other");
    fs::create_dir_all(&other).unwrap();
    stage(&other, "a.ndjson", &rows);
    let size = fs::metadata(other.join("a.ndjson")).unwrap().len();
    drop(data);

    let (feed, script) = scripted_feed();
    let receipt = script.deliver(notification(&[("other/a.ndjson", size)]));
    let l = launch_on_notifications("s3", dir.path(), Arc::new(feed), |_| {});

    wait_until(DEADLINE, "the notification is failed back", || {
        !script.failed().is_empty()
    });
    let failed = script.failed();
    assert_eq!(failed[0].0, receipt);
    assert!(
        failed[0].1.contains("outside the source's prefix"),
        "{}",
        failed[0].1
    );
    assert!(script.acknowledged().is_empty());
    assert!(
        written_rows(&l.script).is_empty(),
        "an object outside the prefix was read"
    );
    stop(&l);
}

/// A run over `s3://b/data/` that reads the directory `root` through `store`,
/// with 64 KiB windows.
fn launch_on_store(
    store: Arc<ScriptedStore>,
    feed: Arc<dyn Feed>,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let yaml = PipelineYaml::endpoint("s3-overwrite-test", "http://127.0.0.1:9")
        .source("chunk_bytes", "64KiB")
        .source("prefetch_bytes", "64KiB")
        .build();
    launch_customized(&yaml, test_options(), pre, move |source, _io| {
        line_framer(source).with_store(store).with_feed(feed)
    })
}

/// A notification in bucket `b` creating `data/<name>` at `size` bytes,
/// pinned to `etag` sent without its quotes, as S3 sends it, after
/// `re_enqueues` re-enqueues.
fn pinned_notification(name: &str, size: u64, etag: &str, re_enqueues: u32) -> Unit {
    let record = serde_json::json!({
        "eventName": "ObjectCreated:Put",
        "spateReEnqueues": re_enqueues,
        "s3": {
            "bucket": { "name": "b" },
            "object": {
                "key": format!("data/{name}"),
                "size": size,
                "eTag": etag.trim_matches('"'),
            },
        },
    });
    Unit::new(
        "notification",
        serde_json::to_vec(&serde_json::json!({ "Records": [record] })).unwrap(),
    )
}

/// The key, ETag, size and re-enqueue count a re-enqueued unit's body names.
fn re_enqueued(unit: &Unit) -> (String, String, u64, u64) {
    let body: serde_json::Value = serde_json::from_slice(&unit.body).unwrap();
    let record = &body["Records"][0];
    let object = &record["s3"]["object"];
    (
        object["key"].as_str().unwrap().to_owned(),
        object["eTag"].as_str().unwrap().to_owned(),
        object["size"].as_u64().unwrap(),
        record["spateReEnqueues"].as_u64().unwrap(),
    )
}

fn size_of(data: &Path, name: &str) -> u64 {
    fs::metadata(data.join(name)).unwrap().len()
}

/// Pins that a notification unit whose object was overwritten before its
/// read publishes, under its own id, a unit for the key at the object's
/// current ETag and size with a re-enqueue count of 1; that the old unit is
/// renewed and not acknowledged until the feed confirms that publish; and
/// that the run writes exactly the object's current rows.
#[test]
fn a_412_re_enqueues_the_key_at_its_current_etag_before_acknowledging() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "a.ndjson", &recs("old", 4));
    let (old_etag, old_size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    let current = recs("current", 7);
    stage(&data, "a.ndjson", &current);
    let (etag, size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    assert_ne!(etag, old_etag);

    let (feed, script) = scripted_feed();
    script.hold_publishes();
    let first = script.deliver(pinned_notification("a.ndjson", old_size, &old_etag, 0));
    let store = ScriptedStore::new(dir.path());
    let l = launch_on_store(Arc::clone(&store), Arc::new(feed), |_| {});

    wait_until(DEADLINE, "the key is re-enqueued", || {
        script.published().len() == 1
    });
    let published = &script.published()[0];
    assert_eq!(published.id, "notification");
    assert_eq!(
        re_enqueued(published),
        ("data/a.ndjson".to_owned(), etag, size, 1)
    );
    let renewed = renewals_of(&script, first);
    wait_until(DEADLINE, "the old unit is renewed while it waits", || {
        renewals_of(&script, first) >= renewed + 2
    });
    assert!(
        script.acknowledged().is_empty(),
        "acknowledged before the publish settled"
    );

    script.confirm_held_publishes();
    wait_until(DEADLINE, "both units are acknowledged", || {
        script.acknowledged().len() == 2
    });
    stop(&l);
    assert_eq!(script.acknowledged()[0], first);
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    assert_eq!(sorted(written_rows(&l.script)), sorted(current));
    assert_eq!(store.heads(), 1);
}

/// Pins that an object overwritten while its read is under way delivers the
/// records read before the overwrite, and that its unit is acknowledged only
/// after the sink has acknowledged them.
#[test]
fn a_412_mid_read_acknowledges_only_after_emitted_records_are_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let old = recs("old", 10_000);
    stage(&data, "a.ndjson", &old);
    let (etag, size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    assert!(size > 2 * 64 * 1024, "the read takes more than two windows");

    let (feed, script) = scripted_feed();
    let first = script.deliver(pinned_notification("a.ndjson", size, &etag, 0));
    let store = ScriptedStore::new(dir.path());
    store.hold_get(2);
    let l = launch_on_store(Arc::clone(&store), Arc::new(feed), |sink| {
        sink.enqueue_global(WriteOutcome::ok().held());
    });
    wait_until(DEADLINE, "the second window's GET is held", || {
        store.holding()
    });
    let current = recs("current", 3);
    stage(&data, "a.ndjson", &current);
    store.release();

    wait_until(DEADLINE, "the key is re-enqueued", || {
        script.published().len() == 1
    });
    wait_until(DEADLINE, "the first write is held", || {
        l.script.held_writes() == 1
    });
    let renewed = renewals_of(&script, first);
    wait_until(
        DEADLINE,
        "the old unit is renewed while its records are held",
        || renewals_of(&script, first) >= renewed + 2,
    );
    assert!(
        !script.acknowledged().contains(&first),
        "acknowledged before the sink acknowledged its records"
    );
    l.script.release_held();
    wait_until(DEADLINE, "the old unit is acknowledged", || {
        script.acknowledged().contains(&first)
    });
    wait_until(DEADLINE, "the re-enqueued unit is acknowledged", || {
        script.acknowledged().len() == 2
    });
    stop(&l);
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    let rows = written_rows(&l.script);
    let read_old: Vec<String> = rows
        .iter()
        .filter(|r| r.contains("old-"))
        .cloned()
        .collect();
    assert!(
        !read_old.is_empty() && read_old.len() < old.len(),
        "{}",
        read_old.len()
    );
    assert_eq!(
        sorted(read_old.clone()),
        sorted(old[..read_old.len()].to_vec()),
        "the old rows written are the ones before the overwrite"
    );
    let read_current: Vec<String> = rows
        .into_iter()
        .filter(|r| r.contains("current-"))
        .collect();
    assert_eq!(sorted(read_current), sorted(current));
}

/// Pins that on the in-process feed an overwritten notification unit is
/// acknowledged, never failed back toward its dead-letter cap, and the
/// object's current rows are read through the unit it re-enqueued.
#[test]
fn a_412_notification_unit_is_acknowledged_without_dead_lettering() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "a.ndjson", &recs("old", 4));
    let (old_etag, old_size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    let current = recs("current", 5);
    stage(&data, "a.ndjson", &current);

    let feed = MemoryFeed::with_attempt_cap(std::num::NonZeroU32::new(1).unwrap());
    rt_block_on(feed.publish(pinned_notification("a.ndjson", old_size, &old_etag, 0))).unwrap();
    let store = ScriptedStore::new(dir.path());
    let l = launch_on_store(store, Arc::new(feed.clone()), |_| {});
    wait_until(DEADLINE, "both units are acknowledged", || {
        feed.counts().acknowledged == 2
    });
    stop(&l);
    assert!(feed.dead_letters().is_empty());
    assert_eq!(sorted(written_rows(&l.script)), sorted(current));
}

/// Pins that a unit at the re-enqueue cap whose object was overwritten is
/// acknowledged with no re-enqueue and no rows read.
#[test]
fn a_unit_at_the_re_enqueue_cap_412s_and_is_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "a.ndjson", &recs("old", 4));
    let (old_etag, old_size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    stage(&data, "a.ndjson", &recs("current", 5));

    let (feed, script) = scripted_feed();
    let first = script.deliver(pinned_notification("a.ndjson", old_size, &old_etag, 10));
    let l = launch_on_store(ScriptedStore::new(dir.path()), Arc::new(feed), |_| {});
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![first]
    });
    stop(&l);
    assert!(script.published().is_empty());
    assert!(script.failed().is_empty(), "{:?}", script.failed());
    assert!(written_rows(&l.script).is_empty());
}

/// Pins that the HEAD after a 412 is classified as the read is: a 404 fails
/// the unit back naming the key, and a 403 fails the pipeline naming it,
/// with no re-enqueue either way.
#[test]
fn a_head_after_a_412_classifies_404_and_403_as_the_read_does() {
    for answer in [HeadAnswer::NotFound, HeadAnswer::Denied] {
        let dir = tempfile::tempdir().unwrap();
        let data = data_dir(&dir);
        stage(&data, "a.ndjson", &recs("old", 4));
        let (old_etag, old_size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
        stage(&data, "a.ndjson", &recs("current", 5));

        let (feed, script) = scripted_feed();
        let first = script.deliver(pinned_notification("a.ndjson", old_size, &old_etag, 0));
        let store = ScriptedStore::new(dir.path());
        store.answer_head(answer);
        let l = launch_on_store(Arc::clone(&store), Arc::new(feed), |_| {});
        match answer {
            HeadAnswer::NotFound => {
                wait_until(DEADLINE, "the unit is failed back", || {
                    !script.failed().is_empty()
                });
                stop(&l);
                let (receipt, reason) = &script.failed()[0];
                assert_eq!(*receipt, first);
                assert!(reason.contains("data/a.ndjson"), "{reason}");
            }
            HeadAnswer::Denied => {
                let report = l.run.wait_exit(DEADLINE).expect("the run exits").unwrap();
                let ExitState::Failed(failure) = report.state else {
                    panic!(
                        "a 403 on the HEAD must fail the pipeline: {:?}",
                        report.state
                    );
                };
                assert!(
                    failure.reason.contains("data/a.ndjson"),
                    "{}",
                    failure.reason
                );
                assert!(script.acknowledged().is_empty());
            }
        }
        assert!(script.published().is_empty(), "{answer:?}");
        assert_eq!(store.heads(), 1, "{answer:?}");
    }
}

/// Pins that a lister's unit over an object overwritten under its pin is
/// failed back without a HEAD or a re-enqueue.
#[test]
fn a_412_on_a_lister_unit_is_failed_back() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "a.ndjson", &recs("old", 4));
    let object = DescriptorObject {
        key: "data/a.ndjson".to_owned(),
        size: size_of(&data, "a.ndjson"),
        etag: Some(etag_of(&data, "a.ndjson")),
        last_modified_ms: 1,
    };
    stage(&data, "a.ndjson", &recs("current", 5));

    let (feed, script) = scripted_feed();
    let first = script.deliver(Unit::new(
        "listed",
        SplitDescriptor::new(vec![object]).encode().unwrap(),
    ));
    let store = ScriptedStore::new(dir.path());
    let l = launch_on_store(Arc::clone(&store), Arc::new(feed), |_| {});
    wait_until(DEADLINE, "the unit is failed back", || {
        !script.failed().is_empty()
    });
    stop(&l);
    assert_eq!(script.failed()[0].0, first);
    assert!(script.published().is_empty());
    assert_eq!(store.heads(), 0);
}

/// Pins that a unit waiting on an unsettled re-enqueue is not acknowledged
/// at shutdown.
#[test]
fn a_412_unit_with_an_unsettled_publish_is_not_acknowledged_at_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage(&data, "a.ndjson", &recs("old", 4));
    let (old_etag, old_size) = (etag_of(&data, "a.ndjson"), size_of(&data, "a.ndjson"));
    stage(&data, "a.ndjson", &recs("current", 5));

    let (feed, script) = scripted_feed();
    script.hold_publishes();
    script.deliver(pinned_notification("a.ndjson", old_size, &old_etag, 0));
    let l = launch_on_store(ScriptedStore::new(dir.path()), Arc::new(feed), |_| {});
    wait_until(DEADLINE, "the key is re-enqueued", || {
        script.published().len() == 1
    });
    stop(&l);
    assert!(script.acknowledged().is_empty());
}

fn rt_block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}
