//! End-to-end registry behavior against a stub Confluent-compatible
//! schema-registry server: cache misses report not-ready and resolve after
//! the asynchronous fetch; failures negative-cache with a TTL; pre-warm
//! loads subjects at startup.

#![expect(deprecated, reason = "fixtures call the datum free functions directly")]
#![expect(
    clippy::disallowed_methods,
    reason = "each test awaits its blocking decodes"
)]

use apache_avro::Schema;
use apache_avro::to_avro_datum;
use spate_avro::{
    AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, AvroValueDeserializer,
    RegistrySection,
};
use spate_core::checkpoint::AckRef;
use spate_core::clock::{Clock, TestClock};
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::DeserError;
use spate_test::{EmitCollector, raw_payload};
use spate_test_support::{RegistryRequest, RegistryStub, run_in_child};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SCHEMA_V1: &str =
    r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;

fn schema_body(schema: &str) -> String {
    serde_json::json!({ "schema": schema }).to_string()
}

fn confluent_payload(id: u32, event_id: i64) -> Vec<u8> {
    let schema = Schema::parse_str(SCHEMA_V1).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("id", event_id);
    let datum = to_avro_datum(&schema, rec).unwrap();
    spate_test::confluent_frame(id, &datum)
}

fn settings(addr: std::net::SocketAddr, ttl: Duration) -> AvroSettings {
    settings_at(format!("http://{addr}"), ttl)
}

fn settings_at(url: String, ttl: Duration) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection::new(url)),
        negative_cache_ttl: ttl,
        ..AvroSettings::default()
    }
}

/// A builder whose negative cache and fetch backoff read `clock`.
fn builder_on(
    addr: std::net::SocketAddr,
    ttl: Duration,
    clock: &Arc<TestClock>,
) -> AvroDeserializerBuilder {
    AvroDeserializerBuilder::from_settings_with_clock(
        &settings(addr, ttl),
        &tokio::runtime::Handle::current(),
        Arc::clone(clock) as _,
    )
    .unwrap()
}

/// [`drive_until_ready`] for a deserializer built on `clock`: each
/// `NotReady` moves `clock` past the longest fetch backoff, so a transient
/// failure's refetch waits only for the HTTP round trip.
fn drive_until_ready_on<F, O>(
    deser: &mut dyn Deserializer<F>,
    payload: &[u8],
    out: &mut O,
    clock: &TestClock,
) -> Result<(), DeserError>
where
    F: RecFamily,
    O: for<'buf> EmitRecord<'buf, F::Rec<'buf>>,
{
    spate_test::deserialize_until_ready(
        spate_test::HANG,
        deser,
        &raw_payload(payload),
        &spate_test::test_ack(),
        out,
        || clock.advance(Duration::from_secs(5)),
    )
}

/// Replays `payload` until the asynchronous fetch settles, as the driver
/// does, and returns the first answer other than `NotReady`.
fn drive_until_ready<F, O>(
    deser: &mut dyn Deserializer<F>,
    payload: &[u8],
    out: &mut O,
) -> Result<(), DeserError>
where
    F: RecFamily,
    O: for<'buf> EmitRecord<'buf, F::Rec<'buf>>,
{
    spate_test::deserialize_until_ready(
        spate_test::HANG,
        deser,
        &raw_payload(payload),
        &spate_test::test_ack(),
        out,
        || {},
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn miss_reports_not_ready_then_decodes_after_fetch() {
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/42", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");

    let payload = confluent_payload(42, 7);
    let (ack, _rx) = AckRef::test_pair();
    let mut out = EmitCollector::new();

    // First call: not ready (fetch just triggered), nothing emitted.
    let err = deser
        .deserialize(&raw_payload(&payload), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::NotReady { .. }), "{err}");
    assert!(out.records.is_empty());

    // The driver's retry loop, condensed.
    let result = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.len(), 1, "exactly one record after the fetch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retriable_registry_errors_are_retried() {
    let stub = RegistryStub::start();
    // A transient failure leaves the id absent, and the deserializer's replay
    // refetches it (bounded by per-id fetch backoff); it is never negatively
    // cached.
    stub.script("/schemas/ids/9", 503, "shard warming up", 2);
    stub.script("/schemas/ids/9", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_secs(30), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(9, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready_on(&mut deser, &payload, &mut out, &clock).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "fetch retried through the 500s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_id_negative_caches_until_ttl_expiry() {
    let stub = RegistryStub::start();
    // No script for id 5: the stub answers 404 (and counts hits).
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_millis(300), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(5, 1);

    // Drive to the negative-cache verdict.
    let (mut deser, first) = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        let r = drive_until_ready(&mut deser, &payload, &mut out);
        (deser, r)
    })
    .await
    .unwrap();
    let err = first.unwrap_err();
    assert!(
        matches!(err, DeserError::SchemaUnavailable { .. }),
        "poison id surfaces as unavailable (policy applies): {err}"
    );
    let hits_after_first = stub.hits();

    // Within the TTL, with the clock unmoved: answered from the negative
    // cache, no new requests.
    let payload = confluent_payload(5, 1);
    let (ack, _rx) = AckRef::test_pair();
    let mut out = EmitCollector::new();
    let err = deser
        .deserialize(&raw_payload(&payload), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
    assert_eq!(stub.hits(), hits_after_first);

    // A nanosecond short of the configured TTL: still answered from the
    // negative cache.
    clock.advance(Duration::from_millis(300) - Duration::from_nanos(1));
    let err = deser
        .deserialize(&raw_payload(&payload), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
    assert_eq!(stub.hits(), hits_after_first);

    // After expiry the schema exists now: refetch succeeds.
    stub.script("/schemas/ids/5", 200, schema_body(SCHEMA_V1), 0);
    clock.advance(Duration::from_nanos(1));
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "expired negative entry allows a refetch");
}

/// A resolved fetch clears the id's fetch backoff, so a negative entry at
/// the 100 ms TTL floor is refetched as soon as it expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_negative_entry_at_the_ttl_floor_refetches_on_expiry() {
    let stub = RegistryStub::start();
    // No script for id 6 yet: the stub answers 404.
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_millis(100), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(6, 1);

    let (mut deser, first) = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        let r = drive_until_ready(&mut deser, &payload, &mut out);
        (deser, r)
    })
    .await
    .unwrap();
    let err = first.unwrap_err();
    assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");

    stub.script("/schemas/ids/6", 200, schema_body(SCHEMA_V1), 0);
    clock.advance(Duration::from_millis(100));
    let payload = confluent_payload(6, 1);
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .expect("the expired negative entry refetches with the clock unmoved");
    assert_eq!(rows.len(), 1);
    assert_eq!(stub.path_hits("/schemas/ids/6"), 2);
}

/// The registry's 404 warning, as the fetcher logs it.
const UNKNOWN_ID: &str = "registry reports schema id unknown";

/// A current-thread runtime, so the fetcher's warnings reach this thread's
/// subscriber.
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Decodes one record per id in `ids`, each naming an id the registry does
/// not know, until each one fails as unavailable.
fn decode_unknown_ids(
    rt: &tokio::runtime::Runtime,
    deser: &mut AvroValueDeserializer,
    ids: std::ops::Range<u32>,
) {
    let mut out = EmitCollector::new();
    for id in ids {
        let payload = confluent_payload(id, 1);
        // The fetcher runs on `rt`, so each replay first drives it.
        let result = spate_test::deserialize_until_ready(
            spate_test::HANG,
            deser,
            &raw_payload(&payload),
            &spate_test::test_ack(),
            &mut out,
            || rt.block_on(async { tokio::time::sleep(Duration::from_millis(1)).await }),
        );
        assert!(
            matches!(result, Err(DeserError::SchemaUnavailable { .. })),
            "id {id}: {result:?}"
        );
    }
}

/// The 404 warnings in `lines`, as the text after the level and target.
fn unknown_id_warnings(lines: &[String]) -> Vec<&str> {
    lines
        .iter()
        .filter(|l| l.contains(UNKNOWN_ID))
        .map(|l| l.split_once(UNKNOWN_ID).map_or("", |(_, rest)| rest.trim()))
        .collect()
}

/// Records naming many distinct ids the registry does not know log the 404
/// warning five times per window of the deserializer's clock, while every id
/// is still fetched.
#[test]
fn unknown_id_warnings_are_rate_limited() {
    const IDS: u32 = 20;
    let stub = RegistryStub::start();
    let lines = spate_test::capture_logs(tracing::Level::WARN, || {
        let rt = current_thread();
        let clock = TestClock::frozen();
        let mut deser = rt.block_on(async {
            let addr = stub.addr();
            builder_on(addr, Duration::from_secs(3600), &clock)
                .build_value()
                .expect("apache builder")
        });
        decode_unknown_ids(&rt, &mut deser, 100..100 + IDS);
        clock.advance(Duration::from_secs(10));
        decode_unknown_ids(&rt, &mut deser, 100 + IDS..101 + IDS);
    });
    assert!(stub.hits() > IDS as usize, "every id is fetched");
    let warned = unknown_id_warnings(&lines);
    assert_eq!(warned.len(), 6, "{warned:#?}");
    assert!(
        warned[5].starts_with("suppressed=15 schema_id=120 "),
        "{warned:#?}"
    );
}

/// Each deserializer builder limits its own 404 warnings: one builder's
/// full window does not suppress another's first warning.
#[test]
fn each_builder_limits_its_own_unknown_id_warnings() {
    let stub = RegistryStub::start();
    let lines = spate_test::capture_logs(tracing::Level::WARN, || {
        let rt = current_thread();
        let clock = TestClock::frozen();
        let (mut first, mut second) = rt.block_on(async {
            let addr = stub.addr();
            let build = || {
                builder_on(addr, Duration::from_secs(3600), &clock)
                    .build_value()
                    .expect("apache builder")
            };
            (build(), build())
        });
        decode_unknown_ids(&rt, &mut first, 100..106);
        decode_unknown_ids(&rt, &mut second, 200..201);
    });
    let warned = unknown_id_warnings(&lines);
    assert_eq!(warned.len(), 6, "{warned:#?}");
    assert!(warned[5].starts_with("schema_id=200 "), "{warned:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_503s_then_success_decodes_and_never_drops() {
    // The critical regression (registry.rs poison-cache): three transient
    // 503s must not poison the id. Each leaves it absent; the deserializer's
    // replay refetches (bounded by per-id backoff) until the schema resolves.
    // The record is never dropped/acked as poison.
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/42", 503, "shard warming up", 3);
    stub.script("/schemas/ids/42", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_secs(30), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(42, 7);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready_on(&mut deser, &payload, &mut out, &clock).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "record decodes after the transient blips");
}

/// After a transient failure the fetcher refuses each replayed request for
/// that id while the deserializer's clock stays short of the backoff, and
/// refetches once the clock passes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_refetch_while_the_backoff_holds() {
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/13", 503, "shard warming up", 1);
    stub.script("/schemas/ids/13", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let test_clock = TestClock::frozen();
    let clock = Arc::new(CountingClock {
        inner: Arc::clone(&test_clock),
        reads: AtomicUsize::new(0),
    });
    let builder = AvroDeserializerBuilder::from_settings_with_clock(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
        Arc::clone(&clock) as _,
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(13, 1);
    let probe = stub.clone();

    let (frozen_hits, rows) = tokio::task::spawn_blocking(move || {
        let ack = spate_test::test_ack();
        let mut out = EmitCollector::new();
        let reads = || clock.reads.load(Ordering::SeqCst);
        assert!(matches!(
            deser.deserialize(&raw_payload(&payload), &ack, &mut out),
            Err(DeserError::NotReady { .. })
        ));
        spate_test::wait_until(spate_test::HANG, "the 503 is stamped", || reads() >= 1);
        // Each replay requests the id once, and the backoff gate reads the
        // clock once to refuse it.
        for replay in 1..=10 {
            assert!(matches!(
                deser.deserialize(&raw_payload(&payload), &ack, &mut out),
                Err(DeserError::NotReady { .. })
            ));
            spate_test::wait_until(spate_test::HANG, "the backoff refuses the replay", || {
                reads() > replay
            });
        }
        let frozen_hits = probe.path_hits("/schemas/ids/13");
        test_clock.advance(Duration::from_millis(200));
        let rows = drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads());
        (frozen_hits, rows)
    })
    .await
    .unwrap();
    assert_eq!(frozen_hits, 1, "no refetch before the backoff passes");
    assert_eq!(
        rows.expect("decodes once the clock passes the backoff")
            .len(),
        1
    );
    assert_eq!(stub.path_hits("/schemas/ids/13"), 2);
}

/// A frozen [`TestClock`] that counts its reads.
struct CountingClock {
    inner: Arc<TestClock>,
    reads: AtomicUsize,
}

impl Clock for CountingClock {
    fn now(&self) -> Instant {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.now()
    }
}

/// The fetcher stamps each transient failure on the deserializer's clock:
/// the refetch is admitted once that clock has moved exactly the backoff past
/// the failure, first 200 ms and then 400 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_failure_is_stamped_on_the_deserializers_clock() {
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/11", 503, "shard warming up", 2);
    stub.script("/schemas/ids/11", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let test_clock = TestClock::frozen();
    let clock = Arc::new(CountingClock {
        inner: Arc::clone(&test_clock),
        reads: AtomicUsize::new(0),
    });
    let builder = AvroDeserializerBuilder::from_settings_with_clock(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
        Arc::clone(&clock) as _,
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(11, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let (ack, _rx) = AckRef::test_pair();
        let mut out = EmitCollector::new();
        let reads = || clock.reads.load(Ordering::SeqCst);
        // No backoff and no negative entry yet, so the first read is the
        // stamp of the first 503.
        assert!(matches!(
            deser.deserialize(&raw_payload(&payload), &ack, &mut out),
            Err(DeserError::NotReady { .. })
        ));
        spate_test::wait_until(spate_test::HANG, "the first 503 is stamped", || {
            reads() >= 1
        });
        test_clock.advance(Duration::from_millis(200));
        // One request: the gate's read admits it, the second 503's stamp is
        // the next read.
        assert!(matches!(
            deser.deserialize(&raw_payload(&payload), &ack, &mut out),
            Err(DeserError::NotReady { .. })
        ));
        spate_test::wait_until(spate_test::HANG, "the second 503 is stamped", || {
            reads() >= 3
        });
        test_clock.advance(Duration::from_millis(400));
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .expect("the id decodes once the clock passes each backoff");
    assert_eq!(rows.len(), 1);
    assert_eq!(stub.path_hits("/schemas/ids/11"), 3);
}

/// Each refetch of a transiently failing id waits on the deserializer's
/// clock: six 503s grow the backoff to its cap, and the id decodes as fast as
/// the clock moves, with one request per failure. On real time the six waits
/// would add up to 11.2 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_id_is_refetched_once_its_backoff_passes() {
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/11", 503, "shard warming up", 6);
    stub.script("/schemas/ids/11", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_secs(30), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(11, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready_on(&mut deser, &payload, &mut out, &clock).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .expect("the id decodes once its backoff passes on the clock");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        stub.path_hits("/schemas/ids/11"),
        7,
        "one request per failure, then the schema"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_retriable_5xx_is_transient_not_poison() {
    // A single non-retriable 500 (the registry restarting behind an LB) must
    // NOT be negatively cached: doing so would surface SchemaUnavailable for
    // the whole TTL and silently drop valid records under ErrorPolicy::Skip.
    // The id is left absent and refetched. Calling insert_failed on any
    // non-retriable error fails this test.
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/7", 500, "internal error", 1);
    stub.script("/schemas/ids/7", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let clock = TestClock::frozen();
    let builder = builder_on(addr, Duration::from_secs(30), &clock);
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(7, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready_on(&mut deser, &payload, &mut out, &clock).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .expect("a transient 500 must never poison the id");
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_fetch_does_not_block_other_ids() {
    // Regression for the serial-fetcher head-of-line block: id 100's fetch is
    // gated (a black-holed registry node); id 200's fetch must still resolve
    // concurrently rather than waiting behind it. On the old serial fetcher,
    // id 200 would never resolve while 100 is stuck.
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/100", 200, schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/200", 200, schema_body(SCHEMA_V1), 0);
    stub.hold("/schemas/ids/100");
    let addr = stub.addr();

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let slow = confluent_payload(100, 1);
    let fast = confluent_payload(200, 2);

    // Kick off the slow (gated) fetch first, then the fast one.
    let (ack, _rx) = AckRef::test_pair();
    let mut sink = EmitCollector::new();
    assert!(matches!(
        deser
            .deserialize(&raw_payload(&slow), &ack, &mut sink)
            .unwrap_err(),
        DeserError::NotReady { .. }
    ));

    let slow_probe = slow.clone();
    let mut deser = tokio::task::spawn_blocking(move || {
        // The fast id resolves while the slow id is still gated.
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &fast, &mut out).unwrap();
        assert_eq!(
            out.records.len(),
            1,
            "fast id resolved despite the gated slow id"
        );

        // The slow id is still unavailable (its fetch is blocked).
        let (ack, _rx) = AckRef::test_pair();
        let mut out = EmitCollector::new();
        assert!(matches!(
            deser
                .deserialize(&raw_payload(&slow_probe), &ack, &mut out)
                .unwrap_err(),
            DeserError::NotReady { .. }
        ));
        deser
    })
    .await
    .unwrap();

    // Release the gate: the slow id now resolves too.
    stub.release();
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &slow, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "slow id resolves after the gate is released");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prewarm_loads_subjects_at_startup() {
    let stub = RegistryStub::start();
    stub.script(
        "/subjects/events-value/versions/latest",
        200,
        serde_json::json!({
            "schema": SCHEMA_V1, "id": 42, "version": 3, "subject": "events-value"
        })
        .to_string(),
        0,
    );
    // Fallback for the startup race: a payload arriving before the
    // pre-warm lands triggers an on-demand by-id fetch.
    stub.script("/schemas/ids/42", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();

    let mut cfg = settings(addr, Duration::from_millis(100));
    cfg.prewarm_subjects = vec!["events-value".into()];
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let mut deser = builder.build_value().expect("apache builder");

    let payload = confluent_payload(42, 7);
    let hits = stub.clone();
    let rows = tokio::task::spawn_blocking(move || {
        // The pre-warm must request the subject's latest version at startup.
        spate_test::wait_until(
            spate_test::HANG,
            "the pre-warm requests the subject",
            || hits.path_hits("/subjects/events-value/versions/latest") > 0,
        );
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1);
}

/// The registry URL a re-executed child test builds against.
const CHILD_REGISTRY_URL: &str = "SPATE_TEST_AVRO_REGISTRY_URL";

/// With an empty system trust store, an `http://` registry builds and
/// decodes. An `https://` registry fails at startup with an error that names
/// `registry.tls.root_ca` and the trust store, except on macOS and Windows,
/// where the platform verifier reads the OS store and `SSL_CERT_FILE` does not
/// empty it. Regression for #624.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_trust_store_fails_only_an_https_build() {
    const NAME: &str = "an_empty_trust_store_fails_only_an_https_build";
    let runtime = tokio::runtime::Handle::current();
    if let Ok(url) = std::env::var(CHILD_REGISTRY_URL) {
        let https = settings_at("https://registry.invalid".into(), Duration::from_secs(30));
        let built = AvroDeserializerBuilder::from_settings(&https, &runtime);
        if spate_core::tls::PLATFORM_VERIFIER {
            built.expect("the platform verifier reads the OS store");
        } else {
            let Err(err) = built else {
                panic!("an https registry with no trust root built");
            };
            let err = err.to_string();
            assert!(
                err.starts_with(
                    "avro schema registry client: registry.tls.root_ca: the system trust store \
                     has no certificates"
                ),
                "{err}"
            );
        }
        let builder = AvroDeserializerBuilder::from_settings(
            &settings_at(url, Duration::from_secs(30)),
            &runtime,
        )
        .expect("http registry builds");
        let mut deser = builder.build_value().expect("apache builder");
        let payload = confluent_payload(42, 7);
        let rows = tokio::task::spawn_blocking(move || {
            let mut out = EmitCollector::new();
            drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.payloads())
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rows.len(), 1);
        return;
    }
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/42", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "").unwrap();
    tokio::task::block_in_place(|| {
        run_in_child(NAME, |child| {
            child
                .env_remove("SSL_CERT_DIR")
                .env("SSL_CERT_FILE", &empty)
                .env(CHILD_REGISTRY_URL, format!("http://{addr}"))
        });
    });
}

/// A schema the parser refuses — here a record named `"my-record"`, which
/// the Avro name rules reject — surfaces as a per-record poison
/// (SchemaUnavailable) the ErrorPolicy can act on, never a permanent
/// NotReady stall and never an unwind on whichever pipeline thread touched
/// it first. The compile catches a panicking refusal too; if one happens it
/// prints a backtrace to stderr, which is expected and harmless.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_schema_poisons_the_id_rather_than_stalling() {
    let stub = RegistryStub::start();
    let bad = r#"{"type":"record","name":"my-record","fields":[{"name":"id","type":"long"}]}"#;
    stub.script("/schemas/ids/77", 200, schema_body(bad), 0);
    let addr = stub.addr();

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(77, 1);

    let err = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(err, DeserError::SchemaUnavailable { .. }),
        "a refused schema parse must poison the id, not stall at NotReady: {err}"
    );
}

/// A schema the registry types as other than Avro poisons the id, though its
/// text also parses as an Avro schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_schema_that_is_not_avro_poisons_the_id() {
    let stub = RegistryStub::start();
    let body = serde_json::json!({ "schema": r#"{"type":"string"}"#, "schemaType": "JSON" });
    stub.script("/schemas/ids/78", 200, body.to_string(), 0);
    let addr = stub.addr();
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(78, 1);
    let err = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("JSON")),
        "{err}"
    );
}

/// A pre-warmed subject whose schema the registry types as other than Avro is
/// not cached, so its id is fetched and poisoned on first use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prewarm_skips_a_schema_that_is_not_avro() {
    let stub = RegistryStub::start();
    let json =
        serde_json::json!({ "schema": r#"{"type":"string"}"#, "schemaType": "JSON", "id": 79 })
            .to_string();
    stub.script("/subjects/json-value/versions/latest", 200, json.clone(), 0);
    stub.script("/schemas/ids/79", 200, json, 0);
    let addr = stub.addr();
    let mut cfg = settings(addr, Duration::from_secs(30));
    // Subjects are pre-warmed in order, so a request for the second subject
    // means the first has been handled. The second answers 404.
    cfg.prewarm_subjects = vec!["json-value".into(), "sentinel-value".into()];
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let mut deser = builder.build_value().unwrap();
    let hits = stub.clone();
    tokio::task::spawn_blocking(move || {
        spate_test::wait_until(
            spate_test::HANG,
            "the pre-warm reaches the second subject",
            || hits.path_hits("/subjects/sentinel-value/versions/latest") > 0,
        );
    })
    .await
    .unwrap();
    // Id 79, then the Avro string "a".
    let payload = spate_test::confluent_frame(79, &[0x02, b'a']);
    let result = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.records.len())
    })
    .await
    .unwrap();
    assert!(
        matches!(&result, Err(DeserError::SchemaUnavailable { reason }) if reason.contains("JSON")),
        "{result:?}"
    );
}

/// An unknown id's poison reason carries the registry's error body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_id_names_the_registry_error() {
    let stub = RegistryStub::start();
    // Unscripted: the stub answers 404 {"error_code":40403,"message":"Schema not found"}.
    let addr = stub.addr();
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(5, 1);
    let err = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("40403")),
        "{err}"
    );
}

/// A `403` on the pre-warm is logged, and the id still decodes from the
/// by-id fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prewarm_forbidden_is_not_a_rejection() {
    let stub = RegistryStub::start();
    stub.script("/subjects/events-value/versions/latest", 403, "{}", 0);
    stub.script("/schemas/ids/42", 200, schema_body(SCHEMA_V1), 0);
    let addr = stub.addr();
    let mut cfg = settings(addr, Duration::from_secs(30));
    // Pre-warm is sequential and ends on a recorded rejection, so a request
    // for the second subject means the first was handled and not recorded.
    cfg.prewarm_subjects = vec!["events-value".into(), "sentinel-value".into()];
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let mut deser = builder.build_value().unwrap();
    let hits = stub.clone();
    tokio::task::spawn_blocking(move || {
        spate_test::wait_until(
            spate_test::HANG,
            "the pre-warm reaches the second subject",
            || hits.path_hits("/subjects/sentinel-value/versions/latest") > 0,
        );
    })
    .await
    .unwrap();
    let payload = confluent_payload(42, 1);
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.records.len())
    })
    .await
    .unwrap();
    assert_eq!(rows.unwrap(), 1);
}

/// A stub that serves `SCHEMA_V1` under id 5 at `path`, and records each
/// request it receives.
fn recording(path: &str) -> RegistryStub {
    let stub = RegistryStub::start();
    stub.script(
        path,
        200,
        format!(
            r#"{{"id":5,"schema":{}}}"#,
            serde_json::to_string(SCHEMA_V1).unwrap()
        ),
        0,
    );
    stub
}

/// By-id fetches preserve configured basic auth and unauthenticated requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetch_sends_basic_auth() {
    for (username, password, expected) in [
        (Some("svc"), Some("hunter2"), vec!["Basic c3ZjOmh1bnRlcjI="]),
        (None, None, vec![]),
        (Some("svc"), None, vec!["Basic c3ZjOg=="]),
        (Some(""), Some("hunter2"), vec!["Basic Omh1bnRlcjI="]),
    ] {
        let stub = recording("/schemas/ids/5");
        let mut cfg = settings_at(stub.endpoint(), Duration::from_secs(30));
        let registry = cfg.registry.as_mut().unwrap();
        registry.username = username.map(Into::into);
        registry.password = password.map(Into::into);
        let builder =
            AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current())
                .unwrap();
        let mut deser = builder.build_value().unwrap();
        let payload = confluent_payload(5, 1);
        let records = tokio::task::spawn_blocking(move || {
            let mut out = EmitCollector::new();
            drive_until_ready(&mut deser, &payload, &mut out).unwrap();
            out.payloads()
        })
        .await
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            AvroValue::Record(vec![("id".into(), AvroValue::Long(1))])
        );
        assert_eq!(
            stub.requests(),
            [RegistryRequest {
                target: "/schemas/ids/5?deleted=true".to_owned(),
                authorization: expected.into_iter().map(str::to_owned).collect(),
            }]
        );
    }
}

/// Startup prewarm preserves configured basic auth and unauthenticated requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prewarm_sends_basic_auth() {
    for (username, expected) in [
        (Some("svc"), vec!["Basic c3ZjOmh1bnRlcjI=".to_owned()]),
        (None, vec![]),
    ] {
        let stub = recording("/subjects/orders-value/versions/latest");
        let mut cfg = settings_at(stub.endpoint(), Duration::from_secs(30));
        let registry = cfg.registry.as_mut().unwrap();
        registry.username = username.map(Into::into);
        registry.password = username.map(|_| "hunter2".into());
        cfg.prewarm_subjects = vec!["orders-value".into()];
        let _builder =
            AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current())
                .unwrap();
        spate_test::wait_until(spate_test::HANG, "prewarm request is received", || {
            stub.hits() > 0
        });
        assert_eq!(
            stub.requests(),
            [RegistryRequest {
                target: "/subjects/orders-value/versions/latest".to_owned(),
                authorization: expected,
            }]
        );
    }
}

// ---------------------------------------------------------------------------
// Registry rejections that stop the pipeline
// ---------------------------------------------------------------------------

/// Drives `payload` through a fresh value deserializer until the result is
/// not `NotReady`, and returns the `DeserError::Fatal` reason.
async fn fatal_reason(deser: spate_avro::AvroValueDeserializer, payload: Vec<u8>) -> String {
    let mut deser = deser;
    let result = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &payload, &mut out)
    })
    .await
    .unwrap();
    match result {
        Err(DeserError::Fatal { reason }) => reason,
        other => panic!("expected DeserError::Fatal, got {other:?}"),
    }
}

/// A `401` or `403` on a schema fetch is fatal, and the reason names the
/// registry without its credentials.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_auth_rejection_is_fatal() {
    for status in [401, 403] {
        let stub = RegistryStub::start();
        stub.script(
            "/schemas/ids/1",
            status,
            r#"{"error_code":401,"message":"Unauthorized"}"#,
            0,
        );
        let addr = stub.addr();
        let mut cfg = settings_at(format!("http://{addr}"), Duration::from_secs(30));
        let registry = cfg.registry.as_mut().unwrap();
        registry.username = Some("svc".into());
        registry.password = Some("hunter2".into());
        let builder =
            AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current())
                .unwrap();
        let reason = fatal_reason(builder.build_value().unwrap(), confluent_payload(1, 1)).await;
        assert!(reason.contains(&format!("{status}")), "{reason}");
        assert!(reason.contains(&addr.to_string()), "{reason}");
        assert!(reason.contains("error_code"), "{reason}");
        assert!(!reason.contains("hunter2"), "{reason}");
    }
}

/// A `401` on the pre-warm makes the first cache miss fatal, even while the
/// by-id fetch only ever sees a transient `503`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prewarm_auth_rejection_is_fatal_at_the_first_miss() {
    let stub = RegistryStub::start();
    stub.script(
        "/subjects/events-value/versions/latest",
        401,
        r#"{"error_code":40101,"message":"Unauthorized"}"#,
        0,
    );
    stub.script("/schemas/ids/42", 503, "{}", 0);
    let addr = stub.addr();
    let mut cfg = settings(addr, Duration::from_secs(30));
    cfg.prewarm_subjects = vec!["events-value".into()];
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let reason = fatal_reason(builder.build_value().unwrap(), confluent_payload(42, 1)).await;
    assert!(reason.contains("401"), "{reason}");
    assert!(reason.contains("40101"), "{reason}");
}

/// Once a rejection is recorded, a schema already cached keeps decoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cached_schema_decodes_after_a_rejection() {
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/1", 200, schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/2", 401, "{}", 0);
    let addr = stub.addr();
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().unwrap();
    let (cached, rejected) = (confluent_payload(1, 7), confluent_payload(2, 8));
    let (first, second, again) = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        let first = drive_until_ready(&mut deser, &cached, &mut out);
        let second = drive_until_ready(&mut deser, &rejected, &mut out);
        let again = drive_until_ready(&mut deser, &cached, &mut out);
        (first.map(|()| out.records.len()), second, again)
    })
    .await
    .unwrap();
    first.unwrap();
    assert!(
        matches!(second, Err(DeserError::Fatal { .. })),
        "{second:?}"
    );
    again.expect("a cached schema still decodes");
}

/// Serves `https://127.0.0.1:<port>` with a certificate from a CA no trust
/// store holds, and returns the URL.
async fn serve_untrusted_https() -> String {
    let ca = spate_test_support::TestCa::new("untrusted");
    let addr = spate_test_support::serve_tls(ca.server_config(None), |_| async {}).await;
    format!("https://{addr}")
}

/// A registry certificate the client does not trust is fatal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untrusted_registry_certificate_is_fatal() {
    let url = serve_untrusted_https().await;
    let builder = AvroDeserializerBuilder::from_settings(
        &settings_at(url, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let reason = fatal_reason(builder.build_value().unwrap(), confluent_payload(1, 1)).await;
    assert!(reason.contains("certificate"), "{reason}");
}

/// A TLS alert with which the registry rejects the handshake is fatal, and
/// the reason names the alert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejecting_tls_alert_is_fatal() {
    use rustls::AlertDescription as A;
    for alert in [A::HandshakeFailure, A::ProtocolVersion] {
        let addr = spate_test::tls_alert_server(b"", u8::from(alert));
        let builder = AvroDeserializerBuilder::from_settings(
            &settings_at(format!("https://{addr}"), Duration::from_secs(30)),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let reason = fatal_reason(builder.build_value().unwrap(), confluent_payload(1, 1)).await;
        assert!(reason.contains(&format!("{alert:?}")), "{reason}");
    }
}

/// A TLS 1.3 registry that refuses the client for presenting no certificate,
/// which it signals after the handshake, is fatal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_client_certificate_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, clients) = (
        spate_test_support::TestCa::new("registry"),
        spate_test_support::TestCa::new("clients"),
    );
    let addr =
        spate_test_support::serve_tls(registry.server_config(Some(&clients)), |_| async {}).await;
    let mut cfg = settings_at(format!("https://{addr}"), Duration::from_secs(30));
    cfg.registry.as_mut().unwrap().tls.root_ca = Some(registry.write(dir.path()));
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let reason = fatal_reason(builder.build_value().unwrap(), confluent_payload(1, 1)).await;
    assert!(reason.contains("CertificateRequired"), "{reason}");
}

// ---------------------------------------------------------------------------
// The single-pass datum path against the registry
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct EventRec {
    id: i64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datum_path_not_ready_then_decodes_and_interleaves_ids() {
    const SCHEMA_V2: &str = r#"{"type":"record","name":"Event2","fields":[
        {"name":"id","type":"long"},
        {"name":"tag","type":"string"}]}"#;
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/61", 200, schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/62", 200, schema_body(SCHEMA_V2), 0);
    let addr = stub.addr();

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder
        .build_serde_datum::<EventRec>()
        .expect("datum builder");

    // id 61: plain Event.
    let p61 = confluent_payload(61, 7);
    // id 62: Event2, whose extra `tag` field the target type skips, giving a
    // different writer schema (and datum spec) on the same deserializer.
    let p62 = {
        let schema = Schema::parse_str(SCHEMA_V2).unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", 8i64);
        rec.put("tag", "extra");
        let datum = to_avro_datum(&schema, rec).unwrap();
        spate_test::confluent_frame(62, &datum)
    };

    // First call misses: NotReady, and the contract demands zero emits.
    let (ack, _rx) = AckRef::test_pair();
    let mut out = EmitCollector::new();
    let err = deser
        .deserialize(&raw_payload(&p61), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::NotReady { .. }), "{err}");
    assert!(out.records.is_empty());

    let decoded = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &p61, &mut out).unwrap();
        drive_until_ready(&mut deser, &p62, &mut out).unwrap();
        // And interleave again from the (now warm) per-deserializer memo.
        drive_until_ready(&mut deser, &p61, &mut out).unwrap();
        out.payloads()
    })
    .await
    .unwrap();
    assert_eq!(
        decoded,
        vec![EventRec { id: 7 }, EventRec { id: 8 }, EventRec { id: 7 }]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duration_schema_gates_only_the_datum_path() {
    // A schema the datum path refuses (`duration` logical type) must stay
    // fully usable on the Value path: the id is published Ready with the
    // datum-side reason stored per path, and never negative-cached.
    const DURATION_SCHEMA: &str = r#"{"type":"record","name":"D","fields":[
        {"name":"id","type":"long"},
        {"name":"d","type":{"type":"fixed","name":"F","size":12,"logicalType":"duration"}}]}"#;
    let stub = RegistryStub::start();
    stub.script(
        "/schemas/ids/77",
        200,
        schema_body(&DURATION_SCHEMA.replace('\n', " ")),
        0,
    );
    let addr = stub.addr();

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();

    let payload = {
        let schema = Schema::parse_str(DURATION_SCHEMA).unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", 5i64);
        rec.put(
            "d",
            apache_avro::types::Value::Duration(apache_avro::Duration::from([
                0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3,
            ])),
        );
        let datum = to_avro_datum(&schema, rec).unwrap();
        spate_test::confluent_frame(77, &datum)
    };

    // Value path: decodes fine once fetched.
    let mut value_deser = builder.build_value().expect("value builder");
    let p = payload.clone();
    let values = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut value_deser, &p, &mut out).unwrap();
        out.payloads()
    })
    .await
    .unwrap();
    assert_eq!(values.len(), 1);

    // Datum path on the SAME (now Ready) id: per-record SchemaUnavailable
    // with the stored reason, not NotReady and not Malformed.
    let mut datum_deser = builder
        .build_serde_datum::<EventRec>()
        .expect("datum builder");
    let err = tokio::task::spawn_blocking(move || {
        let mut out = EmitCollector::new();
        drive_until_ready(&mut datum_deser, &payload, &mut out).unwrap_err()
    })
    .await
    .unwrap();
    assert!(
        matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("datum path")),
        "{err}"
    );
}

/// Confluent string truncation is malformed after registry resolution.
/// Regression for #878.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confluent_truncated_string_is_malformed() {
    const SCH: &str = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let stub = RegistryStub::start();
    stub.script("/schemas/ids/83", 200, schema_body(SCH), 0);
    let addr = stub.addr();
    let b = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = b.build_value().unwrap();
    tokio::task::spawn_blocking(move || {
        let frame = |bytes: &[u8]| spate_test::confluent_frame(83, bytes);
        let mut out = EmitCollector::new();
        drive_until_ready(&mut deser, &frame(&[2, b'a']), &mut out).unwrap();
        assert_eq!(
            out.payloads(),
            vec![AvroValue::Record(vec![(
                "s".into(),
                AvroValue::String("a".into())
            )])]
        );
        let mut out = EmitCollector::new();
        let result = drive_until_ready(&mut deser, &frame(&[0x22]), &mut out);
        assert!(
            matches!(result, Err(DeserError::Malformed { .. })),
            "result={result:?}, emitted={:?}",
            out.payloads()
        );
        assert!(out.records.is_empty());
    })
    .await
    .unwrap();
}

/// Reader projection preserves malformed writer-field rejection.
/// Regression for #878.
#[tokio::test]
async fn reader_projection_does_not_hide_truncated_writer_string() {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(spate_avro::SchemaSource::inline(
            r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"}]}"#,
        )),
        reader_schema: Some(spate_avro::SchemaSource::inline(
            r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"#,
        )),
        ..AvroSettings::default()
    };
    let b = AvroDeserializerBuilder::from_settings(&settings, &tokio::runtime::Handle::current())
        .unwrap();
    let mut deser = b.build_value().unwrap();
    let mut out = EmitCollector::new();
    deser
        .deserialize(
            &raw_payload(&[18, 2, b'a']),
            &spate_test::test_ack(),
            &mut out,
        )
        .unwrap();
    assert_eq!(
        out.payloads(),
        vec![AvroValue::Record(vec![("a".into(), AvroValue::Long(9))])]
    );
    for bytes in [&[18, 34][..], &[18, 4, b'a'][..]] {
        let mut out = EmitCollector::new();
        let result = deser.deserialize(&raw_payload(bytes), &spate_test::test_ack(), &mut out);
        assert!(
            matches!(result, Err(DeserError::Malformed { .. })),
            "result={result:?}, emitted={:?}",
            out.payloads()
        );
        assert!(out.records.is_empty());
    }
}
