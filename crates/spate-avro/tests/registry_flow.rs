//! End-to-end registry behavior against a stub Confluent-compatible
//! schema-registry server: cache misses report not-ready and resolve after
//! the asynchronous fetch; failures negative-cache with a TTL; pre-warm
//! loads subjects at startup.

use apache_avro::Schema;
use apache_avro::to_avro_datum;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, RegistrySection};
use spate_core::checkpoint::AckRef;
use spate_core::deser::{Deserializer, EmitRecord, RecFamily};
use spate_core::error::DeserError;
use spate_core::record::{Flow, PartitionId, RawPayload, Record};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SCHEMA_V1: &str =
    r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;

/// A scripted response: repeat `status`/`body` for the first `times`
/// matching requests (0 = forever).
#[derive(Clone)]
struct Scripted {
    status: u16,
    body: String,
    times: usize,
}

/// Stub registry: path → response script queue; unmatched paths 404.
#[derive(Clone, Default)]
struct StubRegistry {
    routes: Arc<Mutex<HashMap<String, Vec<Scripted>>>>,
    hits: Arc<AtomicUsize>,
    paths: Arc<Mutex<Vec<String>>>,
    /// While `true`, requests to `hold_path` block until released — used to
    /// prove one slow id does not head-of-line-block other fetches.
    hold: Arc<AtomicBool>,
    hold_path: Option<String>,
}

impl StubRegistry {
    fn script(&self, path: &str, status: u16, body: &str, times: usize) {
        self.routes
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .push(Scripted {
                status,
                body: body.to_string(),
                times,
            });
    }

    fn path_hits(&self, path: &str) -> usize {
        self.paths
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p == &path)
            .count()
    }

    fn respond(&self, path: &str) -> (u16, String) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.paths.lock().unwrap().push(path.to_string());
        let mut routes = self.routes.lock().unwrap();
        if let Some(queue) = routes.get_mut(path)
            && let Some(first) = queue.first_mut()
        {
            let response = (first.status, first.body.clone());
            if first.times > 0 {
                first.times -= 1;
                if first.times == 0 {
                    queue.remove(0);
                }
            }
            return response;
        }
        (
            404,
            r#"{"error_code":40403,"message":"Schema not found"}"#.into(),
        )
    }

    async fn serve(self) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let stub = self.clone();
                tokio::spawn(async move {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let stub = stub.clone();
                        async move {
                            let path = req.uri().path().to_string();
                            // Gate: block this path until the test releases it.
                            if stub.hold_path.as_deref() == Some(path.as_str()) {
                                while stub.hold.load(Ordering::Relaxed) {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            }
                            let (status, body) = stub.respond(&path);
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(StatusCode::from_u16(status).unwrap())
                                    .header(
                                        "content-type",
                                        "application/vnd.schemaregistry.v1+json",
                                    )
                                    .body(Full::new(Bytes::from(body)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        });
        addr
    }
}

fn schema_body(schema: &str) -> String {
    serde_json::json!({ "schema": schema }).to_string()
}

fn confluent_payload(id: u32, event_id: i64) -> Vec<u8> {
    let schema = Schema::parse_str(SCHEMA_V1).unwrap();
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("id", event_id);
    let datum = to_avro_datum(&schema, rec).unwrap();
    let mut payload = vec![0x00];
    payload.extend_from_slice(&id.to_be_bytes());
    payload.extend_from_slice(&datum);
    payload
}

fn raw(bytes: &[u8]) -> RawPayload<'_> {
    RawPayload {
        bytes,
        key: None,
        partition: PartitionId(0),
        offset: 1,
        timestamp_ms: 0,
    }
}

struct Collected(Vec<AvroValue>);
impl EmitRecord<'_, AvroValue> for Collected {
    fn emit(&mut self, rec: Record<AvroValue>) -> Flow {
        self.0.push(rec.payload);
        Flow::Continue
    }
}

fn settings(addr: std::net::SocketAddr, ttl: Duration) -> AvroSettings {
    AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection {
            url: format!("http://{addr}"),
            username: None,
            password: None,
        }),
        negative_cache_ttl: ttl,
        ..AvroSettings::default()
    }
}

/// Retry `deserialize` until the async fetch lands or the deadline passes.
///
/// Generic over the deserializer family and emitter so the value path and the
/// serde-typed path share one driver. The 20ms backoff is a
/// retry cadence, not a sleep-poll: `deserialize` is itself the readiness
/// probe (there is no external signal to block on), so no blocking wait helper
/// applies here.
fn drive_until_ready<F, O>(
    deser: &mut dyn Deserializer<F>,
    payload: &[u8],
    out: &mut O,
) -> Result<(), DeserError>
where
    F: RecFamily,
    O: for<'buf> EmitRecord<'buf, F::Rec<'buf>>,
{
    let (ack, _rx) = AckRef::test_pair();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match deser.deserialize(&raw(payload), &ack, out) {
            Err(DeserError::NotReady { .. }) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            other => return other,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn miss_reports_not_ready_then_decodes_after_fetch() {
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/42", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");

    let payload = confluent_payload(42, 7);
    let (ack, _rx) = AckRef::test_pair();
    let mut out = Collected(Vec::new());

    // First call: not ready (fetch just triggered), nothing emitted.
    let err = deser
        .deserialize(&raw(&payload), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::NotReady { .. }), "{err}");
    assert!(out.0.is_empty());

    // The driver's retry loop, condensed.
    let result = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.len(), 1, "exactly one record after the fetch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retriable_registry_errors_are_retried() {
    let stub = StubRegistry::default();
    // 502/503 are retriable in the registry client, but every transient
    // failure now leaves the id absent and is retried by the deserializer
    // replaying the payload (bounded by per-id fetch backoff), never
    // negatively cached.
    stub.script("/schemas/ids/9", 503, "shard warming up", 2);
    stub.script("/schemas/ids/9", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(9, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "fetch retried through the 500s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_id_negative_caches_until_ttl_expiry() {
    let stub = StubRegistry::default();
    // No script for id 5: the stub answers 404 (and counts hits).
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_millis(300)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(5, 1);

    // Drive to the negative-cache verdict.
    let (mut deser, first) = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
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
    let hits_after_first = stub.hits.load(Ordering::Relaxed);

    // Within the TTL: answered from the negative cache, no new requests.
    let payload = confluent_payload(5, 1);
    let (ack, _rx) = AckRef::test_pair();
    let mut out = Collected(Vec::new());
    let err = deser
        .deserialize(&raw(&payload), &ack, &mut out)
        .unwrap_err();
    assert!(matches!(err, DeserError::SchemaUnavailable { .. }), "{err}");
    assert_eq!(stub.hits.load(Ordering::Relaxed), hits_after_first);

    // After expiry the schema exists now: refetch succeeds.
    stub.script("/schemas/ids/5", 200, &schema_body(SCHEMA_V1), 0);
    std::thread::sleep(Duration::from_millis(350));
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "expired negative entry allows a refetch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_503s_then_success_decodes_and_never_drops() {
    // The critical regression (registry.rs poison-cache): three transient
    // 503s must not poison the id. Each leaves it absent; the deserializer's
    // replay refetches (bounded by per-id backoff) until the schema resolves.
    // The record is never dropped/acked as poison.
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/42", 503, "shard warming up", 3);
    stub.script("/schemas/ids/42", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(42, 7);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "record decodes after the transient blips");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_retriable_5xx_is_transient_not_poison() {
    // A single non-retriable 500 (the registry restarting behind an LB) must
    // NOT be negatively cached: doing so would surface SchemaUnavailable for
    // the whole TTL and silently drop valid records under ErrorPolicy::Skip.
    // The id is left absent and refetched — this fails on the old code, which
    // called insert_failed on any non-retriable error.
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/7", 500, "internal error", 1);
    stub.script("/schemas/ids/7", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(7, 1);

    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
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
    let mut stub = StubRegistry::default();
    stub.script("/schemas/ids/100", 200, &schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/200", 200, &schema_body(SCHEMA_V1), 0);
    stub.hold.store(true, Ordering::Relaxed);
    stub.hold_path = Some("/schemas/ids/100".into());
    let released = Arc::clone(&stub.hold);
    let addr = stub.clone().serve().await;

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
    let mut sink = Collected(Vec::new());
    assert!(matches!(
        deser.deserialize(&raw(&slow), &ack, &mut sink).unwrap_err(),
        DeserError::NotReady { .. }
    ));

    let slow_probe = slow.clone();
    let mut deser = tokio::task::spawn_blocking(move || {
        // The fast id resolves while the slow id is still gated.
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &fast, &mut out).unwrap();
        assert_eq!(out.0.len(), 1, "fast id resolved despite the gated slow id");

        // The slow id is still unavailable (its fetch is blocked).
        let (ack, _rx) = AckRef::test_pair();
        let mut out = Collected(Vec::new());
        assert!(matches!(
            deser
                .deserialize(&raw(&slow_probe), &ack, &mut out)
                .unwrap_err(),
            DeserError::NotReady { .. }
        ));
        deser
    })
    .await
    .unwrap();

    // Release the gate: the slow id now resolves too.
    released.store(false, Ordering::Relaxed);
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &slow, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1, "slow id resolves after the gate is released");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prewarm_loads_subjects_at_startup() {
    let stub = StubRegistry::default();
    stub.script(
        "/subjects/events-value/versions/latest",
        200,
        &serde_json::json!({
            "schema": SCHEMA_V1, "id": 42, "version": 3, "subject": "events-value"
        })
        .to_string(),
        0,
    );
    // Fallback for the startup race: a payload arriving before the
    // pre-warm lands triggers an on-demand by-id fetch.
    stub.script("/schemas/ids/42", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let mut cfg = settings(addr, Duration::from_millis(100));
    cfg.prewarm_subjects = vec!["events-value".into()];
    let builder =
        AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current()).unwrap();
    let mut deser = builder.build_value().expect("apache builder");

    // The pre-warm must request the subject's latest version at startup.
    let deadline = Instant::now() + Duration::from_secs(10);
    while stub.path_hits("/subjects/events-value/versions/latest") == 0 {
        assert!(Instant::now() < deadline, "pre-warm never hit the registry");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let payload = confluent_payload(42, 7);
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1);
}

/// apache-avro 0.21 `Schema::parse_str` *panics* (not `Err`) on some
/// malformed names — `"my-record"` trips an internal unwrap. The compile
/// catches the panic and stores it as an ordinary failure, so the id
/// surfaces a per-record poison (SchemaUnavailable) the ErrorPolicy can act
/// on — never a permanent NotReady stall, and never an unwind on whichever
/// pipeline thread touched it first. (The caught panic prints a backtrace to
/// stderr; that is expected and harmless.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_schema_parse_poisons_the_id_rather_than_stalling() {
    let stub = StubRegistry::default();
    let bad = r#"{"type":"record","name":"my-record","fields":[{"name":"id","type":"long"}]}"#;
    stub.script("/schemas/ids/77", 200, &schema_body(bad), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(77, 1);

    let err = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(err, DeserError::SchemaUnavailable { .. }),
        "a panicking schema parse must poison the id, not stall at NotReady: {err}"
    );
}

// ---------------------------------------------------------------------------
// The single-pass datum path against the registry
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, PartialEq)]
struct EventRec {
    id: i64,
}

struct CollectedRec(Vec<EventRec>);
impl EmitRecord<'_, EventRec> for CollectedRec {
    fn emit(&mut self, rec: Record<EventRec>) -> Flow {
        self.0.push(rec.payload);
        Flow::Continue
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datum_path_not_ready_then_decodes_and_interleaves_ids() {
    const SCHEMA_V2: &str = r#"{"type":"record","name":"Event2","fields":[
        {"name":"id","type":"long"},
        {"name":"tag","type":"string"}]}"#;
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/61", 200, &schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/62", 200, &schema_body(SCHEMA_V2), 0);
    let addr = stub.clone().serve().await;

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
    // id 62: Event2, whose extra `tag` field the target type skips — a
    // different writer schema (and datum spec) on the same deserializer.
    let p62 = {
        let schema = Schema::parse_str(SCHEMA_V2).unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", 8i64);
        rec.put("tag", "extra");
        let datum = to_avro_datum(&schema, rec).unwrap();
        let mut payload = vec![0x00];
        payload.extend_from_slice(&62u32.to_be_bytes());
        payload.extend_from_slice(&datum);
        payload
    };

    // First call misses: NotReady, and the contract demands zero emits.
    let (ack, _rx) = AckRef::test_pair();
    let mut out = CollectedRec(Vec::new());
    let err = deser.deserialize(&raw(&p61), &ack, &mut out).unwrap_err();
    assert!(matches!(err, DeserError::NotReady { .. }), "{err}");
    assert!(out.0.is_empty());

    let decoded = tokio::task::spawn_blocking(move || {
        let mut out = CollectedRec(Vec::new());
        drive_until_ready(&mut deser, &p61, &mut out).unwrap();
        drive_until_ready(&mut deser, &p62, &mut out).unwrap();
        // And interleave again from the (now warm) per-deserializer memo.
        drive_until_ready(&mut deser, &p61, &mut out).unwrap();
        out.0
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
    // datum-side reason stored per path — never negative-cached.
    const DURATION_SCHEMA: &str = r#"{"type":"record","name":"D","fields":[
        {"name":"id","type":"long"},
        {"name":"d","type":{"type":"fixed","name":"F","size":12,"logicalType":"duration"}}]}"#;
    let stub = StubRegistry::default();
    stub.script(
        "/schemas/ids/77",
        200,
        &schema_body(&DURATION_SCHEMA.replace('\n', " ")),
        0,
    );
    let addr = stub.clone().serve().await;

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
        let mut payload = vec![0x00];
        payload.extend_from_slice(&77u32.to_be_bytes());
        payload.extend_from_slice(&datum);
        payload
    };

    // Value path: decodes fine once fetched.
    let mut value_deser = builder.build_value().expect("value builder");
    let p = payload.clone();
    let values = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut value_deser, &p, &mut out).unwrap();
        out.0
    })
    .await
    .unwrap();
    assert_eq!(values.len(), 1);

    // Datum path on the SAME (now Ready) id: per-record SchemaUnavailable
    // with the stored reason — not NotReady, not Malformed.
    let mut datum_deser = builder
        .build_serde_datum::<EventRec>()
        .expect("datum builder");
    let err = tokio::task::spawn_blocking(move || {
        let mut out = CollectedRec(Vec::new());
        drive_until_ready(&mut datum_deser, &payload, &mut out).unwrap_err()
    })
    .await
    .unwrap();
    assert!(
        matches!(&err, DeserError::SchemaUnavailable { reason } if reason.contains("datum path")),
        "{err}"
    );
}
