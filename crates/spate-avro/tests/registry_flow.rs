//! End-to-end registry behavior against a stub Confluent-compatible
//! schema-registry server: cache misses report not-ready and resolve after
//! the asynchronous fetch; failures negative-cache with a TTL; pre-warm
//! loads subjects at startup.

#![expect(deprecated, reason = "fixtures call the datum free functions directly")]

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
use spate_core::record::{Flow, Record};
use spate_test::raw_payload;
use spate_test_support::run_in_child;
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
    /// While `true`, requests to `hold_path` block until released, which
    /// shows one slow id does not head-of-line-block other fetches.
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

struct Collected(Vec<AvroValue>);
impl EmitRecord<'_, AvroValue> for Collected {
    fn emit(&mut self, rec: Record<AvroValue>) -> Flow {
        self.0.push(rec.payload);
        Flow::Continue
    }
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
        match deser.deserialize(&raw_payload(payload), &ack, out) {
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
        .deserialize(&raw_payload(&payload), &ack, &mut out)
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
    // A transient failure leaves the id absent, and the deserializer's replay
    // refetches it (bounded by per-id fetch backoff); it is never negatively
    // cached.
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
        .deserialize(&raw_payload(&payload), &ack, &mut out)
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
    // The id is left absent and refetched. Calling insert_failed on any
    // non-retriable error fails this test.
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
        deser
            .deserialize(&raw_payload(&slow), &ack, &mut sink)
            .unwrap_err(),
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
                .deserialize(&raw_payload(&slow_probe), &ack, &mut out)
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

/// The registry URL a re-executed child test builds against.
const CHILD_REGISTRY_URL: &str = "SPATE_TEST_AVRO_REGISTRY_URL";

/// With an empty system trust store, an `http://` registry builds and
/// decodes, and an `https://` registry builds. Regression for #624.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_trust_store_does_not_fail_the_build() {
    const NAME: &str = "an_empty_trust_store_does_not_fail_the_build";
    let runtime = tokio::runtime::Handle::current();
    if let Ok(url) = std::env::var(CHILD_REGISTRY_URL) {
        let https = settings_at("https://registry.invalid".into(), Duration::from_secs(30));
        AvroDeserializerBuilder::from_settings(&https, &runtime).expect("https registry builds");
        let builder = AvroDeserializerBuilder::from_settings(
            &settings_at(url, Duration::from_secs(30)),
            &runtime,
        )
        .expect("http registry builds");
        let mut deser = builder.build_value().expect("apache builder");
        let payload = confluent_payload(42, 7);
        let rows = tokio::task::spawn_blocking(move || {
            let mut out = Collected(Vec::new());
            drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rows.len(), 1);
        return;
    }
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/42", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.serve().await;
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
        "a refused schema parse must poison the id, not stall at NotReady: {err}"
    );
}

/// A schema the registry types as other than Avro poisons the id, though its
/// text also parses as an Avro schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_schema_that_is_not_avro_poisons_the_id() {
    let stub = StubRegistry::default();
    let body = serde_json::json!({ "schema": r#"{"type":"string"}"#, "schemaType": "JSON" });
    stub.script("/schemas/ids/78", 200, &body.to_string(), 0);
    let addr = stub.clone().serve().await;
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(78, 1);
    let err = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
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
    let stub = StubRegistry::default();
    let json =
        serde_json::json!({ "schema": r#"{"type":"string"}"#, "schemaType": "JSON", "id": 79 })
            .to_string();
    stub.script("/subjects/json-value/versions/latest", 200, &json, 0);
    stub.script("/schemas/ids/79", 200, &json, 0);
    let addr = stub.clone().serve().await;
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
            Duration::from_secs(10),
            "the pre-warm reaches the second subject",
            || hits.path_hits("/subjects/sentinel-value/versions/latest") > 0,
        );
    })
    .await
    .unwrap();
    // Confluent framing for id 79, then the Avro string "a".
    let mut payload = vec![0x00];
    payload.extend_from_slice(&79u32.to_be_bytes());
    payload.extend_from_slice(&[0x02, b'a']);
    let result = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0.len())
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
    let stub = StubRegistry::default();
    // Unscripted: the stub answers 404 {"error_code":40403,"message":"Schema not found"}.
    let addr = stub.clone().serve().await;
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().expect("apache builder");
    let payload = confluent_payload(5, 1);
    let err = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
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
    let stub = StubRegistry::default();
    stub.script("/subjects/events-value/versions/latest", 403, "{}", 0);
    stub.script("/schemas/ids/42", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;
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
            Duration::from_secs(10),
            "the pre-warm reaches the second subject",
            || hits.path_hits("/subjects/sentinel-value/versions/latest") > 0,
        );
    })
    .await
    .unwrap();
    let payload = confluent_payload(42, 1);
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &payload, &mut out).map(|()| out.0.len())
    })
    .await
    .unwrap();
    assert_eq!(rows.unwrap(), 1);
}

/// The target and `Authorization` headers of each request a stub received.
type Seen = Arc<Mutex<Vec<(String, Vec<String>)>>>;

/// Serves `SCHEMA_V1` to every request and records each request's target and
/// `Authorization` headers.
async fn serve_recording() -> (std::net::SocketAddr, Seen) {
    let seen = Seen::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let record = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let record = Arc::clone(&record);
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let record = Arc::clone(&record);
                    async move {
                        let auth = req
                            .headers()
                            .get_all(hyper::header::AUTHORIZATION)
                            .iter()
                            .map(|v| v.to_str().unwrap().to_owned())
                            .collect();
                        let target = req
                            .uri()
                            .path_and_query()
                            .map(ToString::to_string)
                            .unwrap_or_default();
                        record.lock().unwrap().push((target, auth));
                        Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                            schema_body(SCHEMA_V1),
                        ))))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (addr, seen)
}

/// The by-id fetch sends `registry.username`/`password`, or the URL userinfo,
/// as basic auth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetch_sends_basic_auth() {
    for (userinfo, username, expected) in [
        ("", Some("svc"), "Basic c3ZjOmh1bnRlcjI="), // svc:hunter2
        ("urluser:urlsecret@", None, "Basic dXJsdXNlcjp1cmxzZWNyZXQ="), // urluser:urlsecret
    ] {
        let (addr, seen) = serve_recording().await;
        let mut cfg = settings_at(format!("http://{userinfo}{addr}"), Duration::from_secs(30));
        let registry = cfg.registry.as_mut().unwrap();
        registry.username = username.map(Into::into);
        registry.password = username.map(|_| "hunter2".into());
        let builder =
            AvroDeserializerBuilder::from_settings(&cfg, &tokio::runtime::Handle::current())
                .unwrap();
        let mut deser = builder.build_value().unwrap();
        let payload = confluent_payload(5, 1);
        tokio::task::spawn_blocking(move || {
            let mut out = Collected(Vec::new());
            drive_until_ready(&mut deser, &payload, &mut out)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [(
                "/schemas/ids/5?deleted=true".to_owned(),
                vec![expected.to_owned()]
            )]
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
        let mut out = Collected(Vec::new());
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
        let stub = StubRegistry::default();
        stub.script(
            "/schemas/ids/1",
            status,
            r#"{"error_code":401,"message":"Unauthorized"}"#,
            0,
        );
        let addr = stub.serve().await;
        let mut cfg = settings_at(
            format!("http://urluser:urlsecret@{addr}"),
            Duration::from_secs(30),
        );
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
        for secret in ["urlsecret", "hunter2"] {
            assert!(!reason.contains(secret), "{reason}");
        }
    }
}

/// A `401` on the pre-warm makes the first cache miss fatal, even while the
/// by-id fetch only ever sees a transient `503`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prewarm_auth_rejection_is_fatal_at_the_first_miss() {
    let stub = StubRegistry::default();
    stub.script(
        "/subjects/events-value/versions/latest",
        401,
        r#"{"error_code":40101,"message":"Unauthorized"}"#,
        0,
    );
    stub.script("/schemas/ids/42", 503, "{}", 0);
    let addr = stub.serve().await;
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
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/1", 200, &schema_body(SCHEMA_V1), 0);
    stub.script("/schemas/ids/2", 401, "{}", 0);
    let addr = stub.serve().await;
    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value().unwrap();
    let (cached, rejected) = (confluent_payload(1, 7), confluent_payload(2, 8));
    let (first, second, again) = tokio::task::spawn_blocking(move || {
        let mut out = Collected(Vec::new());
        let first = drive_until_ready(&mut deser, &cached, &mut out);
        let second = drive_until_ready(&mut deser, &rejected, &mut out);
        let again = drive_until_ready(&mut deser, &cached, &mut out);
        (first.map(|()| out.0.len()), second, again)
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
    // id 62: Event2, whose extra `tag` field the target type skips, giving a
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
    let err = deser
        .deserialize(&raw_payload(&p61), &ack, &mut out)
        .unwrap_err();
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
    // datum-side reason stored per path, and never negative-cached.
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
    // with the stored reason, not NotReady and not Malformed.
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

/// Confluent string truncation is malformed after registry resolution.
/// Regression for #878.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confluent_truncated_string_is_malformed() {
    const SCH: &str = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let stub = StubRegistry::default();
    stub.script("/schemas/ids/83", 200, &schema_body(SCH), 0);
    let addr = stub.serve().await;
    let b = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = b.build_value().unwrap();
    tokio::task::spawn_blocking(move || {
        let frame = |bytes: &[u8]| {
            let mut p = vec![0, 0, 0, 0, 83];
            p.extend_from_slice(bytes);
            p
        };
        let mut out = Collected(Vec::new());
        drive_until_ready(&mut deser, &frame(&[2, b'a']), &mut out).unwrap();
        assert_eq!(
            out.0,
            vec![AvroValue::Record(vec![(
                "s".into(),
                AvroValue::String("a".into())
            )])]
        );
        let mut out = Collected(Vec::new());
        let result = drive_until_ready(&mut deser, &frame(&[0x22]), &mut out);
        assert!(
            matches!(result, Err(DeserError::Malformed { .. })),
            "result={result:?}, emitted={:?}",
            out.0
        );
        assert!(out.0.is_empty());
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
    let mut out = Collected(Vec::new());
    deser
        .deserialize(
            &raw_payload(&[18, 2, b'a']),
            &spate_test::test_ack(),
            &mut out,
        )
        .unwrap();
    assert_eq!(
        out.0,
        vec![AvroValue::Record(vec![("a".into(), AvroValue::Long(9))])]
    );
    for bytes in [&[18, 34][..], &[18, 4, b'a'][..]] {
        let mut out = Collected(Vec::new());
        let result = deser.deserialize(&raw_payload(bytes), &spate_test::test_ack(), &mut out);
        assert!(
            matches!(result, Err(DeserError::Malformed { .. })),
            "result={result:?}, emitted={:?}",
            out.0
        );
        assert!(out.0.is_empty());
    }
}
