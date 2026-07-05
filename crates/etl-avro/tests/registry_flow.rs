//! End-to-end registry behavior against a stub Confluent-compatible
//! schema-registry server: cache misses report not-ready and resolve after
//! the asynchronous fetch; failures negative-cache with a TTL; pre-warm
//! loads subjects at startup.

use apache_avro::Schema;
use apache_avro::to_avro_datum;
use etl_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, RegistrySection};
use etl_core::checkpoint::AckRef;
use etl_core::deser::{Deserializer, EmitRecord};
use etl_core::error::DeserError;
use etl_core::record::{Flow, PartitionId, RawPayload, Record};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
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
                            let (status, body) = stub.respond(req.uri().path());
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
fn drive_until_ready(
    deser: &mut etl_avro::AvroValueDeserializer,
    payload: &[u8],
    out: &mut Collected,
) -> Result<(), DeserError> {
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
    let mut deser = builder.build_value();

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
    // Only 502/503 (and transport errors) are retriable in the
    // registry client; 500s negative-cache immediately.
    stub.script("/schemas/ids/9", 503, "shard warming up", 2);
    stub.script("/schemas/ids/9", 200, &schema_body(SCHEMA_V1), 0);
    let addr = stub.clone().serve().await;

    let builder = AvroDeserializerBuilder::from_settings(
        &settings(addr, Duration::from_secs(30)),
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut deser = builder.build_value();
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
    let mut deser = builder.build_value();
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
    let mut deser = builder.build_value();

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
