//! An in-process HTTP/1 server that stands in for ClickHouse in the
//! write-path benches.
//!
//! It answers every request that is not an insert with the
//! `system.columns` rows it was built with, so a sink built by
//! `config::build(..).with_row()` points at it like a real server, and it
//! accepts every insert. A request is an insert when its URL carries a
//! `query` pair, which only the client's insert requests write. The client
//! validates the schema answer against its `RowBinaryWithNamesAndTypes`
//! header, and a compressing client decodes it in its own codec, so the
//! answer follows the request as ClickHouse does:
//!
//! - `compress=1` (the client under lz4) gets ClickHouse-framed lz4 blocks;
//! - `enable_http_compression=1` with `Accept-Encoding: zstd` (the client
//!   under zstd) gets a plain zstd stream under `Content-Encoding: zstd`;
//! - anything else gets plain RowBinary.
//!
//! An insert body is read frame by frame and discarded, and the server
//! answers `200` once the body ends. A server built with [`BenchServer::recording`]
//! keeps each request's query pairs, headers and body instead, for tests.
//! The benches never record.
//!
//! The server runs on its own multi-thread runtime, so its work does not
//! share the writer's thread. A case's `cpu_ns_per_iter` reads the whole
//! process's CPU, so it covers the writer and this server's discard loop
//! alike. Both legs of a comparison carry the same server cost.

#![allow(dead_code, reason = "each target uses a different subset")]

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::testing::{ColumnRow, system_columns};
use spate_clickhouse::{ClickHouseEndpoint, ClickHouseWriter, serialize_row};
use spate_core::deser::Owned;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use super::rows::EventRow;

/// One request as the server received it.
#[derive(Clone, Debug)]
pub(crate) struct Recorded {
    /// The URL's query pairs, in the order the client wrote them.
    pub(crate) query: Vec<(String, String)>,
    /// Every header, names lowercased.
    pub(crate) headers: Vec<(String, String)>,
    /// The request body, exactly as it arrived.
    pub(crate) body: Bytes,
}

impl Recorded {
    /// The value of query pair `name`, if the request carried one.
    pub(crate) fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// What the server answers and keeps, shared by every connection.
struct State {
    /// The `system.columns` answer as plain RowBinaryWithNamesAndTypes.
    columns: Bytes,
    recorded: Option<Mutex<Vec<Recorded>>>,
}

/// A running server on `127.0.0.1` and an OS-chosen port. Dropping it stops
/// the runtime and closes the listener.
pub(crate) struct BenchServer {
    url: String,
    state: Arc<State>,
    runtime: Option<Runtime>,
}

impl BenchServer {
    /// A server that answers the schema query with `columns`, as
    /// `(name, type)` pairs in table order, and discards every insert.
    pub(crate) fn discarding(columns: &[(&str, &str)]) -> Self {
        Self::start(columns, false)
    }

    /// A server like [`BenchServer::discarding`] that also records every
    /// request.
    pub(crate) fn recording(columns: &[(&str, &str)]) -> Self {
        Self::start(columns, true)
    }

    fn start(columns: &[(&str, &str)], record: bool) -> Self {
        let specs: Vec<(&str, &str, &str)> = columns.iter().map(|(n, t)| (*n, *t, "")).collect();
        let state = Arc::new(State {
            columns: columns_answer(&system_columns(&specs)),
            recorded: record.then(|| Mutex::new(Vec::new())),
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("bench-server")
            .enable_io()
            .build()
            .expect("the server runtime builds");
        let listener = runtime
            .block_on(TcpListener::bind("127.0.0.1:0"))
            .expect("a loopback port");
        let addr = listener.local_addr().expect("the bound address");
        let accept = Arc::clone(&state);
        runtime.spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let state = Arc::clone(&accept);
                tokio::spawn(async move {
                    let service = service_fn(move |req| answer(Arc::clone(&state), req));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self {
            url: format!("http://{addr}"),
            state,
            runtime: Some(runtime),
        }
    }

    /// The server's `http://` URL.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Every request received so far, in arrival order.
    ///
    /// # Panics
    ///
    /// Panics on a server built with [`BenchServer::discarding`].
    pub(crate) fn requests(&self) -> Vec<Recorded> {
        self.state
            .recorded
            .as_ref()
            .expect("a recording server")
            .lock()
            .expect("the record lock")
            .clone()
    }
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// The writer and the one endpoint of a Native sink over the `events` table
/// at `url`, built by `config::build(..).with_row()` as a pipeline builds it.
///
/// `compression` is the sink's `compression` value: `lz4`, `zstd:3`, `off`.
///
/// # Panics
///
/// Panics when the config is invalid or the schema fetch fails.
pub(crate) async fn events_sink(
    url: &str,
    compression: &str,
) -> (ClickHouseWriter, ClickHouseEndpoint) {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: events
format: native
compression: "{compression}"
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    let sink = config::build(cfg)
        .expect("valid sink config")
        .with_row::<Owned<EventRow>>()
        .await
        .expect("the schema fetch");
    let endpoint = sink
        .endpoints
        .into_iter()
        .next()
        .and_then(|replicas| replicas.into_iter().next())
        .expect("one shard with one replica");
    (sink.writer, endpoint)
}

/// `rows` as ClickHouse answers `FORMAT RowBinaryWithNamesAndTypes`: the
/// column count, the names, the types, then each row.
fn columns_answer(rows: &[ColumnRow]) -> Bytes {
    let mut buf = BytesMut::new();
    // Three columns, each name and type a short LEB128-prefixed string.
    buf.extend_from_slice(&[3]);
    for name in ["name", "type", "default_kind"] {
        put_str(&mut buf, name);
    }
    for _ in 0..3 {
        put_str(&mut buf, "String");
    }
    for row in rows {
        serialize_row(row, &mut buf).expect("a column row encodes");
    }
    buf.freeze()
}

fn put_str(buf: &mut BytesMut, s: &str) {
    let len = u8::try_from(s.len()).expect("a header string fits a byte");
    assert!(len < 0x80, "a one-byte LEB128 length");
    buf.extend_from_slice(&[len]);
    buf.extend_from_slice(s.as_bytes());
}

async fn answer(
    state: Arc<State>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let query: Vec<(String, String)> = req
        .uri()
        .query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    let accepts_zstd = headers
        .iter()
        .any(|(k, v)| k == "accept-encoding" && v.contains("zstd"));

    let mut body = req.into_body();
    let mut kept = state.recorded.as_ref().map(|_| BytesMut::new());
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            return Ok(status(StatusCode::BAD_REQUEST));
        };
        if let (Some(kept), Some(data)) = (kept.as_mut(), frame.data_ref()) {
            kept.extend_from_slice(data);
        }
    }

    let insert = query.iter().any(|(k, _)| k == "query");
    let flag = |name: &str| query.iter().any(|(k, v)| k == name && v == "1");
    let response = if insert {
        status(StatusCode::OK)
    } else if flag("compress") {
        let block = clickhouse::_priv::lz4_compress(&state.columns).expect("lz4 frames the answer");
        Response::new(Full::new(block))
    } else if flag("enable_http_compression") && accepts_zstd {
        let stream = zstd::encode_all(&state.columns[..], 3).expect("zstd encodes the answer");
        Response::builder()
            .header("content-encoding", "zstd")
            .body(Full::new(Bytes::from(stream)))
            .expect("a valid response")
    } else {
        Response::new(Full::new(state.columns.clone()))
    };

    if let (Some(recorded), Some(kept)) = (state.recorded.as_ref(), kept) {
        recorded.lock().expect("the record lock").push(Recorded {
            query,
            headers,
            body: kept.freeze(),
        });
    }
    Ok(response)
}

fn status(code: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = code;
    response
}
