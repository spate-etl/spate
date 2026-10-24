// Inserts whose frames the sink's codec coded at seal, read back from a real
// server on every lane: every compression in both formats, an insert of many
// blocks of very different sizes, incompressible rows, empty and one-row
// batches, a block the server rejects, and a retry after a lost response.

use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
use hyper::server::conn::http1;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use spate_clickhouse::{Compression, NativeEncoder, serialize_row};
use spate_core::error::{ErrorClass, SinkError};
use std::sync::{Arc, Mutex};

/// The sink settings for `format` and `compression`, with the fixture's
/// credentials.
fn settings(format: &str, compression: &str) -> String {
    format!("format: {format}\ncompression: \"{compression}\"\n{SERVER_CREDENTIALS}")
}

/// `chunks` of `rows`, each encoded in the sink's format as one frame, then
/// coded by one instance of the codec for `compression`, as one chain seals
/// and codes its chunks in turn.
fn coded<T>(
    sink: &config::ClickHouseSink,
    compression: &str,
    chunks: &[&[T]],
    token: &str,
) -> SealedBatch
where
    T: Serialize + Clone + Send + 'static,
{
    let mut native = (sink.format() == config::Format::Native)
        .then(|| NativeEncoder::<Owned<T>>::new(sink.native_schema().expect("a Native schema")));
    let raw: Vec<Bytes> = chunks
        .iter()
        .map(|chunk| {
            let mut buf = BytesMut::new();
            match native.as_mut() {
                Some(encoder) => {
                    use spate_core::sink::RowEncoder;
                    for row in *chunk {
                        encoder
                            .encode(&record(row.clone()), &mut buf)
                            .expect("encode");
                    }
                    encoder.finish_chunk(&mut buf).expect("finish the block");
                }
                None => {
                    for row in *chunk {
                        serialize_row(row, &mut buf).expect("encode");
                    }
                }
            }
            buf.freeze()
        })
        .collect();
    let rows = chunks.iter().map(|c| c.len() as u64).sum();
    support::coded_batch(parse(compression), raw, rows, token)
}

fn parse(compression: &str) -> Compression {
    compression.parse().expect("a compression value")
}

async fn read_orders(admin: &clickhouse::Client) -> Vec<Order> {
    admin
        .query("SELECT ?fields FROM orders ORDER BY id")
        .fetch_all()
        .await
        .expect("read back")
}

async fn truncate(admin: &clickhouse::Client, table: &str) {
    admin
        .query(&format!("TRUNCATE TABLE {table}"))
        .execute()
        .await
        .expect("truncate");
}

/// Coded frames insert and read back under every compression in both
/// formats.
#[tokio::test]
#[ignore = "requires Docker"]
async fn coded_frames_insert_under_every_compression() {
    let srv = server().await;
    let expected = orders(0..600);
    let chunks: Vec<&[Order]> = expected.chunks(200).collect();
    for format in ["native", "rowbinary"] {
        for compression in ["lz4", "zstd", "zstd:1", "zstd:19", "off"] {
            let case = format!("{format} {compression}");
            truncate(&srv.admin, "orders").await;
            let sink =
                sink_with::<Owned<Order>>(&srv.url, "orders", &settings(format, compression)).await;
            let batch = coded(&sink, compression, &chunks, &format!("every-{case}"));
            sink.writer
                .write_batch(&sink.endpoints[0][0], &batch)
                .await
                .unwrap_or_else(|e| panic!("{case}: write: {e}"));
            assert_eq!(read_orders(&srv.admin).await, expected, "{case}");
        }
    }
}

/// A block that passes the writer's own check but not the server's, one
/// payload byte flipped so the checksum no longer matches, fails the write
/// `Fatal` with server code 271, and inserts nothing.
#[tokio::test]
#[ignore = "requires Docker"]
#[expect(
    clippy::print_stderr,
    reason = "the code each lane answers is read from the test's output"
)]
async fn a_block_the_server_rejects_is_fatal() {
    let srv = server().await;
    let lane = std::env::var("SPATE_CLICKHOUSE_LANE").unwrap_or_else(|_| "primary".into());
    let rows = orders(0..100);
    for compression in ["lz4", "zstd"] {
        let sink =
            sink_with::<Owned<Order>>(&srv.url, "orders", &settings("native", compression)).await;
        let mut batch = coded(&sink, compression, &[&rows], &format!("flip-{compression}"));
        let mut block = batch.frames[0].to_vec();
        // Past the 16-byte checksum and the 9-byte header: the payload.
        block[30] ^= 0xff;
        batch.frames[0] = Bytes::from(block);
        let err = sink
            .writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect_err("the server rejects the block");
        let SinkError::Client { class, reason } = &err else {
            panic!("{compression}: {err:?}");
        };
        let code = reason.find("Code: ").and_then(|at| {
            reason[at + 6..]
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .and_then(|d| d.parse::<u32>().ok())
        });
        eprintln!("lane {lane}, {compression}: the server answered code {code:?}");
        assert_eq!(
            code,
            Some(271),
            "{compression}: the decode failure code: {reason}"
        );
        assert_eq!(*class, ErrorClass::Fatal, "{compression}: {reason}");
        assert_eq!(
            count(&srv.admin).await,
            0,
            "{compression}: nothing inserted"
        );
    }
}

/// One insert carrying a chunk far above the codec's scratch floor, then
/// several small chunks, so the codec replaces its scratch between blocks of
/// one request. Every row reads back in order.
#[tokio::test]
#[ignore = "requires Docker"]
async fn one_insert_carries_blocks_of_very_different_sizes() {
    let srv = server().await;
    // About 1.2 MB of RowBinary in the first chunk, against a 256 KiB floor.
    let expected = orders(0..41_000);
    let (big, rest) = expected.split_at(40_000);
    let mut chunks: Vec<&[Order]> = vec![big];
    chunks.extend(rest.chunks(200));
    for format in ["native", "rowbinary"] {
        for compression in ["lz4", "zstd"] {
            let case = format!("{format} {compression}");
            truncate(&srv.admin, "orders").await;
            let sink =
                sink_with::<Owned<Order>>(&srv.url, "orders", &settings(format, compression)).await;
            let batch = coded(&sink, compression, &chunks, &format!("sizes-{case}"));
            assert_eq!(batch.frames.len(), 6, "{case}: one big block, five small");
            sink.writer
                .write_batch(&sink.endpoints[0][0], &batch)
                .await
                .unwrap_or_else(|e| panic!("{case}: write: {e}"));
            assert_eq!(read_orders(&srv.admin).await, expected, "{case}");
        }
    }
}

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize, ClickHouseRow)]
struct Noise {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

/// `n` rows of pseudo-random words from a fixed seed (splitmix64).
fn noise(n: usize) -> Vec<Noise> {
    let mut state = 0x5eed_u64;
    let mut next = || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    (0..n)
        .map(|_| Noise {
            a: next(),
            b: next(),
            c: next(),
            d: next(),
        })
        .collect()
}

/// Rows of random words, whose lz4 block is longer than the chunk it codes,
/// insert and read back under lz4 and zstd in both formats.
#[tokio::test]
#[ignore = "requires Docker"]
async fn incompressible_rows_insert_and_read_back() {
    let srv = server().await;
    srv.admin
        .query(
            "CREATE TABLE noise (a UInt64, b UInt64, c UInt64, d UInt64) \
             ENGINE = MergeTree ORDER BY a",
        )
        .execute()
        .await
        .expect("create noise");
    let mut expected = noise(5_000);
    expected.sort_by_key(|n| n.a);
    for format in ["native", "rowbinary"] {
        for compression in ["lz4", "zstd"] {
            let case = format!("{format} {compression}");
            truncate(&srv.admin, "noise").await;
            let sink =
                sink_with::<Owned<Noise>>(&srv.url, "noise", &settings(format, compression)).await;
            let batch = coded(&sink, compression, &[&expected], &format!("noise-{case}"));
            if compression == "lz4" {
                let block = batch.frames[0].len() - 25;
                assert!(
                    block > usize::try_from(batch.bytes).expect("fits"),
                    "{case}: an lz4 payload of {block} bytes for {} raw",
                    batch.bytes
                );
            }
            sink.writer
                .write_batch(&sink.endpoints[0][0], &batch)
                .await
                .unwrap_or_else(|e| panic!("{case}: write: {e}"));
            let got: Vec<Noise> = srv
                .admin
                .query("SELECT ?fields FROM noise ORDER BY a")
                .fetch_all()
                .await
                .expect("read back");
            assert_eq!(got, expected, "{case}");
        }
    }
}

/// A batch with no rows succeeds and inserts nothing. A RowBinary writer
/// sends one request carrying only the header; a Native writer sends none.
/// A one-row batch reads back. Both under lz4, zstd and `off`.
#[tokio::test]
#[ignore = "requires Docker"]
async fn empty_and_one_row_batches() {
    let srv = server().await;
    let proxy = Proxy::start(&srv.url, false).await;
    for format in ["native", "rowbinary"] {
        for compression in ["lz4", "zstd", "off"] {
            let case = format!("{format} {compression}");
            truncate(&srv.admin, "orders").await;
            let sink =
                sink_with::<Owned<Order>>(proxy.url(), "orders", &settings(format, compression))
                    .await;
            let before = proxy.inserts().len();
            let empty = SealedBatch {
                frames: Vec::new(),
                rows: 0,
                bytes: 0,
                dedup_token: format!("empty-{case}"),
            };
            sink.writer
                .write_batch(&sink.endpoints[0][0], &empty)
                .await
                .unwrap_or_else(|e| panic!("{case}: the empty write: {e}"));
            let sent = proxy.inserts().len() - before;
            let expected_requests = usize::from(format == "rowbinary");
            assert_eq!(sent, expected_requests, "{case}: requests for no rows");
            assert_eq!(count(&srv.admin).await, 0, "{case}: no rows");

            let one = orders(7..8);
            let batch = coded(&sink, compression, &[&one], &format!("one-{case}"));
            sink.writer
                .write_batch(&sink.endpoints[0][0], &batch)
                .await
                .unwrap_or_else(|e| panic!("{case}: the one-row write: {e}"));
            assert_eq!(read_orders(&srv.admin).await, one, "{case}");
        }
    }
}

/// A coded insert whose response is lost after the server committed it fails
/// `Retryable`. The retry of the same batch sends the same bytes, and the
/// table holds the batch's rows once.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_retry_resends_identical_bytes_and_inserts_once() {
    let srv = server().await;
    let proxy = Proxy::start(&srv.url, true).await;
    let sink = sink_with::<Owned<Order>>(proxy.url(), "orders", &settings("native", "lz4")).await;
    let rows = orders(0..500);
    let (first, second) = rows.split_at(250);
    let batch = coded(&sink, "lz4", &[first, second], "retry-1");

    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect_err("the response is lost");
    let SinkError::Client { class, reason } = &err else {
        panic!("{err:?}");
    };
    assert_eq!(*class, ErrorClass::Retryable, "{reason}");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("the retry");

    let inserts = proxy.inserts();
    assert_eq!(inserts.len(), 2, "the attempt and its retry");
    assert!(inserts[0] == inserts[1], "the retry resends the same body");
    assert_eq!(count(&srv.admin).await, 500, "the rows land once");
}

/// An HTTP relay in front of a server that records every insert body. Built
/// with `lose_first`, it forwards the first insert, waits for the server's
/// answer, and closes the connection without answering the client.
struct Proxy {
    url: String,
    inserts: Arc<Mutex<Vec<Bytes>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Proxy {
    async fn start(upstream: &str, lose_first: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let url = format!("http://{}", listener.local_addr().expect("the address"));
        let inserts = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Relay {
            upstream: upstream.to_owned(),
            client: HyperClient::builder(TokioExecutor::new()).build_http(),
            inserts: Arc::clone(&inserts),
            lose_first: Mutex::new(lose_first),
        });
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        let state = Arc::clone(&state);
                        async move { state.relay(req).await }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Proxy { url, inserts, task }
    }

    fn url(&self) -> &str {
        &self.url
    }

    /// Every insert body forwarded so far, in arrival order.
    fn inserts(&self) -> Vec<Bytes> {
        self.inserts.lock().expect("the record lock").clone()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Relay {
    upstream: String,
    client: HyperClient<HttpConnector, Full<Bytes>>,
    inserts: Arc<Mutex<Vec<Bytes>>>,
    lose_first: Mutex<bool>,
}

impl Relay {
    async fn relay(&self, req: Request<Incoming>) -> std::io::Result<Response<Full<Bytes>>> {
        let (parts, body) = req.into_parts();
        let body = body.collect().await.map_err(io)?.to_bytes();
        // Only the client's insert requests carry the SQL as a `query` pair.
        let insert = parts
            .uri
            .query()
            .is_some_and(|q| q.split('&').any(|pair| pair.starts_with("query=")));
        let target = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let mut forward = Request::builder()
            .method(parts.method)
            .uri(format!("{}{target}", self.upstream));
        for (name, value) in &parts.headers {
            if ![HOST, TRANSFER_ENCODING, CONTENT_LENGTH].contains(name) {
                forward = forward.header(name, value);
            }
        }
        let answer = self
            .client
            .request(forward.body(Full::new(body.clone())).map_err(io)?)
            .await
            .map_err(io)?;
        let (answer, answer_body) = answer.into_parts();
        let answer_body = answer_body.collect().await.map_err(io)?.to_bytes();
        if insert {
            self.inserts.lock().expect("the record lock").push(body);
            if std::mem::take(&mut *self.lose_first.lock().expect("the flag lock")) {
                return Err(io("the first insert's answer is lost"));
            }
        }
        let mut reply = Response::builder().status(answer.status);
        for (name, value) in &answer.headers {
            if ![TRANSFER_ENCODING, CONTENT_LENGTH, CONNECTION].contains(name) {
                reply = reply.header(name, value);
            }
        }
        reply.body(Full::new(answer_body)).map_err(io)
    }
}

fn io(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::other(e)
}
