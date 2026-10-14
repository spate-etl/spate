//! The insert request a sink sends when its frames are coded at seal,
//! recorded by the in-process server in `benches/support/http_server.rs`.
//!
//! A compressing sink frames its own blocks on the pipeline threads and
//! sends them on a client that does not compress. These tests pin that the
//! request is the one a compressing `clickhouse` client sends for the raw
//! frames, that a sink hands its codec to the pipeline only when it
//! compresses, and that a frame the codec did not make never reaches the
//! server.

mod support;

#[allow(dead_code, reason = "this target writes the events corpus only")]
#[path = "../benches/support/rows.rs"]
mod rows;

#[path = "../benches/support/frames.rs"]
mod frames;

#[path = "../benches/support/http_server.rs"]
mod http_server;

use bytes::{BufMut, Bytes, BytesMut};
use http_server::{BenchServer, Recorded};
use spate_clickhouse::{ClickHouseSink, Compression};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::sink::{SealedBatch, ShardWriter, SinkBundle};

const TOKEN: &str = "coded-1";

fn compression(s: &str) -> Compression {
    s.parse().expect("a compression value")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// The raw frames for `format`, with the rows they hold: three Native blocks
/// of the `events` corpus, or the corpus's first 1,500 rows as three
/// RowBinary frames.
fn raw_frames(format: &str) -> (Vec<Bytes>, u64) {
    if format == "native" {
        let frames = frames::native_frames(3);
        let rows = frames.iter().map(|f| f.rows).sum();
        (frames.into_iter().map(|f| f.bytes).collect(), rows)
    } else {
        let events: Vec<_> = (0..1_500).map(rows::event).collect();
        let batch = support::sealed(&events, TOKEN, 3);
        (batch.frames, batch.rows)
    }
}

/// The `RowBinaryWithNamesAndTypes` header for the `events` columns: a
/// column count, then every name, then every type, each length-prefixed.
fn rowbinary_header() -> Bytes {
    let columns = rows::event_columns();
    let mut buf = BytesMut::new();
    let put = |buf: &mut BytesMut, s: &str| {
        let len = u8::try_from(s.len()).expect("a one-byte length");
        assert!(len < 0x80, "a one-byte LEB128 length");
        buf.put_u8(len);
        buf.put_slice(s.as_bytes());
    };
    buf.put_u8(u8::try_from(columns.len()).expect("a one-byte column count"));
    for (name, _) in &columns {
        put(&mut buf, name);
    }
    for (_, type_) in &columns {
        put(&mut buf, type_);
    }
    buf.freeze()
}

/// The insert a stock `clickhouse` client sends for `raw` under
/// `compression`: the sink's client settings, the writer's insert settings,
/// and the client compressing each frame inside `send`.
async fn stock_insert(
    url: &str,
    compression: Compression,
    insert_sql: &str,
    header: Option<Bytes>,
    raw: &[Bytes],
) {
    let client = clickhouse::Client::default().with_url(url);
    let method = match compression {
        Compression::None => clickhouse::Compression::None,
        Compression::Lz4 => clickhouse::Compression::Lz4,
        Compression::Zstd(level) => clickhouse::Compression::Zstd(level),
        other => panic!("no client compression for {other:?}"),
    };
    let mut insert = client
        .with_compression(method)
        .insert_formatted_with(insert_sql.to_owned())
        .with_setting("insert_deduplicate", "1")
        .with_setting("wait_end_of_query", "1")
        .with_setting("insert_deduplication_token", TOKEN)
        .with_setting("input_format_with_names_use_header", "1")
        .with_setting("input_format_with_types_use_header", "1");
    for frame in header.iter().chain(raw) {
        insert.send(frame.clone()).await.expect("the stock send");
    }
    insert.end().await.expect("the stock insert");
}

/// A request's query pairs as a sorted multiset. The client iterates its
/// settings from a hash map, so their order varies between requests.
fn params(r: &Recorded) -> Vec<(String, String)> {
    let mut query = r.query.clone();
    query.sort();
    query
}

/// A request's headers as a sorted multiset, without `host`, whose port
/// differs between clients.
fn headers(r: &Recorded) -> Vec<(String, String)> {
    let mut headers: Vec<_> = r
        .headers
        .iter()
        .filter(|(k, _)| k != "host")
        .cloned()
        .collect();
    headers.sort();
    headers
}

/// For Native and RowBinary, under lz4 and zstd at levels 1, 3 and 19, the
/// sink's insert of coded frames carries the same body, the same headers
/// apart from `host`, and the same query pairs as a stock compressing client
/// sending the raw frames, with `decompress` once. Under `off` it carries no
/// `decompress` and the raw frames. Two writes of one batch send the same
/// body and the same query pairs.
#[test]
fn a_coded_insert_is_the_request_a_compressing_client_sends() {
    let runtime = runtime();
    for format in ["native", "rowbinary"] {
        let (raw, rows) = raw_frames(format);
        let header = (format == "rowbinary").then(rowbinary_header);
        for name in ["lz4", "zstd:1", "zstd:3", "zstd:19", "off"] {
            let case = format!("{format} {name}");
            let server = BenchServer::recording(&rows::event_columns());
            let sink = runtime.block_on(http_server::events_sink_in(server.url(), name, format));
            let endpoint = &sink.endpoints[0][0];
            let batch = support::coded_batch(compression(name), raw.clone(), rows, TOKEN);
            for _ in 0..2 {
                runtime
                    .block_on(sink.writer.write_batch(endpoint, &batch))
                    .unwrap_or_else(|e| panic!("{case}: the write: {e}"));
            }
            runtime.block_on(stock_insert(
                server.url(),
                compression(name),
                sink.writer.insert_sql(),
                header.clone(),
                &raw,
            ));

            let requests = server.requests();
            assert_eq!(
                requests.len(),
                4,
                "{case}: the schema query, two writes, the stock insert"
            );
            let (first, second, stock) = (&requests[1], &requests[2], &requests[3]);
            assert!(first.body == second.body, "{case}: two writes, one body");
            assert_eq!(params(first), params(second), "{case}: two writes");
            assert_eq!(params(first), params(stock), "{case}: query pairs");
            assert_eq!(headers(first), headers(stock), "{case}: headers");
            assert!(
                first.body == stock.body,
                "{case}: body of {} bytes against the stock client's {}",
                first.body.len(),
                stock.body.len()
            );
            let decompress = first.query.iter().filter(|(k, _)| k == "decompress");
            if name == "off" {
                assert_eq!(decompress.count(), 0, "{case}: no decompress");
                let plain: Vec<u8> = header
                    .iter()
                    .chain(&raw)
                    .flat_map(|f| f.iter().copied())
                    .collect();
                assert!(first.body[..] == plain[..], "{case}: the raw frames");
            } else {
                assert_eq!(decompress.count(), 1, "{case}: decompress once");
            }
        }
    }
}

/// A sink hands the pipeline a frame codec only when it compresses, and
/// that codec makes blocks with the configured method byte.
#[test]
fn into_parts_attaches_a_codec_only_when_compressing() {
    let runtime = runtime();
    let frame = &frames::native_frames(1)[0].bytes;
    for (name, method) in [("off", None), ("lz4", Some(0x82)), ("zstd:3", Some(0x90))] {
        let server = BenchServer::discarding(&rows::event_columns());
        let sink: ClickHouseSink =
            runtime.block_on(http_server::events_sink_in(server.url(), name, "native"));
        let codec = sink.into_parts().frame_codec;
        match method {
            None => assert!(codec.is_none(), "{name}: no codec"),
            Some(method) => {
                let mut codec = codec.unwrap_or_else(|| panic!("{name}: a codec")).make();
                let block = codec.encode_frame(frame).expect("the frame codes");
                // A 16-byte checksum, then the method byte.
                assert_eq!(block[16], method, "{name}: the block's method");
            }
        }
    }
}

/// A compressing sink's writer given a frame its codec did not make, a raw
/// Native block or a frame too short to hold a block header, fails `Fatal`
/// naming the codec hook, and sends no request.
#[test]
fn an_uncoded_frame_is_fatal_before_any_request() {
    let runtime = runtime();
    let server = BenchServer::recording(&rows::event_columns());
    let sink = runtime.block_on(http_server::events_sink_in(server.url(), "lz4", "native"));
    let raw = frames::native_frames(1).remove(0);
    let batches = [
        ("a raw Native block", raw.bytes, raw.rows),
        ("a 10-byte frame", Bytes::from_static(&[0x82; 10]), 1),
    ];
    for (what, frame, rows) in batches {
        let bytes = frame.len() as u64;
        let batch = SealedBatch {
            frames: vec![frame],
            rows,
            bytes,
            dedup_token: TOKEN.into(),
        };
        let err = runtime
            .block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
            .expect_err(what);
        let SinkError::Client { class, reason } = &err else {
            panic!("{what}: {err:?}");
        };
        assert_eq!(*class, ErrorClass::Fatal, "{what}: {reason}");
        assert!(
            reason.contains("ShardQueues::with_frame_codec"),
            "{what}: {reason}"
        );
    }
    assert_eq!(
        server.requests().len(),
        1,
        "only the schema query reached the server"
    );
}

/// Each of the writer's method and size checks rejects a frame on its own: a
/// well-formed zstd block sent to an lz4 writer fails the method check, and an
/// lz4 block with one byte appended fails the size check. Both fail `Fatal`
/// and send no request.
#[test]
fn a_wrong_method_or_size_frame_is_fatal_before_any_request() {
    let runtime = runtime();
    let server = BenchServer::recording(&rows::event_columns());
    let sink = runtime.block_on(http_server::events_sink_in(server.url(), "lz4", "native"));
    let raw = frames::native_frames(1).remove(0);
    let zstd = support::coded_batch(
        compression("zstd:3"),
        vec![raw.bytes.clone()],
        raw.rows,
        TOKEN,
    );
    let mut long = support::coded_batch(compression("lz4"), vec![raw.bytes], raw.rows, TOKEN);
    let mut frame = long.frames[0].to_vec();
    frame.push(0);
    long.frames[0] = Bytes::from(frame);
    for (what, batch) in [
        ("a zstd block", zstd),
        ("an lz4 block and a stray byte", long),
    ] {
        let err = runtime
            .block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
            .expect_err(what);
        let SinkError::Client { class, reason } = &err else {
            panic!("{what}: {err:?}");
        };
        assert_eq!(*class, ErrorClass::Fatal, "{what}: {reason}");
    }
    assert_eq!(
        server.requests().len(),
        1,
        "only the schema query reached the server"
    );
}

/// A compressing sink's writer given a batch whose first frame is a block of
/// its method and whose second is a raw Native block fails `Fatal` and sends
/// no request.
#[test]
fn an_uncoded_frame_after_a_coded_one_is_fatal_before_any_request() {
    let runtime = runtime();
    let server = BenchServer::recording(&rows::event_columns());
    let sink = runtime.block_on(http_server::events_sink_in(server.url(), "lz4", "native"));
    let raw = frames::native_frames(2);
    let mut batch = support::coded_batch(
        compression("lz4"),
        vec![raw[0].bytes.clone()],
        raw[0].rows,
        TOKEN,
    );
    batch.frames.push(raw[1].bytes.clone());
    batch.rows += raw[1].rows;
    batch.bytes += raw[1].bytes.len() as u64;
    let err = runtime
        .block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
        .expect_err("the second frame is not a block");
    let SinkError::Client { class, reason } = &err else {
        panic!("{err:?}");
    };
    assert_eq!(*class, ErrorClass::Fatal, "{reason}");
    assert!(reason.starts_with("frame 1 "), "{reason}");
    assert_eq!(
        server.requests().len(),
        1,
        "only the schema query reached the server"
    );
}

/// An insert answered 271 for a stored block the server read, here a
/// materialized view reading a damaged part, fails `Retryable` from an `off`
/// sink's writer and `Fatal` from an lz4 sink's writer.
#[test]
fn a_server_side_decode_failure_fails_fatal_only_from_a_coding_writer() {
    const STORED_BLOCK_271: &str = "Code: 271. DB::Exception: Cannot decompress LZ4-encoded data: While reading or decompressing /var/lib/clickhouse/store/a16/a1609cef-2585-4c66-be6e-1707b7b286c6/all_1_1_0/k.bin (position: 32796, typename: DB::AsynchronousReadBufferFromFileWithDescriptorsCache, compressed data header: <uninitialized>): (while reading column k): (while reading from part /var/lib/clickhouse/store/a16/a1609cef-2585-4c66-be6e-1707b7b286c6/all_1_1_0/ in table default.dim (a1609cef-2585-4c66-be6e-1707b7b286c6) located on disk default of type local, from mark 0 with max_rows_to_read = 49152): While reading part all_1_1_0: While executing MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder): while pushing to view default.mv: While executing WaitForAsyncInsert. (CANNOT_DECOMPRESS) (version 26.8.17.4 (official build))";
    let runtime = runtime();
    let server = BenchServer::failing_inserts(&rows::event_columns(), 271, STORED_BLOCK_271);
    let raw = frames::native_frames(1);
    for (compression, expected) in [("off", ErrorClass::Retryable), ("lz4", ErrorClass::Fatal)] {
        let sink = runtime.block_on(http_server::events_sink_in(
            server.url(),
            compression,
            "native",
        ));
        let batch = frames::coded(&raw, compression, TOKEN);
        let err = runtime
            .block_on(sink.writer.write_batch(&sink.endpoints[0][0], &batch))
            .expect_err(compression);
        let SinkError::Client { class, reason } = &err else {
            panic!("{compression}: {err:?}");
        };
        assert_eq!(*class, expected, "{compression}: {reason}");
        assert!(reason.contains("Code: 271."), "{compression}: {reason}");
    }
}
