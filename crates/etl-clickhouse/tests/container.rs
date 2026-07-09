//! End-to-end tests against a real ClickHouse server (Docker).
//!
//! Ignored by default; run explicitly:
//! `cargo test -p etl-clickhouse --test container -- --ignored`

use bytes::BytesMut;
use etl_clickhouse::config::{self, ClickHouseSinkConfig};
use etl_clickhouse::serialize_row;
use etl_core::sink::{SealedBatch, ShardWriter};
use serde::{Deserialize, Serialize};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
struct Order {
    id: u64,
    name: String,
    amount: Option<f64>,
}

struct Server {
    _container: testcontainers_modules::testcontainers::ContainerAsync<ClickHouse>,
    url: String,
    admin: clickhouse::Client,
}

async fn server() -> Server {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default().with_url(&url);
    admin
        .query(
            "CREATE TABLE orders (id UInt64, name String, amount Nullable(Float64)) \
             ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create table");
    Server {
        _container: container,
        url,
        admin,
    }
}

fn sink_for(url: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
columns: [id, name, amount]
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

fn sealed(rows: &[Order], token: &str, frames: usize) -> SealedBatch {
    let per = rows.len().div_ceil(frames);
    let mut out = Vec::new();
    let mut bytes = 0u64;
    for chunk in rows.chunks(per) {
        let mut buf = BytesMut::new();
        for row in chunk {
            serialize_row(row, &mut buf).expect("encode");
        }
        bytes += buf.len() as u64;
        out.push(buf.freeze());
    }
    SealedBatch {
        frames: out,
        rows: rows.len() as u64,
        bytes,
        dedup_token: token.to_string(),
    }
}

fn orders(range: std::ops::Range<u64>) -> Vec<Order> {
    range
        .map(|i| Order {
            id: i,
            name: format!("order-{i}"),
            amount: (i % 2 == 0).then_some(i as f64 * 1.5),
        })
        .collect()
}

async fn count(admin: &clickhouse::Client) -> u64 {
    admin
        .query("SELECT count() FROM orders")
        .fetch_one::<u64>()
        .await
        .expect("count")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn multi_frame_batches_land_and_read_back_exactly() {
    let srv = server().await;
    let sink = sink_for(&srv.url);

    let expected = orders(0..1_000);
    let batch = sealed(&expected, "e2e-batch-1", 4);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");

    let mut got: Vec<Order> = srv
        .admin
        .query("SELECT ?fields FROM orders ORDER BY id")
        .fetch_all()
        .await
        .expect("read back");
    got.sort_by_key(|o| o.id);
    assert_eq!(got, expected, "typed read-back must match encoded rows");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn same_token_dedupes_different_token_inserts() {
    let srv = server().await;
    let sink = sink_for(&srv.url);
    let rows = orders(0..100);

    let batch = sealed(&rows, "dedup-proof", 2);
    for _ in 0..2 {
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write");
    }
    assert_eq!(
        count(&srv.admin).await,
        100,
        "identical batch + identical token must deduplicate"
    );

    let renamed = sealed(&rows, "dedup-proof-DIFFERENT", 2);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &renamed)
        .await
        .expect("write");
    assert_eq!(
        count(&srv.admin).await,
        200,
        "same rows under a different token must insert"
    );
}

// ---- helpers for encoder-path and schema-validation tests --------------------

fn record<T>(payload: T) -> etl_core::record::Record<T> {
    let (ack, rx) = etl_core::checkpoint::AckRef::test_pair();
    std::mem::forget(rx);
    etl_core::record::Record {
        payload,
        meta: etl_core::record::RecordMeta {
            partition: etl_core::record::PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
}

fn sink_with(
    url: &str,
    table: &str,
    columns: &[&str],
    mode: &str,
    settings: &str,
) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: {table}
columns: [{}]
shards:
  - replicas: ["{url}"]
validate_schema: {mode}
{settings}
"#,
        columns.join(", ")
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

/// Encode `rows` through a (possibly schema-checked) encoder into one
/// sealed batch — the same CPU path a pipeline thread runs.
fn encode_batch<T, E>(
    encoder: &mut E,
    rows: Vec<T>,
    token: &str,
) -> Result<SealedBatch, etl_core::error::SinkError>
where
    E: etl_core::sink::RowEncoder<etl_core::deser::Owned<T>>,
    T: Send + 'static,
{
    let mut buf = BytesMut::new();
    let n = rows.len() as u64;
    for row in rows {
        encoder.encode(&record(row), &mut buf)?;
    }
    let frame = buf.freeze();
    let bytes = frame.len() as u64;
    Ok(SealedBatch {
        frames: vec![frame],
        rows: n,
        bytes,
        dedup_token: token.to_string(),
    })
}

/// Encode `rows` through a [`NativeEncoder`](etl_clickhouse::NativeEncoder)
/// into one sealed batch: `encode` per row (buffering columnar), then one
/// `finish_chunk` producing exactly one Native block frame.
fn encode_native_batch<T>(
    encoder: &mut etl_clickhouse::NativeEncoder<T>,
    rows: Vec<T>,
    token: &str,
) -> Result<SealedBatch, etl_core::error::SinkError>
where
    T: Serialize + Send + 'static,
{
    use etl_core::sink::RowEncoder;
    let mut buf = BytesMut::new();
    let n = rows.len() as u64;
    for row in rows {
        encoder.encode(&record(row), &mut buf)?;
    }
    encoder.finish_chunk(&mut buf)?;
    let frame = buf.freeze();
    let bytes = frame.len() as u64;
    Ok(SealedBatch {
        frames: vec![frame],
        rows: n,
        bytes,
        dedup_token: token.to_string(),
    })
}

/// A pinned-version server. Newer official images set up a required
/// password unless one is provided, so this always configures explicit
/// credentials (unlike the module's ancient default image).
async fn bare_server(tag: &str, password: &str) -> Server {
    use testcontainers_modules::testcontainers::ImageExt;
    let container = ClickHouse::default()
        .with_tag(tag)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", password)
        .start()
        .await
        .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default")
        .with_password(password);
    Server {
        _container: container,
        url,
        admin,
    }
}

// ---- the wide-type table: every supported column against a real server -------
//
// The unit tests prove bytes match our understanding; the mock proves
// compatibility with clickhouse-rs's deserializer; only a real server
// validates what neither can: Int256/UInt256 layout (the client cannot
// decode them), the JSON insert setting, LowCardinality transparency,
// server-side Enum validation, Decimal scale semantics, DateTime64
// timezone/precision handling.
//
// Comparison strategy: row id=1 goes through OUR encoder (full schema
// validation on), row id=2 through a plain SQL INSERT of literals — the
// server's own parser is the ground truth. Then every column must satisfy
// toString(row 1) == toString(row 2), which sidesteps both client-side
// decode limits and hand-computed server formatting.
//
// `Time`/`Time64` columns are exercised at the unit/mock layers only:
// they need ClickHouse ≥ 25.6 plus `enable_time_time64_type=1`, and this
// test pins 25.3 (LTS; first release with production-ready JSON).
#[cfg(all(feature = "uuid", feature = "chrono", feature = "time"))]
mod wide {
    use super::*;
    use ::chrono::{DateTime, TimeZone, Utc};
    use etl_clickhouse::{
        ClickHouseEncoder, DateTime64Millis, Decimal32, Decimal64, Decimal128, Int256,
        MultiPolygon, Point, Polygon, Ring, UInt256,
    };
    use serde_repr::Serialize_repr;
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use uuid::Uuid;

    #[derive(Clone, Serialize_repr)]
    #[repr(i8)]
    enum Level8 {
        Lo = -1,
    }

    #[derive(Clone, Serialize_repr)]
    #[repr(i16)]
    enum Level16 {
        Big = 300,
    }

    #[derive(Clone, Serialize)]
    struct WideRow {
        id: u64,
        b: bool,
        c_i8: i8,
        c_i16: i16,
        c_i32: i32,
        c_i64: i64,
        c_i128: i128,
        c_u8: u8,
        c_u16: u16,
        c_u32: u32,
        c_u64: u64,
        c_u128: u128,
        c_f32: f32,
        c_f64: f64,
        s: String,
        fs: [u8; 4],
        #[serde(with = "etl_clickhouse::serde::uuid")]
        uid: Uuid,
        #[serde(with = "etl_clickhouse::serde::ipv4")]
        ip4: Ipv4Addr,
        ip6: Ipv6Addr,
        #[serde(with = "etl_clickhouse::serde::chrono::date")]
        d: ::chrono::NaiveDate,
        #[serde(with = "etl_clickhouse::serde::time::date32")]
        d32: ::time::Date,
        #[serde(with = "etl_clickhouse::serde::chrono::datetime")]
        dt: DateTime<Utc>,
        dt64: DateTime64Millis,
        #[serde(with = "etl_clickhouse::serde::chrono::datetime64::micros")]
        dt64_6: DateTime<Utc>,
        e8: Level8,
        e16: Level16,
        dec9: Decimal32<2>,
        dec18: Decimal64<4>,
        dec38: Decimal128<10>,
        big: Int256,
        ubig: UInt256,
        lc: String,
        j: String,
        pt: Point,
        ring: Ring,
        poly: Polygon,
        mpoly: MultiPolygon,
        arr: Vec<Option<String>>,
        map: BTreeMap<String, u64>,
        n_f64: Option<f64>,
    }

    const COLUMNS: &[&str] = &[
        "id", "b", "c_i8", "c_i16", "c_i32", "c_i64", "c_i128", "c_u8", "c_u16", "c_u32", "c_u64",
        "c_u128", "c_f32", "c_f64", "s", "fs", "uid", "ip4", "ip6", "d", "d32", "dt", "dt64",
        "dt64_6", "e8", "e16", "dec9", "dec18", "dec38", "big", "ubig", "lc", "j", "pt", "ring",
        "poly", "mpoly", "arr", "map", "n_f64",
    ];

    const DDL: &str = "CREATE TABLE wide (\
        id UInt64, b Bool, \
        c_i8 Int8, c_i16 Int16, c_i32 Int32, c_i64 Int64, c_i128 Int128, \
        c_u8 UInt8, c_u16 UInt16, c_u32 UInt32, c_u64 UInt64, c_u128 UInt128, \
        c_f32 Float32, c_f64 Float64, \
        s String, fs FixedString(4), uid UUID, ip4 IPv4, ip6 IPv6, \
        d Date, d32 Date32, dt DateTime('UTC'), \
        dt64 DateTime64(3, 'UTC'), dt64_6 DateTime64(6, 'UTC'), \
        e8 Enum8('lo' = -1, 'hi' = 2), e16 Enum16('big' = 300), \
        dec9 Decimal(9, 2), dec18 Decimal(18, 4), dec38 Decimal(38, 10), \
        big Int256, ubig UInt256, \
        lc LowCardinality(String), j JSON, \
        pt Point, ring Ring, poly Polygon, mpoly MultiPolygon, \
        arr Array(Nullable(String)), map Map(String, UInt64), \
        n_f64 Nullable(Float64)\
    ) ENGINE = MergeTree ORDER BY id";

    /// Row id=2, same values as [`encoded_row`], as server-parsed literals.
    const LITERAL_INSERT: &str = "INSERT INTO wide VALUES (2, true, \
        -128, -32768, -2147483648, -9223372036854775808, \
        toInt128('-170141183460469231731687303715884105728'), \
        255, 65535, 4294967295, 18446744073709551615, \
        toUInt128('340282366920938463463374607431768211455'), \
        -0.5, 2.5, \
        'héllo,wörld', 'ab', \
        '01020304-0506-0708-090a-0b0c0d0e0f10', '1.2.3.4', '2001:db8::8a2e:370:7334', \
        '1970-01-02', '1900-01-01', 1700000000, \
        toDateTime64(-1, 3, 'UTC'), '2023-11-14 22:13:20.000001', \
        'lo', 'big', \
        -150.12, 0.0001, toDecimal128('1234567890123456789.0123456789', 10), \
        toInt256('-170141183460469231731687303715884105728'), \
        toUInt256('340282366920938463463374607431768211455'), \
        'repeat', '{\"a\":1,\"b\":\"x\"}', \
        (1.5, -2.5), [(0, 0), (10, 0), (10, 10)], [[(0, 0), (10, 0), (10, 10)]], \
        [[[(0, 0), (10, 0), (10, 10)]]], \
        ['x', NULL], map('k', 42), NULL)";

    fn encoded_row() -> WideRow {
        let ring: Ring = vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)];
        WideRow {
            id: 1,
            b: true,
            c_i8: i8::MIN,
            c_i16: i16::MIN,
            c_i32: i32::MIN,
            c_i64: i64::MIN,
            c_i128: i128::MIN,
            c_u8: u8::MAX,
            c_u16: u16::MAX,
            c_u32: u32::MAX,
            c_u64: u64::MAX,
            c_u128: u128::MAX,
            c_f32: -0.5,
            c_f64: 2.5,
            s: "héllo,wörld".into(),
            fs: *b"ab\0\0",
            uid: Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
            ip4: Ipv4Addr::new(1, 2, 3, 4),
            ip6: "2001:db8::8a2e:370:7334".parse().unwrap(),
            d: ::chrono::NaiveDate::from_ymd_opt(1970, 1, 2).unwrap(),
            d32: ::time::macros::date!(1900 - 01 - 01),
            dt: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            dt64: DateTime64Millis(-1_000),
            dt64_6: Utc.timestamp_micros(1_700_000_000_000_001).unwrap(),
            e8: Level8::Lo,
            e16: Level16::Big,
            dec9: Decimal32::<2>(-15_012),
            dec18: Decimal64::<4>(1),
            dec38: Decimal128::<10>(12_345_678_901_234_567_890_123_456_789),
            big: Int256::from_i128(i128::MIN),
            ubig: UInt256::from_u128(u128::MAX),
            lc: "repeat".into(),
            j: "{\"a\":1,\"b\":\"x\"}".into(),
            pt: (1.5, -2.5),
            ring: ring.clone(),
            poly: vec![ring.clone()],
            mpoly: vec![vec![ring]],
            arr: vec![Some("x".into()), None],
            map: BTreeMap::from([("k".to_string(), 42u64)]),
            n_f64: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn wide_type_table_round_trips() {
        let srv = bare_server("25.3-alpine", "wide-secret").await;
        let ddl_client = srv
            .admin
            .clone()
            .with_setting("allow_experimental_json_type", "1");
        ddl_client.query(DDL).execute().await.expect("create wide");

        // Row 1: full startup validation against the real system.columns,
        // then the encoder's first-record struct check, then the write.
        let sink = sink_with(
            &srv.url,
            "wide",
            COLUMNS,
            "full",
            "user: default\npassword: wide-secret\n\
             settings: { input_format_binary_read_json_as_string: \"1\" }",
        );
        let schema = sink
            .validate_schema()
            .await
            .expect("startup validation against the real table")
            .expect("full mode returns a schema");
        let mut encoder = ClickHouseEncoder::<WideRow>::with_schema(schema);
        let batch =
            encode_batch(&mut encoder, vec![encoded_row()], "wide-1").expect("first-record check");
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write");

        // Row 2: the same values as server-parsed SQL literals.
        ddl_client
            .query(LITERAL_INSERT)
            .execute()
            .await
            .expect("literal insert");

        // Ground truth: per column, our binary encoding stored exactly
        // what the server's own literal parser stored.
        for col in COLUMNS.iter().filter(|c| **c != "id") {
            // ifNull keeps the projection a plain String for Nullable
            // columns (toString(NULL) is Nullable(String)).
            let read = |id: u64| {
                let sql =
                    format!("SELECT ifNull(toString(`{col}`), '<NULL>') FROM wide WHERE id = {id}");
                let admin = srv.admin.clone();
                async move { admin.query(&sql).fetch_one::<String>().await }
            };
            let ours = read(1)
                .await
                .unwrap_or_else(|e| panic!("read back `{col}` (encoded row): {e}"));
            let literal = read(2)
                .await
                .unwrap_or_else(|e| panic!("read back `{col}` (literal row): {e}"));
            assert_eq!(
                ours, literal,
                "column `{col}` diverged from the literal row"
            );
        }
    }
}

// ---- Native format: end-to-end against a real server --------------------------
//
// Proves the columnar encoder against ClickHouse itself: row 1 is inserted
// through the NativeEncoder (`INSERT ... FORMAT Native`), then read back and
// compared to what we sent. The server is the arbiter — it parses our block
// bytes into its own columnar storage and re-serializes for the read, so a
// misencoded column fails the write or diverges on read-back. The table
// deliberately exercises the Native-specific risk cases the `wide` table
// lacks: Array(LowCardinality) (the prefix-ordering rule), Map(LowCardinality),
// Array(Nullable), and LowCardinality(Nullable).
#[cfg(feature = "uuid")]
mod native_e2e {
    use super::*;
    use etl_clickhouse::{DateTime64Millis, Decimal64, NativeEncoder};
    use serde_repr::{Deserialize_repr, Serialize_repr};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use uuid::Uuid;

    #[derive(Clone, Copy, Debug, PartialEq, Serialize_repr, Deserialize_repr)]
    #[repr(i8)]
    enum Level {
        Lo = -1,
        Hi = 2,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, clickhouse::Row)]
    struct NativeRow {
        id: u64,
        b: bool,
        n: i32,
        big_n: i64,
        small: u16,
        f: f64,
        s: String,
        fs: [u8; 4],
        #[serde(with = "etl_clickhouse::serde::uuid")]
        uid: Uuid,
        #[serde(with = "etl_clickhouse::serde::ipv4")]
        ip4: Ipv4Addr,
        ip6: Ipv6Addr,
        ts: DateTime64Millis,
        e: Level,
        price: Decimal64<4>,
        cat: String,
        maybe_cat: Option<String>,
        tags: Vec<String>,
        labels: Vec<String>,
        scores: Vec<Option<f64>>,
        props: BTreeMap<String, u32>,
        dims: BTreeMap<String, f64>,
        pt: (f64, f64),
        opt: Option<f64>,
    }

    const COLUMNS: &[&str] = &[
        "id",
        "b",
        "n",
        "big_n",
        "small",
        "f",
        "s",
        "fs",
        "uid",
        "ip4",
        "ip6",
        "ts",
        "e",
        "price",
        "cat",
        "maybe_cat",
        "tags",
        "labels",
        "scores",
        "props",
        "dims",
        "pt",
        "opt",
    ];

    const DDL: &str = "CREATE TABLE native_wide (\
        id UInt64, b Bool, n Int32, big_n Int64, small UInt16, f Float64, \
        s String, fs FixedString(4), uid UUID, ip4 IPv4, ip6 IPv6, \
        ts DateTime64(3, 'UTC'), e Enum8('lo' = -1, 'hi' = 2), price Decimal(18, 4), \
        cat LowCardinality(String), maybe_cat LowCardinality(Nullable(String)), \
        tags Array(String), labels Array(LowCardinality(String)), \
        scores Array(Nullable(Float64)), props Map(String, UInt32), \
        dims Map(LowCardinality(String), Float64), pt Point, opt Nullable(Float64)\
    ) ENGINE = MergeTree ORDER BY id";

    fn rows() -> Vec<NativeRow> {
        vec![
            NativeRow {
                id: 1,
                b: true,
                n: -42,
                big_n: 9_000_000_000,
                small: u16::MAX,
                f: 2.5,
                s: "héllo,wörld".into(),
                fs: *b"ab\0\0",
                uid: Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
                ip4: Ipv4Addr::new(1, 2, 3, 4),
                ip6: "2001:db8::8a2e:370:7334".parse().unwrap(),
                ts: DateTime64Millis(1_700_000_000_123),
                e: Level::Lo,
                price: Decimal64::<4>(-15_000),
                cat: "repeat".into(),
                maybe_cat: None,
                tags: vec!["x".into(), "y".into()],
                labels: vec!["a".into(), "b".into(), "a".into()],
                scores: vec![Some(1.5), None, Some(-2.0)],
                props: BTreeMap::from([("k".to_string(), 42)]),
                dims: BTreeMap::from([("m".to_string(), 3.5)]),
                pt: (1.5, -2.5),
                opt: None,
            },
            NativeRow {
                id: 2,
                b: false,
                n: 7,
                big_n: -1,
                small: 0,
                f: -0.25,
                s: String::new(),
                fs: *b"\0\0\0\0",
                uid: Uuid::nil(),
                ip4: Ipv4Addr::new(127, 0, 0, 1),
                ip6: Ipv6Addr::LOCALHOST,
                ts: DateTime64Millis(0),
                e: Level::Hi,
                price: Decimal64::<4>(10_000),
                cat: "repeat".into(),
                maybe_cat: Some("present".into()),
                tags: vec![],
                labels: vec![],
                scores: vec![],
                props: BTreeMap::new(),
                dims: BTreeMap::from([("m".to_string(), 1.0), ("n".to_string(), 2.0)]),
                pt: (0.0, 0.0),
                opt: Some(9.5),
            },
        ]
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn native_format_round_trips_through_a_real_server() {
        let srv = bare_server("25.3-alpine", "native-secret").await;
        srv.admin
            .query(DDL)
            .execute()
            .await
            .expect("create native_wide");

        let sink = sink_with(
            &srv.url,
            "native_wide",
            COLUMNS,
            "full",
            "format: native\nuser: default\npassword: native-secret\n",
        );
        assert_eq!(
            sink.writer.insert_sql(),
            "INSERT INTO `native_wide` (`id`, `b`, `n`, `big_n`, `small`, `f`, `s`, `fs`, `uid`, \
             `ip4`, `ip6`, `ts`, `e`, `price`, `cat`, `maybe_cat`, `tags`, `labels`, `scores`, \
             `props`, `dims`, `pt`, `opt`) FORMAT Native"
        );

        let native_schema = sink
            .native_schema()
            .await
            .expect("fetch native schema from system.columns");
        let mut encoder = NativeEncoder::<NativeRow>::new(native_schema);
        let sent = rows();
        let batch = encode_native_batch(&mut encoder, sent.clone(), "native-1").expect("encode");
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write native block");

        let mut got: Vec<NativeRow> = srv
            .admin
            .query("SELECT ?fields FROM native_wide ORDER BY id")
            .fetch_all()
            .await
            .expect("read back native rows");
        got.sort_by_key(|r| r.id);
        assert_eq!(got, sent, "Native round-trip must match what we encoded");
    }

    /// Spot-check the trickiest columns against the server's own rendering
    /// (independent of the crate's decoder): the Array(LowCardinality) and
    /// Map columns whose on-wire layout has no RowBinary analogue.
    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn native_lowcardinality_composites_render_correctly() {
        let srv = bare_server("25.3-alpine", "native-secret2").await;
        srv.admin
            .query(DDL)
            .execute()
            .await
            .expect("create native_wide");

        let sink = sink_with(
            &srv.url,
            "native_wide",
            COLUMNS,
            "names",
            "format: native\nuser: default\npassword: native-secret2\n",
        );
        let native_schema = sink.native_schema().await.expect("native schema");
        let mut encoder = NativeEncoder::<NativeRow>::new(native_schema);
        let batch = encode_native_batch(&mut encoder, rows(), "native-2").expect("encode");
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write");

        let s = |sql: &str| {
            let admin = srv.admin.clone();
            let sql = sql.to_string();
            async move {
                admin
                    .query(&sql)
                    .fetch_one::<String>()
                    .await
                    .expect("query")
            }
        };
        assert_eq!(
            s("SELECT toString(labels) FROM native_wide WHERE id = 1").await,
            "['a','b','a']",
            "Array(LowCardinality(String)) must decode element-for-element"
        );
        assert_eq!(
            s("SELECT toString(dims) FROM native_wide WHERE id = 2").await,
            "{'m':1,'n':2}",
            "Map(LowCardinality(String), Float64) must decode key/value pairs"
        );
        assert_eq!(
            s("SELECT ifNull(toString(maybe_cat), '<NULL>') FROM native_wide WHERE id = 1").await,
            "<NULL>",
            "LowCardinality(Nullable(String)) NULL must round-trip as NULL"
        );
        assert_eq!(
            s("SELECT ifNull(toString(maybe_cat), '<NULL>') FROM native_wide WHERE id = 2").await,
            "present",
            "LowCardinality(Nullable(String)) present value must round-trip"
        );
        assert_eq!(
            s("SELECT toString(scores) FROM native_wide WHERE id = 1").await,
            "[1.5,NULL,-2]",
            "Array(Nullable(Float64)) null-map must be correct"
        );
    }
}

// The 256-bit integers and nested Geo shapes: the client cannot decode
// Int256/UInt256, and the nested Array-of-Array offset layout of Polygon /
// MultiPolygon is otherwise only byte-unit tested. Prove them against a real
// server via the toString oracle — row 1 through the Native encoder, row 2 as
// server-parsed literals, compared column by column.
mod native_edges {
    use super::*;
    use etl_clickhouse::{Int256, MultiPolygon, NativeEncoder, Polygon, Ring, UInt256};

    #[derive(Serialize)]
    struct EdgeRow {
        id: u64,
        big: Int256,
        ubig: UInt256,
        poly: Polygon,
        mpoly: MultiPolygon,
    }

    const COLUMNS: &[&str] = &["id", "big", "ubig", "poly", "mpoly"];

    const DDL: &str = "CREATE TABLE native_edges (\
        id UInt64, big Int256, ubig UInt256, poly Polygon, mpoly MultiPolygon\
    ) ENGINE = MergeTree ORDER BY id";

    // Row id=2: the same values as [`edge_row`], as server-parsed literals.
    const LITERAL_INSERT: &str = "INSERT INTO native_edges VALUES (2, \
        toInt256('-170141183460469231731687303715884105728'), \
        toUInt256('340282366920938463463374607431768211455'), \
        [[(0, 0), (10, 0), (10, 10)]], [[[(0, 0), (10, 0), (10, 10)]]])";

    fn edge_row() -> EdgeRow {
        let ring: Ring = vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)];
        EdgeRow {
            id: 1,
            big: Int256::from_i128(i128::MIN),
            ubig: UInt256::from_u128(u128::MAX),
            poly: vec![ring.clone()],
            mpoly: vec![vec![ring]],
        }
    }

    #[tokio::test]
    #[ignore = "requires Docker"]
    async fn int256_and_nested_geo_match_the_literal_row() {
        let srv = bare_server("25.3-alpine", "edges-secret").await;
        srv.admin
            .query(DDL)
            .execute()
            .await
            .expect("create native_edges");

        let sink = sink_with(
            &srv.url,
            "native_edges",
            COLUMNS,
            "full",
            "format: native\nuser: default\npassword: edges-secret\n",
        );
        let schema = sink.native_schema().await.expect("native schema");
        let mut encoder = NativeEncoder::<EdgeRow>::new(schema);
        let batch = encode_native_batch(&mut encoder, vec![edge_row()], "edges-1").expect("encode");
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write native block");

        srv.admin
            .query(LITERAL_INSERT)
            .execute()
            .await
            .expect("literal insert");

        for col in COLUMNS.iter().filter(|c| **c != "id") {
            let read = |id: u64| {
                let sql = format!("SELECT toString(`{col}`) FROM native_edges WHERE id = {id}");
                let admin = srv.admin.clone();
                async move { admin.query(&sql).fetch_one::<String>().await.expect("read") }
            };
            assert_eq!(
                read(1).await,
                read(2).await,
                "column `{col}`: Native-encoded row diverged from the literal row"
            );
        }
    }
}

// ---- schema validation against a real server ----------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn schema_validation_startup_scenarios() {
    let srv = server().await; // creates `orders`
    for ddl in [
        "CREATE TABLE mat (id UInt64, twice UInt64 MATERIALIZED id * 2, al UInt64 ALIAS id) \
         ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE extras (id UInt64, with_default UInt64 DEFAULT 7, without_default UInt64) \
         ENGINE = MergeTree ORDER BY id",
    ] {
        srv.admin.query(ddl).execute().await.expect("ddl");
    }

    // Happy path: full mode against the real system.columns.
    let sink = sink_with(&srv.url, "orders", &["id", "name", "amount"], "full", "");
    assert!(sink.validate_schema().await.expect("passes").is_some());

    // Off mode: no schema, no failure — today's behavior.
    let sink = sink_with(&srv.url, "orders", &["id", "name", "amount"], "off", "");
    assert!(sink.validate_schema().await.expect("off").is_none());

    // A configured column the table does not have.
    let err = sink_with(&srv.url, "orders", &["id", "nope"], "names", "")
        .validate_schema()
        .await
        .expect_err("missing column");
    assert!(err.to_string().contains("`nope` does not exist"), "{err}");

    // MATERIALIZED and ALIAS columns cannot be insert targets.
    for (col, kind) in [("twice", "MATERIALIZED"), ("al", "ALIAS")] {
        let err = sink_with(&srv.url, "mat", &["id", col], "names", "")
            .validate_schema()
            .await
            .expect_err("non-insertable column");
        assert!(err.to_string().contains(kind), "{col}: {err}");
    }

    // A table that does not exist.
    let err = sink_with(&srv.url, "no_such_table", &["id"], "names", "")
        .validate_schema()
        .await
        .expect_err("missing table");
    assert!(err.to_string().contains("not found"), "{err}");

    // Unconfigured table columns warn but pass; the server fills the
    // DEFAULT and the type default on insert.
    #[derive(Clone, Serialize)]
    struct IdOnly {
        id: u64,
    }
    let sink = sink_with(&srv.url, "extras", &["id"], "full", "");
    let schema = sink
        .validate_schema()
        .await
        .expect("warns, passes")
        .unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<IdOnly>::with_schema(schema);
    let batch = encode_batch(&mut encoder, vec![IdOnly { id: 1 }], "extras-1").expect("encode");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");
    let (with_default, without_default): (u64, u64) = srv
        .admin
        .query("SELECT with_default, without_default FROM extras WHERE id = 1")
        .fetch_one()
        .await
        .expect("read back");
    assert_eq!((with_default, without_default), (7, 0));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn schema_validation_first_record_scenarios() {
    use etl_core::error::{ErrorClass, SinkError};

    let srv = server().await; // `orders` (id UInt64, name String, amount Nullable(Float64))
    for ddl in [
        "CREATE TABLE dt_col (id UInt64, x DateTime) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE plain_s (id UInt64, s String) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE lowcard (id UInt64, lc LowCardinality(String)) \
         ENGINE = MergeTree ORDER BY id",
    ] {
        srv.admin.query(ddl).execute().await.expect("ddl");
    }

    let fatal = |err: SinkError| match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            reason
        }
        other => panic!("unexpected error shape: {other:?}"),
    };

    // Config order differing from TABLE order is fine — the INSERT column
    // list maps by name — as long as the struct follows the CONFIG order.
    // Prove it lands in the right columns on a real server.
    #[derive(Clone, Serialize)]
    struct Reordered {
        name: String,
        id: u64,
        amount: Option<f64>,
    }
    let sink = sink_with(&srv.url, "orders", &["name", "id", "amount"], "full", "");
    let schema = sink
        .validate_schema()
        .await
        .expect("order-by-name is fine")
        .unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<Reordered>::with_schema(schema);
    let batch = encode_batch(
        &mut encoder,
        vec![Reordered {
            name: "alice".into(),
            id: 42,
            amount: Some(1.5),
        }],
        "reorder-1",
    )
    .expect("first record passes");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");
    let (id, name, amount): (u64, String, Option<f64>) = srv
        .admin
        .query("SELECT id, name, amount FROM orders WHERE id = 42")
        .fetch_one()
        .await
        .expect("read back");
    assert_eq!((id, name.as_str(), amount), (42, "alice", Some(1.5)));

    // The same struct against config order [id, name, amount]: the
    // positional wire contract breaks, and the first record says so.
    let sink = sink_with(&srv.url, "orders", &["id", "name", "amount"], "names", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<Reordered>::with_schema(schema);
    let err = encode_batch(
        &mut encoder,
        vec![Reordered {
            name: "bob".into(),
            id: 7,
            amount: None,
        }],
        "reorder-2",
    )
    .expect_err("struct order vs config order");
    let reason = fatal(err);
    assert!(
        reason.contains("position 0: struct field `name` vs configured column `id`"),
        "{reason}"
    );

    // Type-class mismatch: full mode stops it, names mode lets it through
    // (permissiveness is by design).
    #[derive(Clone, Serialize)]
    struct I32X {
        id: u64,
        x: i32,
    }
    let sink = sink_with(&srv.url, "dt_col", &["id", "x"], "full", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<I32X>::with_schema(schema);
    let err = encode_batch(&mut encoder, vec![I32X { id: 1, x: 100 }], "dt-1")
        .expect_err("i32 vs DateTime in full mode");
    let reason = fatal(err);
    assert!(
        reason.contains("not compatible with `x` DateTime"),
        "{reason}"
    );

    let sink = sink_with(&srv.url, "dt_col", &["id", "x"], "names", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<I32X>::with_schema(schema);
    let batch = encode_batch(&mut encoder, vec![I32X { id: 1, x: 100 }], "dt-2")
        .expect("names mode skips type classes");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write still parses (4-byte wire)");

    // The Nullable hard rule, both directions, in full mode: a wire-format
    // difference, not a type ambiguity.
    #[derive(Clone, Serialize)]
    struct PlainAmount {
        id: u64,
        name: String,
        amount: f64, // column is Nullable(Float64)
    }
    let sink = sink_with(&srv.url, "orders", &["id", "name", "amount"], "full", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<PlainAmount>::with_schema(schema);
    let err = encode_batch(
        &mut encoder,
        vec![PlainAmount {
            id: 1,
            name: "x".into(),
            amount: 1.0,
        }],
        "null-1",
    )
    .expect_err("plain field vs Nullable column");
    assert!(fatal(err).contains("not compatible with `amount` Nullable(Float64)"));

    #[derive(Clone, Serialize)]
    struct OptS {
        id: u64,
        s: Option<String>, // column is plain String
    }
    let sink = sink_with(&srv.url, "plain_s", &["id", "s"], "full", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<OptS>::with_schema(schema);
    let err = encode_batch(
        &mut encoder,
        vec![OptS {
            id: 1,
            s: Some("x".into()),
        }],
        "null-2",
    )
    .expect_err("Option field vs plain column");
    assert!(fatal(err).contains("not compatible with `s` String"));

    // LowCardinality is transparent on insert: a plain String field
    // passes full mode and the row lands.
    #[derive(Clone, Serialize)]
    struct LcRow {
        id: u64,
        lc: String,
    }
    let sink = sink_with(&srv.url, "lowcard", &["id", "lc"], "full", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = etl_clickhouse::ClickHouseEncoder::<LcRow>::with_schema(schema);
    let batch = encode_batch(
        &mut encoder,
        vec![LcRow {
            id: 9,
            lc: "tag".into(),
        }],
        "lc-1",
    )
    .expect("LowCardinality unwraps");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");
    let lc: String = srv
        .admin
        .query("SELECT lc FROM lowcard WHERE id = 9")
        .fetch_one()
        .await
        .expect("read back");
    assert_eq!(lc, "tag");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn probe_reflects_connectivity() {
    let srv = server().await;
    let sink = sink_for(&srv.url);
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe healthy server");

    let unreachable = sink_for("http://127.0.0.1:1");
    assert!(
        unreachable
            .writer
            .probe(&unreachable.endpoints[0][0])
            .await
            .is_err()
    );
}
