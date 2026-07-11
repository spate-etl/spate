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
    let srv = bare_server("26.3", "native-secret").await;
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
    let mut encoder = NativeEncoder::<Owned<NativeRow>>::new(native_schema);
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

/// The scale-declaration gate against a real table: under
/// `validate_schema: full` a wire wrapper whose scale disagrees with the
/// column's declared precision fails fatally at the first record (before
/// any block is built), and the matching wrapper lands the exact instant
/// — verified by the server's own `toUnixTimestamp64Micro`, not our
/// decoder.
#[tokio::test]
#[ignore = "requires Docker"]
async fn native_full_mode_gates_datetime64_scale_against_a_real_table() {
    use etl_clickhouse::DateTime64Micros;
    use etl_core::error::{ErrorClass, SinkError};
    use std::sync::Arc;

    let srv = bare_server("26.3", "native-secret3").await;
    srv.admin
        .query(
            "CREATE TABLE dt_micro (id UInt64, ts DateTime64(6, 'UTC')) \
                 ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .expect("create dt_micro");

    let sink = sink_with(
        &srv.url,
        "dt_micro",
        &["id", "ts"],
        "full",
        "format: native\nuser: default\npassword: native-secret3\n",
    );
    let schema = sink.native_schema().await.expect("fetch native schema");

    // Milli-scaled wrapper into the micro column: without the gate every
    // timestamp would land ~1000x too small (January 1970).
    #[derive(Clone, Serialize)]
    struct MilliRow {
        id: u64,
        ts: DateTime64Millis,
    }
    let mut enc = NativeEncoder::<Owned<MilliRow>>::new(Arc::clone(&schema));
    let err = encode_native_batch(
        &mut enc,
        vec![MilliRow {
            id: 1,
            ts: DateTime64Millis(1_700_000_000_123),
        }],
        "dt-1",
    )
    .expect_err("scale mismatch must fail at the first record");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            assert!(
                reason.contains("DateTime64Millis") && reason.contains("DateTime64(6"),
                "{reason}"
            );
        }
        other => panic!("unexpected error shape: {other:?}"),
    }

    // The matching wrapper passes the gate and the server sees the
    // exact micro-scaled instant.
    #[derive(Clone, Serialize)]
    struct MicroRow {
        id: u64,
        ts: DateTime64Micros,
    }
    const TS_MICROS: i64 = 1_700_000_000_123_456;
    let mut enc = NativeEncoder::<Owned<MicroRow>>::new(schema);
    let batch = encode_native_batch(
        &mut enc,
        vec![MicroRow {
            id: 1,
            ts: DateTime64Micros(TS_MICROS),
        }],
        "dt-2",
    )
    .expect("matching scale encodes");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write native block");
    let got: i64 = srv
        .admin
        .query("SELECT toUnixTimestamp64Micro(ts) FROM dt_micro WHERE id = 1")
        .fetch_one()
        .await
        .expect("read back the landed instant");
    assert_eq!(
        got, TS_MICROS,
        "the landed instant matches the declared scale"
    );
}

/// Spot-check the trickiest columns against the server's own rendering
/// (independent of the crate's decoder): the Array(LowCardinality) and
/// Map columns whose on-wire layout has no RowBinary analogue.
#[tokio::test]
#[ignore = "requires Docker"]
async fn native_lowcardinality_composites_render_correctly() {
    let srv = bare_server("26.3", "native-secret2").await;
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
    let mut encoder = NativeEncoder::<Owned<NativeRow>>::new(native_schema);
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
