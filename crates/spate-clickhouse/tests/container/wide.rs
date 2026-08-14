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
// validation on), row id=2 through a plain SQL INSERT of literals, where the
// server's own parser is the ground truth. Then every column must satisfy
// toString(row 1) == toString(row 2), which sidesteps both client-side
// decode limits and hand-computed server formatting.
//
// `Time`/`Time64` columns are exercised at the unit/mock layers only:
// they need ClickHouse ≥ 25.6 plus `enable_time_time64_type=1`, and this
// test pins 26.3 (the current LTS line).

use super::*;
use ::chrono::{DateTime, TimeZone, Utc};
use serde_repr::Serialize_repr;
use spate_clickhouse::{
    ClickHouseEncoder, DateTime64Millis, Decimal32, Decimal64, Decimal128, Int256, MultiPolygon,
    Point, Polygon, Ring, UInt256,
};
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
    #[serde(with = "spate_clickhouse::serde::uuid")]
    uid: Uuid,
    #[serde(with = "spate_clickhouse::serde::ipv4")]
    ip4: Ipv4Addr,
    ip6: Ipv6Addr,
    #[serde(with = "spate_clickhouse::serde::chrono::date")]
    d: ::chrono::NaiveDate,
    #[serde(with = "spate_clickhouse::serde::time::date32")]
    d32: ::time::Date,
    #[serde(with = "spate_clickhouse::serde::chrono::datetime")]
    dt: DateTime<Utc>,
    dt64: DateTime64Millis,
    #[serde(with = "spate_clickhouse::serde::chrono::datetime64::micros")]
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
    "c_u128", "c_f32", "c_f64", "s", "fs", "uid", "ip4", "ip6", "d", "d32", "dt", "dt64", "dt64_6",
    "e8", "e16", "dec9", "dec18", "dec38", "big", "ubig", "lc", "j", "pt", "ring", "poly", "mpoly",
    "arr", "map", "n_f64",
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
    let srv = bare_server("26.3", "wide-secret").await;
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
    let mut encoder = ClickHouseEncoder::<Owned<WideRow>>::with_schema(schema);
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
