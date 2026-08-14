// ---- AggregatingMergeTree ingestion via a Null table + Materialized View ----
//
// AggregateFunction columns hold opaque, version-dependent aggregate STATES,
// so the sink never writes them directly. It INSERTs plain order rows into an
// ENGINE=Null landing table; a Materialized View builds min/max/sumMap states
// into the target AggregatingMergeTree. These tests prove (1) the three states
// round-trip, (2) at-least-once retries stay exactly-once through the view
// (ClickHouse >= 26.1 + deduplicate_blocks_in_dependent_materialized_views),
// and (3) pointing the sink straight at an AggregateFunction column is
// rejected with an actionable error. Pin 26.3 (the LTS the docs target).

use super::*;
use std::collections::BTreeMap;

/// One placed order, already collapsed to a per-SKU quantity: the raw row
/// the sink writes, not the rollup the view builds from it. Only `Serialize`
/// is needed: the wire path is RowBinary and
/// the tests read the merged aggregates back as scalars, not typed rows.
#[derive(Debug, Clone, Serialize)]
struct OrderRollup {
    region: String,
    placed_at: u32,                    // epoch seconds -> DateTime
    qty_by_sku: BTreeMap<String, u64>, // -> Map(String, UInt64)
}

fn qty(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
}

/// Five orders across two regions, the same five the
/// `clickhouse_aggregating_mv` example feeds. `eu-west`: min 1767225600, max
/// 1767229200, sumMap {KBD-01:3, MSE-01:3, MON-01:3}. `us-east`: min
/// 1767235200, max 1767238800, sumMap {CBL-01:15, DCK-01:7}.
fn rollups() -> Vec<OrderRollup> {
    vec![
        OrderRollup {
            region: "eu-west".into(),
            placed_at: 1_767_225_600,
            qty_by_sku: qty(&[("KBD-01", 1), ("MSE-01", 2)]),
        },
        OrderRollup {
            region: "eu-west".into(),
            placed_at: 1_767_229_200,
            qty_by_sku: qty(&[("KBD-01", 2), ("MON-01", 3)]),
        },
        OrderRollup {
            region: "eu-west".into(),
            placed_at: 1_767_227_400,
            qty_by_sku: qty(&[("MSE-01", 1)]),
        },
        OrderRollup {
            region: "us-east".into(),
            placed_at: 1_767_238_800,
            qty_by_sku: qty(&[("CBL-01", 10)]),
        },
        OrderRollup {
            region: "us-east".into(),
            placed_at: 1_767_235_200,
            qty_by_sku: qty(&[("CBL-01", 5), ("DCK-01", 7)]),
        },
    ]
}

/// The fixed-schema target: an AggregatingMergeTree with three
/// AggregateFunction columns and a dedup window (required for token dedup).
async fn create_target(admin: &clickhouse::Client) {
    admin
        .query(
            "CREATE TABLE orders_agg (\
                 region String, \
                 first_placed_at AggregateFunction(min, DateTime), \
                 last_placed_at AggregateFunction(max, DateTime), \
                 qty_by_sku AggregateFunction(sumMap, Map(String, UInt64))\
             ) ENGINE = AggregatingMergeTree ORDER BY region \
             SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create orders_agg");
}

/// The Null landing table plus the Materialized View that builds the states.
async fn create_null_and_mv(admin: &clickhouse::Client) {
    admin
        .query(
            "CREATE TABLE orders_null (\
                 region String, placed_at DateTime, qty_by_sku Map(String, UInt64)\
             ) ENGINE = Null",
        )
        .execute()
        .await
        .expect("create orders_null");
    admin
        .query(
            "CREATE MATERIALIZED VIEW orders_mv TO orders_agg AS \
             SELECT region, minState(placed_at) AS first_placed_at, maxState(placed_at) AS last_placed_at, \
                    sumMapState(qty_by_sku) AS qty_by_sku \
             FROM orders_null GROUP BY region",
        )
        .execute()
        .await
        .expect("create orders_mv");
}

/// A sink pointed at the Null landing table, with MV dedup enabled so the
/// per-batch token reaches the AggregatingMergeTree target.
fn null_sink(url: &str) -> config::ClickHouseSink {
    sink_with(
        url,
        "orders_null",
        &["region", "placed_at", "qty_by_sku"],
        "off",
        "user: default\npassword: agg-secret\n\
         settings: { deduplicate_blocks_in_dependent_materialized_views: \"1\" }",
    )
}

async fn merged_min(admin: &clickhouse::Client, region: &str) -> u32 {
    admin
        .query("SELECT toUnixTimestamp(minMerge(first_placed_at)) FROM orders_agg WHERE region = ?")
        .bind(region)
        .fetch_one::<u32>()
        .await
        .expect("min merge")
}

async fn merged_max(admin: &clickhouse::Client, region: &str) -> u32 {
    admin
        .query("SELECT toUnixTimestamp(maxMerge(last_placed_at)) FROM orders_agg WHERE region = ?")
        .bind(region)
        .fetch_one::<u32>()
        .await
        .expect("max merge")
}

/// The summed value for one key of the merged sumMap (0 if the key is absent).
async fn merged_sum(admin: &clickhouse::Client, region: &str, key: &str) -> u64 {
    admin
        .query(
            "SELECT m[?] FROM \
             (SELECT sumMapMerge(qty_by_sku) AS m FROM orders_agg WHERE region = ?)",
        )
        .bind(key)
        .bind(region)
        .fetch_one::<u64>()
        .await
        .expect("sum map key")
}

/// The states built by the MV finalize to the expected min/max/sumMap.
#[tokio::test]
#[ignore = "requires Docker"]
async fn aggregate_states_round_trip_through_null_and_mv() {
    let srv = bare_server("26.3", "agg-secret").await;
    create_target(&srv.admin).await;
    create_null_and_mv(&srv.admin).await;
    let sink = null_sink(&srv.url);

    let ev = rollups();
    let batch = sealed(&ev, "agg-1", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("insert raw orders");

    assert_eq!(merged_min(&srv.admin, "eu-west").await, 1_767_225_600);
    assert_eq!(merged_max(&srv.admin, "eu-west").await, 1_767_229_200);
    assert_eq!(merged_sum(&srv.admin, "eu-west", "KBD-01").await, 3);
    assert_eq!(merged_sum(&srv.admin, "eu-west", "MSE-01").await, 3);
    assert_eq!(merged_sum(&srv.admin, "eu-west", "MON-01").await, 3);

    assert_eq!(merged_min(&srv.admin, "us-east").await, 1_767_235_200);
    assert_eq!(merged_max(&srv.admin, "us-east").await, 1_767_238_800);
    assert_eq!(merged_sum(&srv.admin, "us-east", "CBL-01").await, 15);
    assert_eq!(merged_sum(&srv.admin, "us-east", "DCK-01").await, 7);
}

/// Replaying a batch under its original token is a no-op through the view;
/// a distinct token re-aggregates. This is the 26.1+ exactly-once claim,
/// verified empirically, and it hinges on the MV-dedup setting, not the
/// version alone.
#[tokio::test]
#[ignore = "requires Docker"]
async fn mv_dedup_keeps_retries_exactly_once() {
    let srv = bare_server("26.3", "agg-secret").await;
    create_target(&srv.admin).await;
    create_null_and_mv(&srv.admin).await;
    let sink = null_sink(&srv.url);

    let ev = rollups();
    let batch = sealed(&ev, "agg-dup", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("first insert");
    assert_eq!(merged_sum(&srv.admin, "eu-west", "KBD-01").await, 3);

    // Replaying the identical batch (same dedup token) must not double-count
    // through the MV; the token propagates to the target's dedup window.
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("replay");
    assert_eq!(
        merged_sum(&srv.admin, "eu-west", "KBD-01").await,
        3,
        "same token must not double-count through the materialized view"
    );

    // Control: a distinct token re-aggregates (sumMap doubles). min/max are
    // idempotent regardless, so they stay put.
    let fresh = sealed(&ev, "agg-fresh", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &fresh)
        .await
        .expect("distinct-token insert");
    assert_eq!(
        merged_sum(&srv.admin, "eu-west", "KBD-01").await,
        6,
        "a distinct token re-aggregates through the view"
    );
    assert_eq!(merged_min(&srv.admin, "eu-west").await, 1_767_225_600);
    assert_eq!(merged_max(&srv.admin, "eu-west").await, 1_767_229_200);
}

/// A sink pointed directly at AggregateFunction columns fails validation with
/// an actionable message; it cannot write aggregate states on the wire.
#[tokio::test]
#[ignore = "requires Docker"]
async fn direct_insert_into_aggregate_function_column_is_rejected() {
    let srv = bare_server("26.3", "agg-secret").await;
    create_target(&srv.admin).await;

    let sink = sink_with(
        &srv.url,
        "orders_agg",
        &["region", "first_placed_at", "last_placed_at", "qty_by_sku"],
        "names",
        "user: default\npassword: agg-secret\n",
    );
    let err = sink
        .validate_schema()
        .await
        .expect_err("the sink must refuse to write aggregate states directly");
    let msg = err.to_string();
    assert!(msg.contains("AggregateFunction"), "{msg}");
    assert!(msg.contains("Null"), "{msg}");
    assert!(msg.contains("MATERIALIZED VIEW"), "{msg}");
}
