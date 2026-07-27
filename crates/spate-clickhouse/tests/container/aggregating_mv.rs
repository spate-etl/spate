// ---- AggregatingMergeTree ingestion via a Null table + Materialized View ----
//
// AggregateFunction columns hold opaque, version-dependent aggregate STATES,
// so the sink never writes them directly. It INSERTs plain event rows into an
// ENGINE=Null landing table; a Materialized View builds min/max/sumMap states
// into the target AggregatingMergeTree. These tests prove (1) the three states
// round-trip, (2) at-least-once retries stay exactly-once through the view
// (ClickHouse >= 26.1 + deduplicate_blocks_in_dependent_materialized_views),
// and (3) pointing the sink straight at an AggregateFunction column is
// rejected with an actionable error. Pin 26.3 (the LTS the docs target).

use super::*;
use std::collections::BTreeMap;

/// A raw event. Only `Serialize` is needed — the wire path is RowBinary and
/// the tests read the merged aggregates back as scalars, not typed rows.
#[derive(Debug, Clone, Serialize)]
struct Event {
    bucket: String,
    dt: u32,                       // epoch seconds -> DateTime
    counts: BTreeMap<String, u64>, // -> Map(String, UInt64)
}

fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
}

/// Five events across two buckets. Bucket `a`: min 1000, max 2000,
/// sumMap {x:3, y:3, z:3}. Bucket `b`: min 4000, max 5000, sumMap {p:15, q:7}.
fn events() -> Vec<Event> {
    vec![
        Event {
            bucket: "a".into(),
            dt: 1000,
            counts: counts(&[("x", 1), ("y", 2)]),
        },
        Event {
            bucket: "a".into(),
            dt: 2000,
            counts: counts(&[("x", 2), ("z", 3)]),
        },
        Event {
            bucket: "a".into(),
            dt: 1500,
            counts: counts(&[("y", 1)]),
        },
        Event {
            bucket: "b".into(),
            dt: 5000,
            counts: counts(&[("p", 10)]),
        },
        Event {
            bucket: "b".into(),
            dt: 4000,
            counts: counts(&[("p", 5), ("q", 7)]),
        },
    ]
}

/// The fixed-schema target: an AggregatingMergeTree with three
/// AggregateFunction columns and a dedup window (required for token dedup).
async fn create_target(admin: &clickhouse::Client) {
    admin
        .query(
            "CREATE TABLE events_agg (\
                 bucket String, \
                 dt_min AggregateFunction(min, DateTime), \
                 dt_max AggregateFunction(max, DateTime), \
                 counts AggregateFunction(sumMap, Map(String, UInt64))\
             ) ENGINE = AggregatingMergeTree ORDER BY bucket \
             SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create events_agg");
}

/// The Null landing table plus the Materialized View that builds the states.
async fn create_null_and_mv(admin: &clickhouse::Client) {
    admin
        .query(
            "CREATE TABLE events_null (\
                 bucket String, dt DateTime, counts Map(String, UInt64)\
             ) ENGINE = Null",
        )
        .execute()
        .await
        .expect("create events_null");
    admin
        .query(
            "CREATE MATERIALIZED VIEW events_mv TO events_agg AS \
             SELECT bucket, minState(dt) AS dt_min, maxState(dt) AS dt_max, \
                    sumMapState(counts) AS counts \
             FROM events_null GROUP BY bucket",
        )
        .execute()
        .await
        .expect("create events_mv");
}

/// A sink pointed at the Null landing table, with MV dedup enabled so the
/// per-batch token reaches the AggregatingMergeTree target.
fn null_sink(url: &str) -> config::ClickHouseSink {
    sink_with(
        url,
        "events_null",
        &["bucket", "dt", "counts"],
        "off",
        "user: default\npassword: agg-secret\n\
         settings: { deduplicate_blocks_in_dependent_materialized_views: \"1\" }",
    )
}

async fn merged_min(admin: &clickhouse::Client, bucket: &str) -> u32 {
    admin
        .query("SELECT toUnixTimestamp(minMerge(dt_min)) FROM events_agg WHERE bucket = ?")
        .bind(bucket)
        .fetch_one::<u32>()
        .await
        .expect("min merge")
}

async fn merged_max(admin: &clickhouse::Client, bucket: &str) -> u32 {
    admin
        .query("SELECT toUnixTimestamp(maxMerge(dt_max)) FROM events_agg WHERE bucket = ?")
        .bind(bucket)
        .fetch_one::<u32>()
        .await
        .expect("max merge")
}

/// The summed value for one key of the merged sumMap (0 if the key is absent).
async fn merged_sum(admin: &clickhouse::Client, bucket: &str, key: &str) -> u64 {
    admin
        .query(
            "SELECT m[?] FROM \
             (SELECT sumMapMerge(counts) AS m FROM events_agg WHERE bucket = ?)",
        )
        .bind(key)
        .bind(bucket)
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

    let ev = events();
    let batch = sealed(&ev, "agg-1", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("insert raw events");

    assert_eq!(merged_min(&srv.admin, "a").await, 1000);
    assert_eq!(merged_max(&srv.admin, "a").await, 2000);
    assert_eq!(merged_sum(&srv.admin, "a", "x").await, 3);
    assert_eq!(merged_sum(&srv.admin, "a", "y").await, 3);
    assert_eq!(merged_sum(&srv.admin, "a", "z").await, 3);

    assert_eq!(merged_min(&srv.admin, "b").await, 4000);
    assert_eq!(merged_max(&srv.admin, "b").await, 5000);
    assert_eq!(merged_sum(&srv.admin, "b", "p").await, 15);
    assert_eq!(merged_sum(&srv.admin, "b", "q").await, 7);
}

/// Replaying a batch under its original token is a no-op through the view;
/// a distinct token re-aggregates. This is the 26.1+ exactly-once claim,
/// verified empirically — and it hinges on the MV-dedup setting, not the
/// version alone.
#[tokio::test]
#[ignore = "requires Docker"]
async fn mv_dedup_keeps_retries_exactly_once() {
    let srv = bare_server("26.3", "agg-secret").await;
    create_target(&srv.admin).await;
    create_null_and_mv(&srv.admin).await;
    let sink = null_sink(&srv.url);

    let ev = events();
    let batch = sealed(&ev, "agg-dup", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("first insert");
    assert_eq!(merged_sum(&srv.admin, "a", "x").await, 3);

    // Replaying the identical batch (same dedup token) must not double-count
    // through the MV — the token propagates to the target's dedup window.
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("replay");
    assert_eq!(
        merged_sum(&srv.admin, "a", "x").await,
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
        merged_sum(&srv.admin, "a", "x").await,
        6,
        "a distinct token re-aggregates through the view"
    );
    assert_eq!(merged_min(&srv.admin, "a").await, 1000);
    assert_eq!(merged_max(&srv.admin, "a").await, 2000);
}

/// A sink pointed directly at AggregateFunction columns fails validation with
/// an actionable message — it cannot write aggregate states on the wire.
#[tokio::test]
#[ignore = "requires Docker"]
async fn direct_insert_into_aggregate_function_column_is_rejected() {
    let srv = bare_server("26.3", "agg-secret").await;
    create_target(&srv.admin).await;

    let sink = sink_with(
        &srv.url,
        "events_agg",
        &["bucket", "dt_min", "dt_max", "counts"],
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
