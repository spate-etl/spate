//! Multi-table (multi-sink) behavior at the connector level: several
//! config-built ClickHouse sinks, each writing to its own table, with a
//! failure on one isolated from the rest.
//!
//! The split *terminal* and the full-pipeline at-least-once-across-tables
//! contract (a watermark held until every table wrote, worst-status merge) are
//! covered where they live, in `spate-core`'s `ops` tests and `spate-test`'s
//! `tests/split.rs`. Here we prove the connector half: N sinks to N tables,
//! independent and correctly isolated.

use super::*;

async fn make_table(admin: &clickhouse::Client, table: &str) {
    admin
        .query(&format!(
            "CREATE TABLE {table} (id UInt64, name String, amount Nullable(Float64)) \
             ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100"
        ))
        .execute()
        .await
        .expect("create table");
}

async fn sink_for_table(url: &str, table: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: {table}\nshards:\n  - replicas: [\"{url}\"]\n{SERVER_CREDENTIALS}"
    ))
    .expect("config yaml");
    config::build(cfg)
        .expect("valid sink config")
        .with_row::<Owned<Order>>()
        .await
        .expect("schema fetch")
}

async fn count_table(admin: &clickhouse::Client, table: &str) -> u64 {
    admin
        .query(&format!("SELECT count() FROM {table}"))
        .fetch_one::<u64>()
        .await
        .expect("count")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn sinks_write_to_independent_tables() {
    let srv = server().await; // also creates the unrelated `orders` table
    make_table(&srv.admin, "orders_a").await;
    make_table(&srv.admin, "orders_b").await;
    let sink_a = sink_for_table(&srv.url, "orders_a").await;
    let sink_b = sink_for_table(&srv.url, "orders_b").await;

    // Route even ids to table a, odd ids to table b, the shape a split
    // terminal produces, exercised straight against the two sinks' writers.
    let (evens, odds): (Vec<Order>, Vec<Order>) =
        orders(0..200).into_iter().partition(|o| o.id % 2 == 0);
    sink_a
        .writer
        .write_batch(&sink_a.endpoints[0][0], &sealed(&evens, "a-1", 2))
        .await
        .expect("write a");
    sink_b
        .writer
        .write_batch(&sink_b.endpoints[0][0], &sealed(&odds, "b-1", 2))
        .await
        .expect("write b");

    assert_eq!(count_table(&srv.admin, "orders_a").await, 100);
    assert_eq!(count_table(&srv.admin, "orders_b").await, 100);
    // Each table holds only its routed rows, and the unrelated table is untouched.
    let a_odd: u64 = srv
        .admin
        .query("SELECT count() FROM orders_a WHERE id % 2 = 1")
        .fetch_one()
        .await
        .expect("query");
    assert_eq!(a_odd, 0, "table a received only its (even) routed rows");
    assert_eq!(
        count(&srv.admin).await,
        0,
        "the unrelated `orders` table is untouched"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn a_failed_table_write_is_isolated_from_the_others() {
    use spate_core::error::{ErrorClass, SinkError};

    let srv = server().await;
    make_table(&srv.admin, "orders_a").await;
    let healthy = sink_for_table(&srv.url, "orders_a").await;

    // A second sink on its own server, which then goes away: the shard is
    // down. In a full pipeline that write is retried and stalls the source
    // watermark (worst-status merge; see spate-test's split tests). The sink
    // reads its table's schema when it is built, so the server has to be
    // alive for that and unreachable afterwards.
    let doomed = server().await;
    make_table(&doomed.admin, "orders_b").await;
    let unreachable = sink_for_table(&doomed.url, "orders_b").await;
    drop(doomed);

    // A third whose table is dropped out from under it: the same isolation
    // property reached through a fatal server exception rather than a
    // retryable transport failure.
    make_table(&srv.admin, "orders_c").await;
    let dropped = sink_for_table(&srv.url, "orders_c").await;
    srv.admin
        .query("DROP TABLE orders_c")
        .execute()
        .await
        .expect("drop");

    healthy
        .writer
        .write_batch(&healthy.endpoints[0][0], &sealed(&orders(0..50), "a-1", 1))
        .await
        .expect("healthy table write succeeds");

    let err = unreachable
        .writer
        .write_batch(
            &unreachable.endpoints[0][0],
            &sealed(&orders(0..50), "b-1", 1),
        )
        .await
        .expect_err("a write to a shard that is down must fail");
    match err {
        SinkError::Client { class, reason } => assert_eq!(
            class,
            ErrorClass::Retryable,
            "a shard being down is retried, which is what stalls the watermark: {reason}"
        ),
        other => panic!("unexpected error shape: {other:?}"),
    }

    let err = dropped
        .writer
        .write_batch(&dropped.endpoints[0][0], &sealed(&orders(0..50), "c-1", 1))
        .await
        .expect_err("a write to a dropped table must fail");
    match err {
        SinkError::Client { class, reason } => assert_eq!(
            class,
            ErrorClass::Fatal,
            "UNKNOWN_TABLE cannot succeed on retry: {reason}"
        ),
        other => panic!("unexpected error shape: {other:?}"),
    }

    assert_eq!(
        count_table(&srv.admin, "orders_a").await,
        50,
        "the healthy table is unaffected by the other sinks' failures"
    );
}
