//! Multi-table (multi-sink) behaviour at the connector level: several
//! config-built ClickHouse sinks, each writing to its own table, with a
//! failure on one isolated from the rest.
//!
//! The split *terminal* and the full-pipeline at-least-once-across-tables
//! contract (a watermark held until every table wrote, worst-status merge) are
//! covered where they live — `spate-core`'s `ops` tests and `spate-test`'s
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

fn sink_for_table(url: &str, table: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: {table}\ncolumns: [id, name, amount]\nshards:\n  - replicas: [\"{url}\"]\n"
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
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
    let sink_a = sink_for_table(&srv.url, "orders_a");
    let sink_b = sink_for_table(&srv.url, "orders_b");

    // Route even ids to table a, odd ids to table b — the shape a split
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
    let srv = server().await;
    make_table(&srv.admin, "orders_a").await;
    let healthy = sink_for_table(&srv.url, "orders_a");
    // A second sink pointed at an unreachable endpoint — a table whose shard is
    // down. In a full pipeline this failing write stalls the source watermark
    // (worst-status merge; see spate-test's split tests); here we prove the
    // failure is isolated to that sink and does not corrupt the healthy table.
    let dead = sink_for_table("http://127.0.0.1:1", "orders_b");

    healthy
        .writer
        .write_batch(&healthy.endpoints[0][0], &sealed(&orders(0..50), "a-1", 1))
        .await
        .expect("healthy table write succeeds");
    let result = dead
        .writer
        .write_batch(&dead.endpoints[0][0], &sealed(&orders(0..50), "b-1", 1))
        .await;
    assert!(
        result.is_err(),
        "a write to an unreachable table must fail (a real pipeline then stalls its watermark)"
    );
    assert_eq!(
        count_table(&srv.admin, "orders_a").await,
        50,
        "the healthy table is unaffected by the other sink's failure"
    );
}
