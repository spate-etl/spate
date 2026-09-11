// ---- what the sink's account actually needs ------------------------------------
//
// The permissions page tells an operator which grants to hand the writer, and
// a claim there that is too generous costs a real deployment a grant it did
// not need. `system.columns` is row-filtered by what the user can already see,
// so `INSERT` on the target table is enough to introspect it. This holds that
// page to a server rather than to reasoning.

use super::*;

/// A user granted only `INSERT` on one table reads that table's columns from
/// `system.columns`, sees nothing of a table it has no grant on, and inserts.
#[tokio::test]
#[ignore = "requires Docker"]
async fn insert_alone_reaches_system_columns() {
    let srv = bare_server("admin-secret").await;
    for ddl in [
        "CREATE TABLE orders (id UInt64, name String, amount Nullable(Float64)) \
         ENGINE = MergeTree ORDER BY id \
         SETTINGS non_replicated_deduplication_window = 100",
        "CREATE TABLE ledger (id UInt64, name String, amount Nullable(Float64)) \
         ENGINE = MergeTree ORDER BY id",
        "CREATE USER writer IDENTIFIED BY 'writer-secret'",
        // The only grant. Notably not `SELECT ON system.columns`.
        "GRANT INSERT ON default.orders TO writer",
    ] {
        srv.admin.query(ddl).execute().await.expect("ddl");
    }

    let as_writer = async |table: &str| {
        try_sink_with::<Owned<Order>>(&srv.url, table, "user: writer\npassword: writer-secret\n")
            .await
    };

    // The grant it holds: the fetch reads the table's columns and validates.
    let sink = as_writer("orders")
        .await
        .expect("INSERT alone reads system.columns");
    let schema = sink.schema();

    // And the write the grant is for goes through, managed settings included.
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<Order>>::with_schema(schema);
    let batch = encode_batch(
        &mut encoder,
        vec![Order {
            id: 1,
            name: "x".into(),
            amount: Some(2.5),
        }],
        "perm-1",
    )
    .expect("first-record check");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");
    let landed: u64 = srv
        .admin
        .query("SELECT count() FROM orders")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(landed, 1);

    // The grant it does not hold: `system.columns` returns no rows rather than
    // an error, so the table reads as absent.
    let err = as_writer("ledger")
        .await
        .expect_err("a table the writer cannot see");
    let msg = err.to_string();
    assert!(
        msg.contains("not found (or not visible to this user)"),
        "row filtering should read as an absent table, got: {msg}"
    );
}
