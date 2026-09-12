// ---- what the sink's account actually needs ------------------------------------
//
// The permissions page tells an operator which grants to hand the writer, and
// a claim there that is too generous costs a real deployment a grant it did
// not need. `system.columns` is row-filtered by what the user can already see,
// so `INSERT` on the target table is enough to introspect it. `system.tables`
// is filtered the same way, so the `distributed_check` guard reaches a
// `Distributed` table through a grant on that table. This holds that page to a
// server rather than to reasoning.

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

/// Three accounts against one `Distributed` table. `SELECT ON system.tables`
/// leaves its row filtered out, `SHOW TABLES` on the table makes it visible,
/// and the topology read needs `SELECT ON system.clusters`.
///
/// Regression for #415.
#[tokio::test]
#[ignore = "requires Docker"]
async fn distributed_check_sees_the_table_through_a_grant_on_it() {
    let srv = bare_server("admin-secret").await;
    let cluster = single_shard_cluster(&srv.admin).await;

    let mut ddl = vec![
        "CREATE TABLE orders (id UInt64, name String, amount Nullable(Float64)) \
         ENGINE = MergeTree ORDER BY id"
            .to_owned(),
        format!(
            "CREATE TABLE orders_dist AS orders ENGINE = \
             Distributed('{cluster}', currentDatabase(), 'orders', xxHash64(name))"
        ),
    ];
    for (user, grants) in [
        (
            "sys_reader",
            ["SELECT ON system.clusters", "SELECT ON system.tables"].as_slice(),
        ),
        (
            "obj_reader",
            [
                "SELECT ON system.clusters",
                "SHOW TABLES ON default.orders_dist",
            ]
            .as_slice(),
        ),
        (
            "no_clusters",
            ["SHOW TABLES ON default.orders_dist"].as_slice(),
        ),
    ] {
        ddl.push(format!("CREATE USER {user} IDENTIFIED BY '{user}-secret'"));
        // Each account needs INSERT on the local table for the sink to build
        // at all, before the guard runs.
        ddl.push(format!("GRANT INSERT ON default.orders TO {user}"));
        for grant in grants {
            ddl.push(format!("GRANT {grant} TO {user}"));
        }
    }
    for stmt in &ddl {
        srv.admin.query(stmt).execute().await.expect("ddl");
    }

    let guard = async |user: &str| {
        sink_with::<Owned<Order>>(
            &srv.url,
            "orders",
            &format!(
                r#"
user: {user}
password: {user}-secret
distributed_check:
  cluster: {cluster}
  table: orders_dist
  sharding_key: name
"#
            ),
        )
        .await
        .validate_distributed()
        .await
    };

    // The grant the page used to name. The row stays filtered out, so the
    // guard reports the table as absent.
    let err = guard("sys_reader")
        .await
        .expect_err("SELECT ON system.tables must not reveal the Distributed table");
    assert!(
        matches!(err, spate_clickhouse::DistributedCheckError::Mismatch(_)),
        "the topology read must succeed, leaving the table as the failure: {err}"
    );
    assert!(
        err.to_string()
            .contains("not found (or not visible to this user)"),
        "row filtering should read as an absent table, got: {err}"
    );

    // The grant the page names, holding nothing on `system.tables`.
    guard("obj_reader")
        .await
        .expect("SHOW TABLES on the Distributed table must make its DDL readable");

    // `system.clusters` is not reached by any object grant.
    let err = guard("no_clusters")
        .await
        .expect_err("the topology read needs SELECT ON system.clusters");
    assert!(
        err.to_string().contains("system.clusters"),
        "the failure must name the grant it lacks: {err}"
    );
}
