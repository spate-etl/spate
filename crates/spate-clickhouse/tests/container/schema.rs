// ---- schema validation against a real server ----------------------------------

use super::*;

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

    // Off mode: no schema, no failure.
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<IdOnly>>::with_schema(schema);
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
    use spate_core::error::{ErrorClass, SinkError};

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

    // Config order differing from TABLE order is fine, since the INSERT
    // column list maps by name, as long as the struct follows the CONFIG
    // order.
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<Reordered>>::with_schema(schema);
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<Reordered>>::with_schema(schema);
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<I32X>>::with_schema(schema);
    let err = encode_batch(&mut encoder, vec![I32X { id: 1, x: 100 }], "dt-1")
        .expect_err("i32 vs DateTime in full mode");
    let reason = fatal(err);
    assert!(
        reason.contains("not compatible with `x` DateTime"),
        "{reason}"
    );

    let sink = sink_with(&srv.url, "dt_col", &["id", "x"], "names", "");
    let schema = sink.validate_schema().await.unwrap().unwrap();
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<I32X>>::with_schema(schema);
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
    let mut encoder =
        spate_clickhouse::ClickHouseEncoder::<Owned<PlainAmount>>::with_schema(schema);
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<OptS>>::with_schema(schema);
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
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<LcRow>>::with_schema(schema);
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
