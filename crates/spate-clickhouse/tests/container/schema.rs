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

    // Happy path: the fetch against the real system.columns.
    sink_with::<Owned<Order>>(&srv.url, "orders", "").await;

    // A declared column the table does not have.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct IdNope {
        id: u64,
        nope: u64,
    }
    let err = try_sink_with::<Owned<IdNope>>(&srv.url, "orders", "")
        .await
        .expect_err("missing column");
    assert!(err.to_string().contains("`nope` does not exist"), "{err}");

    // MATERIALIZED and ALIAS columns cannot be insert targets.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct IdTwice {
        id: u64,
        twice: u64,
    }
    let err = try_sink_with::<Owned<IdTwice>>(&srv.url, "mat", "")
        .await
        .expect_err("non-insertable column");
    assert!(err.to_string().contains("MATERIALIZED"), "twice: {err}");

    #[derive(Clone, Serialize, ClickHouseRow)]
    struct IdAl {
        id: u64,
        al: u64,
    }
    let err = try_sink_with::<Owned<IdAl>>(&srv.url, "mat", "")
        .await
        .expect_err("non-insertable column");
    assert!(err.to_string().contains("ALIAS"), "al: {err}");

    // A table that does not exist.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct IdOnly {
        id: u64,
    }
    let err = try_sink_with::<Owned<IdOnly>>(&srv.url, "no_such_table", "")
        .await
        .expect_err("missing table");
    assert!(err.to_string().contains("not found"), "{err}");

    // Unconfigured table columns warn but pass; the server fills the
    // DEFAULT and the type default on insert. The insert column list is
    // narrower than the table, so the header describes a subset of it.
    let sink = sink_with::<Owned<IdOnly>>(&srv.url, "extras", "").await;
    let schema = sink.schema();
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
        "CREATE TABLE dec_col (id UInt64, amount Decimal(18, 2)) \
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

    // Declaration order differing from TABLE order is fine, since the
    // INSERT column list maps by name, as long as the struct being encoded
    // follows the DECLARED order.
    // Prove it lands in the right columns on a real server.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct Reordered {
        name: String,
        id: u64,
        amount: Option<f64>,
    }
    let sink = sink_with::<Owned<Reordered>>(&srv.url, "orders", "").await;
    let schema = sink.schema();
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

    // The same struct against declared order [id, name, amount]: the
    // positional wire contract breaks, and the first record says so.
    let sink = sink_with::<Owned<Order>>(&srv.url, "orders", "").await;
    let schema = sink.schema();
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
    .expect_err("struct order vs declared order");
    let reason = fatal(err);
    assert!(
        reason.contains("position 0: struct field `name` vs declared column `id`"),
        "{reason}"
    );

    // A type-class mismatch the wire cannot show: both are four bytes, so
    // the insert would parse and store a nonsense timestamp.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct I32X {
        id: u64,
        x: i32,
    }
    let sink = sink_with::<Owned<I32X>>(&srv.url, "dt_col", "").await;
    let schema = sink.schema();
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<I32X>>::with_schema(schema);
    let err = encode_batch(&mut encoder, vec![I32X { id: 1, x: 100 }], "dt-1")
        .expect_err("i32 vs DateTime");
    let reason = fatal(err);
    assert!(
        reason.contains("not compatible with `x` DateTime"),
        "{reason}"
    );

    // The Nullable hard rule, both directions: a wire-format
    // difference, not a type ambiguity.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct PlainAmount {
        id: u64,
        name: String,
        amount: f64, // column is Nullable(Float64)
    }
    let sink = sink_with::<Owned<PlainAmount>>(&srv.url, "orders", "").await;
    let schema = sink.schema();
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

    #[derive(Clone, Serialize, ClickHouseRow)]
    struct OptS {
        id: u64,
        s: Option<String>, // column is plain String
    }
    let sink = sink_with::<Owned<OptS>>(&srv.url, "plain_s", "").await;
    let schema = sink.schema();
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
    // passes the check and the row lands.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct LcRow {
        id: u64,
        lc: String,
    }
    let sink = sink_with::<Owned<LcRow>>(&srv.url, "lowcard", "").await;
    let schema = sink.schema();
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

    // A decimal wrapper's scale against the column's declared scale. Both
    // rows below mean 1.5; the wrapper's scale decides what the server
    // stores.
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct Scale4 {
        id: u64,
        amount: spate_clickhouse::Decimal64<4>,
    }
    #[derive(Clone, Serialize, ClickHouseRow)]
    struct Scale2 {
        id: u64,
        amount: spate_clickhouse::Decimal64<2>,
    }
    let read_amount = async |id: u64| -> String {
        srv.admin
            .query("SELECT toString(amount) FROM dec_col WHERE id = ?")
            .bind(id)
            .fetch_one()
            .await
            .expect("read back")
    };

    let sink = sink_with::<Owned<Scale4>>(&srv.url, "dec_col", "").await;
    let schema = sink.schema();
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<Scale4>>::with_schema(schema);
    let err = encode_batch(
        &mut encoder,
        vec![Scale4 {
            id: 1,
            amount: spate_clickhouse::Decimal64::<4>(15_000),
        }],
        "dec-1",
    )
    .expect_err("scale 4 vs a scale 2 column");
    let reason = fatal(err);
    assert!(
        reason.contains("Decimal64<4>") && reason.contains("Decimal(18, 2)"),
        "{reason}"
    );

    // The agreeing scale encodes and stores the value. Without the check
    // above, the same row against a scale-2 column stores 150 rather than
    // 1.5: same width, so nothing on the wire objects.
    let sink = sink_with::<Owned<Scale2>>(&srv.url, "dec_col", "").await;
    let schema = sink.schema();
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<Scale2>>::with_schema(schema);
    let batch = encode_batch(
        &mut encoder,
        vec![Scale2 {
            id: 3,
            amount: spate_clickhouse::Decimal64::<2>(150),
        }],
        "dec-3",
    )
    .expect("scale 2 matches the column");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");
    assert_eq!(read_amount(3).await, "1.5");
}
