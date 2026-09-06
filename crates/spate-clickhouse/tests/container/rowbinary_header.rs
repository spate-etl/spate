// ---- the RowBinary header against a live table ---------------------------------
//
// RowBinary maps bytes to columns by position, so an `ALTER` that keeps a
// column's byte width is invisible to it: the server reads the same bytes
// under the new type and stores a different value. The header names each
// column and its type, and the server checks it on every insert, which turns
// that class of drift into a fatal error.

use super::*;
use spate_core::error::{ErrorClass, SinkError};

const DDL: &str = "CREATE TABLE drifting (id UInt8, d Decimal(18, 4), ts DateTime64(3, 'UTC')) \
     ENGINE = MergeTree ORDER BY id";

#[derive(Clone, Serialize, ClickHouseRow)]
struct DriftRow {
    id: u8,
    d: spate_clickhouse::Decimal64<4>,
    ts: spate_clickhouse::DateTime64Millis,
}

fn row(id: u8) -> DriftRow {
    DriftRow {
        id,
        // 1.5 at scale 4, and 2026-09-06 12:00:00 in milliseconds.
        d: spate_clickhouse::Decimal64::<4>(15_000),
        ts: spate_clickhouse::DateTime64Millis(1_788_696_000_000),
    }
}

/// An `ALTER` that keeps every column's width is rejected on the next insert
/// rather than silently storing rescaled values.
///
/// Regression for #410.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_same_width_alter_is_rejected_rather_than_silently_miswritten() {
    let srv = bare_server("26.3", "drift-secret").await;
    srv.admin.query(DDL).execute().await.expect("ddl");

    let sink = sink_with::<Owned<DriftRow>>(
        &srv.url,
        "drifting",
        "user: default\npassword: drift-secret\n",
    )
    .await;
    let mut encoder =
        spate_clickhouse::ClickHouseEncoder::<Owned<DriftRow>>::with_schema(sink.schema());

    let batch = encode_batch(&mut encoder, vec![row(1)], "drift-1").expect("encode");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("the schema the sink fetched is the table's");

    // Same width either side: Decimal(18, S) is Int64-backed for any S, and a
    // DateTime64 tick is an Int64 whatever its precision.
    for alter in [
        "ALTER TABLE drifting MODIFY COLUMN d Decimal(18, 2)",
        "ALTER TABLE drifting MODIFY COLUMN ts DateTime64(6, 'UTC')",
    ] {
        srv.admin.query(alter).execute().await.expect("alter");
    }

    let batch = encode_batch(&mut encoder, vec![row(2)], "drift-2").expect("encode");
    let err = sink
        .writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect_err("the header no longer describes the table");
    match err {
        SinkError::Client { class, reason } => {
            assert_eq!(class, ErrorClass::Fatal, "{reason}");
            assert!(
                reason.contains("Code: 117"),
                "the server rejects the header as INCORRECT_DATA: {reason}"
            );
        }
        other => panic!("unexpected error shape: {other:?}"),
    }

    // The rejected batch stored nothing, and the accepted one still reads as
    // the value it was written as: the `ALTER` converted it, where a
    // positional insert under the new type would have stored 150.
    let stored: Vec<String> = srv
        .admin
        .query("SELECT toString(d) FROM drifting ORDER BY id")
        .fetch_all()
        .await
        .expect("read back");
    assert_eq!(stored, vec!["1.5".to_string()], "only the first row landed");
}
