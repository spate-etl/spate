// ---- Flattened `Nested` columns: end-to-end against a real server ------------
//
// Under `flatten_nested = 1`, the server default, a `Nested` column is stored
// as parallel columns named `outer.inner`, and the insert names them that way.
// The server is the arbiter of whether a backtick-quoted dotted name resolves
// to the flattened column rather than to a qualified reference, so both
// formats write one and read it back for comparison.

use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, clickhouse::Row)]
struct NestedRow {
    id: u64,
    // A Rust field cannot carry the dot, and the first-record check compares
    // the serde name against the configured column.
    #[serde(rename = "tags.key")]
    tags_key: Vec<String>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<String>,
}

const TAG: &str = "26.3";

const COLUMNS: &[&str] = &["id", "tags.key", "tags.value"];

fn ddl(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (id UInt64, tags Nested(key String, value String)) \
         ENGINE = MergeTree ORDER BY id"
    )
}

fn select(table: &str) -> String {
    format!("SELECT `id`, `tags.key`, `tags.value` FROM {table} ORDER BY id")
}

fn rows() -> Vec<NestedRow> {
    vec![
        NestedRow {
            id: 1,
            tags_key: vec!["foo".to_string(), "baz".to_string()],
            tags_value: vec!["bar".to_string(), "qux".to_string()],
        },
        NestedRow {
            id: 2,
            tags_key: vec![],
            tags_value: vec![],
        },
    ]
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn nested_native_format_round_trips_through_a_real_server() {
    let password = "nested-native-secret";
    let srv = bare_server(TAG, password).await;
    srv.admin
        .query(&ddl("nested_native"))
        .execute()
        .await
        .expect("create nested_native");

    let sink = sink_with(
        &srv.url,
        "nested_native",
        COLUMNS,
        "full",
        &format!("format: native\nuser: default\npassword: {password}\n"),
    );
    assert_eq!(
        sink.writer.insert_sql(),
        "INSERT INTO `nested_native` (`id`, `tags.key`, `tags.value`) FORMAT Native"
    );

    let native_schema = sink
        .native_schema()
        .await
        .expect("fetch native schema from system.columns");
    let mut encoder = spate_clickhouse::NativeEncoder::<Owned<NestedRow>>::new(native_schema);
    let sent = rows();
    let batch = encode_native_batch(&mut encoder, sent.clone(), "nested-native-1").expect("encode");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write native block");

    let mut got: Vec<NestedRow> = srv
        .admin
        .query(&select("nested_native"))
        .fetch_all()
        .await
        .expect("read back nested native rows");
    got.sort_by_key(|r| r.id);
    assert_eq!(
        got, sent,
        "Native round-trip must match what we encoded, per flattened column"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn nested_rowbinary_format_round_trips_through_a_real_server() {
    let password = "nested-rowbinary-secret";
    let srv = bare_server(TAG, password).await;
    srv.admin
        .query(&ddl("nested_rowbinary"))
        .execute()
        .await
        .expect("create nested_rowbinary");

    let sink = sink_with(
        &srv.url,
        "nested_rowbinary",
        COLUMNS,
        "full",
        &format!("format: rowbinary\nuser: default\npassword: {password}\n"),
    );
    assert_eq!(
        sink.writer.insert_sql(),
        "INSERT INTO `nested_rowbinary` (`id`, `tags.key`, `tags.value`) FORMAT RowBinary"
    );

    // `full` mode: the flattened columns are ordinary `Array(String)`, so the
    // `Vec<String>` fields pass the per-position type check.
    let schema = sink
        .validate_schema()
        .await
        .expect("schema validates against the flattened columns")
        .unwrap();
    let mut encoder = spate_clickhouse::ClickHouseEncoder::<Owned<NestedRow>>::with_schema(schema);
    let sent = rows();
    let batch = encode_batch(&mut encoder, sent.clone(), "nested-rowbinary-1").expect("encode");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write rowbinary rows");

    let mut got: Vec<NestedRow> = srv
        .admin
        .query(&select("nested_rowbinary"))
        .fetch_all()
        .await
        .expect("read back nested rowbinary rows");
    got.sort_by_key(|r| r.id);
    assert_eq!(
        got, sent,
        "RowBinary round-trip must match what we encoded, per flattened column"
    );
}
