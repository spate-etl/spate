// The 256-bit integers and nested Geo shapes: the client cannot decode
// Int256/UInt256, and the nested Array-of-Array offset layout of Polygon /
// MultiPolygon is otherwise only byte-unit tested. Prove them against a real
// server via the toString oracle: row 1 through the Native encoder, row 2 as
// server-parsed literals, compared column by column.

use super::*;
use spate_clickhouse::{Int256, MultiPolygon, NativeEncoder, Polygon, Ring, UInt256};

#[derive(Serialize)]
struct EdgeRow {
    id: u64,
    big: Int256,
    ubig: UInt256,
    poly: Polygon,
    mpoly: MultiPolygon,
}

const COLUMNS: &[&str] = &["id", "big", "ubig", "poly", "mpoly"];

const DDL: &str = "CREATE TABLE native_edges (\
        id UInt64, big Int256, ubig UInt256, poly Polygon, mpoly MultiPolygon\
    ) ENGINE = MergeTree ORDER BY id";

// Row id=2: the same values as [`edge_row`], as server-parsed literals.
const LITERAL_INSERT: &str = "INSERT INTO native_edges VALUES (2, \
        toInt256('-170141183460469231731687303715884105728'), \
        toUInt256('340282366920938463463374607431768211455'), \
        [[(0, 0), (10, 0), (10, 10)]], [[[(0, 0), (10, 0), (10, 10)]]])";

fn edge_row() -> EdgeRow {
    let ring: Ring = vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)];
    EdgeRow {
        id: 1,
        big: Int256::from_i128(i128::MIN),
        ubig: UInt256::from_u128(u128::MAX),
        poly: vec![ring.clone()],
        mpoly: vec![vec![ring]],
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn int256_and_nested_geo_match_the_literal_row() {
    let srv = bare_server("26.3", "edges-secret").await;
    srv.admin
        .query(DDL)
        .execute()
        .await
        .expect("create native_edges");

    let sink = sink_with(
        &srv.url,
        "native_edges",
        COLUMNS,
        "full",
        "format: native\nuser: default\npassword: edges-secret\n",
    );
    let schema = sink.native_schema().await.expect("native schema");
    let mut encoder = NativeEncoder::<Owned<EdgeRow>>::new(schema);
    let batch = encode_native_batch(&mut encoder, vec![edge_row()], "edges-1").expect("encode");
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write native block");

    srv.admin
        .query(LITERAL_INSERT)
        .execute()
        .await
        .expect("literal insert");

    for col in COLUMNS.iter().filter(|c| **c != "id") {
        let read = |id: u64| {
            let sql = format!("SELECT toString(`{col}`) FROM native_edges WHERE id = {id}");
            let admin = srv.admin.clone();
            async move { admin.query(&sql).fetch_one::<String>().await.expect("read") }
        };
        assert_eq!(
            read(1).await,
            read(2).await,
            "column `{col}`: Native-encoded row diverged from the literal row"
        );
    }
}
