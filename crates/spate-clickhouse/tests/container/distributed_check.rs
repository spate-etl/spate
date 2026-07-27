// ---- single-node Distributed-parity oracle + DDL guard ----------------------
//
// These tests use ONE real ClickHouse server to pin down two things the whole
// Distributed-parity feature rests on:
//
//   1. that `DistributedRouter::hash_key` reproduces the server's own
//      `xxHash64(...)` bit-for-bit (the placement oracle — if this drifts,
//      every parity claim is void), and
//   2. that `validate_distributed()` reads a real `Distributed` table's DDL
//      out of `system.tables` and agrees (or, on drift, disagrees with a
//      diff naming both expressions).
//
// The stock 26.3 image ships a handful of single-shard clusters in its
// default `remote_servers` (e.g. `default` / `test_shard_localhost`); the
// guard tests build their `Distributed` table over whichever one is present,
// so a single container is enough — no cluster wiring required.

use super::*;
use spate_clickhouse::config::ClickHouseSinkConfig;
use spate_clickhouse::{DistributedRouter, ShardKey};
use spate_core::deser::Owned;

/// The concrete family the (family-generic but family-agnostic) `hash_key`
/// associated fn is called through; its payload type is irrelevant.
type Fam = Owned<Vec<u8>>;

const PW: &str = "distributed-secret";

/// A single-shard cluster from the server's default `remote_servers`,
/// preferring `default` when the image defines it. The DDL guard only reads
/// `system.clusters`/`system.tables`, so the shard never has to be
/// reachable — any single-shard cluster is a valid fixture.
async fn single_shard_cluster(admin: &clickhouse::Client) -> String {
    admin
        .query(
            "SELECT cluster FROM system.clusters \
             GROUP BY cluster HAVING max(shard_num) = 1 \
             ORDER BY cluster = 'default' DESC, cluster \
             LIMIT 1",
        )
        .fetch_one::<String>()
        .await
        .expect("the stock image ships at least one single-shard cluster")
}

/// Build a sink whose `distributed_check` targets `dist_table` over
/// `cluster`, keyed on `name` (expected DDL expression `xxHash64(name)`).
fn checked_sink(url: &str, cluster: &str, dist_table: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
columns: [id, name]
shards:
  - replicas: ["{url}"]
user: default
password: {PW}
distributed_check:
  cluster: {cluster}
  table: {dist_table}
  sharding_key: name
"#
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

/// The live oracle: the server's `xxHash64` over strings and both integer
/// widths must equal `DistributedRouter::hash_key` for the matching
/// [`ShardKey`], byte-for-byte. Every other parity claim in this crate is
/// downstream of this equality holding on a real server.
#[tokio::test]
#[ignore = "requires Docker"]
async fn server_hash_parity_holds_for_string_and_u64_keys() {
    let srv = bare_server("26.3", PW).await;

    // `hex()` renders the 64-bit hash as 16 uppercase hex digits — exactly
    // what `{:016X}` produces from our computed hash, so the two sides are
    // compared as strings without any endianness ambiguity.
    let (empty, abc, u64_42, u32_42) = srv
        .admin
        .query(
            "SELECT hex(xxHash64('')), hex(xxHash64('abc')), \
             hex(xxHash64(toUInt64(42))), hex(xxHash64(toUInt32(42)))",
        )
        .fetch_one::<(String, String, String, String)>()
        .await
        .expect("hash oracle query");

    let ours = |k| format!("{:016X}", DistributedRouter::<Fam>::hash_key(k));
    assert_eq!(
        empty,
        ours(ShardKey::Str("")),
        "empty-string xxHash64 must match the server (routing of the empty key)"
    );
    assert_eq!(
        abc,
        ours(ShardKey::Str("abc")),
        "string xxHash64 must match the server"
    );
    assert_eq!(
        u64_42,
        ours(ShardKey::U64(42)),
        "UInt64 key must hash its 8 little-endian bytes exactly as the server"
    );
    assert_eq!(
        u32_42,
        ours(ShardKey::U32(42)),
        "UInt32 key must hash its 4 little-endian bytes exactly as the server \
         (the declared column width is part of the hash input)"
    );
}

/// The guard accepts a `Distributed` table whose sharding expression is
/// exactly `xxHash64(name)` — the shipped identifier-key form. This proves
/// the `engine_full` parser and the normalized comparison line up with what
/// ClickHouse actually stores in `system.tables`.
#[tokio::test]
#[ignore = "requires Docker"]
async fn distributed_check_passes_against_a_real_distributed_table() {
    let srv = bare_server("26.3", PW).await;
    let cluster = single_shard_cluster(&srv.admin).await;

    srv.admin
        .query("CREATE TABLE orders (id UInt64, name String) ENGINE = MergeTree ORDER BY id")
        .execute()
        .await
        .expect("create local table");
    // A real `Distributed` table — the guard reads its stored `engine_full`,
    // it is never inserted into. `currentDatabase()` resolves to the admin
    // session's database at DDL time.
    srv.admin
        .query(&format!(
            "CREATE TABLE orders_dist AS orders \
             ENGINE = Distributed('{cluster}', currentDatabase(), 'orders', xxHash64(name))"
        ))
        .execute()
        .await
        .expect("create distributed table");

    checked_sink(&srv.url, &cluster, "orders_dist")
        .validate_distributed()
        .await
        .expect("a matching cluster + xxHash64(name) DDL must pass the parity check");
}

/// The guard rejects a `Distributed` table whose sharding expression drifted
/// from the sink's — here `cityHash64(name)` against an expected
/// `xxHash64(name)` — and the `Mismatch` prints BOTH expressions so an
/// operator diagnoses it in one read. This is the failure the feature
/// exists to catch: a pruned SELECT over a drifted table returns wrong
/// results silently, never an error.
#[tokio::test]
#[ignore = "requires Docker"]
async fn drifted_sharding_expression_fails_against_a_real_table() {
    let srv = bare_server("26.3", PW).await;
    let cluster = single_shard_cluster(&srv.admin).await;

    srv.admin
        .query("CREATE TABLE orders (id UInt64, name String) ENGINE = MergeTree ORDER BY id")
        .execute()
        .await
        .expect("create local table");
    srv.admin
        .query(&format!(
            "CREATE TABLE orders_dist_drift AS orders \
             ENGINE = Distributed('{cluster}', currentDatabase(), 'orders', cityHash64(name))"
        ))
        .execute()
        .await
        .expect("create drifted distributed table");

    let err = checked_sink(&srv.url, &cluster, "orders_dist_drift")
        .validate_distributed()
        .await
        .expect_err("a cityHash64 DDL must fail an xxHash64 sink config");
    let msg = err.to_string();
    assert!(
        msg.contains("cityHash64") && msg.contains("xxHash64"),
        "the mismatch diff must print both the DDL and the expected expression: {msg}"
    );
}
