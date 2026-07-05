//! End-to-end tests against a real ClickHouse server (Docker).
//!
//! Ignored by default; run explicitly:
//! `cargo test -p etl-clickhouse --test container -- --ignored`

use bytes::BytesMut;
use etl_clickhouse::config::{self, ClickHouseSinkConfig};
use etl_clickhouse::serialize_row;
use etl_core::sink::{SealedBatch, ShardWriter};
use serde::{Deserialize, Serialize};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize)]
struct Order {
    id: u64,
    name: String,
    amount: Option<f64>,
}

struct Server {
    _container: testcontainers_modules::testcontainers::ContainerAsync<ClickHouse>,
    url: String,
    admin: clickhouse::Client,
}

async fn server() -> Server {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default().with_url(&url);
    admin
        .query(
            "CREATE TABLE orders (id UInt64, name String, amount Nullable(Float64)) \
             ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create table");
    Server {
        _container: container,
        url,
        admin,
    }
}

fn sink_for(url: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
columns: [id, name, amount]
shards:
  - replicas: ["{url}"]
"#
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

fn sealed(rows: &[Order], token: &str, frames: usize) -> SealedBatch {
    let per = rows.len().div_ceil(frames);
    let mut out = Vec::new();
    let mut bytes = 0u64;
    for chunk in rows.chunks(per) {
        let mut buf = BytesMut::new();
        for row in chunk {
            serialize_row(row, &mut buf).expect("encode");
        }
        bytes += buf.len() as u64;
        out.push(buf.freeze());
    }
    SealedBatch {
        frames: out,
        rows: rows.len() as u64,
        bytes,
        dedup_token: token.to_string(),
    }
}

fn orders(range: std::ops::Range<u64>) -> Vec<Order> {
    range
        .map(|i| Order {
            id: i,
            name: format!("order-{i}"),
            amount: (i % 2 == 0).then_some(i as f64 * 1.5),
        })
        .collect()
}

async fn count(admin: &clickhouse::Client) -> u64 {
    admin
        .query("SELECT count() FROM orders")
        .fetch_one::<u64>()
        .await
        .expect("count")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn multi_frame_batches_land_and_read_back_exactly() {
    let srv = server().await;
    let sink = sink_for(&srv.url);

    let expected = orders(0..1_000);
    let batch = sealed(&expected, "e2e-batch-1", 4);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");

    let mut got: Vec<Order> = srv
        .admin
        .query("SELECT ?fields FROM orders ORDER BY id")
        .fetch_all()
        .await
        .expect("read back");
    got.sort_by_key(|o| o.id);
    assert_eq!(got, expected, "typed read-back must match encoded rows");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn same_token_dedupes_different_token_inserts() {
    let srv = server().await;
    let sink = sink_for(&srv.url);
    let rows = orders(0..100);

    let batch = sealed(&rows, "dedup-proof", 2);
    for _ in 0..2 {
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write");
    }
    assert_eq!(
        count(&srv.admin).await,
        100,
        "identical batch + identical token must deduplicate"
    );

    let renamed = sealed(&rows, "dedup-proof-DIFFERENT", 2);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &renamed)
        .await
        .expect("write");
    assert_eq!(
        count(&srv.admin).await,
        200,
        "same rows under a different token must insert"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn probe_reflects_connectivity() {
    let srv = server().await;
    let sink = sink_for(&srv.url);
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe healthy server");

    let unreachable = sink_for("http://127.0.0.1:1");
    assert!(
        unreachable
            .writer
            .probe(&unreachable.endpoints[0][0])
            .await
            .is_err()
    );
}
