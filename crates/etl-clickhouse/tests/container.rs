//! End-to-end tests against a real ClickHouse server (Docker).
//!
//! Ignored by default; run explicitly:
//! `cargo test -p etl-clickhouse --test container -- --ignored`

use bytes::BytesMut;
use etl_clickhouse::config::{self, ClickHouseSinkConfig};
use etl_clickhouse::serialize_row;
use etl_core::deser::Owned;
use etl_core::sink::SealedBatch;
// The concern modules under tests/container/ reach the writer trait through
// `use super::*`; re-export it so that stays a no-op for the root helpers.
pub(crate) use etl_core::sink::ShardWriter;
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

fn sealed<T: Serialize>(rows: &[T], token: &str, frames: usize) -> SealedBatch {
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

// ---- helpers for encoder-path and schema-validation tests --------------------

fn record<T>(payload: T) -> etl_core::record::Record<T> {
    let (ack, rx) = etl_core::checkpoint::AckRef::test_pair();
    std::mem::forget(rx);
    etl_core::record::Record {
        payload,
        meta: etl_core::record::RecordMeta {
            partition: etl_core::record::PartitionId(0),
            offset: 0,
            event_time_ms: 0,
            key_hash: None,
        },
        ack,
    }
}

fn sink_with(
    url: &str,
    table: &str,
    columns: &[&str],
    mode: &str,
    settings: &str,
) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: {table}
columns: [{}]
shards:
  - replicas: ["{url}"]
validate_schema: {mode}
{settings}
"#,
        columns.join(", ")
    ))
    .expect("config yaml");
    config::build(cfg).expect("valid sink config")
}

/// Encode `rows` through a (possibly schema-checked) encoder into one
/// sealed batch — the same CPU path a pipeline thread runs.
fn encode_batch<T, E>(
    encoder: &mut E,
    rows: Vec<T>,
    token: &str,
) -> Result<SealedBatch, etl_core::error::SinkError>
where
    E: etl_core::sink::RowEncoder<etl_core::deser::Owned<T>>,
    T: Send + 'static,
{
    let mut buf = BytesMut::new();
    let n = rows.len() as u64;
    for row in rows {
        encoder.encode(&record(row), &mut buf)?;
    }
    let frame = buf.freeze();
    let bytes = frame.len() as u64;
    Ok(SealedBatch {
        frames: vec![frame],
        rows: n,
        bytes,
        dedup_token: token.to_string(),
    })
}

/// Encode `rows` through a [`NativeEncoder`](etl_clickhouse::NativeEncoder)
/// into one sealed batch: `encode` per row (buffering columnar), then one
/// `finish_chunk` producing exactly one Native block frame.
fn encode_native_batch<T>(
    encoder: &mut etl_clickhouse::NativeEncoder<Owned<T>>,
    rows: Vec<T>,
    token: &str,
) -> Result<SealedBatch, etl_core::error::SinkError>
where
    T: Serialize + Send + 'static,
{
    use etl_core::sink::RowEncoder;
    let mut buf = BytesMut::new();
    let n = rows.len() as u64;
    for row in rows {
        encoder.encode(&record(row), &mut buf)?;
    }
    encoder.finish_chunk(&mut buf)?;
    let frame = buf.freeze();
    let bytes = frame.len() as u64;
    Ok(SealedBatch {
        frames: vec![frame],
        rows: n,
        bytes,
        dedup_token: token.to_string(),
    })
}

/// A pinned-version server. Newer official images set up a required
/// password unless one is provided, so this always configures explicit
/// credentials (unlike the module's ancient default image).
async fn bare_server(tag: &str, password: &str) -> Server {
    use testcontainers_modules::testcontainers::ImageExt;
    let container = ClickHouse::default()
        .with_tag(tag)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", password)
        .start()
        .await
        .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default")
        .with_password(password);
    Server {
        _container: container,
        url,
        admin,
    }
}

// The tests are split by concern into the modules below; the shared fixtures
// and batch/encoder helpers they all use live in this file. See the per-concern
// files under tests/container/.

// `container.rs` is the test binary's crate root, so its modules would resolve
// as siblings in tests/ (each becoming its own binary). `#[path]` keeps them in
// the tests/container/ subdirectory, part of this single binary.
#[path = "container/basics.rs"]
mod basics;
#[cfg(feature = "uuid")]
#[path = "container/native_e2e.rs"]
mod native_e2e;
#[path = "container/native_edges.rs"]
mod native_edges;
#[path = "container/partition_dedup.rs"]
mod partition_dedup;
#[path = "container/schema.rs"]
mod schema;
#[cfg(all(feature = "uuid", feature = "chrono", feature = "time"))]
#[path = "container/wide.rs"]
mod wide;
