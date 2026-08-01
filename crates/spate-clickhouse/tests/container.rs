//! End-to-end tests against a real ClickHouse server (Docker).
//!
//! Ignored by default; run explicitly:
//! `cargo test -p spate-clickhouse --test container -- --ignored`

use bytes::BytesMut;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::serialize_row;
use spate_core::deser::Owned;
use spate_core::sink::SealedBatch;
// The concern modules under tests/container/ reach the writer trait through
// `use super::*`; re-export it so that stays a no-op for the root helpers.
use serde::{Deserialize, Serialize};
pub(crate) use spate_core::sink::ShardWriter;
use std::time::{Duration, Instant};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};

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

/// How long `docker start` itself gets. Readiness is not in here — the
/// fixtures wait for that themselves, in [`wait_for_queries`], so that a
/// server which never comes up can be reported rather than merely timed out.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a started node gets to answer an authenticated query.
///
/// A node answers in about a second, so this is already two orders of
/// magnitude of headroom, and raising it buys nothing a passing test wants:
/// it only decides how long a *broken* fixture burns before it reports. The
/// old 60s budget was not the problem — arriving at the end of it with no
/// information was.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How much of a failed node's stderr the panic carries.
const LOG_TAIL: usize = 40;

/// Hand readiness to the caller: `.start()` returns once the container is
/// started, without waiting on any condition.
///
/// The stock condition is an *unauthenticated* `GET /` returning 200, which
/// answers before the entrypoint has necessarily applied `CLICKHOUSE_PASSWORD`
/// — and it is awaited inside `.start()`, which drops the container handle on
/// failure. Waiting ourselves buys both the stronger condition and, on
/// timeout, the container's logs.
fn started_only(req: impl Into<ContainerRequest<ClickHouse>>) -> ContainerRequest<ClickHouse> {
    req.into()
        .with_ready_conditions(vec![WaitFor::Nothing])
        .with_startup_timeout(START_TIMEOUT)
}

/// A single readiness probe's own bound.
///
/// The `clickhouse` client sets no request timeout, so an attempt against a
/// half-open socket can hang indefinitely. A poll loop that only checks its
/// deadline *between* attempts therefore has no deadline at all — one hung
/// attempt outlasts any budget. Every probe below is wrapped in this.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Block until `admin` can run a query, or panic saying why it never could.
///
/// A ClickHouse server that dies on startup and one that is merely slow
/// present identically — as a wait that never finishes — so the panic carries
/// the container's liveness, exit code and stderr. Without those a failure
/// here is unactionable, and the only recourse is to re-run it.
async fn wait_for_queries(
    container: &ContainerAsync<ClickHouse>,
    admin: &clickhouse::Client,
    who: &str,
) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last;
    loop {
        let probe = tokio::time::timeout(PROBE_TIMEOUT, admin.query("SELECT 1").execute()).await;
        match probe {
            Ok(Ok(())) => return,
            Ok(Err(e)) => last = e.to_string(),
            Err(_) => last = format!("no response within {PROBE_TIMEOUT:?}"),
        }
        if Instant::now() >= deadline {
            let running = container.is_running().await;
            let exit = container.exit_code().await;
            let logs =
                String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap_or_default())
                    .into_owned();
            // The tail, not the whole log: a fatal is the last thing a
            // ClickHouse server writes, and a full startup log buries it.
            let tail: Vec<&str> = logs.lines().rev().take(LOG_TAIL).collect();
            let tail = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
            panic!(
                "{who} never answered a query within {READY_TIMEOUT:?} \
                 (running={running:?}, exit_code={exit:?}); last error: {last}\n\
                 ---- last {LOG_TAIL} lines of {who} stderr ----\n{tail}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn server() -> Server {
    let container = started_only(ClickHouse::default())
        .start()
        .await
        .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default().with_url(&url);
    wait_for_queries(&container, &admin, "clickhouse").await;
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

fn record<T>(payload: T) -> spate_core::record::Record<T> {
    let (ack, rx) = spate_core::checkpoint::AckRef::test_pair();
    std::mem::forget(rx);
    spate_core::record::Record {
        payload,
        meta: spate_core::record::RecordMeta {
            partition: spate_core::record::PartitionId(0),
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
) -> Result<SealedBatch, spate_core::error::SinkError>
where
    E: spate_core::sink::RowEncoder<spate_core::deser::Owned<T>>,
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

/// Encode `rows` through a [`NativeEncoder`](spate_clickhouse::NativeEncoder)
/// into one sealed batch: `encode` per row (buffering columnar), then one
/// `finish_chunk` producing exactly one Native block frame.
fn encode_native_batch<T>(
    encoder: &mut spate_clickhouse::NativeEncoder<Owned<T>>,
    rows: Vec<T>,
    token: &str,
) -> Result<SealedBatch, spate_core::error::SinkError>
where
    T: Serialize + Send + 'static,
{
    use spate_core::sink::RowEncoder;
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
    let container = started_only(
        ClickHouse::default()
            .with_tag(tag)
            .with_env_var("CLICKHOUSE_USER", "default")
            .with_env_var("CLICKHOUSE_PASSWORD", password),
    )
    .start()
    .await
    .expect("start clickhouse");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default")
        .with_password(password);
    wait_for_queries(&container, &admin, "clickhouse").await;
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
#[path = "container/aggregating_mv.rs"]
mod aggregating_mv;
#[path = "container/basics.rs"]
mod basics;
#[path = "container/distributed_check.rs"]
mod distributed_check;
#[path = "container/distributed_parity.rs"]
mod distributed_parity;
#[path = "container/multi_table.rs"]
mod multi_table;
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
