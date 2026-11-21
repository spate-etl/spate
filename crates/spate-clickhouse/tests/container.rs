//! End-to-end tests against a real ClickHouse server (Docker).
//!
//! Ignored by default; run explicitly:
//! `cargo test -p spate-clickhouse --test container -- --ignored`

mod support;

use bytes::BytesMut;
use spate_clickhouse::ClickHouseSinkBuilder;
use spate_clickhouse::config::{self, ClickHouseSinkConfig};
use spate_clickhouse::{ClickHouseRow, ClickHouseRowFamily, Compression};
use spate_core::deser::Owned;
use spate_core::sink::SealedBatch;
use spate_test::record;
use spate_test_support::container_image;
// The concern modules under tests/container/ reach the writer trait through
// `use super::*`; re-export it so that stays a no-op for the root helpers.
use serde::{Deserialize, Serialize};
pub(crate) use spate_core::sink::ShardWriter;
use std::time::Duration;
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};

#[derive(Debug, Clone, PartialEq, clickhouse::Row, Serialize, Deserialize, ClickHouseRow)]
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

/// How long `docker start` itself gets. Readiness is not in here; the
/// fixtures wait for that in [`answering`], which reports a server that exits
/// or never answers with its exit code and output.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// The password every fixture in this binary gives the `default` user.
const SERVER_PASSWORD: &str = "container-secret";

/// The `settings` fragment naming [`SERVER_PASSWORD`], for the helpers that
/// take one.
const SERVER_CREDENTIALS: &str = "user: default\npassword: container-secret\n";

/// The image pinned by the lane in `SPATE_CLICKHOUSE_LANE`, as `name` and
/// `tag`, falling back to the lane named in `ci/clickhouse/PRIMARY`.
///
/// CI and `cargo xtask integration-test` pull the pinned bytes by digest first.
/// Panics on a lane with no manifest, so a typo in the CI matrix fails the job.
fn lane_image() -> (String, String) {
    container_image(&["clickhouse"])
}

/// [`lane_image`] for a named lane, so a test can cover every one of them.
///
/// `cargo test` runs this binary's tests in one process, where `set_var` beside
/// another thread's `getenv` is undefined behaviour, so the lane is named on
/// the command line.
fn image_for_lane(lane: &str) -> (String, String) {
    container_image(&["clickhouse", lane])
}

/// Hand readiness to the caller: `.start()` returns once the container is
/// started, without waiting on any condition.
///
/// The stock condition is an *unauthenticated* `GET /` returning 200, which
/// answers before the entrypoint has necessarily applied
/// `CLICKHOUSE_PASSWORD`, and it is awaited inside `.start()`, which drops
/// the container handle on failure. Waiting ourselves buys the stronger
/// condition, and the container's exit code and output when it never answers.
fn started_only(req: impl Into<ContainerRequest<ClickHouse>>) -> ContainerRequest<ClickHouse> {
    req.into()
        .with_ready_conditions(vec![WaitFor::Nothing])
        .with_startup_timeout(START_TIMEOUT)
}

/// A single readiness probe's own bound.
///
/// The `clickhouse` client sets no request timeout, so an attempt against a
/// half-open socket can hang indefinitely. A poll loop that only checks its
/// deadline *between* attempts therefore has no deadline at all: one hung
/// attempt outlasts any budget. Every probe below is wrapped in this.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Waits until a started server answers a query as `default` with
/// `password`, and returns its URL and an admin client.
///
/// Each probe looks up the HTTP port first, so a server that exits before
/// Docker maps it fails as an exit.
///
/// # Errors
///
/// Fails once the server exits or [`spate_test::HANG`] passes, as
/// [`spate_test_support::ready_async`] does, which also writes the tail of the
/// server's output to stderr.
async fn answering(
    container: &ContainerAsync<ClickHouse>,
    password: &str,
    who: &str,
) -> Result<(String, clickhouse::Client), String> {
    let probe = move || async move {
        let url = http_url(container).await?;
        match tokio::time::timeout(
            PROBE_TIMEOUT,
            admin(&url, password).query("SELECT 1").execute(),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("no response within {PROBE_TIMEOUT:?}")),
        }
    };
    spate_test_support::ready_async(who, spate_test::HANG, probe, container).await?;
    let url = http_url(container).await?;
    let admin = admin(&url, password);
    Ok((url, admin))
}

/// The URL of `container`'s HTTP interface on the host.
async fn http_url(container: &ContainerAsync<ClickHouse>) -> Result<String, String> {
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .map_err(|e| format!("HTTP port: {e}"))?;
    Ok(format!("http://127.0.0.1:{port}"))
}

/// A client for `url` as `default` with `password`.
fn admin(url: &str, password: &str) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(url)
        .with_user("default")
        .with_password(password)
}

/// [`bare_server`] with `orders` already created.
async fn server() -> Server {
    let srv = bare_server(SERVER_PASSWORD).await;
    srv.admin
        .query(
            "CREATE TABLE orders (id UInt64, name String, amount Nullable(Float64)) \
             ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create table");
    srv
}

/// [`support::sealed_for`] under the sinks' default compression, which every
/// sink in this binary uses unless a test sets another.
fn sealed<T: Serialize>(rows: &[T], token: &str, frames: usize) -> SealedBatch {
    support::sealed_for(Compression::default(), rows, token, frames)
}

async fn sink_for(url: &str) -> config::ClickHouseSink {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: orders
shards:
  - replicas: ["{url}"]
{SERVER_CREDENTIALS}
"#
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .build::<Owned<Order>>()
        .await
        .expect("schema fetch")
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

async fn sink_with<F: ClickHouseRowFamily>(
    url: &str,
    table: &str,
    settings: &str,
) -> config::ClickHouseSink {
    try_sink_with::<F>(url, table, settings)
        .await
        .expect("schema fetch and column check")
}

/// [`sink_with`] without the unwrap, for the tests whose subject is the
/// failure.
async fn try_sink_with<F: ClickHouseRowFamily>(
    url: &str,
    table: &str,
    settings: &str,
) -> Result<config::ClickHouseSink, spate_clickhouse::SchemaError> {
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        r#"
table: {table}
shards:
  - replicas: ["{url}"]
{settings}
"#
    ))
    .expect("config yaml");
    ClickHouseSinkBuilder::new(cfg)
        .expect("valid sink config")
        .build::<F>()
        .await
}

/// Encode `rows` through a (possibly schema-checked) encoder into one
/// sealed batch, coded under the sinks' default compression, the same CPU
/// path a pipeline thread runs.
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
    Ok(support::coded_batch(
        Compression::default(),
        vec![buf.freeze()],
        n,
        token,
    ))
}

/// Encode `rows` through a [`NativeEncoder`](spate_clickhouse::NativeEncoder)
/// into one sealed batch: `encode` per row (buffering columnar), then one
/// `finish_chunk` producing exactly one Native block frame, coded under the
/// sinks' default compression.
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
    Ok(support::coded_batch(
        Compression::default(),
        vec![buf.freeze()],
        n,
        token,
    ))
}

/// A server on the lane [`lane_image`] selects, with no tables created.
///
/// `default` holds `access_management`, which the image otherwise withholds,
/// so a test can mint a restricted user and check what that user can reach.
async fn bare_server(password: &str) -> Server {
    let (name, tag) = lane_image();
    let container = started_only(
        ClickHouse::default()
            .with_name(name)
            .with_tag(tag)
            .with_env_var("CLICKHOUSE_USER", "default")
            .with_env_var("CLICKHOUSE_PASSWORD", password)
            .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"),
    )
    .start()
    .await
    .expect("start clickhouse");
    let (url, admin) = answering(&container, password, "clickhouse")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    Server {
        _container: container,
        url,
        admin,
    }
}

/// A single-shard cluster from the server's default `remote_servers`,
/// preferring `default` when the image defines it. The DDL guard only reads
/// `system.clusters`/`system.tables`, so the shard never has to be
/// reachable; any single-shard cluster is a valid fixture.
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

/// Every shipped lane names a ClickHouse server image, and the tagged half the
/// fixtures pass to testcontainers carries a non-empty tag.
#[test]
fn every_lane_resolves_to_a_clickhouse_server_image() {
    for lane in ["lts", "lts-previous", "stable"] {
        let (name, tag) = image_for_lane(lane);
        assert_eq!(name, "clickhouse/clickhouse-server", "lane {lane}");
        assert!(!tag.is_empty(), "lane {lane} has an empty tag");
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
#[path = "container/fixtures.rs"]
mod fixtures;
#[path = "container/frame_codec.rs"]
mod frame_codec;
#[path = "container/multi_table.rs"]
mod multi_table;
#[cfg(feature = "uuid")]
#[path = "container/native_e2e.rs"]
mod native_e2e;
#[path = "container/native_edges.rs"]
mod native_edges;
#[path = "container/nested_flat.rs"]
mod nested_flat;
#[path = "container/partition_dedup.rs"]
mod partition_dedup;
#[path = "container/permissions.rs"]
mod permissions;
#[path = "container/reserved_settings.rs"]
mod reserved_settings;
#[path = "container/rowbinary_header.rs"]
mod rowbinary_header;
#[path = "container/schema.rs"]
mod schema;
#[cfg(all(feature = "uuid", feature = "chrono", feature = "time"))]
#[path = "container/wide.rs"]
mod wide;
