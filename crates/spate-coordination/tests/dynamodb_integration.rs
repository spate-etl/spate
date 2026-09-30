//! The coordination protocol over DynamoDB Local: the `DynamoDbStore`
//! mapping against the service's API, covering table creation and the
//! startup checks, conditional writes, the item a failed condition returns
//! and job isolation, by running the scenarios the in-memory suite proves.
//!
//! Ignored by default; run with Docker available:
//!
//! ```sh
//! cargo test -p spate-coordination --all-features --test dynamodb_integration -- --ignored
//! ```
#![cfg(feature = "dynamodb")]

mod support;
#[macro_use]
mod scenarios;

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, LocalSecondaryIndex, Projection,
    ProjectionType, ScalarAttributeType, TimeToLiveStatus,
};
use spate_coordination::store::dynamodb::{DynamoDbConfig, DynamoDbStore};
use spate_coordination::store::{CoordinationStore, Keyspace, StoreError};
use spate_coordination::{
    CoordinationConfig, DynamoDbCoordinator, SplitCoordinator as _, StoreCoordinator,
};
use spate_test_support::container_image;
use std::time::{Duration, Instant};
use support::{Held, PhasedPlanner, runtime};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const PORT: u16 = 8000;

/// Timing assertions scale from this.
const LEASE: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(500);
const OP_TIMEOUT: Duration = Duration::from_secs(1);
const TABLE: &str = "spate-coordination";

/// DynamoDB Local from the `ci/dynamodb/` pin, pulled by digest, and its
/// endpoint, once it answers a request.
fn start_local() -> (Container<GenericImage>, String) {
    let (name, tag) = container_image(&["--pull", "dynamodb"]);
    let container = GenericImage::new(name, tag)
        .with_exposed_port(PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Initializing DynamoDB Local"))
        .with_cmd(["-jar", "DynamoDBLocal.jar", "-inMemory"])
        .start()
        .expect("start DynamoDB Local (is Docker running? first run pulls the image)");
    let port = container.get_host_port_ipv4(PORT).expect("mapped port");
    let endpoint = format!("http://127.0.0.1:{port}");
    // The log line comes before the listener accepts.
    let (rt, client) = (runtime(), client(&endpoint));
    spate_test::wait_until(support::DEADLINE, "DynamoDB Local answers", || {
        rt.block_on(client.list_tables().send()).is_ok()
    });
    (container, endpoint)
}

fn config(endpoint: &str, table: &str, job: &str) -> DynamoDbConfig {
    let mut config = DynamoDbConfig::new(table, job);
    config.region = Some("us-east-1".into());
    config.endpoint = Some(endpoint.into());
    config.create_table = true;
    config.poll_interval = POLL;
    config
}

fn store_with(config: DynamoDbConfig) -> DynamoDbStore {
    DynamoDbStore::with_static_credentials(config, LEASE, OP_TIMEOUT, "local", "local")
        .expect("dynamodb store")
}

fn store(endpoint: &str, job: &str) -> DynamoDbStore {
    store_with(config(endpoint, TABLE, job))
}

/// A client of its own, for what the store never does to a table.
fn client(endpoint: &str) -> aws_sdk_dynamodb::Client {
    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("local", "local", None, None, "static"))
        .endpoint_url(endpoint)
        .build();
    aws_sdk_dynamodb::Client::from_conf(config)
}

fn tuning(instance_id: Option<&str>) -> CoordinationConfig {
    let mut cfg = support::config_for(LEASE, instance_id);
    cfg.op_timeout = OP_TIMEOUT;
    cfg.reconcile_interval = Duration::from_secs(1);
    cfg
}

/// `op` once it stops reporting the table as being created.
async fn settled<T>(
    what: &str,
    mut op: impl AsyncFnMut() -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        match op().await {
            Err(StoreError::Retryable(e)) if Instant::now() < deadline => {
                assert!(e.contains("being created"), "{what}: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            other => return other,
        }
    }
}

/// One DynamoDB Local per scenario; every worker has a handle of its own.
struct DynamoDbBackend {
    _local: Container<GenericImage>,
    endpoint: String,
}

impl DynamoDbBackend {
    fn start() -> DynamoDbBackend {
        let (local, endpoint) = start_local();
        DynamoDbBackend {
            _local: local,
            endpoint,
        }
    }
}

impl support::Backend for DynamoDbBackend {
    type Store = DynamoDbStore;

    fn store(&self) -> DynamoDbStore {
        store(&self.endpoint, "scenario")
    }

    fn lease(&self) -> Duration {
        LEASE
    }

    /// The suite's scaled tuning, with a store deadline for a container.
    fn config(&self, instance_id: Option<&str>) -> CoordinationConfig {
        tuning(instance_id)
    }
}

multi_worker_scenarios!(DynamoDbBackend::start(); #[ignore = "needs Docker; run explicitly"]);

#[test]
#[ignore = "needs Docker; run explicitly"]
fn the_store_contract_holds_over_dynamodb_local() {
    let (_local, endpoint) = start_local();
    let rt = runtime();
    let store = store(&endpoint, "contract");
    rt.block_on(async {
        settled("probe", async || store.get(Keyspace::Durable, "none").await)
            .await
            .expect("table ready");
        support::contract::all(&store, async |by| tokio::time::sleep(by).await).await;
    });
}

/// A missing table is Fatal without `create_table`; with it the store
/// creates the table and enables TTL on `x`, and a second store adopts the
/// table the first created.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn create_table_creates_and_adopts_the_table() {
    let (_local, endpoint) = start_local();
    let rt = runtime();
    rt.block_on(async {
        let mut refused = config(&endpoint, TABLE, "refused");
        refused.create_table = false;
        let err = store_with(refused)
            .get(Keyspace::Durable, "k")
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Fatal(_)), "{err}");
        assert!(err.to_string().contains("create_table"), "{err}");

        let first = store(&endpoint, "first");
        settled("create", async || first.get(Keyspace::Durable, "k").await)
            .await
            .expect("created");
        let ttl = client(&endpoint)
            .describe_time_to_live()
            .table_name(TABLE)
            .send()
            .await
            .expect("describe ttl");
        let ttl = ttl.time_to_live_description().expect("ttl description");
        assert_eq!(ttl.time_to_live_status(), Some(&TimeToLiveStatus::Enabled));
        assert_eq!(ttl.attribute_name(), Some("x"));

        let second = store(&endpoint, "second");
        second
            .get(Keyspace::Durable, "k")
            .await
            .expect("adopts the table on its first call");
    });
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap()
}

fn key(name: &str, kind: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(kind)
        .build()
        .unwrap()
}

/// A table with the wrong key schema, or with a local secondary index, is
/// refused at startup, naming what is wrong.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn startup_rejects_the_wrong_key_schema_and_an_lsi() {
    let (_local, endpoint) = start_local();
    let rt = runtime();
    rt.block_on(async {
        let client = client(&endpoint);
        client
            .create_table()
            .table_name("wrong-keys")
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(attr("id"))
            .key_schema(key("id", KeyType::Hash))
            .send()
            .await
            .expect("create wrong-keys");
        let index = LocalSecondaryIndex::builder()
            .index_name("by-owner")
            .key_schema(key("pk", KeyType::Hash))
            .key_schema(key("owner", KeyType::Range))
            .projection(
                Projection::builder()
                    .projection_type(ProjectionType::KeysOnly)
                    .build(),
            )
            .build()
            .unwrap();
        client
            .create_table()
            .table_name("with-lsi")
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(attr("pk"))
            .attribute_definitions(attr("sk"))
            .attribute_definitions(attr("owner"))
            .key_schema(key("pk", KeyType::Hash))
            .key_schema(key("sk", KeyType::Range))
            .local_secondary_indexes(index)
            .send()
            .await
            .expect("create with-lsi");

        for (table, expect) in [
            ("wrong-keys", "hash key `pk`"),
            ("with-lsi", "local secondary index"),
        ] {
            let err = store_with(config(&endpoint, table, "job"))
                .get(Keyspace::Durable, "k")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::Fatal(_)), "{table}: {err}");
            assert!(err.to_string().contains(expect), "{table}: {err}");
        }
    });
}

/// Two jobs on one table see nothing of each other's keys.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn a_second_job_on_one_table_is_isolated() {
    let (_local, endpoint) = start_local();
    let rt = runtime();
    rt.block_on(async {
        let a = store(&endpoint, "job-a");
        let b = store(&endpoint, "job-b");
        let created = settled("create", async || {
            a.create(Keyspace::Durable, "split.x", b"a".to_vec()).await
        })
        .await
        .expect("a creates");
        assert!(created.won().is_some());
        let leased = a
            .create(Keyspace::Ephemeral, "split.x", b"a".to_vec())
            .await
            .expect("a leases");
        assert!(leased.won().is_some());
        for ks in [Keyspace::Durable, Keyspace::Ephemeral] {
            assert_eq!(b.get(ks, "split.x").await.unwrap(), None, "{ks:?}");
            assert!(b.list(ks, "").await.unwrap().is_empty(), "{ks:?}");
            assert!(
                b.create(ks, "split.x", b"b".to_vec())
                    .await
                    .unwrap()
                    .won()
                    .is_some(),
                "{ks:?}"
            );
        }
        let held = a.get(Keyspace::Durable, "split.x").await.unwrap().unwrap();
        assert_eq!(held.value, b"a");
    });
}

fn worker(endpoint: &str, io: &tokio::runtime::Handle) -> DynamoDbCoordinator {
    StoreCoordinator::new(
        store(endpoint, "release"),
        tuning(Some("worker-a")),
        io.clone(),
        None,
    )
    .expect("coordinator")
}

/// A coordinator dropped after its io runtime stopped hands its split back
/// through the store's client, whose pooled connections that runtime drove,
/// within the release deadline.
#[test]
#[ignore = "needs Docker; run explicitly"]
fn a_drop_time_release_reaches_the_table_after_the_io_runtime_stops() {
    let (_local, endpoint) = start_local();
    let io = runtime();
    let mut w = worker(&endpoint, io.handle());
    w.start(Box::new(PhasedPlanner::one_final("release:v1", &["d0"])))
        .unwrap();
    support::drive(&mut w, &mut Held::default(), "claiming d0", |h| {
        h.splits.len() == 1
    });

    drop(io);
    let started = Instant::now();
    drop(w);
    let released_in = started.elapsed();

    let rt = runtime();
    let reader = store(&endpoint, "release");
    let (record, lease) = rt.block_on(async {
        (
            reader.get(Keyspace::Durable, "split.d0").await.unwrap(),
            reader.get(Keyspace::Ephemeral, "split.d0").await.unwrap(),
        )
    });
    let record: serde_json::Value =
        serde_json::from_slice(&record.expect("the split record").value).unwrap();
    assert!(
        record["owner"].is_null(),
        "the split was not released in {released_in:?}: {record}"
    );
    assert_eq!(lease, None, "the lease is still there");
    assert!(released_in < OP_TIMEOUT * 2, "{released_in:?}");
}
