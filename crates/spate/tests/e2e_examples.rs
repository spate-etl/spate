//! The container tier of the example suite: every example that needs
//! infrastructure, driven as the shipped binary against real servers.
//!
//! The infrastructure-free examples carry `[[example]] test = true` and run on
//! every pull request. The ones listed by `scripts/examples.sh --tiers` as
//! `infra` cannot: they need a broker, a table, or a coordination store, and
//! most of them run until SIGTERM. This binary is what runs those — one test
//! per infrastructure shape, each booting its containers once and then
//! spawning the real example binaries against them.
//!
//! Three properties make it worth the wall time:
//!
//! - The **shipped YAML is what runs.** Every endpoint in those files is an
//!   `${VAR:-default}`, so pointing an example at a container is setting
//!   environment variables, not maintaining a second copy of the config. The
//!   file a reader runs against their own servers is the file in the repository.
//! - The **drain path is exercised against real infrastructure.** An example
//!   with no stop condition is stopped the way production stops it: wait until
//!   the expected rows have landed, send `SIGTERM`, require exit status 0.
//!   Nothing else in the suite drives a shipped binary through that.
//! - **Both halves are asserted.** Exit status alone would pass on a pipeline
//!   that started, did nothing, and shut down politely, so every example is
//!   also asserted on what it produced.
//!
//! These are `#[ignore]`d and excluded from `[profile.docker]`'s default
//! filter, because they report nightly rather than per pull request:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate --all-features --locked \
//!   --run-ignored ignored-only -E 'binary(e2e_examples)' --ignore-default-filter \
//!   --test-threads 1
//! ```

#[path = "e2e_support/mod.rs"]
mod support;

use apache_avro::types::Value;
use apache_avro::{Schema, to_avro_datum};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use support::{CH_PASSWORD, Harness};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

/// How long an example may take to reach its expected output before the test
/// gives up. Generous: a cold JIT-free binary against a freshly booted broker
/// spends most of this on metadata refreshes, not on records.
const OUTPUT_DEADLINE: Duration = Duration::from_secs(180);

/// How long a drain may take after `SIGTERM`. The shipped configs set
/// `drain_timeout: 25s`, and the runtime abandons loudly past it.
const DRAIN_DEADLINE: Duration = Duration::from_secs(90);

/// Registry id the flagship example's payloads are framed under. The harness
/// stub already serves its own `Event` schema; `Order` gets one of its own.
const ORDER_SCHEMA_ID: u32 = 77;

const ORDER_SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[
    {"name":"id","type":"long"},
    {"name":"customer","type":"string"},
    {"name":"amount_cents","type":"long"},
    {"name":"ts_ms","type":"long"}]}"#;

/// Writer schema of `kafka_avro_flatmap_clickhouse.yaml`. Restated rather than
/// parsed out of the YAML: a drift here fails the test loudly (the decoder
/// rejects every payload and no row ever lands), which is the failure mode
/// worth having.
const SENSOR_SCHEMA: &str = r#"{"type":"record","name":"SensorBatch","fields":[
    {"name":"sensor","type":"string"},
    {"name":"batch_ts_ms","type":"long"},
    {"name":"events","type":{"type":"array","items":
      {"type":"record","name":"Event","fields":[
        {"name":"name","type":"string"},
        {"name":"value","type":"long"},
        {"name":"unit","type":"string"}]}}}]}"#;

/// Writer schema of `multi_table_split.yaml`; see [`SENSOR_SCHEMA`].
const METRIC_SCHEMA: &str = r#"{"type":"record","name":"MetricBatch","fields":[
    {"name":"host","type":"string"},
    {"name":"ts_ms","type":"long"},
    {"name":"readings","type":{"type":"array","items":
      {"type":"record","name":"Reading","fields":[
        {"name":"kind","type":"string"},
        {"name":"name","type":"string"},
        {"name":"value","type":"long"},
        {"name":"text","type":"string"}]}}}]}"#;

// ── Locating and running an example ────────────────────────────────────────

/// Path to a built example binary.
///
/// Cargo puts examples in `target/<profile>/examples/<name>` and this test
/// binary in `target/<profile>/deps/<test>-<hash>`, so the location is derived
/// from `current_exe` rather than guessed from a profile name. The existence
/// assertion is the point: if the runner ever stops building example targets
/// alongside test targets, this suite must fail loudly instead of quietly
/// testing nothing.
fn example_bin(name: &str) -> PathBuf {
    let mut dir = std::env::current_exe().expect("current_exe");
    dir.pop();
    if dir.file_name().is_some_and(|d| d == "deps") {
        dir.pop();
    }
    let bin = dir.join("examples").join(name);
    assert!(
        bin.is_file(),
        "example binary `{name}` is not at {}: the runner built test targets \
         without building example targets, so this suite would test nothing",
        bin.display()
    );
    bin
}

/// Copy an example's shipped YAML into `CARGO_TARGET_TMPDIR`, rebinding the
/// admin server to an ephemeral loopback port.
///
/// That rebind is the one edit. Endpoints are not touched — they interpolate
/// from the environment — but `metrics.listen` does not, and the runtime binds
/// it whatever the exporter is set to, so every example shipping the
/// `0.0.0.0:9090` default would fight for one port on the host running this.
fn render_config(name: &str) -> PathBuf {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(format!("{name}.yaml"));
    let shipped =
        std::fs::read_to_string(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
    let rendered = if shipped.contains("0.0.0.0:9090") {
        shipped.replace("0.0.0.0:9090", "127.0.0.1:0")
    } else {
        // No `listen:` key at all, so the default 0.0.0.0:9090 applies.
        shipped.replacen("metrics:\n", "metrics:\n  listen: 127.0.0.1:0\n", 1)
    };
    assert!(
        rendered.contains("127.0.0.1:0") && !rendered.contains("9090"),
        "{name}.yaml: could not rebind the admin server off the shared default port"
    );
    let dst = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}.yaml"));
    std::fs::write(&dst, rendered).expect("write rendered config");
    dst
}

/// One example under test: the child process plus where its output went.
struct Example {
    name: &'static str,
    child: Child,
    log: PathBuf,
}

/// Spawn an example with `SPATE_CONFIG` pointing at its rendered config and
/// `env` carrying the container endpoints. Output is redirected to a file
/// rather than piped: these binaries log continuously, and an unread pipe
/// buffer fills and blocks the process being measured.
fn spawn(name: &'static str, config: Option<&Path>, env: &[(&str, String)]) -> Example {
    let log = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}.log"));
    let out = std::fs::File::create(&log).expect("create example log");
    let errs = out.try_clone().expect("clone example log handle");
    let mut cmd = Command::new(example_bin(name));
    if let Some(config) = config {
        cmd.env("SPATE_CONFIG", config);
    }
    for (key, value) in env {
        cmd.env(key, value);
    }
    let child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(errs))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn example `{name}`: {e}"));
    Example { name, child, log }
}

impl Example {
    /// The example's captured stdout and stderr so far.
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Poll `cond` until it holds, failing early — with the log — if the
    /// example exits first. A pipeline that dies on a bad table or an
    /// unreachable broker should report that, not time out in silence.
    fn wait_for(&mut self, what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = Instant::now() + OUTPUT_DEADLINE;
        loop {
            if cond() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                panic!(
                    "{}: exited {status} before {what}\n--- log ---\n{}",
                    self.name,
                    self.log()
                );
            }
            assert!(
                Instant::now() < deadline,
                "{}: timed out waiting for {what}\n--- log ---\n{}",
                self.name,
                self.log()
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Wait for an example that stops on its own, and require a clean exit.
    fn wait_exit(mut self, within: Duration) -> String {
        self.reap("exit", within)
    }

    /// Stop an example the way production does — `SIGTERM`, then drain — and
    /// require a clean exit. `ExitReport::exit_code` is 0 only for
    /// `ExitState::Completed`, which for a drain additionally means every
    /// batch was acknowledged and the final watermark commit persisted.
    fn terminate(mut self) -> String {
        let pid = self.child.id().to_string();
        let killed = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("run kill");
        assert!(killed.success(), "{}: SIGTERM to {pid} failed", self.name);
        self.reap("drain after SIGTERM", DRAIN_DEADLINE)
    }

    fn reap(&mut self, what: &str, within: Duration) -> String {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                let log = self.log();
                assert!(
                    status.success(),
                    "{}: {what} exited {status}\n--- log ---\n{log}",
                    self.name
                );
                return log;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "{}: {what} did not finish within {within:?}\n--- log ---\n{}",
                    self.name,
                    self.log()
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

// ── Fixtures ───────────────────────────────────────────────────────────────

/// Run a DDL statement against the harness's ClickHouse.
fn ddl(h: &Harness, sql: &str) {
    h.rt.block_on(h.ch_client().query(sql).execute())
        .unwrap_or_else(|e| panic!("DDL failed: {e}\n{sql}"));
}

/// The endpoint environment every ClickHouse-bound example reads. The shipped
/// configs default `CLICKHOUSE_PASSWORD` to empty; the harness image sets one.
fn ch_env(h: &Harness) -> Vec<(&'static str, String)> {
    vec![
        ("KAFKA_BROKERS", h.brokers.clone()),
        ("SCHEMA_REGISTRY_URL", h.registry_url.clone()),
        ("CLICKHOUSE_URL", h.ch_url.clone()),
        ("CLICKHOUSE_PASSWORD", CH_PASSWORD.to_string()),
    ]
}

/// Produce keyed payloads to `topic`, letting the client's default partitioner
/// place them — an example that routes on the key sees the placement a real
/// producer would give it.
fn produce_raw(brokers: &str, topic: &str, payloads: &[(Vec<u8>, Vec<u8>)]) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("producer");
    for (key, payload) in payloads {
        loop {
            let record = BaseRecord::to(topic).payload(payload).key(key);
            if producer.send(record).is_ok() {
                break;
            }
            producer.poll(Duration::from_millis(50));
        }
        producer.poll(Duration::ZERO);
    }
    producer.flush(Duration::from_secs(30)).expect("flush");
}

/// A Confluent frame: magic byte, big-endian schema id, then the datum.
fn confluent(id: u32, datum: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + datum.len());
    frame.push(0u8);
    frame.extend_from_slice(&id.to_be_bytes());
    frame.extend_from_slice(datum);
    frame
}

fn encode(schema: &Schema, fields: Vec<(&str, Value)>) -> Vec<u8> {
    let record = Value::Record(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    );
    to_avro_datum(schema, record).expect("avro datum")
}

/// Drain a topic from the beginning with a throwaway group and count what is
/// on it. Ten consecutive empty polls end the drain: the first few cover the
/// wait for an assignment, and a gap after that means the topic is exhausted.
fn topic_count(brokers: &str, topic: &str) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("probe-{topic}-{}", std::process::id()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("probe consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let (mut seen, mut idle) = (0usize, 0u32);
    while idle < 10 {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(_)) => {
                seen += 1;
                idle = 0;
            }
            _ => idle += 1,
        }
    }
    seen
}

// ── Kafka + schema registry + ClickHouse ───────────────────────────────────

/// The three `full`-feature examples that read Kafka and write ClickHouse.
/// One container set drives all three: they use disjoint topics and tables,
/// and booting Kafka and ClickHouse once instead of three times is most of
/// this test's wall clock.
#[test]
#[ignore = "requires Docker"]
fn kafka_to_clickhouse_examples_deliver_and_drain() {
    let h = Harness::up();
    let env = ch_env(&h);

    // ── kafka_avro_to_clickhouse: Confluent Avro → RowBinary ───────────────
    h.register_schema(ORDER_SCHEMA_ID, ORDER_SCHEMA);
    let order_schema = Schema::parse_str(ORDER_SCHEMA).expect("order schema");
    ddl(
        &h,
        "CREATE TABLE orders (id UInt64, customer String, amount_cents Int64, ts_ms Int64) \
         ENGINE = MergeTree ORDER BY id \
         SETTINGS non_replicated_deduplication_window = 100",
    );
    h.create_topic("orders", 2);
    let orders: i64 = 500;
    let payloads: Vec<(Vec<u8>, Vec<u8>)> = (0..orders)
        .map(|i| {
            let datum = encode(
                &order_schema,
                vec![
                    ("id", Value::Long(i)),
                    ("customer", Value::String(format!("cust-{}", i % 7))),
                    ("amount_cents", Value::Long(i * 10)),
                    ("ts_ms", Value::Long(1_700_000_000_000 + i)),
                ],
            );
            (
                format!("cust-{}", i % 7).into_bytes(),
                confluent(ORDER_SCHEMA_ID, &datum),
            )
        })
        .collect();
    produce_raw(&h.brokers, "orders", &payloads);

    let config = render_config("kafka_avro_to_clickhouse");
    let mut example = spawn("kafka_avro_to_clickhouse", Some(&config), &env);
    example.wait_for("every order row", || {
        h.count("orders") >= u64::try_from(orders).expect("orders")
    });
    example.terminate();
    assert_eq!(
        h.uniq("orders"),
        u64::try_from(orders).expect("orders"),
        "every produced order landed exactly once"
    );

    // ── kafka_avro_flatmap_clickhouse: raw Avro → flat_map → Native ────────
    let sensor_schema = Schema::parse_str(SENSOR_SCHEMA).expect("sensor schema");
    ddl(
        &h,
        "CREATE TABLE sensor_events (\
             sensor LowCardinality(String), batch_ts_ms DateTime64(3), \
             name LowCardinality(String), value Int64, unit LowCardinality(String)) \
         ENGINE = MergeTree ORDER BY (sensor, batch_ts_ms)",
    );
    h.create_topic("sensor-batches", 2);
    let batches: i64 = 100;
    let per_batch: i64 = 5;
    let payloads: Vec<(Vec<u8>, Vec<u8>)> = (0..batches)
        .map(|i| {
            let events: Vec<Value> = (0..per_batch)
                .map(|e| {
                    Value::Record(vec![
                        ("name".into(), Value::String(format!("m{e}"))),
                        ("value".into(), Value::Long(i * 10 + e)),
                        ("unit".into(), Value::String("c".into())),
                    ])
                })
                .collect();
            let datum = encode(
                &sensor_schema,
                vec![
                    ("sensor", Value::String(format!("sensor-{}", i % 4))),
                    ("batch_ts_ms", Value::Long(1_700_000_000_000 + i)),
                    ("events", Value::Array(events)),
                ],
            );
            (format!("sensor-{}", i % 4).into_bytes(), datum)
        })
        .collect();
    produce_raw(&h.brokers, "sensor-batches", &payloads);

    let rows = u64::try_from(batches * per_batch).expect("sensor rows");
    let config = render_config("kafka_avro_flatmap_clickhouse");
    let mut example = spawn("kafka_avro_flatmap_clickhouse", Some(&config), &env);
    example.wait_for("every exploded sensor row", || {
        h.count("sensor_events") >= rows
    });
    example.terminate();

    // ── multi_table_split: one stream, two tables ──────────────────────────
    let metric_schema = Schema::parse_str(METRIC_SCHEMA).expect("metric schema");
    for (table, last) in [
        ("metrics_gauge", "value Int64"),
        ("metrics_text", "text String"),
    ] {
        ddl(
            &h,
            &format!(
                "CREATE TABLE {table} (\
                     host LowCardinality(String), ts_ms DateTime64(3), \
                     name LowCardinality(String), {last}) \
                 ENGINE = MergeTree ORDER BY (host, name, ts_ms)"
            ),
        );
    }
    h.create_topic("metric-batches", 2);
    let payloads: Vec<(Vec<u8>, Vec<u8>)> = (0..batches)
        .map(|i| {
            // One gauge and one text reading per batch, plus one kind that
            // matches no branch and must follow the `unmatched` policy.
            let readings: Vec<Value> = ["gauge", "text", "histogram"]
                .iter()
                .map(|kind| {
                    Value::Record(vec![
                        ("kind".into(), Value::String((*kind).to_string())),
                        ("name".into(), Value::String(format!("m{i}"))),
                        ("value".into(), Value::Long(i)),
                        ("text".into(), Value::String(format!("t{i}"))),
                    ])
                })
                .collect();
            let datum = encode(
                &metric_schema,
                vec![
                    ("host", Value::String(format!("host-{}", i % 3))),
                    ("ts_ms", Value::Long(1_700_000_000_000 + i)),
                    ("readings", Value::Array(readings)),
                ],
            );
            (format!("host-{}", i % 3).into_bytes(), datum)
        })
        .collect();
    produce_raw(&h.brokers, "metric-batches", &payloads);

    let per_table = u64::try_from(batches).expect("metric rows");
    let config = render_config("multi_table_split");
    let mut example = spawn("multi_table_split", Some(&config), &env);
    example.wait_for("both split branches", || {
        h.count("metrics_gauge") >= per_table && h.count("metrics_text") >= per_table
    });
    example.terminate();
    assert_eq!(
        h.count("metrics_gauge"),
        per_table,
        "the gauge branch took the gauge readings and nothing else"
    );
    assert_eq!(
        h.count("metrics_text"),
        per_table,
        "the text branch took the text readings and nothing else"
    );
}

// ── Kafka only ─────────────────────────────────────────────────────────────

/// `kafka_to_kafka_split`: one region-prefixed stream fanned out to a topic
/// per region, with an unroutable prefix exercising the `unmatched` policy.
#[test]
#[ignore = "requires Docker"]
fn kafka_to_kafka_split_example_fans_out_and_drains() {
    // The shared harness boots ClickHouse too; this example does not use it.
    // Reusing one container fixture is worth more than the boot it saves.
    let h = Harness::up();
    for topic in ["orders", "orders-eu", "orders-us"] {
        h.create_topic(topic, 2);
    }
    let per_region: usize = 200;
    let mut payloads = Vec::new();
    for i in 0..per_region {
        for region in ["eu", "us", "apac"] {
            payloads.push((
                format!("k{i}").into_bytes(),
                format!("{region}:cust-{}:order-{i}", i % 5).into_bytes(),
            ));
        }
    }
    produce_raw(&h.brokers, "orders", &payloads);

    let config = render_config("kafka_to_kafka_split");
    let env = vec![("KAFKA_BROKERS", h.brokers.clone())];
    let mut example = spawn("kafka_to_kafka_split", Some(&config), &env);
    example.wait_for("both region topics", || {
        topic_count(&h.brokers, "orders-eu") >= per_region
            && topic_count(&h.brokers, "orders-us") >= per_region
    });
    example.terminate();
    // `apac` matched no branch, so it went nowhere: had it been routed, a
    // region topic would hold half again as much as was produced for it.
    assert_eq!(
        topic_count(&h.brokers, "orders-eu"),
        per_region,
        "the eu topic holds the eu records and nothing else"
    );
}

// ── ClickHouse only ────────────────────────────────────────────────────────

/// `clickhouse_aggregating_mv`: rows into a `Null` landing table, aggregate
/// states built by the materialized view. This example feeds a fixed set of
/// events and stops itself, so there is no SIGTERM to send — only a clean exit
/// and the states to read back.
#[test]
#[ignore = "requires Docker"]
fn clickhouse_aggregating_mv_example_builds_states() {
    let h = Harness::up();
    ddl(
        &h,
        "CREATE TABLE events_agg (\
             bucket String, \
             dt_min AggregateFunction(min, DateTime), \
             dt_max AggregateFunction(max, DateTime), \
             counts AggregateFunction(sumMap, Map(String, UInt64))) \
         ENGINE = AggregatingMergeTree ORDER BY bucket \
         SETTINGS non_replicated_deduplication_window = 100",
    );
    ddl(
        &h,
        "CREATE TABLE events_null (bucket String, dt DateTime, counts Map(String, UInt64)) \
         ENGINE = Null",
    );
    ddl(
        &h,
        "CREATE MATERIALIZED VIEW events_mv TO events_agg AS \
         SELECT bucket, minState(dt) AS dt_min, maxState(dt) AS dt_max, \
                sumMapState(counts) AS counts \
         FROM events_null GROUP BY bucket",
    );

    let config = render_config("clickhouse_aggregating_mv");
    let env = vec![
        ("CLICKHOUSE_URL", h.ch_url.clone()),
        ("CLICKHOUSE_PASSWORD", CH_PASSWORD.to_string()),
    ];
    spawn("clickhouse_aggregating_mv", Some(&config), &env).wait_exit(OUTPUT_DEADLINE);

    // The view aggregated the five demo events into their two buckets, and
    // the states finalize to the values those events carry.
    assert_eq!(
        h.scalar("SELECT uniqExact(bucket) FROM events_agg"),
        2,
        "one aggregate row per bucket"
    );
    assert_eq!(
        h.scalar(
            "SELECT toUInt64(maxMerge(dt_max)) FROM events_agg WHERE bucket = 'a' GROUP BY bucket"
        ),
        2000,
        "the max state over bucket a's three events"
    );
}

// ── NATS JetStream ─────────────────────────────────────────────────────────

/// `nats_coordinated_backfill`: a bounded backfill divided over the durable
/// coordination store. A single instance is assigned every split and exits
/// `Completed` once they are all done, so this one also stops on its own.
#[test]
#[ignore = "requires Docker"]
fn nats_coordinated_backfill_example_covers_the_prefix() {
    // The store version floor is 2.11: the coordinator refuses anything older
    // at startup, so the tag is part of what this test asserts.
    let nats: Container<GenericImage> = GenericImage::new("nats", "2.11-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS (is Docker running? first run pulls nats:2.11-alpine)");
    let port = nats.get_host_port_ipv4(4222).expect("nats client port");

    // The example builds its config in code and stages its own objects, so it
    // needs no `SPATE_CONFIG`; the environment supplies the store and the
    // instance identity a deployment would.
    let env = vec![
        ("NATS_URL", format!("nats://127.0.0.1:{port}")),
        ("POD_NAME", "worker-e2e".to_string()),
    ];
    // The example paces itself at 2ms a record over 96 objects of 250, so a
    // single instance spends about a minute on the backfill by design.
    let log = spawn("nats_coordinated_backfill", None, &env).wait_exit(Duration::from_secs(300));

    // The example prints its own share. One instance holds every split, so
    // its share is the whole prefix — which is the at-least-once coverage
    // claim the example makes, asserted rather than read.
    assert!(
        log.contains("covering 96 of 96 objects"),
        "the sole instance covered the whole prefix\n--- log ---\n{log}"
    );
}
