//! The container tier of the example suite: every example that needs
//! infrastructure, driven as the shipped binary against real servers.
//!
//! The infrastructure-free examples carry `[[example]] test = true` and run on
//! every pull request. The ones whose stanza carries no `test = true` cannot:
//! they need a broker, a table, or a coordination store, and most of them run
//! until SIGTERM. This binary is what runs those — one test per infrastructure
//! shape, each booting its containers once and then spawning the real example
//! binaries against them.
//!
//! What every test here holds:
//!
//! - **The shipped YAML runs unmodified.** Every endpoint in those files is an
//!   `${VAR:-default}`, so an example is pointed at a container by setting
//!   environment variables; the admin listen address is the only rewrite.
//! - **An example with no stop condition is stopped by `SIGTERM`**: wait until
//!   the expected output has landed, signal, and require exit status 0 within
//!   the drain deadline.
//! - **Exit status is never the whole assertion.** Every example is asserted on
//!   what it produced, and every example stopped by `SIGTERM` is additionally
//!   asserted on the watermark its drain committed — exit status 0 does not
//!   imply one on a signal-initiated shutdown; see `Example::terminate`.
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
/// gives up. Sized for a cold start against a freshly booted broker, where
/// metadata refreshes dominate the wait rather than the records themselves.
const OUTPUT_DEADLINE: Duration = Duration::from_secs(180);

/// How long a drain may take after `SIGTERM`. The shipped configs set
/// `drain_timeout: 25s`, and the runtime abandons loudly past it.
const DRAIN_DEADLINE: Duration = Duration::from_secs(90);

/// Registry id the flagship example's payloads are framed under. The harness
/// stub already serves its own `Event` schema; `OrderPlaced` gets one of its
/// own.
const ORDER_SCHEMA_ID: u32 = 77;

const ORDER_SCHEMA: &str = r#"{"type":"record","name":"OrderPlaced","namespace":"spate.datagen","fields":[
    {"name":"order_id","type":"long"},
    {"name":"customer_id","type":"int"},
    {"name":"region","type":"string"},
    {"name":"placed_at","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"lines","type":{"type":"array","items":
      {"type":"record","name":"OrderLine","fields":[
        {"name":"sku","type":"string"},
        {"name":"qty","type":"int"},
        {"name":"unit_cents","type":"int"}]}}}]}"#;

/// Writer schema of `kafka_avro_flatmap_clickhouse.yaml`, restated here. It
/// must stay identical to that file's `inline:` schema, or the example's
/// decoder cannot read what this test produces.
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
/// from `current_exe`. Keep the existence assertion: if the runner ever stops
/// building example targets alongside test targets, this suite must fail
/// loudly instead of quietly testing nothing.
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
/// The listen address is the only edit. Endpoints are left alone: they
/// interpolate from the environment. Every shipped file states `admin.listen`
/// as a literal, and the ones naming a port all name the same `0.0.0.0:9090`,
/// so concurrent runs would contend for one host port. A file asking for no
/// server needs no rebinding and gets none.
fn render_config(name: &str) -> PathBuf {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(format!("{name}.yaml"));
    let shipped =
        std::fs::read_to_string(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
    let rendered = shipped.replace("0.0.0.0:9090", "127.0.0.1:0");
    // Assert the section exists before asserting what it says: matching only a
    // loopback literal would pass a file that declares no `admin:` at all and
    // then takes the shared default from somewhere else in the document.
    assert!(
        rendered.contains("admin:"),
        "{name}.yaml: declares no `admin:` section, so it would take the default port"
    );
    assert!(
        (rendered.contains("127.0.0.1:0") || rendered.contains("listen: none"))
            && !rendered.contains("9090"),
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
/// buffer fills and blocks the example under test.
///
/// `log_name` names the capture file, so one example driven twice in a test
/// keeps both runs.
fn spawn_as(
    name: &'static str,
    log_name: &str,
    config: Option<&Path>,
    env: &[(&str, String)],
) -> Example {
    let log = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{log_name}.log"));
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

/// Spawn an example, capturing its output under its own name.
fn spawn(name: &'static str, config: Option<&Path>, env: &[(&str, String)]) -> Example {
    spawn_as(name, name, config, env)
}

/// Kill any example still running when its handle goes away. `Child` does not
/// reap on drop, so without this a panicking assertion leaves the example
/// alive after testcontainers has removed the servers under it.
impl Drop for Example {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

impl Example {
    /// The example's captured stdout and stderr so far.
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Poll `cond` until it holds, failing early — with the log — if the
    /// example exits first.
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
    /// require a clean exit within [`DRAIN_DEADLINE`].
    ///
    /// Exit status 0 is `ExitState::Completed`: the drain ran without a fatal
    /// error. It does not on its own say the tail was acknowledged — the
    /// unacknowledged-tail backstop fires only for a bounded source's drained
    /// exit, and a signal-initiated drain that abandons batches at
    /// `drain_timeout` still exits 0. Callers assert what landed, and where a
    /// source keeps a watermark, what the drain committed.
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

/// Records on `topic`, summed over its partitions as `high - low`.
///
/// Read from the broker's watermarks rather than by consuming: the answer is
/// exact and immediate, an empty topic answers `0` without waiting out a quiet
/// period, and there is no consumer group whose initial rebalance delay could
/// be mistaken for an exhausted topic.
fn topic_count(brokers: &str, topic: &str) -> usize {
    let probe: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("probe-{}", std::process::id()))
        .create()
        .expect("probe consumer");
    let timeout = Duration::from_secs(10);
    let metadata = probe
        .fetch_metadata(Some(topic), timeout)
        .expect("topic metadata");
    let partitions = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map_or(0, |t| t.partitions().len());
    (0..i32::try_from(partitions).expect("partition count"))
        .map(|p| {
            let (low, high) = probe
                .fetch_watermarks(topic, p, timeout)
                .expect("partition watermarks");
            usize::try_from(high - low).expect("record count")
        })
        .sum()
}

// ── Kafka + schema registry + ClickHouse ───────────────────────────────────

/// The three `full`-feature examples that read Kafka and write ClickHouse.
/// One container set drives all three; they use disjoint topics and tables.
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
        "CREATE TABLE orders (\
             order_id UInt64, customer_id UInt32, region LowCardinality(String), \
             placed_at DateTime64(3), total_cents UInt64) \
         ENGINE = MergeTree ORDER BY order_id \
         SETTINGS non_replicated_deduplication_window = 100",
    );
    h.create_topic("orders", 2);
    let orders: i64 = 500;
    // Ten orders carrying no lines. There is nothing for the example's
    // `try_map` to total, so `ErrorPolicy::Skip` drops each one — they are
    // produced, and the row count below is what says they never landed.
    //
    // They are produced **first**, and that ordering is load-bearing: a
    // skipped record puts no row in the table, so nothing observable would
    // prove the tail had been consumed before the drain. Sitting at the head
    // of each partition, they are covered by construction once every lined
    // order behind them has landed.
    let unlined: i64 = 10;
    let order_line = |sku: &str, qty: i32, unit_cents: i32| {
        Value::Record(vec![
            ("sku".into(), Value::String(sku.to_string())),
            ("qty".into(), Value::Int(qty)),
            ("unit_cents".into(), Value::Int(unit_cents)),
        ])
    };
    let payloads: Vec<(Vec<u8>, Vec<u8>)> = (0..orders + unlined)
        .map(|i| {
            let lines = if i < unlined {
                vec![]
            } else {
                vec![
                    order_line("KBD-01", 2, 7_900),
                    order_line("MSE-01", 1, 3_500),
                ]
            };
            let datum = encode(
                &order_schema,
                vec![
                    ("order_id", Value::Long(i)),
                    (
                        "customer_id",
                        Value::Int(i32::try_from(i % 1024).expect("customer")),
                    ),
                    ("region", Value::String("eu-west".into())),
                    ("placed_at", Value::Long(1_700_000_000_000 + i)),
                    ("lines", Value::Array(lines)),
                ],
            );
            // The order id is the key, as the generator sets it: a payment and
            // a refund carry only that, so it is what colocates an order's
            // events on a shard.
            (
                i.to_string().into_bytes(),
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
    // `uniqExact` holds the at-least-once claim (INV-1) — every lined order
    // landed, and nothing was invented — and is by construction blind to a
    // replayed duplicate, which inflates `count`. It is also what says the ten
    // line-less orders were skipped rather than landed with a zero total.
    assert_eq!(
        h.scalar("SELECT uniqExact(order_id) FROM orders"),
        u64::try_from(orders).expect("orders"),
        "every lined order landed, and no line-less one did"
    );
    // The `try_map` really totalled the lines: 2 x 7900 + 1 x 3500.
    assert_eq!(
        h.scalar("SELECT uniqExact(total_cents) FROM orders"),
        1,
        "every row carries the same total, summed from its lines"
    );
    assert_eq!(
        h.scalar("SELECT any(total_cents) FROM orders"),
        19_300,
        "the total is the sum of qty x unit_cents over the order's lines"
    );
    // What the drain committed, which exit status 0 does not imply on a
    // signal-initiated shutdown: the group's watermark covers every record
    // produced — the skipped ones included — so nothing was left for a restart
    // to replay. The wait above is what makes this reachable: every lined
    // order landing means every partition was consumed past the line-less
    // ones ahead of them.
    let committed: i64 = h.committed("orders", "orders-etl", 2).into_iter().sum();
    assert_eq!(
        committed,
        orders + unlined,
        "the drain committed a watermark covering every order"
    );

    // ── kafka_avro_flatmap_clickhouse: raw Avro → flat_map → Native ────────
    let sensor_schema = Schema::parse_str(SENSOR_SCHEMA).expect("sensor schema");
    ddl(
        &h,
        // The deduplication window is what makes an exact row count assertable
        // under at-least-once (INV-1): the sink stamps a token per batch, so a
        // replayed batch is dropped by the server instead of doubling the table.
        "CREATE TABLE sensor_events (\
             sensor LowCardinality(String), batch_ts_ms DateTime64(3), \
             name LowCardinality(String), value Int64, unit LowCardinality(String)) \
         ENGINE = MergeTree ORDER BY (sensor, batch_ts_ms) \
         SETTINGS non_replicated_deduplication_window = 100",
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
    // `flat_map` exploded each batch into exactly its `per_batch` readings —
    // an under-count would have hung the wait above, an over-count lands here.
    assert_eq!(
        h.count("sensor_events"),
        rows,
        "each batch exploded into its readings and no more"
    );
    let committed: i64 = h
        .committed("sensor-batches", "sensor-events-etl", 2)
        .into_iter()
        .sum();
    assert_eq!(
        committed, batches,
        "the drain committed a watermark covering every batch"
    );

    // ── multi_table_split: one stream, two tables ──────────────────────────
    let metric_schema = Schema::parse_str(METRIC_SCHEMA).expect("metric schema");
    for (table, last) in [
        ("metrics_gauge", "value Int64"),
        ("metrics_text", "text String"),
    ] {
        ddl(
            &h,
            &format!(
                // Deduplication window: see the `sensor_events` DDL above.
                "CREATE TABLE {table} (\
                     host LowCardinality(String), ts_ms DateTime64(3), \
                     name LowCardinality(String), {last}) \
                 ENGINE = MergeTree ORDER BY (host, name, ts_ms) \
                 SETTINGS non_replicated_deduplication_window = 100"
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
    // One reading of each kind per batch, so an equality here fails if the
    // `histogram` readings — which match no branch and follow the `unmatched`
    // policy — had reached either table.
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
    // Both branches clone the source batch's ack, so the watermark advances
    // only once every table a batch's readings landed in has written.
    let committed: i64 = h
        .committed("metric-batches", "metric-split-etl", 2)
        .into_iter()
        .sum();
    assert_eq!(
        committed, batches,
        "the drain committed a watermark covering every batch"
    );
}

// ── Kafka only ─────────────────────────────────────────────────────────────

/// `kafka_to_kafka_split`: one region-prefixed stream fanned out to a topic
/// per region, with an unroutable prefix exercising the `unmatched` policy.
#[test]
#[ignore = "requires Docker"]
fn kafka_to_kafka_split_example_fans_out_and_drains() {
    // The shared harness boots ClickHouse too; this example does not use it.
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
    // Equal thirds were produced, one third of them unroutable. Both
    // equalities fail if the `apac` records — which match no branch and follow
    // the `unmatched` policy — had reached either destination topic.
    assert_eq!(
        topic_count(&h.brokers, "orders-eu"),
        per_region,
        "the eu topic holds the eu records and nothing else"
    );
    assert_eq!(
        topic_count(&h.brokers, "orders-us"),
        per_region,
        "the us topic holds the us records and nothing else"
    );
    // Delivery reports for every derived record are what let the watermark
    // advance, so a committed offset per source record is the drain's receipt.
    let committed: i64 = h
        .committed("orders", "orders-split-etl", 2)
        .into_iter()
        .sum();
    assert_eq!(
        committed,
        i64::try_from(payloads.len()).expect("produced"),
        "the drain committed a watermark covering every source record"
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
    assert_eq!(
        h.scalar(
            "SELECT toUInt64(minMerge(dt_min)) FROM events_agg WHERE bucket = 'a' GROUP BY bucket"
        ),
        1000,
        "the min state over bucket a's three events"
    );
    // Bucket a's counts are x=1,y=2 then x=2,z=3 then y=1, so the summed map
    // is x=3,y=3,z=3. Reading the values back through the state is what holds
    // the sink's `Map(String, UInt64)` encoding: `validate_schema: names`
    // checks column names, never the column's type.
    assert_eq!(
        h.scalar(
            "SELECT toUInt64(arraySum(mapValues(sumMapMerge(counts)))) FROM events_agg \
             WHERE bucket = 'a' GROUP BY bucket"
        ),
        9,
        "the summed map over bucket a's three events"
    );
}

// ── NATS JetStream ─────────────────────────────────────────────────────────

/// `nats_coordinated_backfill`: a bounded backfill divided over the durable
/// coordination store. A single instance is assigned every split and exits
/// `Completed` once they are all done, so this one also stops on its own.
///
/// Coverage on its own is not a coordination result — an `S3Source` with no
/// coordinator injected reads the whole prefix too, over an in-process store
/// that dies with it. What separates the two is durability, so the store is
/// what this asserts: the first run must not fall back to the solo store, and
/// a second run against the same NATS must find the job already finished.
#[test]
#[ignore = "requires Docker"]
fn nats_coordinated_backfill_example_covers_the_prefix() {
    // The store version floor is 2.11 — the coordinator refuses anything older
    // at startup, so this tag cannot be lowered.
    let nats: Container<GenericImage> = GenericImage::new("nats", "2.11-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS (is Docker running? first run pulls nats:2.11-alpine)");
    let port = nats.get_host_port_ipv4(4222).expect("nats client port");
    let url = format!("nats://127.0.0.1:{port}");

    // The example builds its config in code and stages its own objects, so it
    // needs no `SPATE_CONFIG`; the environment supplies the store and the
    // instance identity a deployment would.
    let env = vec![
        ("NATS_URL", url.clone()),
        ("POD_NAME", "worker-e2e-a".to_string()),
    ];
    // The example paces itself at 2ms a record over 96 objects of 250, so a
    // single instance spends about a minute on the backfill by design.
    let first = spawn_as(
        "nats_coordinated_backfill",
        "nats_coordinated_backfill.1",
        None,
        &env,
    )
    .wait_exit(Duration::from_secs(300));

    // The source logs this WARN at open whenever no coordinator was injected
    // and it built the in-process one instead. Its absence is what says the
    // NATS store is the store this run used.
    assert!(
        !first.contains("no coordinator injected"),
        "the example ran against the durable store, not the solo fallback\n--- log ---\n{first}"
    );
    // The example prints its own share. One instance holds every split, so its
    // share is the whole prefix, and every record of every object.
    assert!(
        first.contains("24000 records, covering 96 of 96 objects"),
        "the sole instance covered the whole prefix\n--- log ---\n{first}"
    );

    // Second instance, second identity, same job and same NATS. The split
    // records the first run committed are durable, so this one finds every
    // split complete, holds none, reads nothing, and still exits 0. A run
    // over the solo in-process store cannot reach this state: its store went
    // with the process, so it would list the prefix and cover it again.
    let env = vec![("NATS_URL", url), ("POD_NAME", "worker-e2e-b".to_string())];
    let second = spawn_as(
        "nats_coordinated_backfill",
        "nats_coordinated_backfill.2",
        None,
        &env,
    )
    .wait_exit(Duration::from_secs(120));
    assert!(
        second.contains("0 records, covering 0 of 96 objects"),
        "a finished job stays finished: the second instance read nothing\n--- log ---\n{second}"
    );
}
