//! Ad-hoc repro harness: broker instability against the FULL pipeline
//! runtime (controller + drivers + split capture sinks), 3-broker
//! MockCluster.
//!
//! Observes, per second: connected brokers (`*_broker_up`), group health,
//! `spate_checkpoint_pending_batches`, commit counters, and the group's
//! committed offsets from a probe consumer.
//!
//! Run one scenario per process (nextest does this by default):
//!
//! ```sh
//! cargo nextest run -p etl-kafka --test broker_instability --no-capture --locked
//! ```

use spate_core::config::PipelineConfig;
use spate_core::deser::Owned;
use spate_core::error::ErrorPolicy;
use spate_core::ops::{ChunkConfig, chain_owned};
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::sink::KeyHashRouter;
use spate_kafka::{KafkaSource, KafkaSourceConfig};
use spate_test::{BytesPassthrough, TestEncoder, capture_sink};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const TOPIC: &str = "orders";
const PARTITIONS: i32 = 6;
const BROKER_COUNT: i32 = 3;
/// Deliberately small so a breach is observable quickly; the production
/// report had 8192 breached by 500x.
const MAX_PENDING: usize = 256;

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,librdkafka=info".into());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_ansi(false)
        .try_init();
}

fn source_config(brokers: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: TOPIC.to_string(),
        group_id: group.to_string(),
        commit_interval: Duration::from_millis(250),
        startup_timeout: Duration::from_secs(30),
        statistics_interval: Duration::from_millis(250),
        rdkafka: BTreeMap::from([
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            // Floors mirrored from the committed mock-cluster tests: the mock
            // broker paces established-group rebalances at session.timeout-1s.
            ("session.timeout.ms".to_string(), "6000".to_string()),
            ("heartbeat.interval.ms".to_string(), "2000".to_string()),
        ]),
    }
}

fn pipeline_yaml(name: &str, port: u16, brokers: &str, group: &str) -> String {
    format!(
        r#"
pipeline: {{ name: {name}, threads: 2, io_threads: 1 }}
checkpoint: {{ interval: 250ms, max_pending_batches: {MAX_PENDING}, drain_timeout: 5s, stalled_fail_after: 600s }}
admin: {{ listen: "127.0.0.1:{port}" }}
source: {{ kafka: {{ brokers: "{brokers}", topic: {TOPIC}, group_id: {group} }} }}
sinks:
  s0: {{ capture: {{}} }}
  s1: {{ capture: {{}} }}
  s2: {{ capture: {{}} }}
  s3: {{ capture: {{}} }}
"#
    )
}

struct Rig {
    cluster: MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
    brokers: String,
    group: String,
    admin: SocketAddr,
    shutdown: spate_core::pipeline::ShutdownHandle,
    scripts: Vec<spate_test::SinkScript>,
    pipeline: Option<spate_test::PipelineRun>,
    producer_stop: Arc<AtomicBool>,
    producer_join: Option<std::thread::JoinHandle<()>>,
    produced: Arc<AtomicU64>,
    probe: BaseConsumer,
}

impl Rig {
    /// Mock cluster + full runtime + continuous background producer.
    fn start(name: &'static str, port: u16, rate_per_sec: u64) -> Rig {
        init_tracing();
        let cluster = MockCluster::new(BROKER_COUNT).expect("mock cluster");
        cluster
            .create_topic(TOPIC, PARTITIONS, BROKER_COUNT)
            .expect("create topic");
        let brokers = cluster.bootstrap_servers();
        let group = format!("{name}-group");

        // Continuous producer: `rate_per_sec` small records, round-robin
        // over partitions; keeps trying through outages (like real
        // upstream producers would).
        let producer_stop = Arc::new(AtomicBool::new(false));
        let produced = Arc::new(AtomicU64::new(0));
        let producer_join = {
            let brokers = brokers.clone();
            let stop = Arc::clone(&producer_stop);
            let produced = Arc::clone(&produced);
            std::thread::Builder::new()
                .name("repro-producer".into())
                .spawn(move || {
                    let producer: BaseProducer = ClientConfig::new()
                        .set("bootstrap.servers", &brokers)
                        .set("message.timeout.ms", "30000")
                        .create()
                        .expect("producer");
                    let mut seq: u64 = 0;
                    let batch = (rate_per_sec / 20).max(1); // 50ms cadence
                    while !stop.load(Ordering::Relaxed) {
                        for _ in 0..batch {
                            let payload = format!("{seq:010}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                            let key = format!("k{seq}");
                            let partition = i32::try_from(seq % PARTITIONS as u64).unwrap();
                            match producer.send(
                                BaseRecord::to(TOPIC)
                                    .partition(partition)
                                    .payload(payload.as_bytes())
                                    .key(key.as_bytes()),
                            ) {
                                Ok(()) => {
                                    seq += 1;
                                    produced.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    // Local queue full (outage): let it drain.
                                    producer.poll(Duration::from_millis(50));
                                }
                            }
                        }
                        producer.poll(Duration::ZERO);
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    let _ = producer.flush(Duration::from_secs(5));
                })
                .expect("spawn producer")
        };

        // Full runtime: kafka source -> passthrough chain -> 4-way split
        // with a skewed route (s3 sees ~1% of rows: the low-volume branch
        // shape from the field report).
        let yaml = pipeline_yaml(name, port, &brokers, &group);
        let config = PipelineConfig::from_str(&yaml).expect("pipeline config");
        let admin: SocketAddr = config.admin.listen.expect("admin listen");
        let source = KafkaSource::new(source_config(&brokers, &group));
        let (sink0, script0) = capture_sink(1, 1);
        let (sink1, script1) = capture_sink(1, 1);
        let (sink2, script2) = capture_sink(1, 1);
        let (sink3, script3) = capture_sink(1, 1);
        let scripts = vec![script0, script1, script2, script3];

        let runtime = Pipeline::from_config(config)
            .expect("builder")
            .add_sink("s0", sink0)
            .expect("s0")
            .add_sink("s1", sink1)
            .expect("s1")
            .add_sink("s2", sink2)
            .expect("s2")
            .add_sink("s3", sink3)
            .expect("s3")
            .chains(|ctx| {
                let mut split = chain_owned::<Vec<u8>, _>(BytesPassthrough)
                    .with_metrics(ctx.pipeline.clone(), "main")
                    .split(ErrorPolicy::Skip);
                let b0 =
                    split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("s0"));
                let b1 =
                    split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("s1"));
                let b2 =
                    split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("s2"));
                let b3 =
                    split.add::<Owned<Vec<u8>>, _, _>(TestEncoder, KeyHashRouter, ctx.sink("s3"));
                split
                    .route(move |row: Vec<u8>, out| {
                        let seq: u64 = std::str::from_utf8(&row[..10])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        if seq % 97 == 0 {
                            out.emit(b3, row); // ~1%: the low-volume branch
                        } else if seq % 7 == 0 {
                            out.emit(b2, row);
                        } else if seq % 2 == 0 {
                            out.emit(b1, row);
                        } else {
                            out.emit(b0, row);
                        }
                    })
                    .build()
            })
            .runtime_options(RuntimeOptions {
                handle_signals: false,
                ..RuntimeOptions::default()
            })
            .into_runtime(source)
            .expect("into_runtime");

        let shutdown = runtime.shutdown_handle();
        let pipeline = spate_test::PipelineRun::spawn(move || runtime.run());

        let probe: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .create()
            .expect("probe consumer");

        Rig {
            cluster,
            brokers,
            group,
            admin,
            shutdown,
            scripts,
            pipeline: Some(pipeline),
            producer_stop,
            producer_join: Some(producer_join),
            produced,
            probe,
        }
    }

    fn sample(&self) -> Sample {
        let body = http_get(self.admin, "/metrics").1;
        Sample {
            brokers_up: metric_values(&body, "broker_up{").into_iter().sum::<f64>() as i64,
            group_healthy: metric_values(&body, "group_healthy{")
                .into_iter()
                .next_back()
                .unwrap_or(-1.0) as i64,
            pending: metric_values(&body, "spate_checkpoint_pending_batches{")
                .into_iter()
                .fold(0.0, f64::max) as i64,
            commits_total: metric_values(&body, "spate_checkpoint_commits_total{")
                .into_iter()
                .sum::<f64>() as i64,
            watermark_age_s: metric_values(&body, "spate_checkpoint_watermark_age_seconds{")
                .into_iter()
                .fold(0.0, f64::max),
            lag: metric_values(&body, "spate_source_lag_records{")
                .into_iter()
                .sum::<f64>() as i64,
            produced: self.produced.load(Ordering::Relaxed),
            committed: self.committed_sum(),
        }
    }

    /// Sum of committed offsets across partitions, from the group's
    /// coordinator. `None` while the coordinator is unreachable.
    fn committed_sum(&self) -> Option<i64> {
        let mut tpl = rdkafka::TopicPartitionList::new();
        for p in 0..PARTITIONS {
            tpl.add_partition(TOPIC, p);
        }
        let committed = self
            .probe
            .committed_offsets(tpl, Duration::from_millis(900))
            .ok()?;
        Some(
            committed
                .elements()
                .iter()
                .map(|e| match e.offset() {
                    rdkafka::Offset::Offset(o) => o,
                    _ => 0,
                })
                .sum(),
        )
    }

    /// Sample once a second for `secs`, printing a timeline row each tick.
    fn observe(&self, label: &str, secs: u64) -> Vec<Sample> {
        let mut out = Vec::new();
        for t in 0..secs {
            let s = self.sample();
            println!(
                "[{label:>18} t={t:>3}s] brokers_up={} healthy={} pending={} commits={} wm_age={:.1}s lag={} produced={} committed={}",
                s.brokers_up,
                s.group_healthy,
                s.pending,
                s.commits_total,
                s.watermark_age_s,
                s.lag,
                s.produced,
                s.committed.map_or("?".into(), |c| c.to_string()),
            );
            out.push(s);
            std::thread::sleep(Duration::from_secs(1));
        }
        out
    }

    fn stop(mut self) {
        self.producer_stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.producer_join.take() {
            let _ = j.join();
        }
        if let Some(p) = self.pipeline.take() {
            self.shutdown.trigger();
            match p.wait_exit(Duration::from_secs(30)) {
                Some(r) => println!("pipeline exit: {r:?}"),
                None => println!("pipeline did NOT exit within 30s of shutdown"),
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Sample {
    brokers_up: i64,
    group_healthy: i64,
    pending: i64,
    commits_total: i64,
    watermark_age_s: f64,
    lag: i64,
    produced: u64,
    committed: Option<i64>,
}

/// Scenario 1 — total outage, longer than the session timeout, then full
/// recovery. Expected (healthy client): reconnect to all 3 brokers, the
/// group re-forms, commits resume, pending drains under the limit.
#[test]
fn total_outage_then_full_recovery() {
    let rig = Rig::start("outage", 19941, 2_000);

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s = steady.last().expect("steady sample");
    assert_eq!(s.brokers_up, 3, "steady state must connect all brokers");
    assert!(
        s.committed.unwrap_or(0) > 0,
        "steady state must be committing"
    );

    println!("== ALL brokers down (25s > session.timeout 6s) ==");
    rig.cluster.broker_down(-1).expect("all down");
    rig.observe("outage", 25);

    println!("== ALL brokers back up; watching recovery for 90s ==");
    rig.cluster.broker_up(-1).expect("all up");
    let recovery = rig.observe("recovery", 90);

    let end = recovery.last().expect("recovery sample");
    let pre_outage_committed = s.committed.unwrap_or(0);
    let recovered_commits = end.committed.unwrap_or(0) > pre_outage_committed;
    println!(
        "\nRESULT: brokers_up={}/3 healthy={} pending={} (limit {MAX_PENDING}) committed advanced: {recovered_commits}",
        end.brokers_up, end.group_healthy, end.pending
    );
    rig.stop();

    assert_eq!(end.brokers_up, 3, "BUG: brokers never reconnected");
    assert_eq!(end.group_healthy, 1, "BUG: group never re-formed");
    assert!(recovered_commits, "BUG: commits never resumed");
    assert!(
        end.pending <= MAX_PENDING as i64,
        "BUG: pending {} exceeds max_pending_batches {MAX_PENDING}",
        end.pending
    );
}

/// Scenario 2 — only the group coordinator's broker goes down; partition
/// leaders stay reachable. Fetch continues while commits are fenced: the
/// field report's shape (lag climbs, pending grows). Expected (healthy
/// client): pending capped by the limit; commits resume once the
/// coordinator returns.
#[test]
fn coordinator_down_fetch_continues() {
    let rig = Rig::start("coord", 19942, 2_000);

    // Pin the coordinator to broker 1 and every partition leader onto
    // brokers 2/3, so downing broker 1 kills ONLY the commit path.
    rig.cluster
        .coordinator(rdkafka::mocking::MockCoordinator::Group(rig.group.clone()), 1)
        .expect("pin coordinator");
    for p in 0..PARTITIONS {
        rig.cluster
            .partition_leader(TOPIC, p, Some(2 + (p % 2)))
            .expect("pin leader");
    }

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s = steady.last().expect("steady sample");
    assert!(s.committed.unwrap_or(0) > 0, "steady state must commit");

    println!("== coordinator broker 1 down for 45s; leaders stay up ==");
    rig.cluster.broker_down(1).expect("b1 down");
    let during = rig.observe("coord-down", 45);

    println!("== broker 1 back; watching recovery for 60s ==");
    rig.cluster.broker_up(1).expect("b1 up");
    let recovery = rig.observe("recovery", 60);

    let max_pending_during = during.iter().map(|s| s.pending).max().unwrap_or(0);
    let end = recovery.last().expect("recovery sample");
    println!(
        "\nRESULT: max pending during outage={max_pending_during} (limit {MAX_PENDING}); end: brokers_up={} healthy={} pending={} committed={:?}",
        end.brokers_up, end.group_healthy, end.pending, end.committed
    );
    rig.stop();

    assert!(
        max_pending_during <= MAX_PENDING as i64,
        "BUG: pending reached {max_pending_during}, exceeding max_pending_batches {MAX_PENDING}"
    );
    assert_eq!(end.brokers_up, 3, "BUG: broker 1 never reconnected");
    assert!(
        end.committed.unwrap_or(0) > s.committed.unwrap_or(0),
        "BUG: commits never resumed after the coordinator returned"
    );
}

/// Scenario 3 — rolling restarts: each broker down 10s, one at a time,
/// two full cycles. Leaders are NOT moved (the mock does not fail over),
/// so partitions led by the downed broker stall and recover — the
/// "instability" shape rather than a clean outage.
#[test]
fn rolling_broker_restarts() {
    let rig = Rig::start("rolling", 19943, 2_000);

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s0 = steady.last().expect("steady sample");
    assert_eq!(s0.brokers_up, 3);

    let mut max_pending = 0i64;
    for cycle in 0..2 {
        for b in 1..=BROKER_COUNT {
            println!("== cycle {cycle}: broker {b} down 10s ==");
            rig.cluster.broker_down(b).expect("down");
            let during = rig.observe("restart", 10);
            rig.cluster.broker_up(b).expect("up");
            let after = rig.observe("recover", 10);
            max_pending = during
                .iter()
                .chain(after.iter())
                .map(|s| s.pending)
                .fold(max_pending, i64::max);
        }
    }

    println!("== settling for 60s ==");
    let settled = rig.observe("settle", 60);
    let end = settled.last().expect("settled sample");
    println!(
        "\nRESULT: max pending seen={max_pending} (limit {MAX_PENDING}); end: brokers_up={} healthy={} pending={} committed={:?} produced={}",
        end.brokers_up, end.group_healthy, end.pending, end.committed, end.produced
    );
    rig.stop();

    assert_eq!(end.brokers_up, 3, "BUG: not all brokers reconnected");
    assert_eq!(end.group_healthy, 1, "BUG: group unhealthy after settling");
    assert!(
        max_pending <= MAX_PENDING as i64,
        "BUG: pending reached {max_pending}, exceeding max_pending_batches {MAX_PENDING}"
    );
    assert!(
        end.pending <= MAX_PENDING as i64,
        "BUG: pending {} still over the limit after settling",
        end.pending
    );
}

/// Scenario 4 — the messy shape: rebalance churn (a second member that
/// joins and leaves repeatedly), slow sink writes (scripted latency, the
/// ClickHouse-batch analogue), and a coordinator outage in the middle.
/// Expected (healthy client): pending stays bounded by the limit through
/// the churn, and the group re-forms with commits flowing afterwards.
#[test]
fn rebalance_churn_with_slow_sinks_and_coordinator_outage() {
    let rig = Rig::start("churn", 19944, 2_000);

    // Every write on every sink takes 2ms before acking: sustained sink
    // latency so pending is a real quantity, as with a batching sink.
    for script in &rig.scripts {
        for _ in 0..500_000 {
            script.enqueue_global(spate_test::WriteOutcome::ok().after(Duration::from_millis(2)));
        }
    }
    rig.cluster
        .coordinator(rdkafka::mocking::MockCoordinator::Group(rig.group.clone()), 1)
        .expect("pin coordinator");

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s0 = steady.last().expect("steady sample");
    assert!(s0.committed.unwrap_or(0) > 0, "steady state must commit");

    // A second member that flaps: join, hold 8s, leave, 5s gap — 4 cycles.
    // Every join and every leave is a full eager rebalance for our member.
    let brokers = rig.brokers.clone();
    let group = rig.group.clone();
    let churn_stop = Arc::new(AtomicBool::new(false));
    let churn_stop2 = Arc::clone(&churn_stop);
    let churner = std::thread::spawn(move || {
        for cycle in 0..4 {
            if churn_stop2.load(Ordering::Relaxed) {
                break;
            }
            println!("== churn cycle {cycle}: second member joins ==");
            let c: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group)
                .set("auto.offset.reset", "earliest")
                .set("session.timeout.ms", "6000")
                .set("heartbeat.interval.ms", "2000")
                .create()
                .expect("churn consumer");
            c.subscribe(&[TOPIC]).expect("subscribe");
            let until = Instant::now() + Duration::from_secs(8);
            while Instant::now() < until {
                let _ = c.poll(Duration::from_millis(100));
            }
            println!("== churn cycle {cycle}: second member leaves ==");
            drop(c);
            std::thread::sleep(Duration::from_secs(5));
        }
    });

    // Coordinator outage overlapping the churn window.
    let during_churn = rig.observe("churn", 20);
    println!("== coordinator broker 1 down for 20s, churn continuing ==");
    rig.cluster.broker_down(1).expect("b1 down");
    let during_outage = rig.observe("churn+coord-down", 20);
    rig.cluster.broker_up(1).expect("b1 up");

    churn_stop.store(true, Ordering::Relaxed);
    churner.join().expect("churner");

    println!("== settling for 90s ==");
    let settled = rig.observe("settle", 90);

    let max_pending = during_churn
        .iter()
        .chain(during_outage.iter())
        .chain(settled.iter())
        .map(|s| s.pending)
        .max()
        .unwrap_or(0);
    let end = settled.last().expect("settled sample");
    println!(
        "\nRESULT: max pending seen={max_pending} (limit {MAX_PENDING}); end: brokers_up={} healthy={} pending={} committed={:?}",
        end.brokers_up, end.group_healthy, end.pending, end.committed
    );
    rig.stop();

    assert_eq!(end.brokers_up, 3, "BUG: not all brokers reconnected");
    assert_eq!(end.group_healthy, 1, "BUG: group never re-settled");
    assert!(
        end.committed.unwrap_or(0) > s0.committed.unwrap_or(0),
        "BUG: commits never resumed after churn + coordinator outage"
    );
    assert!(
        max_pending <= MAX_PENDING as i64,
        "BUG: pending reached {max_pending}, exceeding max_pending_batches {MAX_PENDING}"
    );
}

/// Scenario 5 — rebalance-protocol errors. JoinGroup requests fail while a
/// second member forces a rebalance, exercising the rebalance callback's
/// non-assign/non-revoke ("arbitrary error") arm. librdkafka's contract
/// requires `assign(NULL)` in response; the source's handler only records
/// an error intent. Once the injected errors clear, a healthy client must
/// rejoin, re-fetch, and commit again.
#[test]
fn rebalance_error_events_then_recovery() {
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

    let rig = Rig::start("proto", 19945, 1_000);

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s0 = steady.last().expect("steady sample");
    assert!(s0.committed.unwrap_or(0) > 0, "steady state must commit");

    println!("== JoinGroup errors queued; second member forces a rebalance ==");
    rig.cluster.request_errors(
        RDKafkaApiKey::JoinGroup,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED; 40],
    );
    let brokers = rig.brokers.clone();
    let group = rig.group.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let joiner = std::thread::spawn(move || {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .set("session.timeout.ms", "6000")
            .set("heartbeat.interval.ms", "2000")
            .create()
            .expect("second consumer");
        c.subscribe(&[TOPIC]).expect("subscribe");
        while !stop2.load(Ordering::Relaxed) {
            let _ = c.poll(Duration::from_millis(100));
        }
    });
    let during = rig.observe("join-errors", 30);

    println!("== errors cleared; watching recovery for 90s ==");
    rig.cluster.clear_request_errors(RDKafkaApiKey::JoinGroup);
    let recovery = rig.observe("recovery", 90);
    stop.store(true, Ordering::Relaxed);
    joiner.join().expect("joiner");

    let end = recovery.last().expect("recovery sample");
    let max_pending = during
        .iter()
        .chain(recovery.iter())
        .map(|s| s.pending)
        .max()
        .unwrap_or(0);
    println!(
        "\nRESULT: end brokers_up={} healthy={} pending={} committed={:?} (steady committed {:?}); max pending={max_pending}",
        end.brokers_up, end.group_healthy, end.pending, end.committed, s0.committed
    );
    rig.stop();

    assert_eq!(end.group_healthy, 1, "BUG: member never recovered from rebalance errors");
    assert!(
        end.committed.unwrap_or(0) > s0.committed.unwrap_or(0),
        "BUG: commits never resumed after rebalance errors cleared"
    );
    assert!(
        max_pending <= MAX_PENDING as i64,
        "BUG: pending reached {max_pending}, exceeding max_pending_batches {MAX_PENDING}"
    );
}

/// Scenario 6 — coordinator failover: the coordinator's broker goes down
/// PERMANENTLY and the coordinator moves to a surviving broker (what real
/// Kafka does when a broker dies: __consumer_offsets leadership moves).
/// Partition leaders stay on the survivors throughout, so the only thing
/// that changed is where the group lives. Expected (healthy client):
/// FindCoordinator re-discovers the new coordinator, the member rejoins,
/// and ingest + commits resume — with the dead broker still dead.
#[test]
fn coordinator_failover_to_surviving_broker() {
    let rig = Rig::start("failover", 19946, 2_000);

    rig.cluster
        .coordinator(rdkafka::mocking::MockCoordinator::Group(rig.group.clone()), 1)
        .expect("pin coordinator to broker 1");
    for p in 0..PARTITIONS {
        rig.cluster
            .partition_leader(TOPIC, p, Some(2 + (p % 2)))
            .expect("pin leader to brokers 2/3");
    }

    println!("== steady state ==");
    let steady = rig.observe("steady", 15);
    let s0 = steady.last().expect("steady sample");
    assert!(s0.committed.unwrap_or(0) > 0, "steady state must commit");

    println!("== broker 1 (coordinator) down PERMANENTLY ==");
    rig.cluster.broker_down(1).expect("b1 down");
    rig.observe("coord-dead", 10);

    println!("== coordinator moved to broker 2; b1 stays down; watching 120s ==");
    rig.cluster
        .coordinator(rdkafka::mocking::MockCoordinator::Group(rig.group.clone()), 2)
        .expect("move coordinator to broker 2");
    let recovery = rig.observe("failover", 120);

    let end = recovery.last().expect("recovery sample");
    // Ingest resumed = the committed offsets advanced well past the
    // pre-outage position while broker 1 never came back.
    println!(
        "\nRESULT: end healthy={} committed={:?} (steady committed {:?}) brokers_up={}",
        end.group_healthy, end.committed, s0.committed, end.brokers_up
    );
    rig.stop();

    assert_eq!(
        end.group_healthy, 1,
        "BUG: member never rejoined after coordinator failover"
    );
    assert!(
        end.committed.unwrap_or(0) > s0.committed.unwrap_or(0),
        "BUG: ingest/commits never resumed after the coordinator moved \
         (field symptom: reduced ingest until process restart)"
    );
}

// ── plumbing ───────────────────────────────────────────────────────────

fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5)) else {
        return (0, String::new());
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    if write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: repro\r\nConnection: close\r\n\r\n"
    )
    .is_err()
    {
        return (0, String::new());
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Values of every sample whose name+labels start with `prefix` (family
/// name up to and including `{`).
fn metric_values(body: &str, prefix: &str) -> Vec<f64> {
    body.lines()
        .filter(|l| !l.starts_with('#') && l.contains(prefix))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .collect()
}

/// Await a condition with a deadline (kept for parity with the committed
/// suite; the scenarios above sample on a fixed cadence instead).
#[allow(dead_code)]
fn wait_until(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check() {
        assert!(Instant::now() < deadline, "timeout: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}
