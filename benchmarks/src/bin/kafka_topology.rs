//! Kafka consumer source-ceiling harness: how many messages per second one
//! process can pull out of a topic, and how that scales with consumer
//! instances, threads, brokers, and prefetch depth.
//!
//! Two arrangements, both using manual partition assignment (no consumer group
//! protocol) so the measurement isolates the fetch path, queue routing, and
//! threading rather than group-join dynamics:
//!
//!   MODE=perthread  N independent `BaseConsumer`s, one per thread. Each is a
//!                   separate librdkafka client, so this is the *multi-instance*
//!                   arm: N clients means N broker threads per broker and N
//!                   concurrent Fetch pipelines.
//!   MODE=split      INSTANCES consumers whose partition queues are split off
//!                   and drained by THREADS threads. With INSTANCES=1 this is
//!                   the *single-instance* arm — the shape `etl-kafka` itself
//!                   uses.
//!
//! Usage:
//!   kafka_topology produce   # fill the topic for the current config
//!   kafka_topology consume   # run one timed pass, print a JSON line
//!
//! Config via env: BOOTSTRAP, BROKER (redpanda|kafka, default redpanda —
//! recorded as a variant so the two never aggregate), PARTITIONS (16), THREADS (4), INSTANCES (1),
//! PAYLOAD (256), WORK_US (0), DURATION_S (30), MESSAGES (10_000_000, produce
//! only), MODE (perthread|split), GAP_SAMPLING (0), and the prefetch knobs
//! QUEUED_MIN_MESSAGES (0), QUEUED_MAX_KBYTES, FETCH_MAX_BYTES,
//! FETCH_MESSAGE_MAX_BYTES, FETCH_WAIT_MAX_MS.
//!
//! `QUEUED_MIN_MESSAGES=0` means **leave the property unset**, which is what
//! `etl-kafka` ships and therefore what an unqualified run should measure; any
//! other value is passed through verbatim. `e2e_kafka_clickhouse` reads the
//! same variable with the same meaning, so a sweep can drive both rigs.
//!
//! Note that `QUEUED_MAX_KBYTES` is not comparable across modes: librdkafka
//! applies `queued.max.messages.kbytes` per partition only when separate
//! partition queues are used (`MODE=split`), and to the single consumer queue
//! otherwise (`MODE=perthread`). Sweeping it across modes compares two
//! different quantities.
//!
//! Emits under `BENCH` (default `kafka_source_ceiling`). The published
//! `kafka_topology` dataset was recorded by the v1 harness, which incremented a
//! process-wide atomic per message and busy-spun in split mode; its records are
//! not comparable with these and the `harness` variant key keeps the two sets
//! from ever aggregating into one bar.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use benchmarks::report::{Metric, Report};
use benchmarks::{busy_work, docker, ensure_topic, env_str, env_u64, percentile, produce};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::{Offset, TopicPartitionList};

/// Mirrors the framework's driver defaults (`etl-core` `runtime.rs`) so the
/// harness polls on the same cadence the real pipeline does.
const MAX_RECORDS: usize = 512;
const POLL_TIMEOUT: Duration = Duration::from_millis(10);

/// Deadline is checked every this many messages rather than every message: an
/// `Instant::now()` per message is ~25 ns, which is a measurable tax at the
/// multi-million-messages/s rates this rig exists to measure.
const DEADLINE_CHECK_INTERVAL: u64 = 256;

struct Config {
    bootstrap: String,
    /// Which broker implementation served the run. Identity-defining for
    /// any throughput number, so it rides in the variant map.
    broker: String,
    partitions: i32,
    threads: usize,
    instances: usize,
    payload: usize,
    work_us: u64,
    messages: u64,
    duration: Duration,
    mode: String,
    gap_sampling: bool,
    queued_min_messages: u64,
    queued_max_kbytes: Option<u64>,
    fetch_max_bytes: Option<u64>,
    fetch_message_max_bytes: Option<u64>,
    fetch_wait_max_ms: Option<u64>,
}

fn env_opt_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

impl Config {
    fn from_env() -> Self {
        let partitions = env_u64("PARTITIONS", 16) as i32;
        assert!(partitions > 0, "PARTITIONS must be at least 1");
        let threads = env_u64("THREADS", 4) as usize;
        assert!(threads > 0, "THREADS must be at least 1");
        let instances = env_u64("INSTANCES", 1) as usize;
        assert!(instances > 0, "INSTANCES must be at least 1");
        let mode = std::env::var("MODE").unwrap_or_else(|_| "perthread".to_owned());
        // `perthread` builds one client per thread and never reads INSTANCES,
        // so accepting a value here would record a variant the run did not use
        // — two different configurations landing under one identity.
        assert!(
            mode != "perthread" || instances == 1,
            "MODE=perthread builds one client per thread and ignores INSTANCES; \
             INSTANCES={instances} would be recorded but not used"
        );
        let (bootstrap, broker) = docker::resolve_broker();
        let mut messages = env_u64("MESSAGES", 10_000_000);
        messages -= messages % partitions as u64; // even per-partition counts
        Self {
            bootstrap,
            broker,
            partitions,
            threads,
            instances,
            payload: env_u64("PAYLOAD", 256) as usize,
            work_us: env_u64("WORK_US", 0),
            messages,
            duration: Duration::from_secs(env_u64("DURATION_S", 30)),
            mode,
            gap_sampling: env_u64("GAP_SAMPLING", 0) != 0,
            // 0 means "set nothing", which is what `etl-kafka` ships: the
            // connector pins no prefetch depth, so librdkafka's own default
            // applies. Defaulting to any number here would make an
            // unqualified run measure a depth production never uses — which
            // is exactly what the v1 harness did, in the other direction.
            queued_min_messages: env_u64("QUEUED_MIN_MESSAGES", 0),
            queued_max_kbytes: env_opt_u64("QUEUED_MAX_KBYTES"),
            fetch_max_bytes: env_opt_u64("FETCH_MAX_BYTES"),
            fetch_message_max_bytes: env_opt_u64("FETCH_MESSAGE_MAX_BYTES"),
            fetch_wait_max_ms: env_opt_u64("FETCH_WAIT_MAX_MS"),
        }
    }

    fn topic(&self) -> String {
        format!("bench-p{}-b{}", self.partitions, self.payload)
    }
}

fn consumer_config(cfg: &Config) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", format!("bench-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false");
    for (key, value) in [
        // 0 = leave unset, exercising librdkafka's default — the path the
        // shipped connector takes. librdkafka's allowed range starts at 1, so
        // passing 0 through would fail client creation outright.
        (
            "queued.min.messages",
            (cfg.queued_min_messages > 0).then_some(cfg.queued_min_messages),
        ),
        ("queued.max.messages.kbytes", cfg.queued_max_kbytes),
        ("fetch.max.bytes", cfg.fetch_max_bytes),
        ("fetch.message.max.bytes", cfg.fetch_message_max_bytes),
        ("fetch.wait.max.ms", cfg.fetch_wait_max_ms),
    ] {
        if let Some(value) = value {
            config.set(key, value.to_string());
        }
    }
    config
}

/// One pollable source of messages. Both arrangements drain through the same
/// loop so the cadence cannot differ between arms by accident — the v1 harness
/// blocked 100 ms per poll in one mode and busy-spun in the other.
enum Lane {
    /// `MODE=perthread`: the consumer's own main queue.
    Consumer(Arc<BaseConsumer>),
    /// `MODE=split`: one partition queue split off a consumer.
    Queue(PartitionQueue<DefaultConsumerContext>),
}

impl Lane {
    fn poll(&self, timeout: Duration) -> Option<Result<BorrowedMessage<'_>, KafkaError>> {
        match self {
            Lane::Consumer(consumer) => consumer.poll(timeout),
            Lane::Queue(queue) => queue.poll(timeout),
        }
    }
}

/// Per-thread measurement state. Nothing here is shared, so the hot path has no
/// atomics and no cross-core traffic at all: the v1 harness did a CAS, a store
/// and a `fetch_add` on one cache line for every message on every thread, which
/// anti-scaled with thread count and made high-thread arms unusable.
#[derive(Default)]
struct ThreadStat {
    consumed: u64,
    /// When this thread's first message arrived, stamped *before* that message
    /// is counted so the numerator cannot include records from outside the
    /// window. Warm-up before the first message (connect, metadata, assign) is
    /// deliberately excluded.
    first: Option<Instant>,
    /// When this thread left the drain loop — **not** when it last saw a
    /// message. Anchoring on the last message silently deletes a trailing
    /// starvation gap from the denominator, which is precisely the interval a
    /// starved arm needs to be charged for: one committed 256 B arm read 29%
    /// faster than its siblings purely because its clock stopped 0.9 s early.
    stopped: Option<Instant>,
    gaps: Vec<u64>,
}

/// Drives one thread's lanes until `deadline`, mirroring the framework driver's
/// cadence (`etl-core` `pipeline/driver.rs`): round-robin across lanes, and
/// block for `POLL_TIMEOUT` only once a full pass over every lane came back
/// empty. While any lane is producing, polls are non-blocking, so one cold lane
/// can never park a thread that has ready data elsewhere.
fn drain(lanes: &[Lane], cfg: &Config, deadline: Instant, stop: &AtomicBool) -> ThreadStat {
    let mut stat = ThreadStat::default();
    if lanes.is_empty() {
        // Fewer partitions than threads. The v1 harness indexed `queues[0]`
        // unconditionally here and panicked.
        return stat;
    }

    let mut empty_polls = 0usize;
    let mut next_lane = 0usize;
    let mut since_check = 0u64;
    let mut last_yield = Instant::now();
    let mut sampler = 0u64;

    loop {
        if since_check >= DEADLINE_CHECK_INTERVAL {
            since_check = 0;
            if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
        }

        let lane_timeout = if empty_polls >= lanes.len() {
            POLL_TIMEOUT
        } else {
            Duration::ZERO
        };

        let lane = &lanes[next_lane % lanes.len()];
        next_lane = next_lane.wrapping_add(1);

        // One blocking wait for the first message, then a non-blocking gather
        // up to MAX_RECORDS — the same batch shape as `KafkaLane::poll`.
        let mut batch = 0usize;
        let mut timeout = lane_timeout;
        while batch < MAX_RECORDS {
            match lane.poll(timeout) {
                Some(Ok(_msg)) => {
                    if stat.first.is_none() {
                        stat.first = Some(Instant::now());
                    }
                    if cfg.work_us > 0 {
                        busy_work(cfg.work_us);
                    }
                    batch += 1;
                    if cfg.gap_sampling {
                        sampler += 1;
                        if sampler.is_multiple_of(16) {
                            stat.gaps.push(last_yield.elapsed().as_micros() as u64);
                        }
                        last_yield = Instant::now();
                    }
                    timeout = Duration::ZERO;
                }
                // A retryable error counts toward the empty pass exactly as the
                // driver does, so an erroring lane degrades to the blocking
                // cadence instead of spinning hot.
                Some(Err(_)) | None => break,
            }
        }

        if batch == 0 {
            empty_polls += 1;
            // An empty pass is cheap in messages but expensive in wall clock:
            // once every lane has come back empty each one blocks for
            // POLL_TIMEOUT, so amortising the deadline check over
            // DEADLINE_CHECK_INTERVAL passes would let an idle thread run up
            // to 2.5 s past it. The check costs nothing next to a blocking
            // poll, so take it every time.
            if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
                break;
            }
            since_check = 0;
            continue;
        }

        empty_polls = 0;
        stat.consumed += batch as u64;
        since_check += batch as u64;
    }

    // Only meaningful once something was consumed; a thread that never saw a
    // message contributes to neither edge.
    if stat.first.is_some() {
        stat.stopped = Some(Instant::now());
    }
    stat
}

/// Builds the lanes for each thread. Returns one lane list per thread plus the
/// consumers that must be kept alive (and polled for events) for the duration.
fn build_lanes(cfg: &Config) -> (Vec<Vec<Lane>>, Vec<Arc<BaseConsumer>>) {
    let topic = cfg.topic();
    let mut per_thread: Vec<Vec<Lane>> = (0..cfg.threads).map(|_| Vec::new()).collect();

    match cfg.mode.as_str() {
        // One independent client per thread. Partitions are dealt out by
        // thread, and each client polls only its own main queue.
        "perthread" => {
            let mut consumers = Vec::with_capacity(cfg.threads);
            let threads = cfg.threads;
            for (thread, lanes) in per_thread.iter_mut().enumerate() {
                let consumer: BaseConsumer = consumer_config(cfg).create().expect("consumer");
                let consumer = Arc::new(consumer);
                let mut tpl = TopicPartitionList::new();
                for partition in (0..cfg.partitions).filter(|p| *p as usize % threads == thread) {
                    tpl.add_partition_offset(&topic, partition, Offset::Beginning)
                        .expect("add partition");
                }
                consumer.assign(&tpl).expect("assign");
                lanes.push(Lane::Consumer(Arc::clone(&consumer)));
                consumers.push(consumer);
            }
            (per_thread, consumers)
        }
        // INSTANCES clients; every partition queue is split off before the
        // first poll so nothing lands on a main queue, then dealt round-robin
        // across all threads.
        "split" => {
            let mut consumers = Vec::with_capacity(cfg.instances);
            let mut lane_idx = 0usize;
            for instance in 0..cfg.instances {
                let consumer: BaseConsumer = consumer_config(cfg).create().expect("consumer");
                let consumer = Arc::new(consumer);
                let owned: Vec<i32> = (0..cfg.partitions)
                    .filter(|p| *p as usize % cfg.instances == instance)
                    .collect();
                let mut tpl = TopicPartitionList::new();
                for partition in &owned {
                    tpl.add_partition_offset(&topic, *partition, Offset::Beginning)
                        .expect("add partition");
                }
                consumer.assign(&tpl).expect("assign");
                for partition in &owned {
                    let queue = consumer
                        .split_partition_queue(&topic, *partition)
                        .expect("split partition queue");
                    per_thread[lane_idx % cfg.threads].push(Lane::Queue(queue));
                    lane_idx += 1;
                }
                consumers.push(consumer);
            }
            (per_thread, consumers)
        }
        other => {
            eprintln!("unknown MODE {other}");
            std::process::exit(2);
        }
    }
}

/// Total messages sitting in the topic, from the broker's watermarks. Used to
/// prove the run was a saturation and not a drain — consuming the whole backlog
/// measures how fast a topic empties, which is a different quantity entirely.
fn backlog(cfg: &Config) -> u64 {
    let consumer: BaseConsumer = consumer_config(cfg).create().expect("consumer");
    let topic = cfg.topic();
    (0..cfg.partitions)
        .map(|partition| {
            let (low, high) = consumer
                .fetch_watermarks(&topic, partition, Duration::from_secs(10))
                .unwrap_or_else(|e| panic!("fetch watermarks for partition {partition}: {e}"));
            (high - low).max(0) as u64
        })
        .sum()
}

fn main() {
    let cfg = Config::from_env();
    let command = std::env::args().nth(1).unwrap_or_default();
    match command.as_str() {
        "produce" => {
            ensure_topic(&cfg.bootstrap, &cfg.topic(), cfg.partitions);
            let rate = produce(
                &cfg.bootstrap,
                &cfg.topic(),
                cfg.partitions,
                cfg.messages,
                cfg.payload,
            );
            eprintln!("produced {} records at {:.0}/s", cfg.messages, rate);
        }
        "consume" => {
            let available = backlog(&cfg);
            if available == 0 {
                eprintln!(
                    "topic {} is empty — run `kafka_topology produce` first",
                    cfg.topic()
                );
                std::process::exit(2);
            }

            let (per_thread, consumers) = build_lanes(&cfg);
            let stop = AtomicBool::new(false);
            let deadline = Instant::now() + cfg.duration;

            let (stats, leaked): (Vec<ThreadStat>, u64) = std::thread::scope(|scope| {
                let handles: Vec<_> = per_thread
                    .into_iter()
                    .map(|lanes| {
                        let cfg = &cfg;
                        let stop = &stop;
                        scope.spawn(move || drain(&lanes, cfg, deadline, stop))
                    })
                    .collect();

                // Service every client's main queue for events (errors, stats)
                // while the drain threads work. In split mode the main queues
                // should never yield a message; if one does, the split
                // choreography leaked and the arm is invalid.
                let mut leaked = 0u64;
                while Instant::now() < deadline {
                    for consumer in &consumers {
                        if cfg.mode == "split" {
                            while let Some(Ok(_)) = consumer.poll(Duration::ZERO) {
                                leaked += 1;
                            }
                        }
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                stop.store(true, Ordering::Relaxed);
                if leaked > 0 {
                    eprintln!("WARNING: {leaked} messages leaked to a main queue");
                }

                let stats = handles
                    .into_iter()
                    .map(|h| h.join().expect("thread"))
                    .collect();
                (stats, leaked)
            });

            let consumed: u64 = stats.iter().map(|s| s.consumed).sum();
            let first = stats.iter().filter_map(|s| s.first).min();
            let stopped = stats.iter().filter_map(|s| s.stopped).max();
            let elapsed_s = match (first, stopped) {
                (Some(first), Some(stopped)) => stopped.duration_since(first).as_secs_f64(),
                _ => 0.0,
            };
            let records_per_s = if elapsed_s > 0.0 {
                consumed as f64 / elapsed_s
            } else {
                0.0
            };

            // A run that swallowed the whole backlog measured a drain, not a
            // ceiling. It is not a weaker datapoint, it is a different
            // quantity, so it never reaches the results file — a marker in a
            // free-text note would not stop the site aggregating it into the
            // median alongside valid arms.
            if consumed >= available {
                eprintln!(
                    "DRAINED: consumed {consumed} of a {available}-message backlog in \
                     {elapsed_s:.2}s — this measures how fast the topic empties, not a \
                     throughput ceiling. Produce a deeper backlog and rerun. Nothing emitted."
                );
                std::process::exit(3);
            }
            // Leaked messages were consumed off a main queue and discarded, so
            // they shrink the numerator while the window stays the same. Like
            // a drain, that is an invalid arm rather than a noisy one.
            if leaked > 0 {
                eprintln!(
                    "LEAKED: {leaked} messages reached a main queue and were discarded, so \
                     `consumed` under-counts this arm. Nothing emitted."
                );
                std::process::exit(4);
            }

            let mut gaps: Vec<u64> = stats.into_iter().flat_map(|s| s.gaps).collect();
            // Count the librdkafka threads too: each client runs a main thread
            // plus one per broker connection, and `perthread` builds one client
            // per pipeline thread. An unknown parallelism does not flag — the
            // same fallback direction `e2e_kafka_clickhouse` uses.
            let clients = if cfg.mode == "perthread" {
                cfg.threads
            } else {
                cfg.instances
            };
            let oversubscribed = cfg.threads + 2 * clients
                > std::thread::available_parallelism().map_or(usize::MAX, |p| p.get());

            let mut note = format!(
                "backlog={available} consumed={consumed} \
                 lanes_per_thread~{:.1}",
                cfg.partitions as f64 / cfg.threads as f64
            );
            if oversubscribed {
                note.push_str(" OVERSUBSCRIBED");
            }

            let mut report = Report::measurement(env_str("BENCH", "kafka_source_ceiling").as_str())
                // v1 records used a per-message shared atomic and a different
                // poll cadence; v2 anchored the measurement window on the last
                // message seen, which deleted trailing starvation gaps from
                // the denominator, and defaulted prefetch to a depth the
                // connector no longer sets. The key stops the sets aggregating.
                .variant("harness", "v3")
                .variant("broker", cfg.broker.clone())
                .variant("mode", cfg.mode.clone())
                .variant("partitions", cfg.partitions)
                .variant("threads", cfg.threads as u64)
                .variant("instances", cfg.instances as u64)
                .variant("payload_bytes", cfg.payload as u64)
                .variant("work_us", cfg.work_us)
                .variant("duration_s", cfg.duration.as_secs())
                .variant("queued_min_messages", cfg.queued_min_messages)
                .variant("gap_sampling", u64::from(cfg.gap_sampling))
                .metric("consumed", Metric::maximize(consumed as f64, "records"))
                .metric("elapsed_s", Metric::minimize(elapsed_s, "s"))
                .metric(
                    "records_per_s",
                    Metric::maximize(records_per_s, "records/s"),
                )
                .metric(
                    "mb_per_s",
                    Metric::bytes_per_s(records_per_s * cfg.payload as f64),
                )
                .note(note);
            if cfg.gap_sampling {
                report = report.metric(
                    "p99_gap_us",
                    Metric::minimize(percentile(&mut gaps, 0.99) as f64, "us"),
                );
            }
            report.emit();
        }
        _ => {
            eprintln!("usage: kafka_topology <produce|consume>");
            std::process::exit(2);
        }
    }
}
