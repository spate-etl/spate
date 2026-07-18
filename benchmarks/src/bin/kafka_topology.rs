//! Consumer-topology A/B benchmark: N per-thread `BaseConsumer`s versus one
//! consumer with `split_partition_queue` fanned across N threads.
//!
//! Both modes use manual partition assignment (no consumer group protocol)
//! so the measurement isolates the fetch path, queue routing, and threading
//! rather than group-join dynamics.
//!
//! Usage:
//!   kafka_topology produce   # fill the topic for the current config
//!   kafka_topology consume   # run one consume pass, print a JSON line
//!
//! Config via env: BOOTSTRAP, PARTITIONS (16), THREADS (4), PAYLOAD (256),
//! WORK_US (0), MESSAGES (10_000_000), MODE (perthread|split).
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use benchmarks::report::{Metric, Report};
use benchmarks::{busy_work, docker, ensure_topic, env_u64, percentile, produce};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};

struct Config {
    bootstrap: String,
    partitions: i32,
    threads: usize,
    payload: usize,
    work_us: u64,
    messages: u64,
    mode: String,
}

impl Config {
    fn from_env() -> Self {
        let partitions = env_u64("PARTITIONS", 16) as i32;
        let mut messages = env_u64("MESSAGES", 10_000_000);
        messages -= messages % partitions as u64; // even per-partition counts
        Self {
            bootstrap: std::env::var("BOOTSTRAP").unwrap_or_else(|_| docker::ensure_kafka()),
            partitions,
            threads: env_u64("THREADS", 4) as usize,
            payload: env_u64("PAYLOAD", 256) as usize,
            work_us: env_u64("WORK_US", 0),
            messages,
            mode: std::env::var("MODE").unwrap_or_else(|_| "perthread".to_owned()),
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
    config
}

/// Shared measurement state across consumer threads.
struct Shared {
    consumed: AtomicU64,
    first_msg_nanos: AtomicU64,
    last_msg_nanos: AtomicU64,
    epoch: Instant,
}

impl Shared {
    fn new() -> Self {
        Self {
            consumed: AtomicU64::new(0),
            first_msg_nanos: AtomicU64::new(0),
            last_msg_nanos: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    #[inline]
    fn record(&self, n: u64) -> u64 {
        let nanos = self.epoch.elapsed().as_nanos() as u64;
        let _ =
            self.first_msg_nanos
                .compare_exchange(0, nanos, Ordering::Relaxed, Ordering::Relaxed);
        self.last_msg_nanos.store(nanos, Ordering::Relaxed);
        self.consumed.fetch_add(n, Ordering::Relaxed) + n
    }
}

/// Per-thread partition selection: thread `t` owns partitions ≡ t (mod threads).
fn partitions_for(thread: usize, cfg: &Config) -> Vec<i32> {
    (0..cfg.partitions)
        .filter(|p| (*p as usize) % cfg.threads == thread)
        .collect()
}

fn run_perthread(cfg: &Config, shared: &Shared) -> Vec<u64> {
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..cfg.threads)
            .map(|t| {
                let topic = cfg.topic();
                scope.spawn(move || {
                    let consumer: BaseConsumer = consumer_config(cfg).create().expect("consumer");
                    let mut tpl = TopicPartitionList::new();
                    for p in partitions_for(t, cfg) {
                        tpl.add_partition_offset(&topic, p, Offset::Beginning)
                            .expect("add partition");
                    }
                    consumer.assign(&tpl).expect("assign");
                    let mut gaps = Vec::with_capacity(1 << 20);
                    let mut last_yield = Instant::now();
                    let mut sampler = 0u64;
                    while shared.consumed.load(Ordering::Relaxed) < cfg.messages {
                        if let Some(Ok(_msg)) = consumer.poll(Duration::from_millis(100)) {
                            busy_work(cfg.work_us);
                            shared.record(1);
                            sampler += 1;
                            if sampler.is_multiple_of(16) {
                                gaps.push(last_yield.elapsed().as_micros() as u64);
                            }
                            last_yield = Instant::now();
                        }
                    }
                    gaps
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread"))
            .collect()
    })
}

fn run_split(cfg: &Config, shared: &Shared) -> Vec<u64> {
    let topic = cfg.topic();
    let consumer: BaseConsumer = consumer_config(cfg).create().expect("consumer");
    let consumer = Arc::new(consumer);
    let mut tpl = TopicPartitionList::new();
    for p in 0..cfg.partitions {
        tpl.add_partition_offset(&topic, p, Offset::Beginning)
            .expect("add partition");
    }
    consumer.assign(&tpl).expect("assign");
    // Split every partition queue before the first poll so fetched messages
    // never land on the main queue.
    let mut thread_queues: Vec<Vec<_>> = (0..cfg.threads).map(|_| Vec::new()).collect();
    for p in 0..cfg.partitions {
        let queue = consumer.split_partition_queue(&topic, p).expect("split");
        thread_queues[p as usize % cfg.threads].push(queue);
    }

    let main_queue_msgs = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = thread_queues
            .into_iter()
            .enumerate()
            .map(|(t, queues)| {
                let consumer = consumer.clone();
                let main_queue_msgs = &main_queue_msgs;
                scope.spawn(move || {
                    let mut gaps = Vec::with_capacity(1 << 20);
                    let mut last_yield = Instant::now();
                    let mut last_main_poll = Instant::now();
                    let mut sampler = 0u64;
                    while shared.consumed.load(Ordering::Relaxed) < cfg.messages {
                        let mut yielded = false;
                        for q in &queues {
                            while let Some(Ok(_msg)) = q.poll(Duration::ZERO) {
                                busy_work(cfg.work_us);
                                shared.record(1);
                                yielded = true;
                                sampler += 1;
                                if sampler.is_multiple_of(16) {
                                    gaps.push(last_yield.elapsed().as_micros() as u64);
                                }
                                last_yield = Instant::now();
                                if shared.consumed.load(Ordering::Relaxed) >= cfg.messages {
                                    break;
                                }
                            }
                        }
                        // Thread 0 services the main queue for events.
                        if t == 0 && last_main_poll.elapsed() > Duration::from_millis(100) {
                            if let Some(Ok(_)) = consumer.poll(Duration::ZERO) {
                                main_queue_msgs.fetch_add(1, Ordering::Relaxed);
                            }
                            last_main_poll = Instant::now();
                        }
                        if !yielded {
                            // Idle: block briefly on the first queue to avoid spinning.
                            if let Some(Ok(_msg)) = queues[0].poll(Duration::from_millis(2)) {
                                busy_work(cfg.work_us);
                                shared.record(1);
                            }
                        }
                    }
                    gaps
                })
            })
            .collect();
        let gaps = handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread"))
            .collect();
        let leaked = main_queue_msgs.load(Ordering::Relaxed);
        if leaked > 0 {
            eprintln!("WARNING: {leaked} messages leaked to the main queue");
        }
        gaps
    })
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
            let shared = Shared::new();
            let mut gaps = match cfg.mode.as_str() {
                "perthread" => run_perthread(&cfg, &shared),
                "split" => run_split(&cfg, &shared),
                other => {
                    eprintln!("unknown MODE {other}");
                    std::process::exit(2);
                }
            };
            let elapsed_ns = shared.last_msg_nanos.load(Ordering::Relaxed)
                - shared.first_msg_nanos.load(Ordering::Relaxed);
            let elapsed_s = elapsed_ns as f64 / 1e9;
            let consumed = shared.consumed.load(Ordering::Relaxed);
            let records_per_s = consumed as f64 / elapsed_s;
            Report::measurement("kafka_topology")
                .variant("mode", cfg.mode.clone())
                .variant("partitions", cfg.partitions)
                .variant("threads", cfg.threads as u64)
                .variant("payload_bytes", cfg.payload as u64)
                .variant("work_us", cfg.work_us)
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
                .metric(
                    "p99_gap_us",
                    Metric::minimize(percentile(&mut gaps, 0.99) as f64, "us"),
                )
                .emit();
        }
        _ => {
            eprintln!("usage: kafka_topology <produce|consume>");
            std::process::exit(2);
        }
    }
}
