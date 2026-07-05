//! Benchmark harnesses for the `etl-rs` framework.
//!
//! System-level benchmarks are release binaries (not `cargo bench`
//! targets), configured by environment variables and reporting JSON lines:
//!
//! - `kafka_topology` — raw rdkafka consumer-topology A/B (per-thread
//!   consumers vs one consumer with split partition queues).
//! - `pipeline_synthetic` — the framework-overhead ceiling: an in-process
//!   generator source through the real chain, sink pool, and runtime into
//!   a null writer; no broker or network in the loop.
//! - `loadgen` — Kafka producer at a target rate (raw payloads or
//!   Confluent-framed Avro).
//! - `e2e_kafka_clickhouse` — the full pipeline against local containers
//!   or external clusters (pure env configuration; Kubernetes-runnable).
//!
//! Methodology and recorded results live in `docs/BENCHMARKS.md`.

// Bench harnesses narrate progress on stderr by design.
#![allow(clippy::print_stderr)]

pub mod docker;
pub mod prom;
pub mod synthetic;

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

/// Bootstrap servers from `BOOTSTRAP`, defaulting to the local bench broker.
pub fn bootstrap() -> String {
    std::env::var("BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_owned())
}

/// `key` from the environment parsed as `u64`, else `default`.
pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `key` from the environment, else `default`.
pub fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Creates `topic` with `partitions` partitions, ignoring "already exists".
pub fn ensure_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = rt
        .block_on(admin.create_topics(&[new_topic], &AdminOptions::new()))
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) => {}
            Err((name, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
                eprintln!("topic {name} already exists");
            }
            Err((name, code)) => panic!("failed to create topic {name}: {code}"),
        }
    }
}

/// Produces `count` messages of `payload_size` bytes round-robin across
/// `partitions` explicit partitions. Returns the production throughput in
/// records per second.
pub fn produce(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    count: u64,
    payload_size: usize,
) -> f64 {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .create()
        .expect("producer");
    let payload = vec![0xabu8; payload_size];
    let start = Instant::now();
    for i in 0..count {
        let partition = (i % partitions as u64) as i32;
        loop {
            match producer.send(
                BaseRecord::<(), [u8]>::to(topic)
                    .partition(partition)
                    .payload(&payload),
            ) {
                Ok(()) => break,
                Err((e, _))
                    if e.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                {
                    producer.poll(Duration::from_millis(10));
                }
                Err((e, _)) => panic!("produce error: {e}"),
            }
        }
        if i % 8192 == 0 {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
    count as f64 / start.elapsed().as_secs_f64()
}

/// Spins the CPU for approximately `us` microseconds.
#[inline]
pub fn busy_work(us: u64) {
    if us == 0 {
        return;
    }
    let end = Instant::now() + Duration::from_micros(us);
    while Instant::now() < end {
        std::hint::spin_loop();
    }
}

/// p-th percentile (0.0..=1.0) of an unsorted sample set, in place.
pub fn percentile(samples: &mut [u64], p: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() - 1) as f64 * p).round() as usize;
    samples[idx]
}

/// Prints a JSON report line to stdout and appends it to the file named by
/// `RESULTS` (JSON lines) when that variable is set.
pub fn report(value: &serde_json::Value) {
    #[allow(clippy::print_stdout)]
    {
        println!("{value}");
    }
    if let Ok(path) = std::env::var("RESULTS") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open RESULTS file");
        writeln!(f, "{value}").expect("append result");
    }
}

/// Decode an HTTP/1.1 response: split headers, un-chunk when needed.
fn decode_http(raw: &str) -> String {
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    if raw
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = String::new();
        let mut rest = body.as_str();
        while let Some((size_line, tail)) = rest.split_once("\r\n") {
            let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            out.push_str(tail.get(..size).unwrap_or(""));
            rest = tail.get(size + 2..).unwrap_or("");
        }
        return out;
    }
    body
}

/// Plain HTTP/1.1 POST over a `TcpStream` (localhost only): returns the
/// decoded response body.
pub fn http_post(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    Ok(decode_http(&raw))
}

/// Plain HTTP/1.1 GET over a `TcpStream` (localhost admin/ClickHouse
/// endpoints only). Returns the response body.
pub fn http_get(host: &str, port: u16, path: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    Ok(decode_http(&raw))
}
