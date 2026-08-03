//! Benchmark harnesses for the Spate framework.
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
//! - `ch_native_format` — ClickHouse Native vs RowBinary go/no-go.
//! - `ch_sink_saturation` — pushes the ClickHouse sink to its ceiling from an
//!   in-process generator (no broker), sweeping threads, shards, part size,
//!   format, compression, and `async_insert` across `Null` and `MergeTree`
//!   engines, with server-side part/CPU/async accounting (see [`chstats`]).
//!
//! Every binary emits the same versioned record — see [`report::Report`].
//! Methodology and recorded results live in `docs/benchmarks/`.

// Bench harnesses narrate progress on stderr by design.
#![allow(clippy::print_stderr)]

pub mod avro_batch;
pub mod chstats;
pub mod deser_sample;
pub mod docker;
pub mod prom;
pub mod report;
pub mod rss;
pub mod s3data;
pub mod stats;
pub mod synthetic;

use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
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

/// Creates `topic` with `partitions` partitions.
///
/// An existing topic is reused, but only after its partition count is checked
/// against the request. Silently accepting a mismatch is how a sweep ends up
/// measuring one shape while recording another: rigs that pin a topic name and
/// vary `PARTITIONS` would create the topic on the first arm and every later
/// arm would reuse it, reporting the swept value it never ran at.
pub fn ensure_topic(bootstrap: &str, topic: &str, partitions: i32) {
    ensure_topic_with(bootstrap, topic, partitions, &[]);
}

/// [`ensure_topic`] plus topic-level configuration entries (retention, segment
/// sizing) applied at creation.
///
/// Config is only honoured on a **fresh** topic: an existing one keeps
/// whatever it was created with, and the partition check below says nothing
/// about it. A topic first created by plain [`ensure_topic`] and later reused
/// here therefore runs without these settings — delete it if the retention
/// shape matters to the measurement.
pub fn ensure_topic_with(bootstrap: &str, topic: &str, partitions: i32, configs: &[(&str, &str)]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    for (key, value) in configs {
        new_topic = new_topic.set(key, value);
    }
    let results = rt
        .block_on(admin.create_topics(&[new_topic], &AdminOptions::new()))
        .expect("create_topics call");
    for result in results {
        match result {
            Ok(_) => {}
            Err((name, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
                let actual = topic_partitions(bootstrap, &name);
                assert_eq!(
                    actual, partitions,
                    "topic {name} already exists with {actual} partitions, but this run \
                     asked for {partitions}. Reusing it would measure {actual} while \
                     recording {partitions}; delete the topic or pick another name."
                );
                eprintln!("topic {name} already exists with {actual} partitions (matches)");
            }
            Err((name, code)) => panic!("failed to create topic {name}: {code}"),
        }
    }
}

/// Partition count of an existing topic, from the broker's metadata.
///
/// Retries while the broker reports a topic-level error: immediately after a
/// concurrent creation it can answer `LEADER_NOT_AVAILABLE` with an empty
/// partition list, which would otherwise read as "this topic has 0 partitions"
/// and abort a perfectly good run.
fn topic_partitions(bootstrap: &str, topic: &str) -> i32 {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("metadata consumer");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("fetch metadata for {topic}: {e}"));
        let t = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .unwrap_or_else(|| panic!("topic {topic} missing from metadata"));
        match t.error() {
            None if !t.partitions().is_empty() => {
                return i32::try_from(t.partitions().len()).expect("partition count");
            }
            other => {
                assert!(
                    Instant::now() < deadline,
                    "topic {topic} metadata never settled (last error {other:?}, \
                     {} partitions)",
                    t.partitions().len()
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

/// Produces `count` messages of exactly `payload_size` bytes round-robin
/// across `partitions` explicit partitions. Returns the production throughput
/// in records per second.
///
/// Payloads are `<i>,<padding>` in ASCII, truncated or padded to
/// `payload_size`. Two properties matter to the rigs downstream, and the
/// obvious `vec![0xab; payload_size]` has neither:
///
/// - **Valid UTF-8.** `e2e_kafka_clickhouse` parses these through
///   `String::from_utf8_lossy`, and every invalid byte expands to a 3-byte
///   replacement character — a 64-byte payload reaching the sink as a
///   192-byte row, under-sizing every byte-derived budget and making the
///   recorded `payload` variant describe something other than the workload.
/// - **Distinct.** Identical rows collapse under ClickHouse insert
///   deduplication and make any row-count cross-check meaningless.
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
    // Ids must fit, or the prefix is truncated and payloads start colliding.
    let widest = format!("{},", count.saturating_sub(1)).len();
    assert!(
        payload_size >= widest,
        "PAYLOAD={payload_size} is too small to hold a distinct id for {count} messages \
         (needs {widest} bytes)"
    );
    let mut payload = vec![b'x'; payload_size];
    let start = Instant::now();
    for i in 0..count {
        let partition = (i % partitions as u64) as i32;
        // Rewrite the id prefix in place; the tail keeps its padding, so every
        // payload is exactly `payload_size` bytes. No clearing needed between
        // iterations — ids ascend, so each prefix is at least as long as the
        // one it overwrites.
        for (slot, byte) in payload.iter_mut().zip(format!("{i},").into_bytes()) {
            *slot = byte;
        }
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

/// Validates the run's environment before a rig does any work.
///
/// Every rig calls this first. `BENCH_TRIGGER` is otherwise read when the first
/// [`report::Report`] is constructed, which every rig does *after* measuring —
/// so a typo cost a finished sweep and wrote nothing. Resolving it here fails
/// in the first millisecond instead, and the answer is memoised, so nothing is
/// read twice or read differently later.
///
/// # Panics
///
/// If `BENCH_TRIGGER` is set to something that is not a trigger, or is set and
/// empty. See [`report::Trigger::resolve`].
pub fn preflight() {
    let _ = report::Trigger::detect();
}

/// Prints one [`report::Report`] as a JSON line on stdout and appends it to
/// the file named by `RESULTS` when that variable is set.
///
/// Prefer [`report::Report::emit`] at call sites.
///
/// The publication bar is applied here, in the one function that writes, rather
/// than in `emit` — otherwise this would be a public path to `RESULTS` that
/// skips it, and a bar with a way around it is not a bar.
pub fn report(rep: &report::Report) {
    let rep = &rep.for_emission();
    let line = serde_json::to_string(rep).expect("serialize report");
    #[allow(clippy::print_stdout)]
    {
        println!("{line}");
    }
    if let Ok(path) = std::env::var("RESULTS") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open RESULTS file");
        writeln!(f, "{line}").expect("append result");
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

/// Plain HTTP/1.1 POST with a **binary** body (localhost ClickHouse only):
/// used for `INSERT ... FORMAT <binary>` where the body is a text query
/// prefix followed by the raw block/row bytes. Returns the decoded response.
pub fn http_post_bytes(host: &str, port: u16, path: &str, body: &[u8]) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
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

#[cfg(test)]
mod manifest {
    /// `autobins = false` means cargo no longer discovers `src/bin/*.rs` —
    /// a rig added without a matching `[[bin]]` stanza is simply never
    /// built, with no warning from cargo, clippy, or `cargo bench --no-run`.
    /// It would rot silently until someone tried to run it. The explicit
    /// declarations exist so each bin can carry `test = false` (they have no
    /// tests, and each empty libtest harness is another binary to link and
    /// exec); this test is what keeps that bookkeeping honest.
    #[test]
    fn every_rig_is_declared_as_a_bin() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/bin");
        let mut on_disk: Vec<String> = std::fs::read_dir(dir)
            .expect("benchmarks/src/bin is readable")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .map(|p| {
                p.file_stem()
                    .expect("a .rs file has a stem")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        on_disk.sort();

        let manifest = include_str!("../Cargo.toml");
        let mut declared: Vec<String> = manifest
            .lines()
            .map(str::trim)
            .skip_while(|l| *l != "[[bin]]")
            .filter_map(|l| l.strip_prefix("name = \""))
            .filter_map(|l| l.strip_suffix('"'))
            .map(str::to_owned)
            .collect();
        declared.sort();

        assert_eq!(
            on_disk, declared,
            "benchmarks/src/bin and the [[bin]] stanzas in benchmarks/Cargo.toml \
             have diverged. With `autobins = false` an undeclared rig is never \
             compiled by anything. Add a `[[bin]]` stanza with `test = false`."
        );
    }
}
