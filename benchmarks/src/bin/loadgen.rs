//! Kafka load generator: raw payloads or Confluent-framed Avro at a target
//! rate.
//!
//! Usage: loadgen
//! Env: BOOTSTRAP (localhost:9092, auto-starts a container when unset)
//! TOPIC (bench-load) PARTITIONS (4) RATE (0 = unthrottled, records/s)
//! COUNT (1_000_000) MODE (raw|avro, default raw) SCHEMA_ID (1)
//! PAYLOAD (64, raw-mode payload bytes) RESULTS (append JSONL path)
//!
//! Avro mode frames each record as Confluent wire format (0x00 + u32 BE
//! schema id + datum) using the inline schema:
//! `{"type":"record","name":"BenchEvent","fields":[{"name":"id","type":"long"},{"name":"body","type":"string"}]}`
//! — the id must match what the consuming pipeline's registry serves.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use apache_avro::Schema;
use apache_avro::types::{Record as AvroRecord, Value};
use benchmarks::report::{Metric, Report};
use benchmarks::{docker, ensure_topic, env_str, env_u64};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use std::time::{Duration, Instant};

/// Inline Avro schema for `MODE=avro` records.
pub const BENCH_SCHEMA: &str = r#"{"type":"record","name":"BenchEvent","fields":[{"name":"id","type":"long"},{"name":"body","type":"string"}]}"#;

fn main() {
    let bootstrap = std::env::var("BOOTSTRAP").unwrap_or_else(|_| docker::ensure_kafka());
    let topic = env_str("TOPIC", "bench-load");
    let partitions = env_u64("PARTITIONS", 4) as i32;
    let rate = env_u64("RATE", 0);
    let count = env_u64("COUNT", 1_000_000);
    let mode = env_str("MODE", "raw");
    let schema_id = u32::try_from(env_u64("SCHEMA_ID", 1)).expect("schema id");
    let payload_size = env_u64("PAYLOAD", 64) as usize;

    ensure_topic(&bootstrap, &topic, partitions);
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("linger.ms", "5")
        .set("batch.size", "1048576")
        .create()
        .expect("producer");

    let schema = Schema::parse_str(BENCH_SCHEMA).expect("schema");
    let body: String = "x".repeat(payload_size.saturating_sub(16).max(1));

    let start = Instant::now();
    let mut sent = 0u64;
    while sent < count {
        // Rate control: stay at or below the target for the elapsed time.
        if rate > 0 {
            let allowed = (start.elapsed().as_secs_f64() * rate as f64) as u64 + 1;
            if sent >= allowed {
                producer.poll(Duration::from_millis(1));
                continue;
            }
        }
        let payload: Vec<u8> = match mode.as_str() {
            "avro" => {
                let mut rec = AvroRecord::new(&schema).expect("record");
                rec.put("id", Value::Long(sent as i64));
                rec.put("body", Value::String(body.clone()));
                let datum = apache_avro::to_avro_datum(&schema, rec).expect("datum");
                let mut framed = Vec::with_capacity(5 + datum.len());
                framed.push(0);
                framed.extend_from_slice(&schema_id.to_be_bytes());
                framed.extend_from_slice(&datum);
                framed
            }
            _ => format!("{sent},{body}").into_bytes(),
        };
        let key = (sent % 1024).to_string();
        loop {
            match producer.send(
                BaseRecord::to(&topic)
                    .partition((sent % partitions as u64) as i32)
                    .key(&key)
                    .payload(&payload),
            ) {
                Ok(()) => break,
                Err((e, _))
                    if e.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                {
                    producer.poll(Duration::from_millis(5));
                }
                Err((e, _)) => panic!("produce: {e}"),
            }
        }
        sent += 1;
        if sent.is_multiple_of(4096) {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(60)).expect("flush");
    let elapsed = start.elapsed().as_secs_f64();
    Report::measurement("loadgen")
        .variant("mode", mode)
        .variant("topic", topic)
        // Target rate is the RATE config echo (0 = unthrottled), not an
        // achieved quantity — carried in the variant, no direction attached.
        .variant("target_records_per_s", rate)
        .metric("records", Metric::maximize(sent as f64, "records"))
        .metric("elapsed_s", Metric::minimize(elapsed, "s"))
        .metric(
            "achieved_records_per_s",
            Metric::maximize(sent as f64 / elapsed, "records/s"),
        )
        .emit();
}
