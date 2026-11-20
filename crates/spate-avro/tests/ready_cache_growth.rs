//! Pins that a pipeline holds flat memory while its records name ever more
//! distinct schema ids the registry resolves.
//!
//! A registry stub answers each id with a schema. Each test reads process RSS
//! once `n` distinct ids have been decoded and committed, and again at
//! `10 * n`, and asserts the two readings stay within `TOLERANCE_KIB`.

#![allow(clippy::print_stderr, reason = "the run's readings are the output")]

use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, AvroValue, RegistrySection};
use spate_core::config::PipelineConfig;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::sink::KeyHashRouter;
use spate_core::source::LaneId;
use spate_test::{PipelineRun, TestEncoder, capture_sink, memory_source};
use spate_test_support::RegistryStub;
use std::time::Duration;

/// Allowed RSS growth between the two readings. It covers the stub's request
/// log, which grows with each id, and allocator noise.
const TOLERANCE_KIB: u64 = 64 * 1024;
const THREADS: u32 = 4;
/// The hang bound on one wait for commits, which covers thousands of fetches.
const COMMITTED: Duration = spate_test::HANG.saturating_mul(30);

/// Resident set size of this process in KiB, from `ps`.
fn rss_kib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("run ps");
    assert!(out.status.success(), "ps failed: {out:?}");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("ps prints a number")
}

/// A Confluent frame naming schema `id` over `longs` zero longs.
fn framed(id: u32, longs: usize) -> Vec<u8> {
    spate_test::confluent_frame(id, &vec![0u8; longs])
}

/// Runs a pipeline over records naming ids `1..=10 * n`, each resolved to
/// `schema(id)` and decoded from `longs` zero longs, and asserts RSS at
/// `10 * n` ids is within the tolerance of RSS at `n`.
fn holds_flat_memory(name: &str, n: u32, longs: usize, schema: impl Fn(u32) -> String) {
    let stub = RegistryStub::start();
    for id in 1..=10 * n {
        let body = serde_json::json!({ "schema": schema(id) }).to_string();
        stub.script(&format!("/schemas/ids/{id}"), 200, body, 0);
    }
    let config = format!(
        "pipeline: {{ name: {name}, threads: {THREADS} }}\n\
         admin: {{ listen: none }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ memory: {{}} }}\n\
         sink: {{ capture: {{}} }}\n"
    );
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(&config).expect("config")).expect("builder");
    let settings = AvroSettings {
        mode: AvroMode::Confluent,
        registry: Some(RegistrySection::new(stub.endpoint())),
        ..AvroSettings::default()
    };
    let deserializer = AvroDeserializerBuilder::from_settings(&settings, &pipeline.io_handle())
        .expect("builder")
        .build_value()
        .expect("deserializer");

    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let runtime = pipeline
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<AvroValue, _>(deserializer.clone())
                .map(|_: AvroValue| Vec::new())
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .into_runtime(source)
        .expect("into_runtime");
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let partitions: Vec<PartitionId> = (0..THREADS).map(PartitionId).collect();
    let lanes: Vec<(LaneId, PartitionId)> = partitions.iter().map(|p| (LaneId(p.0), *p)).collect();
    handle.assign_lanes(&lanes);

    // Decodes ids `from..to`, each a schema id no earlier record named, and
    // waits for every partition to commit past its last record.
    let decode = |from: u32, to: u32| {
        let mut last = vec![None; partitions.len()];
        for id in from..to {
            let p = (id % THREADS) as usize;
            last[p] = Some(handle.push(partitions[p], None, &framed(id, longs)));
        }
        for (p, offset) in partitions.iter().zip(&last) {
            if let Some(offset) = offset {
                assert!(
                    handle.wait_committed(*p, offset + 1, COMMITTED),
                    "partition {p:?} never committed {offset}"
                );
            }
        }
    };

    decode(1, n + 1);
    let rss_n = rss_kib();
    eprintln!("{name} at {n} ids: rss_kib={rss_n}");
    decode(n + 1, 10 * n + 1);
    let rss_10n = rss_kib();
    eprintln!("{name} at {} ids: rss_kib={rss_10n}", 10 * n);

    shutdown.trigger();
    run.wait_exit(Duration::from_secs(120))
        .expect("the pipeline exits after shutdown")
        .expect("the run succeeds");

    assert!(
        rss_10n <= rss_n + TOLERANCE_KIB,
        "RSS grew from {rss_n} KiB at {n} schema ids to {rss_10n} KiB at {} (+{} KiB, \
         tolerance {TOLERANCE_KIB} KiB)",
        10 * n,
        rss_10n.saturating_sub(rss_n)
    );
}

/// Ordinary schemas, 15 long fields each and distinct per id, reach the
/// cache's count bound.
#[test]
fn distinct_resolvable_schema_ids_hold_flat_memory() {
    const FIELDS: usize = 15;
    let n = 2 * u32::try_from(spate_avro::MAX_READY_ENTRIES).unwrap();
    holds_flat_memory("ready-cache-count", n, FIELDS, |id| {
        let fields: Vec<String> = (0..FIELDS)
            .map(|f| format!(r#"{{"name":"f{f}","type":"long"}}"#))
            .collect();
        format!(
            r#"{{"type":"record","name":"R","namespace":"n{id}","fields":[{}]}}"#,
            fields.join(",")
        )
    });
}

/// Schemas nesting records twelve deep reach the cache's byte bound below a
/// tenth of its count bound.
#[test]
fn nested_schemas_hold_flat_memory() {
    const DEPTH: usize = 12;
    const WIDTH: usize = 12;
    let schema = spate_avro::nested_schema(DEPTH, WIDTH);
    let fit = spate_avro::MAX_READY_BYTES / spate_avro::schema_weight(&schema).unwrap();
    assert!(
        fit * 10 <= spate_avro::MAX_READY_ENTRIES,
        "{fit} of these fit the byte bound"
    );
    // Without the byte bound the cache would hold `n` of these at the first
    // reading and the count bound's worth at the second.
    let n = u32::try_from(spate_avro::MAX_READY_ENTRIES / 5).unwrap();
    holds_flat_memory("ready-cache-bytes", n, DEPTH * WIDTH + 1, |_| {
        schema.clone()
    });
}
