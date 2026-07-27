//! S3 backfill baseline: a bounded object-storage backfill through the
//! real `PipelineRuntime` — local filesystem store, real listing, real
//! per-lane fetchers and framing, null sink. No network — the number is
//! the source's decode-and-frame ceiling per codec, and the reference
//! for spotting regressions.
//!
//! The job is bounded, so the measurement is honest end-to-end wall time:
//! listing, streaming, framing, sink handoff, final commit — everything a
//! real backfill pays.
//!
//! Usage:
//!   s3_backfill                # one report per codec (none, gzip, zstd)
//!   CODECS=zstd s3_backfill    # subset
//!
//! Env: OBJECTS (64) | RECORDS_PER_OBJECT (20000) | PAYLOAD (256)
//! SPLIT_TARGET_MB (64) | THREADS (2) | CODECS (none,gzip,zstd)
//! RESULTS (append JSONL path)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::s3data::stage;
use benchmarks::synthetic::NullWriter;
use benchmarks::{env_str, env_u64};
use bytes::BytesMut;
use spate_core::backpressure::InflightBudget;
use spate_core::config::PipelineConfig;
use spate_core::deser::{BytesPassthrough, Owned};
use spate_core::error::SinkError;
use spate_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use spate_core::ops::{ChunkConfig, chain_owned};
use spate_core::pipeline::{ExitState, PipelineRuntime, RuntimeOptions, SinkRuntime};
use spate_core::record::Record;
use spate_core::sink::{KeyHashRouter, RowEncoder, SinkPool, SinkPoolConfig, shard_queues};
use spate_s3::S3Source;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Length-prefixed rows, like the synthetic rig's encoder.
#[derive(Clone)]
struct OwnedBytesEncoder;

impl RowEncoder<Owned<Vec<u8>>> for OwnedBytesEncoder {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        use bytes::BufMut;
        buf.put_u32_le(u32::try_from(rec.payload.len()).unwrap_or(u32::MAX));
        buf.put_slice(&rec.payload);
        Ok(())
    }
}

fn run_codec(codec: &str) {
    let objects = env_u64("OBJECTS", 64) as usize;
    let records = env_u64("RECORDS_PER_OBJECT", 20_000) as usize;
    let payload = env_u64("PAYLOAD", 256) as usize;
    let split_target_mb = env_u64("SPLIT_TARGET_MB", 64);
    let threads = env_u64("THREADS", 2) as usize;
    let total_records = (objects * records) as u64;

    let dir = tempfile::tempdir().expect("tempdir");
    let decoded_bytes = stage(dir.path(), codec, objects, records, payload) * objects as u64;
    let stored_bytes: u64 = std::fs::read_dir(dir.path().join("data"))
        .expect("dir")
        .map(|e| e.expect("entry").metadata().expect("meta").len())
        .sum();

    let yaml = format!(
        r#"
pipeline: {{ name: s3-backfill-bench, threads: {threads} }}
checkpoint: {{ interval: 500ms }}
metrics: {{ exporter: none, listen: "127.0.0.1:0" }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: {split_target_mb}MiB
sink: {{ nullsink: {{}} }}
"#,
        data = dir.path().join("data").display(),
    );
    let config = PipelineConfig::from_str(&yaml).expect("config");
    let source_section = config.source.clone();

    let shards = 1usize;
    let writer = Arc::new(NullWriter::default());
    let endpoints: Vec<Vec<()>> = (0..shards).map(|_| vec![()]).collect();
    let (queues, receivers) = shard_queues(shards, 64);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");
    let labels = ComponentLabels::new("s3-backfill-bench", "sink", "null");
    let metrics = vec![SinkShardMetrics::new(
        &labels,
        0,
        &["null-0".to_string()],
        E2eBasis::Ingest,
    )];
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.max_rows = 65_536;
        cfg.batch.linger = Duration::from_millis(5);
        cfg
    };
    let pool = SinkPool::spawn(
        Arc::clone(&writer),
        endpoints,
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        metrics,
        "s3-backfill-bench",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: vec![queues.clone()],
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let source = S3Source::from_component_config(&source_section, io.handle().clone())
        .expect("source")
        .with_framer(|| Box::new(spate_json::NdjsonFramer::new(64 << 20)));

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(BytesPassthrough)
            .sink(
                OwnedBytesEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });

    // Bounded job: wall time from start to self-terminated Completed exit
    // is the honest end-to-end figure (listing through final commit).
    let t0 = Instant::now();
    let exit = runtime.run().expect("run");
    let wall = t0.elapsed().as_secs_f64();
    assert_eq!(exit.state, ExitState::Completed, "backfill must complete");
    assert_eq!(
        writer.rows(),
        total_records,
        "conservation: every staged record lands exactly once in a clean run"
    );

    Report::measurement("s3_backfill")
        .variant("codec", codec)
        .variant("objects", objects as u64)
        .variant("records_per_object", records as u64)
        .variant("payload_bytes", payload as u64)
        .variant("split_target_mb", split_target_mb)
        .variant("threads", threads as u64)
        .metric("wall_s", Metric::minimize(wall, "s"))
        .metric(
            "records_per_s",
            Metric::maximize(total_records as f64 / wall, "records/s"),
        )
        .metric(
            "decoded_mb_per_s",
            Metric::bytes_per_s(decoded_bytes as f64 / wall),
        )
        .metric(
            "stored_mb_per_s",
            Metric::bytes_per_s(stored_bytes as f64 / wall),
        )
        .metric(
            "records_total",
            Metric::maximize(total_records as f64, "records"),
        )
        .note(format!(
            "bounded end-to-end incl. listing; exit={:?}",
            exit.state
        ))
        .emit();
}

fn main() {
    let codecs = env_str("CODECS", "none,gzip,zstd");
    for codec in codecs.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        run_codec(codec);
    }
}
