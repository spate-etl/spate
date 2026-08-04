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
//!   REPS=5 s3_backfill         # five repetitions, interleaved across codecs
//!
//! With `REPS > 1` the arms are **interleaved** — every codec is measured once
//! per repetition, rather than one codec being measured five times and then
//! the next. Running arms in sequence lets anything that drifts over the run
//! (thermal state, page cache, a neighbour on the host) load entirely onto
//! whichever arm went last; in a related project that manufactured a 30%
//! difference between two arms that were in fact identical. The report then
//! carries one record per codec with a Student-t interval and the repetition
//! count, so a reader can tell a difference from a spread.
//!
//! The corpus is staged **once per codec, before the clock starts**, and
//! reused across repetitions. `DATA_DIR` reuses one across processes too,
//! which is what lets two builds of this rig — a merge base and a head, say —
//! measure literally the same bytes.
//!
//! `peak_rss_mb` is reported only when this process measured **one** codec at
//! **one** repetition over a **reused** corpus:
//!
//!   DATA_DIR=/tmp/corpus CODECS=none REPS=1 s3_backfill   # run twice
//!
//! The first run stages and reports no memory figure; the second reuses and
//! does. See `benchmarks/src/rss.rs` for why each condition is the difference
//! between a number that means something and one that does not.
//!
//! `GIT_COMMIT` is read by the report layer and should be set explicitly when
//! running a binary built from a commit other than the working tree's: the
//! fallback asks git at run time, so a binary built from one commit and run
//! from another checkout records the wrong one.
//!
//! `SMOKE=1` shrinks this to 2 objects of 500 records over one codec: enough
//! to prove the rig runs, not to measure it. Any knob set explicitly wins.
//!
//! Env: OBJECTS (64) | RECORDS_PER_OBJECT (20000) | PAYLOAD (256)
//! SPLIT_TARGET_MB (64) | THREADS (2) | CODECS (none,gzip,zstd)
//! REPS (1) | DATA_DIR (a fresh temporary directory) | BENCH (s3_backfill)
//! RESULTS (append JSONL path)
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::report::{Metric, Report};
use benchmarks::s3data::stage;
use benchmarks::stats::{median, stats};
use benchmarks::synthetic::NullWriter;
use benchmarks::{env_str, env_str_smoke, env_u64, env_u64_smoke};
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

/// A staged corpus for one codec, and what it holds. Built once and measured
/// repeatedly: staging inside the timed region would measure the generator,
/// and staging per repetition would hand each one a different page-cache
/// state.
struct Corpus {
    codec: String,
    /// Whether this process built the corpus rather than reusing one. A
    /// staging run's resident set is not comparable with a reusing run's, so
    /// it reports no memory figure.
    rebuilt: bool,
    data: std::path::PathBuf,
    decoded_bytes: u64,
    stored_bytes: u64,
    /// Kept so the temporary directory outlives every repetition. `None` when
    /// `DATA_DIR` supplied the location, which the caller owns.
    _dir: Option<tempfile::TempDir>,
}

fn stage_corpus(codec: &str, objects: usize, records: usize, payload: usize) -> Corpus {
    // A shared `DATA_DIR` is per-codec, because the bytes differ per codec.
    let (root, keep) = match std::env::var("DATA_DIR") {
        Ok(base) if !base.is_empty() => {
            let p = std::path::PathBuf::from(base).join(codec);
            std::fs::create_dir_all(&p).expect("data dir");
            (p, None)
        }
        _ => {
            let d = tempfile::tempdir().expect("tempdir");
            (d.path().to_path_buf(), Some(d))
        }
    };

    let staged = stage(&root, codec, objects, records, payload);
    let decoded_bytes = staged.decoded_bytes * objects as u64;
    let data = root.join("data");
    let stored_bytes: u64 = std::fs::read_dir(&data)
        .expect("dir")
        .map(|e| e.expect("entry").metadata().expect("meta").len())
        .sum();

    Corpus {
        codec: codec.to_owned(),
        rebuilt: staged.rebuilt,
        data,
        decoded_bytes,
        stored_bytes,
        _dir: keep,
    }
}

/// One measured repetition against an already-staged corpus. Returns the
/// bounded job's wall time.
fn run_once(corpus: &Corpus, total_records: u64) -> f64 {
    let split_target_mb = env_u64("SPLIT_TARGET_MB", 64);
    let threads = env_u64("THREADS", 2) as usize;

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
        data = corpus.data.display(),
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

    wall
}

fn main() {
    // Validates BENCH_TRIGGER before any work: it is otherwise read when the
    // first report is built, which is after the measurement.
    benchmarks::preflight();
    let objects = env_u64_smoke("OBJECTS", 64, 2) as usize;
    let records = env_u64_smoke("RECORDS_PER_OBJECT", 20_000, 500) as usize;
    let payload = env_u64("PAYLOAD", 256) as usize;
    let split_target_mb = env_u64("SPLIT_TARGET_MB", 64);
    let threads = env_u64("THREADS", 2) as usize;
    let reps = env_u64("REPS", 1).max(1);
    let bench = env_str("BENCH", "s3_backfill");
    let total_records = (objects * records) as u64;

    // Every arm staged before any of them is measured. Doing this inside the
    // repetition loop would put the generator inside the comparison, and doing
    // it per repetition would give each one a different page-cache state.
    let corpora: Vec<Corpus> = env_str_smoke("CODECS", "none,gzip,zstd", "none")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|codec| stage_corpus(codec, objects, records, payload))
        .collect();
    assert!(!corpora.is_empty(), "no codecs selected");

    // One discarded pass over every arm before any measurement. Without it
    // the first arm measured absorbs all the cold-start cost — first touch of
    // the staged files, allocator warm-up, the runtime's first spawn — and it
    // is a large effect, not a rounding one: at four objects the leading arm's
    // interval came out ±74% of its mean against ±1.4% for the one behind it,
    // and swapping the order moved the penalty with the position rather than
    // with the codec. Interleaving alone does not fix that, because rep 1 has
    // to put some arm first.
    for corpus in &corpora {
        run_once(corpus, total_records);
    }

    // The baseline for the memory figure: staging is done and the priming pass
    // has run, so a peak above this mark was set by the measured region.
    let watch = benchmarks::rss::PeakWatch::start();

    // Three conditions decide whether a memory figure is attributable, and each
    // one is about what the number would otherwise mean rather than caution.
    //
    //  - One arm. A process peak cannot be split between arms of an in-process
    //    sweep; it is the mark of whichever arm reached it.
    //  - One repetition. The figure is a *maximum*, and `REPS` is not part of a
    //    record's variant identity — so a max over ten repetitions and a max
    //    over one are two different quantities the schema cannot tell apart,
    //    and the site would median them into one bar. Every other metric here
    //    is a mean or a derived rate, which does not have that problem.
    //  - A reused corpus. Building one grows allocator arenas far past anything
    //    the pipeline needs, and they are never returned, so a staging run's
    //    resident set is not comparable with a reusing run's. `DATA_DIR` is
    //    what makes a reusing run possible.
    let attributable = corpora.len() == 1 && reps == 1;

    // Interleaved: repetition outermost, arm innermost. See the module docs
    // for why the other order is not a comparison.
    // `vec![v; n]` clones, and `Clone` does not carry capacity, so reserving
    // here would be decorative.
    let mut walls: Vec<Vec<f64>> = vec![Vec::new(); corpora.len()];
    for _rep in 0..reps {
        for (i, corpus) in corpora.iter().enumerate() {
            walls[i].push(run_once(corpus, total_records));
        }
    }

    for (corpus, samples) in corpora.iter().zip(&walls) {
        let (mean, lo, hi) = stats(samples);
        let n = samples.len() as u64;
        // One repetition has no spread to report, and a zero-width `ci95`
        // beside `n = 1` reads as certainty rather than absence. Attach the
        // interval only where there is one.
        let wall = Metric::minimize(mean, "s").with_n(n);
        let wall = if n >= 2 { wall.with_ci(lo, hi) } else { wall };
        // Rates are derived from the mean wall time rather than averaged
        // themselves: the mean of a ratio is not the ratio of the mean, and
        // the quantity actually measured here is elapsed time.
        let per_s = |total: f64| Metric::maximize(total / mean, "records/s");

        let mut rep = Report::measurement(&bench)
            .variant("codec", corpus.codec.as_str())
            .variant("objects", objects as u64)
            .variant("records_per_object", records as u64)
            .variant("payload_bytes", payload as u64)
            .variant("split_target_mb", split_target_mb)
            .variant("threads", threads as u64)
            .metric("wall_s", wall)
            .metric(
                "wall_median_s",
                Metric::minimize(median(samples), "s").with_n(n),
            )
            .metric("records_per_s", per_s(total_records as f64).with_n(n))
            .metric(
                "decoded_mb_per_s",
                Metric::bytes_per_s(corpus.decoded_bytes as f64 / mean).with_n(n),
            )
            .metric(
                "stored_mb_per_s",
                Metric::bytes_per_s(corpus.stored_bytes as f64 / mean).with_n(n),
            )
            .metric(
                "records_total",
                Metric::maximize(total_records as f64, "records"),
            )
            .note(format!(
                "bounded end-to-end incl. listing; {n} repetition(s), arms interleaved"
            ));
        // Two conditions, and both are about attribution rather than caution.
        if attributable
            && !corpus.rebuilt
            && let Some(m) = watch.metric()
        {
            rep = rep.metric(benchmarks::rss::PeakWatch::KEY, m);
        }
        rep.emit();
    }
}
