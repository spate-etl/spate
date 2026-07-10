//! Avro decode-backend A/B — the headline CPU-bound rig.
//!
//! One pre-encoded Avro datum is a *sensor batch*: a `sensor` string, a
//! `batch_ts_ms` timestamp, and an array of `EVENTS` `{name, value, unit}`
//! events. The rig replays that datum through a chosen decode backend and,
//! optionally, the full operator chain that explodes the array into one
//! ClickHouse row per event. It is CPU-bound by construction: no broker, no
//! ClickHouse server, no network in the loop.
//!
//! Throughput is reported **per event** — one datum yields `EVENTS` records
//! after `flat_map`, so `records_per_s` and `ns_per_event` count events, not
//! datums.
//!
//! One invocation measures one arm (chosen by env), a median over `REPS`
//! repetitions. Sweep the matrix by running it repeatedly.
//!
//! Env:
//! - `DESER`   `apache_owned` | `fast_owned` | `fast_borrowed`  (default `fast_borrowed`)
//! - `FORMAT`  `rowbinary` | `native`  (pipeline stage only; default `native`)
//! - `STAGE`   `decode` | `pipeline`  (default `decode`)
//! - `EVENTS`  events per datum (default 50)
//! - `THREADS` parallelism (default 1)
//! - `DURATION_S` measurement window per rep (default 3)
//! - `REPS`    repetitions for the median + CI (default 5)
//! - `RESULTS` append the JSONL record to this path
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::avro_batch::{
    BatchFam, EventFam, SensorBatchOwned, SensorEventOwned, encode_batch, explode_borrowed,
    explode_owned, keep_borrowed, keep_owned, native_schema,
};
use benchmarks::report::{Metric, Report};
use benchmarks::synthetic::{NullWriter, SyntheticSource};
use benchmarks::{avro_batch, env_str, env_u64};
use etl_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use etl_clickhouse::{ClickHouseEncoder, NativeEncoder};
use etl_core::backpressure::InflightBudget;
use etl_core::checkpoint::AckRef;
use etl_core::config::PipelineConfig;
use etl_core::deser::{Deserializer, EmitRecord, Owned, RecFamily};
use etl_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use etl_core::ops::{ChunkConfig, RunnableChain, chain};
use etl_core::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl_core::record::{Flow, PartitionId, RawPayload, Record};
use etl_core::sink::{KeyHashRouter, ShardQueues, SinkPool, SinkPoolConfig, shard_queues};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A builder over the `raw`-mode inline sensor-batch schema.
fn builder() -> AvroDeserializerBuilder {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(avro_batch::BATCH_SCHEMA)),
        ..AvroSettings::default()
    };
    // Raw mode resolves the fixed schema locally; the handle hosts no work,
    // but the builder API takes one. Leak the (thread-less) runtime so the
    // handle stays valid for the life of the process.
    let rt = Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime"),
    ));
    AvroDeserializerBuilder::from_settings(&settings, rt.handle()).expect("builder")
}

// ---- decode stage ----------------------------------------------------------

/// Counts emitted records; the decode stage's sink.
struct CountingSink(u64);
impl<T> EmitRecord<'_, T> for CountingSink {
    fn emit(&mut self, _rec: Record<T>) -> Flow {
        self.0 += 1;
        Flow::Continue
    }
}

/// Decode `datum` in a tight loop until `stop`, returning the events decoded
/// (datums × `events`).
fn decode_events<F, D>(deser: &mut D, datum: &[u8], events: u64, stop: &AtomicBool) -> u64
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let (ack, _rx) = AckRef::test_pair();
    let raw = RawPayload {
        bytes: datum,
        key: None,
        partition: PartitionId(0),
        offset: 0,
        timestamp_ms: 0,
    };
    let mut sink = CountingSink(0);
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..4096 {
            deser.deserialize(&raw, &ack, &mut sink).expect("decode");
        }
    }
    sink.0 * events
}

/// Drive the decoder across `threads` threads for `duration`. Returns
/// (events decoded, elapsed seconds).
fn run_decode<F, D>(
    deser: &D,
    datum: &[u8],
    events: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64)
where
    F: RecFamily,
    D: Deserializer<F> + Clone + Send,
{
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let total = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let mut d = deser.clone();
                let stop = &stop;
                scope.spawn(move || decode_events::<F, D>(&mut d, datum, events, stop))
            })
            .collect();
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
        handles
            .into_iter()
            .map(|h| h.join().expect("decode thread"))
            .sum::<u64>()
    });
    (total as f64, start.elapsed().as_secs_f64())
}

// ---- pipeline stage --------------------------------------------------------

/// Run the full chain through the real runtime: synthetic source replaying
/// the datum → deserialize → flat_map → filter → encoder → shard handoff →
/// null writer. Returns (events processed in the window, window seconds,
/// lifetime rows written).
fn run_pipeline<MK>(
    datum: Vec<u8>,
    events: u64,
    threads: usize,
    duration: Duration,
    mut make_chain: MK,
) -> (f64, f64, u64)
where
    MK: FnMut(ShardQueues, Arc<InflightBudget>) -> Box<dyn RunnableChain> + Send + 'static,
{
    let shards = 2usize;
    let config = PipelineConfig::from_str(&format!(
        "pipeline: {{ name: avro-bench, threads: {threads} }}\n\
         checkpoint: {{ interval: 200ms }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ synthetic: {{}} }}\n\
         sink: {{ nullsink: {{}} }}\n"
    ))
    .expect("config");

    let produced = Arc::new(AtomicU64::new(0));
    let source = SyntheticSource::replaying(threads, datum, Arc::clone(&produced));

    let writer = Arc::new(NullWriter::default());
    let endpoints: Vec<Vec<()>> = (0..shards).map(|_| vec![()]).collect();
    let (queues, receivers) = shard_queues(shards, 64);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("io runtime");
    let labels = ComponentLabels::new("avro-bench", "sink", "null");
    let metrics = (0..shards)
        .map(|s| {
            SinkShardMetrics::new(
                &labels,
                u32::try_from(s).expect("shard"),
                &[format!("null-{s}")],
                E2eBasis::Ingest,
            )
        })
        .collect();
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
        "avro-bench",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: queues.clone(),
        drain: Box::new(move |deadline| Box::pin(async move { pool.drain(deadline).await })),
        probe: None,
    };

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| make_chain(chain_queues.clone(), Arc::clone(&chain_budget));

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    std::thread::sleep(Duration::from_secs(2)); // warm up
    let c0 = produced.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    let c1 = produced.load(Ordering::Relaxed);
    let window = t0.elapsed().as_secs_f64();

    shutdown.trigger();
    let _exit = pipeline.join().expect("pipeline thread").expect("run");
    ((c1 - c0) as f64 * events as f64, window, writer.rows())
}

/// The owned pipeline arms (both owned decode backends share this shape).
fn pipeline_owned<D>(
    deser: D,
    format: &str,
    datum: Vec<u8>,
    events: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64)
where
    D: Deserializer<Owned<SensorBatchOwned>> + Clone + Send + 'static,
{
    match format {
        "native" => {
            let enc = NativeEncoder::<Owned<SensorEventOwned>>::new(native_schema());
            let (ev, secs, _) = run_pipeline(datum, events, threads, duration, move |q, b| {
                chain::<Owned<SensorBatchOwned>, _>(deser.clone())
                    .flat_map::<Owned<SensorEventOwned>, _>(explode_owned)
                    .filter(keep_owned)
                    .sink(enc.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                    .build()
            });
            (ev, secs)
        }
        "rowbinary" => {
            let enc = ClickHouseEncoder::<Owned<SensorEventOwned>>::new();
            let (ev, secs, _) = run_pipeline(datum, events, threads, duration, move |q, b| {
                chain::<Owned<SensorBatchOwned>, _>(deser.clone())
                    .flat_map::<Owned<SensorEventOwned>, _>(explode_owned)
                    .filter(keep_owned)
                    .sink(enc.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                    .build()
            });
            (ev, secs)
        }
        other => {
            eprintln!("unknown FORMAT={other}");
            std::process::exit(2);
        }
    }
}

/// The borrowed (zero-copy) pipeline arm.
fn pipeline_borrowed<D>(
    deser: D,
    format: &str,
    datum: Vec<u8>,
    events: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64)
where
    D: Deserializer<BatchFam> + Clone + Send + 'static,
{
    match format {
        "native" => {
            let enc = NativeEncoder::<EventFam>::new(native_schema());
            let (ev, secs, _) = run_pipeline(datum, events, threads, duration, move |q, b| {
                chain::<BatchFam, _>(deser.clone())
                    .flat_map::<EventFam, _>(explode_borrowed)
                    .filter(keep_borrowed)
                    .sink(enc.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                    .build()
            });
            (ev, secs)
        }
        "rowbinary" => {
            let enc = ClickHouseEncoder::<EventFam>::new();
            let (ev, secs, _) = run_pipeline(datum, events, threads, duration, move |q, b| {
                chain::<BatchFam, _>(deser.clone())
                    .flat_map::<EventFam, _>(explode_borrowed)
                    .filter(keep_borrowed)
                    .sink(enc.clone(), KeyHashRouter, ChunkConfig::default(), q, b)
                    .build()
            });
            (ev, secs)
        }
        other => {
            eprintln!("unknown FORMAT={other}");
            std::process::exit(2);
        }
    }
}

/// One measurement of the selected arm: (events processed, elapsed seconds).
#[allow(clippy::too_many_arguments)]
fn run_arm(
    builder: &AvroDeserializerBuilder,
    datum: &[u8],
    stage: &str,
    deser_kind: &str,
    format: &str,
    events: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64) {
    match stage {
        "decode" => match deser_kind {
            "apache_owned" => run_decode::<Owned<SensorBatchOwned>, _>(
                &builder
                    .build_serde::<SensorBatchOwned>()
                    .expect("apache builder"),
                datum,
                events,
                threads,
                duration,
            ),
            "fast_owned" => run_decode::<Owned<SensorBatchOwned>, _>(
                &builder
                    .build_serde_fast::<SensorBatchOwned>()
                    .expect("build fast_owned"),
                datum,
                events,
                threads,
                duration,
            ),
            "fast_borrowed" => run_decode::<BatchFam, _>(
                &builder
                    .build_fast::<BatchFam>()
                    .expect("build fast_borrowed"),
                datum,
                events,
                threads,
                duration,
            ),
            other => {
                eprintln!("unknown DESER={other}");
                std::process::exit(2);
            }
        },
        "pipeline" => {
            let datum = datum.to_vec();
            match deser_kind {
                "apache_owned" => pipeline_owned(
                    builder
                        .build_serde::<SensorBatchOwned>()
                        .expect("apache builder"),
                    format,
                    datum,
                    events,
                    threads,
                    duration,
                ),
                "fast_owned" => pipeline_owned(
                    builder
                        .build_serde_fast::<SensorBatchOwned>()
                        .expect("build fast_owned"),
                    format,
                    datum,
                    events,
                    threads,
                    duration,
                ),
                "fast_borrowed" => pipeline_borrowed(
                    builder
                        .build_fast::<BatchFam>()
                        .expect("build fast_borrowed"),
                    format,
                    datum,
                    events,
                    threads,
                    duration,
                ),
                other => {
                    eprintln!("unknown DESER={other}");
                    std::process::exit(2);
                }
            }
        }
        other => {
            eprintln!("unknown STAGE={other}");
            std::process::exit(2);
        }
    }
}

// ---- statistics ------------------------------------------------------------

fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Student-t critical value t(df, 0.975) — the two-sided 95% multiplier — for
/// the small rep counts this rig uses. Beyond the table it falls back to the
/// normal 1.96, which the t value is within ~2.5% of by df = 30.
fn t_975(df: usize) -> f64 {
    const TABLE: [f64; 15] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131,
    ];
    match df {
        0 => 0.0,
        d if d <= TABLE.len() => TABLE[d - 1],
        _ => 1.96,
    }
}

/// (mean, ci95_low, ci95_high) — a Student-t 95% confidence interval for the
/// mean: mean ± t(n−1, 0.975)·s/√n, centred on the mean it brackets. (The
/// median is reported separately in the note as the robust point estimate.)
fn stats(xs: &[f64]) -> (f64, f64, f64) {
    let n = xs.len();
    if n == 0 {
        return (0.0, 0.0, 0.0);
    }
    let nf = n as f64;
    let mean = xs.iter().sum::<f64>() / nf;
    if n < 2 {
        return (mean, mean, mean);
    }
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (nf - 1.0);
    let sem = (var / nf).sqrt();
    let t = t_975(n - 1);
    (mean, mean - t * sem, mean + t * sem)
}

fn main() {
    let deser_kind = env_str("DESER", "fast_borrowed");
    let format = env_str("FORMAT", "native");
    let stage = env_str("STAGE", "decode");
    let events = env_u64("EVENTS", 50);
    let threads = env_u64("THREADS", 1) as usize;
    let duration = Duration::from_secs(env_u64("DURATION_S", 3));
    let reps = env_u64("REPS", 5);

    let builder = builder();
    let datum = encode_batch(events);
    eprintln!(
        "── avro_pipeline STAGE={stage} DESER={deser_kind} FORMAT={format} \
         EVENTS={events} THREADS={threads} REPS={reps} (datum {} bytes) ──",
        datum.len()
    );

    let mut rps_samples = Vec::with_capacity(reps as usize);
    let mut nspe_samples = Vec::with_capacity(reps as usize);
    for r in 0..reps {
        let (ev, secs) = run_arm(
            &builder,
            &datum,
            &stage,
            &deser_kind,
            &format,
            events,
            threads,
            duration,
        );
        let rps = ev / secs;
        let nspe = if ev > 0.0 { secs * 1e9 / ev } else { 0.0 };
        eprintln!(
            "  rep {}: {:.2}M events/s, {nspe:.1} ns/event",
            r + 1,
            rps / 1e6
        );
        rps_samples.push(rps);
        nspe_samples.push(nspe);
    }

    let (rps_mean, rps_lo, rps_hi) = stats(&rps_samples);
    let (nspe_mean, nspe_lo, nspe_hi) = stats(&nspe_samples);

    let mut rep = Report::measurement("avro_pipeline")
        .variant("deser", deser_kind.clone())
        .variant("stage", stage.clone())
        .variant("events", events)
        .variant("threads", threads as u64);
    if stage == "pipeline" {
        rep = rep.variant("format", format.clone());
    }
    rep.metric(
        "ns_per_event",
        Metric::minimize(nspe_mean, "ns")
            .with_n(reps)
            .with_ci(nspe_lo, nspe_hi),
    )
    .metric(
        "records_per_s",
        Metric::maximize(rps_mean, "records/s")
            .with_n(reps)
            .with_ci(rps_lo, rps_hi),
    )
    .note(format!(
        "mean of {reps} reps, 95% Student-t CI (median {:.2}M records/s)",
        median(&rps_samples) / 1e6
    ))
    .emit();
}
