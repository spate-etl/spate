//! Cross-format deserialization rig — the "which format to decode" comparison.
//!
//! Decodes the **same logical data** encoded as both Avro and JSON, CPU-bound
//! (no broker, no server, no network), reporting throughput **per event** so
//! every format and framing is a fair bar in one chart. Two workloads:
//!
//! - `SHAPE=order` — a single flat 15-field record → `ns_per_record`.
//! - `SHAPE=batch` — a nested batch of `EVENTS` richer readings → `ns_per_event`.
//!   For the batch, JSON is measured in every framing (`single` nested document,
//!   `array` top-level array, `ndjson`) against Avro's single nested datum.
//!   Every batch arm decodes the identical `EVENTS` readings; only the physical
//!   layout differs.
//!
//! Avro's `typed` arm is `apache-avro`'s serde path — note it decodes twice
//! (datum → `AvroValue` → `T`), which is the only typed decode `apache-avro`
//! 0.21 offers, so it is not a like-for-like peer to JSON's single-pass
//! `serde_json`. The `value` arm is Avro's single-decode path.
//!
//! One invocation measures one arm, a mean over `REPS` reps with a Student-t
//! 95% CI. Sweep the matrix by running it repeatedly.
//!
//! JSON arms are tagged with a `backend` variant from the compiled-in
//! [`etl_json::BACKEND_ID`] (`serde_json`, or `simd-json` when the crate is
//! built with the `simd` feature) so the same rig, rebuilt per backend, sweeps
//! the JSON-backend comparison. `COPY_ONLY=1` measures a memcpy-only baseline
//! (`backend=memcpy_baseline`) that isolates the mandatory owned-copy cost a
//! mutable-buffer parser (simd-json) pays over serde_json's immutable-slice
//! parse in this framework — subtract it to recover the raw engine speed. Avro
//! arms carry no `backend` key.
//!
//! Env:
//! - `SHAPE`   `order` | `batch`  (default `batch`)
//! - `FORMAT`  `avro` | `json`  (default `json`)
//! - `FRAMING` `single` | `array` | `ndjson`  (JSON batch only; default `ndjson`)
//! - `RECORD`  `typed` | `value`  (default `typed`)
//! - `EVENTS`  readings per batch (default 50)
//! - `THREADS` parallelism (default 1)
//! - `DURATION_S` measurement window per rep (default 3)
//! - `REPS`    repetitions for the mean + CI (default 5)
//! - `COPY_ONLY` `1` measures the memcpy-only baseline instead of decoding (JSON)
//! - `RESULTS` append the JSONL record to this path
#![allow(clippy::print_stdout, clippy::print_stderr)]

use benchmarks::deser_sample::{self, Order, Reading, SensorBatch};
use benchmarks::report::{Metric, Report};
use benchmarks::{env_str, env_u64};
use etl_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
use etl_core::checkpoint::AckRef;
use etl_core::deser::{Deserializer, EmitRecord, RecFamily};
use etl_core::record::{Flow, PartitionId, RawPayload, Record};
use etl_json::{JsonDeserializerBuilder, JsonFraming, JsonSettings, OnError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Discards emitted records; the decode loop's sink.
struct DropSink;
impl<T> EmitRecord<'_, T> for DropSink {
    fn emit(&mut self, _rec: Record<T>) -> Flow {
        Flow::Continue
    }
}

/// Decode `payload` in a tight loop until `stop`, returning the number of
/// decode calls made (each call processes the whole payload).
fn decode_calls<F, D>(deser: &mut D, payload: &[u8], stop: &AtomicBool) -> u64
where
    F: RecFamily,
    D: Deserializer<F>,
{
    let (ack, _rx) = AckRef::test_pair();
    let raw = RawPayload {
        bytes: payload,
        key: None,
        partition: PartitionId(0),
        offset: 0,
        timestamp_ms: 0,
    };
    let mut sink = DropSink;
    let mut calls = 0u64;
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..4096 {
            deser.deserialize(&raw, &ack, &mut sink).expect("decode");
            calls += 1;
        }
    }
    calls
}

/// Drive the decoder across `threads` for `duration`. Returns
/// (events processed, elapsed seconds), where events = calls × `per_call`.
fn run_decode<F, D>(
    deser: &D,
    payload: &[u8],
    per_call: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64)
where
    F: RecFamily,
    D: Deserializer<F> + Clone + Send,
{
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let calls = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let mut d = deser.clone();
                let stop = &stop;
                scope.spawn(move || decode_calls::<F, D>(&mut d, payload, stop))
            })
            .collect();
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
        handles
            .into_iter()
            .map(|h| h.join().expect("decode thread"))
            .sum::<u64>()
    });
    ((calls * per_call) as f64, start.elapsed().as_secs_f64())
}

/// Copy `payload` into a reused scratch buffer in a tight loop until `stop`,
/// returning the number of copies made. The memcpy-only baseline: it mirrors
/// [`decode_calls`]'s loop structure exactly, minus the parse, so its
/// per-event number is directly subtractable from a parsing arm's.
fn copy_calls(payload: &[u8], stop: &AtomicBool) -> u64 {
    // A reused buffer, exactly as the simd-json backend keeps a thread-local
    // scratch — so the baseline charges only the memcpy, never a per-call alloc.
    let mut scratch: Vec<u8> = Vec::with_capacity(payload.len());
    let mut calls = 0u64;
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..4096 {
            scratch.clear();
            scratch.extend_from_slice(payload);
            std::hint::black_box(scratch.as_slice());
            calls += 1;
        }
    }
    calls
}

/// Drive the memcpy baseline across `threads` for `duration`. Mirrors
/// [`run_decode`] so the per-event normalization is identical.
fn run_copy(payload: &[u8], per_call: u64, threads: usize, duration: Duration) -> (f64, f64) {
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let calls = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let stop = &stop;
                scope.spawn(move || copy_calls(payload, stop))
            })
            .collect();
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
        handles
            .into_iter()
            .map(|h| h.join().expect("copy thread"))
            .sum::<u64>()
    });
    ((calls * per_call) as f64, start.elapsed().as_secs_f64())
}

/// A `raw`-mode Avro builder over `schema`. The runtime hosts no work (raw
/// mode resolves the fixed schema locally) but the builder API takes a handle;
/// leak the thread-less runtime so the handle outlives the process.
fn avro_builder(schema: &str) -> AvroDeserializerBuilder {
    let settings = AvroSettings {
        mode: AvroMode::Raw,
        schema: Some(SchemaSource::inline(schema)),
        ..AvroSettings::default()
    };
    let rt = Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime"),
    ));
    AvroDeserializerBuilder::from_settings(&settings, rt.handle()).expect("avro builder")
}

fn json_builder(framing: JsonFraming) -> JsonDeserializerBuilder {
    JsonDeserializerBuilder::from_settings(JsonSettings {
        framing,
        on_error: OnError::Skip,
        reject_duplicate_keys: false,
    })
}

/// Build the arm's decoder and run it. Returns (events, seconds).
#[allow(clippy::too_many_arguments)]
fn run_arm(
    format: &str,
    shape: &str,
    record: &str,
    framing: &str,
    payload: &[u8],
    per_call: u64,
    threads: usize,
    duration: Duration,
) -> (f64, f64) {
    match (format, shape, record, framing) {
        ("avro", "order", "typed", _) => run_decode(
            &avro_builder(deser_sample::ORDER_SCHEMA)
                .build_serde::<Order>()
                .expect("avro typed order"),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("avro", "order", "value", _) => run_decode(
            &avro_builder(deser_sample::ORDER_SCHEMA)
                .build_value()
                .expect("avro value order"),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("avro", "batch", "typed", _) => run_decode(
            &avro_builder(deser_sample::BATCH_SCHEMA)
                .build_serde::<SensorBatch>()
                .expect("avro typed batch"),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("avro", "batch", "value", _) => run_decode(
            &avro_builder(deser_sample::BATCH_SCHEMA)
                .build_value()
                .expect("avro value batch"),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "order", "typed", _) => run_decode(
            &json_builder(JsonFraming::Single).build_serde::<Order>(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "order", "value", _) => run_decode(
            &json_builder(JsonFraming::Single).build_value(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "typed", "single") => run_decode(
            &json_builder(JsonFraming::Single).build_serde::<SensorBatch>(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "typed", "array") => run_decode(
            &json_builder(JsonFraming::Array).build_serde::<Reading>(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "typed", "ndjson") => run_decode(
            &json_builder(JsonFraming::Ndjson).build_serde::<Reading>(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "value", "single") => run_decode(
            &json_builder(JsonFraming::Single).build_value(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "value", "array") => run_decode(
            &json_builder(JsonFraming::Array).build_value(),
            payload,
            per_call,
            threads,
            duration,
        ),
        ("json", "batch", "value", "ndjson") => run_decode(
            &json_builder(JsonFraming::Ndjson).build_value(),
            payload,
            per_call,
            threads,
            duration,
        ),
        _ => panic!(
            "unsupported arm: format={format} shape={shape} record={record} framing={framing}"
        ),
    }
}

fn build_payload(format: &str, shape: &str, framing: &str, events: u64) -> Vec<u8> {
    match (format, shape, framing) {
        ("avro", "order", _) => deser_sample::avro_order(),
        ("avro", "batch", _) => deser_sample::avro_batch(events),
        ("json", "order", _) => deser_sample::json_order(),
        ("json", "batch", "single") => deser_sample::json_batch_document(events),
        ("json", "batch", "array") => deser_sample::json_batch_array(events),
        ("json", "batch", "ndjson") => deser_sample::json_batch_ndjson(events),
        _ => panic!("unsupported payload: format={format} shape={shape} framing={framing}"),
    }
}

/// Student-t critical value t(df, 0.975).
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

/// (mean, ci95_low, ci95_high) — a Student-t 95% confidence interval.
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

fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let n = v.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn main() {
    let shape = env_str("SHAPE", "batch");
    let format = env_str("FORMAT", "json");
    let record = env_str("RECORD", "typed");
    let events = env_u64("EVENTS", 50);
    let threads = env_u64("THREADS", 1) as usize;
    let duration = Duration::from_secs(env_u64("DURATION_S", 3));
    let reps = env_u64("REPS", 5);
    let copy_only = env_u64("COPY_ONLY", 0) != 0;

    // Effective framing: Avro is always the single nested datum; a JSON order
    // is one document; a JSON batch takes the configured framing.
    let framing = if format == "avro" {
        "datum".to_owned()
    } else if shape == "order" {
        "single".to_owned()
    } else {
        env_str("FRAMING", "ndjson")
    };
    let per_call = if shape == "order" { 1 } else { events };
    let metric_name = if shape == "order" {
        "ns_per_record"
    } else {
        "ns_per_event"
    };

    // JSON arms are tagged with the compiled-in backend; the memcpy baseline is
    // tagged `memcpy_baseline`. Avro carries no `backend` key (it is not a JSON
    // backend, and the running binary's `BACKEND_ID` reflects only how etl-json
    // was compiled, not how the Avro arm decoded).
    let backend: Option<&str> = if copy_only {
        Some("memcpy_baseline")
    } else if format == "json" {
        Some(etl_json::BACKEND_ID)
    } else {
        None
    };

    let payload = build_payload(&format, &shape, &framing, events);
    eprintln!(
        "── deser_formats SHAPE={shape} FORMAT={format} FRAMING={framing} RECORD={record} \
         BACKEND={} EVENTS={events} THREADS={threads} REPS={reps} ({} bytes) ──",
        backend.unwrap_or("-"),
        payload.len()
    );

    let mut rps_samples = Vec::with_capacity(reps as usize);
    let mut nspe_samples = Vec::with_capacity(reps as usize);
    for r in 0..reps {
        let (n, secs) = if copy_only {
            run_copy(&payload, per_call, threads, duration)
        } else {
            run_arm(
                &format, &shape, &record, &framing, &payload, per_call, threads, duration,
            )
        };
        let rps = n / secs;
        let nspe = if n > 0.0 { secs * 1e9 / n } else { 0.0 };
        eprintln!(
            "  rep {}: {:.2}M/s, {nspe:.1} {metric_name}",
            r + 1,
            rps / 1e6
        );
        rps_samples.push(rps);
        nspe_samples.push(nspe);
    }

    let (rps_mean, rps_lo, rps_hi) = stats(&rps_samples);
    let (nspe_mean, nspe_lo, nspe_hi) = stats(&nspe_samples);

    let mut rep = Report::measurement("deser_formats")
        .variant("format", format.clone())
        .variant("shape", shape.clone())
        .variant("framing", framing.clone())
        .variant("record", record.clone())
        .variant("threads", threads as u64);
    if let Some(backend) = backend {
        rep = rep.variant("backend", backend);
    }
    if shape == "batch" {
        rep = rep.variant("events", events);
    }
    rep.metric(
        metric_name,
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
        "mean of {reps} reps, 95% Student-t CI (median {:.2}M/s)",
        median(&rps_samples) / 1e6
    ))
    .emit();
}
