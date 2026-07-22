//! Shared pipeline-assembly helpers for the etl-s3 integration suites.
// Each test binary compiles this module independently and uses a subset.
#![allow(dead_code)]

pub(crate) mod seaweed;
pub(crate) mod spy;

use etl_coordination::store::memory::MemoryStore;
use etl_coordination::{CoordinationConfig, StoreCoordinator};
use etl_core::config::PipelineConfig;
use etl_core::framing::RecordFramer;
use etl_core::ops::{ChunkConfig, chain_owned};
use etl_core::pipeline::{Pipeline, RuntimeOptions, ShutdownHandle};
use etl_core::sink::KeyHashRouter;
use etl_s3::S3Source;
use etl_test::{BytesPassthrough, PipelineRun, SinkScript, TestEncoder, capture_sink};
use std::collections::VecDeque;
use std::io;
use std::time::Duration;

/// A newline-delimited [`RecordFramer`] for the integration suites: `etl-s3`
/// no longer ships a framer, so the tests supply one, mirroring `etl-json`'s
/// `NdjsonFramer` without depending on a format crate. Splits on `\n`, strips
/// one trailing `\r`, skips whitespace-only lines, keeps an unterminated final
/// line.
#[derive(Default)]
pub(crate) struct LineFramer {
    partial: Vec<u8>,
    ready: VecDeque<Vec<u8>>,
    decoded: u64,
}

impl RecordFramer for LineFramer {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.decoded += bytes.len() as u64;
        for &b in bytes {
            if b == b'\n' {
                self.flush_line();
            } else {
                self.partial.push(b);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.partial.is_empty() {
            self.flush_line();
        }
        Ok(())
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }

    fn decoded_bytes(&self) -> u64 {
        self.decoded
    }
}

impl LineFramer {
    fn flush_line(&mut self) {
        if self.partial.last() == Some(&b'\r') {
            self.partial.pop();
        }
        if self.partial.iter().all(u8::is_ascii_whitespace) {
            self.partial.clear();
            return;
        }
        self.ready.push_back(std::mem::take(&mut self.partial));
    }
}

/// The default framer every suite wires into its source (NDJSON line framing).
pub(crate) fn line_framer(source: S3Source) -> S3Source {
    source.with_framer(|| Box::new(LineFramer::default()))
}

/// A pipeline running in the background plus its observation handles.
pub(crate) struct Launched {
    pub(crate) run: PipelineRun,
    pub(crate) script: SinkScript,
    pub(crate) shutdown: ShutdownHandle,
}

/// Assemble and launch a byte-passthrough pipeline over the YAML config's
/// `source: { s3: ... }` section. `pre` runs against the sink script
/// before the pipeline starts (write scripting must be in place before
/// data can flow).
pub(crate) fn launch_scripted(
    yaml: &str,
    options: RuntimeOptions,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    launch_customized(yaml, options, pre, |source, _io| line_framer(source))
}

/// Coordination tuning every suite shares: floors-compliant and fast.
/// The store a test builds (via [`shared_store`]) must use
/// [`TEST_LEASE`] — the coordinator fails fast on a store/config lease
/// divergence.
pub(crate) const TEST_LEASE: Duration = Duration::from_secs(1);

pub(crate) fn test_tuning() -> CoordinationConfig {
    CoordinationConfig {
        lease_duration: TEST_LEASE,
        op_timeout: Duration::from_millis(200),
        replan_interval: TEST_LEASE,
        // Both below are sized against the 30s production lease; left at
        // their defaults they dwarf a 1s test one, and every takeover test
        // pays the full production grace window in wall clock —
        // `rebalance_delay` alone was ~20s of the 32s `coordinated_takeover`
        // run. Scaled here rather than in `Default` so production keeps the
        // documented values, the same split `etl-coordination`'s own
        // `config()` helper makes.
        reconcile_interval: Duration::from_millis(300),
        // Zero: these suites assert that a dead instance's splits flow back
        // promptly, and a grace window is pure latency on every one of them.
        // No etl-s3 test asserts the withhold behaviour — the two arms that
        // do are `etl-coordination`'s, and they set the window themselves.
        rebalance_delay: Duration::ZERO,
        // NOT scaled with the lease. This one bounds how long a *data
        // plane* takes to drain, not how long a protocol step takes, and
        // these sinks are paced at ~100ms/write. Cut to a fraction of a
        // test lease it silently forces drains that would have completed
        // cooperatively — same green suite, different code path, and
        // replayed tails instead of clean hand-offs. It only costs wall
        // clock when a drain actually wedges, which is the case it exists
        // to bound, so there is no speed to win here.
        drain_deadline: Duration::from_secs(5),
        ..CoordinationConfig::default()
    }
}

/// An in-process coordination store tests share across pipeline launches
/// (the "durable" store of the infrastructure-free suites: it outlives a
/// pipeline, not the process).
pub(crate) fn shared_store() -> MemoryStore {
    MemoryStore::new(TEST_LEASE)
}

/// Launch a pipeline whose source coordinates over `store` — the seam
/// that lets several launches (sequential resumes or concurrent
/// instances) share one job.
pub(crate) fn launch_on_store(
    yaml: &str,
    options: RuntimeOptions,
    store: &MemoryStore,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    launch_tuned(yaml, options, store, test_tuning(), pre)
}

/// [`launch_on_store`] with explicit coordinator tuning (instance ids,
/// working-set bounds, attempt caps). `tuning.lease_duration` must stay
/// [`TEST_LEASE`] to match the store.
pub(crate) fn launch_tuned(
    yaml: &str,
    options: RuntimeOptions,
    store: &MemoryStore,
    tuning: CoordinationConfig,
    pre: impl FnOnce(&SinkScript),
) -> Launched {
    let store = store.clone();
    launch_customized(yaml, options, pre, move |source, io| {
        let coordinator =
            StoreCoordinator::new(store, tuning, io, None).expect("coordinator builds");
        line_framer(source).with_coordinator(Box::new(coordinator))
    })
}

/// Like [`launch_scripted`], but `make_source` may customize the source
/// (e.g. `.with_framer(...)`, `.with_coordinator(...)`) before it is
/// handed to the runtime; it receives the pipeline's I/O handle for
/// coordinator construction.
pub(crate) fn launch_customized(
    yaml: &str,
    options: RuntimeOptions,
    pre: impl FnOnce(&SinkScript),
    make_source: impl FnOnce(S3Source, tokio::runtime::Handle) -> S3Source,
) -> Launched {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(yaml).expect("config parses"))
        .expect("pipeline builds");
    let io = pipeline.io_handle();
    let source = make_source(
        S3Source::from_component_config(&pipeline.config().source, io.clone())
            .expect("source config"),
        io,
    );
    let (sink, script) = capture_sink(1, 1);
    pre(&script);
    let runtime = pipeline
        .sink(sink)
        .expect("sink installs")
        .chains(|ctx| {
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    ChunkConfig::default(),
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(options)
        .into_runtime(source)
        .expect("runtime assembles");
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    Launched {
        run,
        script,
        shutdown,
    }
}

pub(crate) fn launch(yaml: &str, options: RuntimeOptions) -> Launched {
    launch_scripted(yaml, options, |_| {})
}

pub(crate) fn test_options() -> RuntimeOptions {
    RuntimeOptions {
        handle_signals: false,
        ..RuntimeOptions::default()
    }
}

/// Every row the capture sink durably wrote, decoded to strings.
pub(crate) fn captured_rows(script: &SinkScript) -> Vec<String> {
    script
        .writes()
        .iter()
        .flat_map(|w| etl_test::decode_rows(&w.payload))
        .map(|r| String::from_utf8(r).unwrap())
        .collect()
}

pub(crate) fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// `n` one-line JSON records tagged with `prefix`.
pub(crate) fn recs(prefix: &str, n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("{{\"k\":\"{prefix}-{i}\"}}"))
        .collect()
}

/// Join lines with trailing newlines into object bytes.
pub(crate) fn lines_bytes(lines: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for l in lines {
        out.extend_from_slice(l.as_bytes());
        out.push(b'\n');
    }
    out
}
