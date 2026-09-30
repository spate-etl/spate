//! Shared pipeline-assembly helpers for the spate-s3 integration suites.
// Each test binary compiles this module independently and uses a subset.
#![allow(dead_code)]

pub(crate) mod seaweed;
pub(crate) mod spy;

use object_store::ObjectStoreExt as _;
use spate_coordination::store::memory::MemoryStore;
use spate_coordination::{CoordinationConfig, StoreCoordinator};
use spate_core::config::PipelineConfig;
use spate_core::coordination::SplitSpec;
use spate_core::framing::RecordFramer;
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions, ShutdownHandle};
use spate_core::sink::KeyHashRouter;
use spate_s3::{S3Source, SplitDescriptor, SplitRange, split_id_for_range};
use spate_test::{BytesPassthrough, PipelineRun, SinkScript, TestEncoder, capture_sink};
use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::time::Duration;

/// Pipeline YAML for an S3 source feeding a capture sink, with the admin
/// server and exporter off.
///
/// Defaults to two threads, a 100ms checkpoint interval and a 1MiB split
/// target.
pub(crate) struct PipelineYaml {
    name: String,
    threads: u32,
    checkpoint: String,
    url: String,
    source: Vec<(String, String)>,
    store: Option<String>,
    sections: String,
}

impl PipelineYaml {
    /// Reads the files under `data`.
    pub(crate) fn file(name: &str, data: &Path) -> Self {
        Self::over(name, format!("file://{}/", data.display()), None)
    }

    /// Reads `data/` in the gateway's bucket.
    pub(crate) fn bucket(name: &str, gw: &seaweed::Gateway) -> Self {
        let url = format!("s3://{}/data/", gw.bucket);
        Self::over(name, url, Some(gw.store_options_yaml()))
    }

    fn over(name: &str, url: String, store: Option<String>) -> Self {
        PipelineYaml {
            name: name.to_owned(),
            threads: 2,
            checkpoint: "100ms".to_owned(),
            url,
            source: vec![("split_target_bytes".to_owned(), "1MiB".to_owned())],
            store,
            sections: String::new(),
        }
    }

    pub(crate) fn threads(mut self, threads: u32) -> Self {
        self.threads = threads;
        self
    }

    pub(crate) fn checkpoint(mut self, interval: &str) -> Self {
        self.checkpoint = interval.to_owned();
        self
    }

    /// Adds `key: value` under `source.s3`.
    pub(crate) fn source(mut self, key: &str, value: &str) -> Self {
        self.source.push((key.to_owned(), value.to_owned()));
        self
    }

    /// Appends a top-level section, written as YAML.
    pub(crate) fn section(mut self, yaml: &str) -> Self {
        self.sections.push_str(yaml.trim_end());
        self.sections.push('\n');
        self
    }

    pub(crate) fn build(&self) -> String {
        let mut yaml = format!(
            "pipeline: {{ name: {}, threads: {} }}\nadmin: {{ listen: none }}\ncheckpoint: {{ interval: {} }}\n",
            self.name, self.threads, self.checkpoint
        );
        yaml.push_str("metrics: { exporter: none }\nsource:\n  s3:\n");
        yaml.push_str(&format!("    url: \"{}\"\n", self.url));
        for (key, value) in &self.source {
            yaml.push_str(&format!("    {key}: {value}\n"));
        }
        if let Some(store) = &self.store {
            yaml.push_str(&format!("    store:\n{store}\n"));
        }
        yaml.push_str("sink: { capture: {} }\n");
        yaml.push_str(&self.sections);
        yaml
    }
}

/// A `coordination:` section naming a NATS server on a port nothing listens
/// on, with timeouts short enough that startup fails fast.
pub(crate) fn unreachable_nats(job: &str) -> String {
    format!(
        "coordination:
  op_timeout: 100ms
  lease_duration: 2s
  replan_interval: 2s
  startup_max_attempts: 1
  store:
    nats: {{ servers: [\"nats://127.0.0.1:1\"], job: {job} }}
"
    )
}

/// A newline-delimited [`RecordFramer`] for the integration suites: `spate-s3`
/// no longer ships a framer, so the tests supply one, mirroring `spate-json`'s
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

    fn resync_delimiter(&self) -> Option<u8> {
        Some(b'\n')
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
/// [`TEST_LEASE`]; the coordinator fails fast on a store/config lease
/// divergence.
pub(crate) const TEST_LEASE: Duration = Duration::from_secs(1);

pub(crate) fn test_tuning() -> CoordinationConfig {
    let mut cfg = CoordinationConfig::default();
    cfg.lease_duration = TEST_LEASE;
    cfg.op_timeout = Duration::from_millis(200);
    cfg.replan_interval = TEST_LEASE;
    // Both below are sized against the 30s production lease; left at their
    // defaults they dwarf a 1s test one, and every takeover test pays the
    // full production grace window in wall clock — `rebalance_delay` alone
    // was ~20s of the 32s `coordinated_takeover` run. Scaled here rather
    // than in `Default` so production keeps the documented values, the same
    // split `spate-coordination`'s own `config()` helper makes.
    cfg.reconcile_interval = Duration::from_millis(300);
    // Zero: these suites assert that a dead instance's splits flow back
    // promptly, and a grace window is pure latency on every one of them. No
    // spate-s3 test asserts the withhold behavior; the two arms that do are
    // `spate-coordination`'s, and they set the window themselves.
    cfg.rebalance_delay = Duration::ZERO;
    // NOT scaled with the lease. This one bounds how long a *data plane*
    // takes to drain, not how long a protocol step takes, and these sinks
    // are paced at ~100ms/write. Cut to a fraction of a test lease it
    // silently forces drains that would have completed cooperatively: same
    // green suite, different code path, and replayed tails instead of clean
    // releases. It only costs wall clock when a drain wedges, which is the
    // case it bounds.
    cfg.drain_deadline = Duration::from_secs(5);
    cfg
}

/// An in-process coordination store tests share across pipeline launches
/// (the "durable" store of the infrastructure-free suites: it outlives a
/// pipeline, not the process).
pub(crate) fn shared_store() -> MemoryStore {
    MemoryStore::new(TEST_LEASE)
}

/// Launch a pipeline whose source coordinates over `store`, the seam that
/// lets several launches (sequential resumes or concurrent instances) share
/// one job.
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
    let mut config = PipelineConfig::from_str(yaml).expect("config parses");
    // Every launch gets its own pipeline name. Metric gauge series have a
    // single live owner per process (`docs/METRICS.md`, "Series ownership")
    // and the pipeline name is part of every key, so two launches of one
    // config (tests running concurrently in a `cargo test` process, or a
    // multi-instance test running two workers in-process) would collide.
    // Coordination is unaffected: splits are namespaced by the store, not by
    // the pipeline name.
    config.pipeline.name = spate_test::unique_name(&config.pipeline.name);
    let pipeline = Pipeline::from_config(config).expect("pipeline builds");
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
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
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
        .flat_map(|w| spate_test::decode_rows(&w.payload))
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

/// The key the source's `file://` store reads `name` under `data` at.
pub(crate) fn key_of(data: &Path, name: &str) -> String {
    data.join(name)
        .to_string_lossy()
        .trim_start_matches('/')
        .to_string()
}

/// A `SplitSpec` for the byte range `[start, end)` of the staged file `name`,
/// pinned to the ETag the `file://` store reports for it.
pub(crate) fn ranged_spec(
    data: &Path,
    name: &str,
    start: u64,
    end: u64,
    delimiter: u8,
) -> SplitSpec {
    let key = key_of(data, name);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let meta = rt
        .block_on(object_store::local::LocalFileSystem::new().head(&key.as_str().into()))
        .unwrap();
    let etag = meta.e_tag.expect("the file store reports an ETag");
    let range = SplitRange::new(start, end, delimiter);
    let object = spate_s3::DescriptorObject {
        key: key.clone(),
        size: meta.size,
        etag: Some(etag.clone()),
        last_modified_ms: 1,
    };
    SplitSpec::new(
        split_id_for_range(&key, &etag, range).unwrap(),
        SplitDescriptor::with_range(object, range).encode().unwrap(),
    )
}

/// Byte offset at which each line of `lines_bytes(lines)` starts.
pub(crate) fn line_starts(lines: &[String]) -> Vec<u64> {
    lines
        .iter()
        .scan(0, |at, line| {
            let start = *at;
            *at += line.len() as u64 + 1;
            Some(start)
        })
        .collect()
}
