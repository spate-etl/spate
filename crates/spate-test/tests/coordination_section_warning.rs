//! The deprecation WARN for a pipeline file's `coordination:` section.
//!
//! A pipeline with no section logs no such line. A pipeline with one logs it
//! once at start, whether the source rejects the section or accepts it and runs.
//!
//! One test in its own binary, because the capture below is a process-wide
//! global subscriber and `cargo test` shares one process across a binary.

#![expect(deprecated, reason = "v0.4.0 removes this with spate-coordination")]

use spate_core::config::{ConfigError, CoordinationSection, PipelineConfig};
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, PipelineRuntime, RuntimeOptions, StartError};
use spate_core::sink::KeyHashRouter;
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent};
use spate_core::{PartitionId, SourceError};
use spate_test::{
    BytesPassthrough, LogCapture, MemorySource, TestEncoder, capture_sink, memory_source,
};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(10);
const DEPRECATED: &str = "`coordination:` section is deprecated";

const NO_SECTION: &str = r#"
pipeline: { name: coordination-warning-none, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
"#;

const REJECTED: &str = r#"
pipeline: { name: coordination-warning-section, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
coordination: { store: { memory: {} } }
"#;

const ACCEPTED: &str = r#"
pipeline: { name: coordination-warning-accepted, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
coordination: { store: { memory: {} } }
"#;

/// A [`MemorySource`] that accepts the `coordination:` section, as a
/// coordinated source does, and otherwise behaves as the memory source.
struct Accepting(MemorySource);

impl Source for Accepting {
    type Lane = <MemorySource as Source>::Lane;

    fn component_type(&self) -> &str {
        self.0.component_type()
    }

    fn configure_coordination(&mut self, _: &CoordinationSection) -> Result<(), ConfigError> {
        Ok(())
    }

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        self.0.open(ctx)
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<Self::Lane>, SourceError> {
        self.0.poll_events(timeout)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        self.0.commit(watermarks)
    }

    fn flush_commits(&mut self) -> Result<(), SourceError> {
        self.0.flush_commits()
    }

    fn pause(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.0.pause(lanes)
    }

    fn resume(&mut self, lanes: &[LaneId]) -> Result<(), SourceError> {
        self.0.resume(lanes)
    }
}

fn runtime<S: Source + 'static>(yaml: &str, source: S) -> PipelineRuntime<S> {
    let (sink, _script) = capture_sink(1, 1);
    Pipeline::from_config(PipelineConfig::from_str(yaml).expect("config"))
        .expect("builder")
        .sink(sink)
        .expect("sink")
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
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
        .expect("into_runtime")
}

/// Runs `rt` until its admin server is listening, then shuts it down and
/// returns the exit code. `from` is the capture length before the start.
fn run_to_listening<S: Source + 'static>(
    capture: &LogCapture,
    from: usize,
    rt: PipelineRuntime<S>,
) -> i32 {
    let shutdown = rt.shutdown_handle();
    let join = std::thread::spawn(move || rt.run());
    capture.wait_for(DEADLINE, "this run's admin server to listen", || {
        capture.lines()[from..]
            .iter()
            .any(|l| l.contains("admin server listening"))
            .then_some(())
    });
    shutdown.trigger();
    join.join().expect("join").expect("run").exit_code()
}

fn deprecation_lines(capture: &LogCapture, from: usize) -> Vec<String> {
    capture.lines()[from..]
        .iter()
        .filter(|l| l.contains(DEPRECATED))
        .cloned()
        .collect()
}

fn assert_one_warn(lines: &[String], run: &str) {
    assert_eq!(lines.len(), 1, "{run}: one deprecation line, got {lines:?}");
    assert!(
        lines[0].contains("WARN") && lines[0].contains("v0.4.0") && lines[0].contains("work feed"),
        "{run}: the line is a WARN naming v0.4.0 and the work feed: {}",
        lines[0]
    );
}

#[test]
fn a_coordination_section_is_warned_at_start_and_its_absence_is_not() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .without_time()
        .init();

    // No section: the pipeline runs and logs no deprecation line.
    let (source, _handle) = memory_source();
    let rt = runtime(NO_SECTION, source);
    let from = capture.lines().len();
    let code = run_to_listening(&capture, from, rt);
    assert_eq!(code, 0, "no section: clean drain");
    assert_eq!(deprecation_lines(&capture, from), Vec::<String>::new());

    // A section the source rejects: the start fails and the WARN is logged.
    let (source, _handle) = memory_source();
    let rt = runtime(REJECTED, source);
    let from = capture.lines().len();
    let err = rt.run().expect_err("memory rejects the section");
    assert!(matches!(err, StartError::Config(_)), "rejected: {err:?}");
    assert_one_warn(&deprecation_lines(&capture, from), "rejected");

    // A section the source accepts: the pipeline runs and the WARN is logged.
    let (source, _handle) = memory_source();
    let rt = runtime(ACCEPTED, Accepting(source));
    let from = capture.lines().len();
    let code = run_to_listening(&capture, from, rt);
    assert_eq!(code, 0, "accepted: clean drain");
    assert_one_warn(&deprecation_lines(&capture, from), "accepted");
}
