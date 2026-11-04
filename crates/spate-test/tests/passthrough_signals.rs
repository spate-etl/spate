//! A SIGTERM leaves a `passthrough_runtime` run going, so a signal stays the
//! test process's. Its own test binary, since the signal goes to the process.
#![cfg(unix)]

use spate_core::config::PipelineConfig;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::record::PartitionId;
use spate_core::source::LaneId;
use spate_test::{PipelineRun, TestEncoder, capture_sink, memory_source, passthrough_runtime};
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};

const DEADLINE: Duration = Duration::from_secs(10);

const CONFIG: &str = r#"
pipeline: { name: signal-off, threads: 1, io_threads: 1 }
admin: { listen: none }
metrics: { exporter: none }
checkpoint: { interval: 100ms }
source: { memory: {} }
sink: { capture: {} }
"#;

/// `handle_signals: true` in the options does not reach the run: records keep
/// committing after each SIGTERM, and the shutdown handle ends the run.
#[test]
fn a_sigterm_leaves_the_run_going() {
    // A listener of the test's own replaces the default action, so the signal
    // does not end the test process, and tells the test it was delivered.
    let signals = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut listener = signals.block_on(async { signal(SignalKind::terminate()).unwrap() });

    let (source, handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config")).expect("builder");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions {
            handle_signals: true,
            ..RuntimeOptions::default()
        },
    );
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());

    let p = PartitionId(0);
    handle.assign_lanes(&[(LaneId(0), p)]);
    let pid = std::process::id().to_string();
    // Each round commits a record before the next signal, so the io runtime
    // has had time to start any listener the run was given.
    for _ in 0..5 {
        let last = handle.push(p, None, b"row");
        assert!(
            handle.wait_committed(p, last + 1, DEADLINE),
            "the run stopped committing after a SIGTERM (last: {:?})",
            handle.last_committed(p)
        );
        let sent = std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        assert!(sent.success());
        signals
            .block_on(async { tokio::time::timeout(DEADLINE, listener.recv()).await })
            .expect("the test's listener saw the signal");
    }

    shutdown.trigger();
    let report = run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");
}
