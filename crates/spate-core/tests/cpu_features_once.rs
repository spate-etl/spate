//! `telemetry::cpu::log_once` over the life of a process.
//!
//! One test in its own binary, because the line is logged once per process and
//! `cargo test` shares one process across a binary.

use spate_core::telemetry::cpu::log_once;

/// Two calls in one process log one line.
#[test]
fn a_second_call_logs_nothing() {
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        log_once();
        log_once();
    });
    let logged = lines.iter().filter(|l| l.contains("cpu features")).count();
    assert_eq!(logged, 1, "{lines:#?}");
}
