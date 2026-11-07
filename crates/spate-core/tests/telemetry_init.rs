//! `telemetry::init` over the process-wide subscriber.
//!
//! The first `init` in a process installs the subscriber for the rest of it, so
//! a test that reads what `init` installed writes from a child process of its
//! own.

use std::process::Command;

use spate_core::telemetry::{LogFormat, init};

/// Set in the child process, which writes the log line its parent reads.
const CHILD: &str = "SPATE_TELEMETRY_INIT_CHILD";

/// Pins that `init` reports whether this call installed the subscriber.
#[test]
fn init_reports_whether_it_installed_the_subscriber() {
    assert!(init(LogFormat::Pretty, "info"));
    assert!(!init(LogFormat::Pretty, "info"));
}

/// Pins that `LogFormat::Pretty` installs the formatter that keeps an event on
/// one line.
#[test]
fn pretty_keeps_a_field_on_one_line() {
    if std::env::var_os(CHILD).is_some() {
        assert!(init(LogFormat::Pretty, "info"));
        tracing::info!(reason = %"first\nsecond", "refused");
        return;
    }
    let test = "pretty_keeps_a_field_on_one_line";
    let child = Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture"])
        .env(CHILD, "1")
        .env_remove("RUST_LOG")
        .output()
        .unwrap();
    assert!(child.status.success(), "{child:?}");
    let out = String::from_utf8(child.stdout).unwrap();
    assert!(out.contains(r"first\nsecond"), "{out}");
}
