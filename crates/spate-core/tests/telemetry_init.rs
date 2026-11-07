//! `telemetry::init` over the process-wide subscriber.
//!
//! One test per binary, because the first `init` in a process installs the
//! subscriber for the rest of it.

use spate_core::telemetry::{LogFormat, init};

/// Pins that `init` reports whether this call installed the subscriber.
#[test]
fn init_reports_whether_it_installed_the_subscriber() {
    assert!(init(LogFormat::Pretty, "info"));
    assert!(!init(LogFormat::Pretty, "info"));
}
