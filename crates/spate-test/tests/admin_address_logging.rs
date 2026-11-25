//! The address the admin server bound, as the start reports it.
//!
//! `admin.listen` accepts a port of `0`, and the kernel then picks the port, so
//! the configuration file does not say where the probes and the exposition are
//! served. This asserts that the start names the address it took, and that the
//! address on that line is the one answering.
//!
//! One test in its own binary, because the capture below is a process-wide
//! global subscriber and `cargo test` shares one process across a binary.

use spate_core::config::PipelineConfig;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_test::{HANG, LogCapture, TestEncoder, capture_sink, memory_source, passthrough_runtime};
use spate_test_support::{admin_addr, http};

// Port 0 is the case under test. `exporter: none` leaves `/metrics` a 404 and
// installs no process-global recorder; the probes do not depend on one.
const CONFIG: &str = r#"
pipeline: { name: admin-addr-test, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
"#;

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the pipeline thread"
)]
fn the_bound_admin_address_is_logged_and_serves() {
    // `info` is the level a deployment runs at, and the level
    // `spate_core::telemetry` assigns to a startup milestone.
    let capture = LogCapture::install_global(tracing::Level::INFO);

    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);

    let pipeline =
        Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config")).expect("builder");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions::default(),
    );

    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    // The parsed field is read, because a formatter that stopped rendering it
    // would also pass a check that the line lacks `:0`.
    let addr = capture.wait_for(HANG, "the admin server to announce its address", || {
        admin_addr(&capture.lines())
    });
    assert_ne!(
        addr.port(),
        0,
        "the line must carry the port the kernel picked, not the one asked for"
    );

    // The address on the line is the one serving. The wait is for the endpoint
    // to answer at all, which needs the spawned server to reach its accept
    // loop; what it answers is asserted once, so a probe reporting 503 fails
    // here rather than being polled past.
    let (status, body) = capture.wait_for(HANG, "/healthz to answer at the logged address", || {
        http(addr, "GET", "/healthz").ok()
    });
    assert_eq!(status, 200, "the logged address serves the probes");
    assert_eq!(body, "ok");

    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");
}
