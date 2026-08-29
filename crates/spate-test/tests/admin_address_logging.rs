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
use spate_core::ops::chain_owned;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_core::sink::KeyHashRouter;
use spate_test::{BytesPassthrough, TestEncoder, capture_sink, memory_source};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEADLINE: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

// Port 0 is the case under test. `exporter: none` leaves `/metrics` a 404 and
// installs no process-global recorder; the probes do not depend on one.
const CONFIG: &str = r#"
pipeline: { name: admin-addr-test, threads: 1, io_threads: 1 }
admin: { listen: "127.0.0.1:0" }
metrics: { exporter: none }
source: { memory: {} }
sink: { capture: {} }
"#;

/// Everything the global subscriber has formatted, shared with the subscriber
/// it is installed into.
#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn new() -> Capture {
        Capture(Arc::new(Mutex::new(Vec::new())))
    }

    fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().expect("capture"))
            .lines()
            .map(str::to_string)
            .collect()
    }
}

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;

    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

/// The `addr` field of the line announcing the bound admin address, once one
/// has been written.
///
/// Reading the field rather than testing the line for the absence of `:0`: a
/// formatter that stopped rendering fields this way produces that absence too,
/// and such an assertion would pass having checked nothing.
fn logged_addr(capture: &Capture) -> Option<SocketAddr> {
    capture
        .lines()
        .iter()
        .find(|l| l.contains("admin server listening"))
        .map(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("addr="))
                .unwrap_or_else(|| panic!("no `addr` field to read on: {l}"))
                .parse()
                .expect("addr is a socket address")
        })
}

/// Wait for `check` to produce a value, so the assertions below run against a
/// pipeline that has finished starting rather than one still mid-flight. The
/// whole capture goes into the panic, since the reason for a timeout is
/// whatever the pipeline logged instead.
fn wait_until<T>(what: &str, capture: &Capture, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if let Some(value) = check() {
            return value;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    panic!(
        "timed out waiting for {what}\n--- captured ---\n{}",
        capture.lines().join("\n")
    );
}

/// Minimal HTTP/1.1 GET. Hand-rolled to keep an HTTP client out of
/// `spate-test`'s dev-dependencies for one request.
fn get(addr: SocketAddr, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = std::net::TcpStream::connect(addr)?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;
    let mut text = String::new();
    stream.read_to_string(&mut text)?;
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status code in: {text}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

#[test]
fn the_bound_admin_address_is_logged_and_serves() {
    let capture = Capture::new();
    // `info` is the level a deployment runs at, and the level
    // `spate_core::telemetry` assigns to a startup milestone.
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .without_time()
        .init();

    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);

    let runtime = Pipeline::from_config(PipelineConfig::from_str(CONFIG).expect("config"))
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
        .expect("into_runtime");

    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    let addr = wait_until("the admin server to announce its address", &capture, || {
        logged_addr(&capture)
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
    let (status, body) = wait_until("/healthz to answer at the logged address", &capture, || {
        get(addr, "/healthz").ok()
    });
    assert_eq!(status, 200, "the logged address serves the probes");
    assert_eq!(body, "ok");

    shutdown.trigger();
    let report = join.join().expect("join").expect("run");
    assert_eq!(report.exit_code(), 0, "clean drain");
}
