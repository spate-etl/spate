//! `telemetry::cpu::log_once` over the life of a process.
//!
//! One test in its own binary, because the line is logged once per process and
//! `cargo test` shares one process across a binary.

use std::io::Write;
use std::sync::{Arc, Mutex};

use spate_core::telemetry::cpu::log_once;

/// An in-memory writer for the subscriber. Clones share the buffer.
#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Two calls in one process log one line.
#[test]
fn a_second_call_logs_nothing() {
    let buf = Buf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        log_once();
        log_once();
    });
    let text = String::from_utf8(buf.0.lock().expect("buffer").clone()).expect("utf-8");
    assert_eq!(text.matches("cpu features").count(), 1, "{text}");
}
