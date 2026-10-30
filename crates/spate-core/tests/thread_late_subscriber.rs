//! A thread spawned before any `tracing` subscriber is set logs to a global
//! subscriber installed after the spawn.
//!
//! One test in its own binary, because it installs the process-wide global
//! subscriber.

use std::sync::{Arc, Mutex, mpsc};

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Pins that `spawn_thread` called with no subscriber set leaves the thread
/// logging to the global default installed after the spawn.
#[test]
fn a_thread_spawned_before_the_global_subscriber_logs_to_it() {
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let handle = spate_core::task::spawn_thread("late-global", move || -> () {
        go_rx.recv().unwrap();
        panic!("late boom");
    })
    .unwrap();

    let buf = Buf::default();
    let writer = buf.clone();
    tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .init();
    go_tx.send(()).unwrap();
    assert!(handle.join().is_err());

    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let errors: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("ERROR"))
        .collect();
    assert_eq!(
        errors,
        vec![r#"ERROR background thread panicked thread="late-global" panic="late boom""#]
    );
}
