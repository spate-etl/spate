//! A thread spawned before any `tracing` subscriber is set logs to a global
//! subscriber installed after the spawn.
//!
//! One test in its own binary, because it installs the process-wide global
//! subscriber.

use std::sync::mpsc;

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

    let capture = spate_test::LogCapture::install_global(tracing::Level::ERROR);
    go_tx.send(()).unwrap();
    assert!(handle.join().is_err());

    assert_eq!(
        capture.lines(),
        vec![
            r#"ERROR spate_core::task: background thread panicked thread="late-global" panic="late boom""#
        ]
    );
}
