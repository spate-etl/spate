//! `LogCapture::install_global`, which installs the process's global
//! subscriber.
//!
//! One test in its own binary, because a process has one global subscriber and
//! `cargo test` shares one process across a binary.

use spate_test::LogCapture;
use tracing::Level;

/// Pins that the capture takes a line from a thread with no subscriber of its
/// own, keeps only lines at the level given, formats them as `capture_logs`
/// does, and that a second install panics.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test joins the thread at once"
)]
fn the_global_capture_takes_other_threads_lines_at_its_level() {
    let capture = LogCapture::install_global(Level::WARN);

    std::thread::spawn(|| {
        tracing::warn!(from = "other", "kept");
        tracing::info!(from = "other", "below the level");
    })
    .join()
    .expect("the logging thread");

    assert_eq!(
        capture.lines(),
        [r#" WARN install_global: kept from="other""#]
    );

    let second = std::panic::catch_unwind(|| LogCapture::install_global(Level::WARN))
        .expect_err("a second install panics");
    let message = second
        .downcast_ref::<String>()
        .expect("the panic carries a message");
    assert!(
        message.starts_with("could not install the global log capture"),
        "{message}"
    );
}
