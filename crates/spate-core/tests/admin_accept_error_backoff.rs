//! While `accept` keeps failing, the admin server waits between attempts, and
//! a shutdown ends it without waiting out the pause.
//!
//! One test in its own binary: it lowers the process's descriptor limit and
//! installs a process-wide subscriber, and `cargo test` shares one process
//! across a binary.
#![cfg(all(unix, not(loom)))]

mod common;

use std::sync::mpsc;
use std::time::Duration;

use common::descriptors;
use spate_core::admin::{AdminServer, HealthState, HealthThresholds};
use spate_test::{HANG, LogCapture};
use tokio::sync::watch;
use tokio::time::Instant;

/// The pause between failed accepts that the user guide states.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// The line the server writes for each failed accept.
const FAILED: &str = "admin accept failed";

/// More failed accepts than the test can see from a server that pauses
/// between them: a count past this is a server retrying at once.
const RUNAWAY: usize = 1000;

fn failures(logs: &LogCapture) -> usize {
    logs.lines().iter().filter(|l| l.contains(FAILED)).count()
}

/// How often the test looks at the log, on the paused clock.
const POLL: Duration = Duration::from_millis(1);

/// Waits on the paused clock until `n` failed accepts are logged, and returns
/// the instant it saw the last, at most `POLL` after it was written. Each
/// sleep lets the paused runtime move its clock on.
async fn wait_for_failures(logs: &LogCapture, n: usize) -> Instant {
    while failures(logs) < n {
        tokio::time::sleep(POLL).await;
    }
    Instant::now()
}

#[test]
fn a_failing_accept_pauses_and_hears_shutdown() {
    let logs = LogCapture::install_global(tracing::Level::DEBUG);
    let watched = logs.clone();
    let (done_tx, done_rx) = mpsc::channel();
    // A server that retries at once never leaves its runtime idle, so the
    // paused clock never moves and the test below would wait forever. The
    // test runs on a thread of its own and this one watches for that.
    #[expect(
        clippy::disallowed_methods,
        reason = "joined below, after the watch loop sees the test end"
    )]
    let test = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let health = HealthState::new(0, HealthThresholds::default());
            let server = AdminServer::bind("127.0.0.1:0".parse().expect("addr"), None, health)
                .await
                .expect("bind");
            let addr = server.local_addr();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            #[expect(
                clippy::disallowed_methods,
                reason = "the test awaits the server task before it returns"
            )]
            let running = tokio::spawn(server.run(shutdown_rx));

            // Each client takes the last descriptor, so the server's accept of
            // it fails. Some kernels drop the pending connection with the
            // failure, so each attempt gets a client of its own.
            let mut full = descriptors::fill();
            full.take_last();
            let first_client = tokio::net::TcpStream::connect(addr).await.expect("connect");
            let first = wait_for_failures(&logs, 1).await;
            drop(first_client);
            let _second_client = tokio::net::TcpStream::connect(addr).await.expect("connect");
            let second = wait_for_failures(&logs, 2).await;
            assert!(
                second - first + POLL >= ACCEPT_BACKOFF,
                "the server retried after {:?}",
                second - first
            );

            // The server is in its pause: a shutdown ends it with the paused
            // clock where it stands.
            let stopped_at = Instant::now();
            shutdown_tx.send(true).expect("signal shutdown");
            running.await.expect("the server task joins");
            assert_eq!(
                Instant::now(),
                stopped_at,
                "the shutdown waited out the pause"
            );
            full.release();
        });
        let _ = done_tx.send(());
    });
    let start = std::time::Instant::now();
    loop {
        match done_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let seen = failures(&watched);
                assert!(
                    seen < RUNAWAY,
                    "{seen} failed accepts with no pause between them"
                );
                assert!(start.elapsed() < HANG, "hung waiting for the server");
            }
        }
    }
    if let Err(panic) = test.join() {
        std::panic::resume_unwind(panic);
    }
}
