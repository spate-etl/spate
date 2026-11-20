//! The admin server serves on after `accept` fails because the process is out
//! of descriptors.
//!
//! One test in its own binary: it lowers the process's descriptor limit and
//! installs a process-wide subscriber, and `cargo test` shares one process
//! across a binary.
#![cfg(all(unix, not(loom)))]

mod common;

use std::time::Duration;

use common::descriptors;
use spate_core::admin::{AdminServer, HealthState, HealthThresholds};
use spate_test::{HANG, LogCapture};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::watch;

#[test]
fn the_admin_server_serves_on_after_accept_fails() {
    let logs = LogCapture::install_global(tracing::Level::WARN);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
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

        // The client takes the last descriptor, so the server's accept of it
        // fails. Some kernels drop the pending connection with the failure,
        // so the check below uses a fresh one.
        let mut full = descriptors::fill();
        full.take_last();
        let first = tokio::net::TcpStream::connect(addr).await.expect("connect");
        tokio::time::timeout(HANG, async {
            while !logs
                .lines()
                .iter()
                .any(|l| l.contains("cannot accept a connection"))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the failed accept is logged");

        // With descriptors free again, a new client is served.
        drop(first);
        full.release();
        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("the server still listens");
        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        let mut response = String::new();
        tokio::time::timeout(HANG, client.read_to_string(&mut response))
            .await
            .expect("an answer")
            .expect("read");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");

        shutdown_tx.send(true).expect("signal shutdown");
        running.await.expect("the server task joins");
    });
}
