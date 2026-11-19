//! The admin server holds at most `MAX_CONNECTIONS` client connections,
//! however many clients connect, and still answers a probe behind them. A
//! connection past the cap closes the oldest open one, and only then.
//!
//! One test in its own binary: it raises the process's descriptor limit and
//! installs a process-wide subscriber, and `cargo test` shares one process
//! across a binary.
#![cfg(all(unix, not(loom)))]

mod common;

use std::io::{ErrorKind, Write as _};
use std::net::{SocketAddr, TcpStream};

use common::descriptors::{open_fds, raise_to};
use spate_core::admin::MAX_CONNECTIONS;
use spate_core::config::PipelineConfig;
use spate_core::pipeline::{Pipeline, RuntimeOptions};
use spate_test::{
    HANG, LogCapture, PipelineRun, TestEncoder, capture_sink, memory_source, passthrough_runtime,
    unique_name, wait_until,
};
use spate_test_support::http;

/// Whether the server has closed `stream`, read without blocking.
fn closed(stream: &TcpStream) -> bool {
    stream.set_nonblocking(true).expect("nonblocking");
    let mut byte = [0u8; 1];
    match stream.peek(&mut byte) {
        Ok(0) => true,
        Ok(_) => panic!("the server answered an unfinished head"),
        Err(e) if e.kind() == ErrorKind::WouldBlock => false,
        Err(e) if e.kind() == ErrorKind::ConnectionReset => true,
        Err(e) => panic!("peek: {e}"),
    }
}

/// A client holding a request head it never finishes.
fn slow_client(addr: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.write_all(b"GET /healthz HTTP/1.1\r\nX-Pad: aaaa")
        .expect("write head");
    s
}

#[test]
fn admin_clients_hold_a_bounded_number_of_server_descriptors() {
    // Each client below and the server's side of it, with room to spare.
    raise_to(4 * MAX_CONNECTIONS);
    let logs = LogCapture::install_global(tracing::Level::DEBUG);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let config = PipelineConfig::from_str(&format!(
        "pipeline: {{ name: {name}, threads: 1, io_threads: 1 }}\n\
         admin: {{ listen: 127.0.0.1:{port} }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ memory: {{}} }}\n\
         sink: {{ capture: {{}} }}\n",
        name = unique_name("admin-conns"),
    ))
    .expect("config");
    let (source, _handle) = memory_source();
    let (sink, _script) = capture_sink(1, 1);
    let pipeline = Pipeline::from_config(config).expect("pipeline");
    let runtime = passthrough_runtime(
        pipeline,
        source,
        sink,
        TestEncoder,
        RuntimeOptions::default(),
    );
    let shutdown = runtime.shutdown_handle();
    let run = PipelineRun::spawn(move || runtime.run());
    // The line follows the bind, so the port takes connections from here on.
    wait_until(HANG, "the admin server listens", || {
        logs.lines()
            .iter()
            .any(|l| l.contains("admin server listening"))
    });
    let (status, _) = http(addr, "GET", "/healthz").expect("the first probe");
    assert_eq!(status, 200);

    // More clients than the cap, each with a request head it never finishes.
    let offered = MAX_CONNECTIONS + 32;
    let before = open_fds();
    let held: Vec<TcpStream> = (0..offered).map(|_| slow_client(addr)).collect();
    // Accept is FIFO: a probe behind the held set is answered once the server
    // has taken every one of them.
    let (status, _) = http(addr, "GET", "/healthz").expect("the probe behind the clients");
    assert_eq!(status, 200);
    // The probe closed one more: the oldest `offered + 1 - MAX_CONNECTIONS`
    // clients are closed and the newest still open. A closed connection gives
    // its descriptor back when its task is next polled, so the count settles
    // rather than drops at once.
    let evicted = offered + 1 - MAX_CONNECTIONS;
    wait_until(HANG, "the oldest clients are closed", || {
        held[..evicted].iter().all(closed)
    });
    let still_open = held[evicted..].iter().filter(|s| !closed(s)).count();
    assert_eq!(still_open, offered - evicted, "only the oldest are closed");
    wait_until(HANG, "the server holds at most the cap", || {
        open_fds().saturating_sub(before + held.len()) <= MAX_CONNECTIONS
    });
    // Each connection's own deadline also closes it, after 30 s. None got
    // that far: the cap closed them.
    let lines = logs.lines();
    assert!(
        !lines.iter().any(|l| l.contains("closed at its deadline")),
        "the deadline, not the cap, closed a connection"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("admin connections at the limit")),
        "the eviction is logged"
    );

    // Once the clients leave, the server holds none of them and serves on.
    drop(held);
    wait_until(HANG, "the server releases every connection", || {
        open_fds() <= before
    });
    let (status, _) = http(addr, "GET", "/healthz").expect("a probe after the clients");
    assert_eq!(status, 200);

    // A connection that has ended no longer counts: one slow client stays
    // open across more probes than the cap, since none of them is open at
    // the same time as many others.
    let slow = slow_client(addr);
    for _ in 0..=MAX_CONNECTIONS {
        let (status, _) = http(addr, "GET", "/healthz").expect("a probe");
        assert_eq!(status, 200);
    }
    assert!(
        !closed(&slow),
        "an ended connection still counted against the cap"
    );
    drop(slow);

    shutdown.trigger();
    run.wait_exit(HANG)
        .expect("the pipeline exits")
        .expect("clean run");
}
