//! Local benchmark infrastructure via the `docker` CLI.
//!
//! Containers are reused if already running (name-based) so repeated bench
//! runs are cheap; set `KEEP=0` semantics are left to the caller — these
//! helpers never tear down.

use std::process::Command;
use std::time::{Duration, Instant};

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker CLI");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn container_running(name: &str) -> bool {
    !docker(&["ps", "-q", "--filter", &format!("name=^{name}$")]).is_empty()
}

fn tcp_open(host: &str, port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("{host}:{port}").parse().expect("addr"),
        Duration::from_millis(300),
    )
    .is_ok()
}

/// Ensure a Kafka broker is reachable on `localhost:9092`, starting an
/// `apache/kafka:4.1.0` container (`etl-bench-kafka`) if nothing answers.
/// Returns the bootstrap string.
pub fn ensure_kafka() -> String {
    let bootstrap = "localhost:9092".to_owned();
    if tcp_open("localhost", 9092) {
        return bootstrap;
    }
    if !container_running("etl-bench-kafka") {
        eprintln!("starting etl-bench-kafka (apache/kafka:4.1.0) ...");
        docker(&[
            "run",
            "-d",
            "--name",
            "etl-bench-kafka",
            "-p",
            "9092:9092",
            "apache/kafka:4.1.0",
        ]);
    }
    let deadline = Instant::now() + Duration::from_secs(90);
    while !tcp_open("localhost", 9092) {
        assert!(Instant::now() < deadline, "kafka did not become reachable");
        std::thread::sleep(Duration::from_millis(500));
    }
    // Port-open precedes broker readiness; give the listener a beat.
    std::thread::sleep(Duration::from_secs(2));
    bootstrap
}

/// Ensure a ClickHouse server on `localhost:18123` (HTTP), starting a
/// `clickhouse/clickhouse-server:25.6` container (`etl-bench-clickhouse`,
/// password `bench`) if nothing answers `/ping`. Returns (host, port,
/// user, password).
pub fn ensure_clickhouse() -> (String, u16, String, String) {
    let (host, port) = ("localhost".to_owned(), 18123u16);
    let creds = ("default".to_owned(), "bench".to_owned());
    let ping = |timeout: Duration| -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if crate::http_get(&host, port, "/ping").is_ok_and(|b| b.contains("Ok")) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    };
    if ping(Duration::from_millis(600)) {
        return (host, port, creds.0, creds.1);
    }
    if !container_running("etl-bench-clickhouse") {
        eprintln!("starting etl-bench-clickhouse (clickhouse-server:25.6) ...");
        docker(&[
            "run",
            "-d",
            "--name",
            "etl-bench-clickhouse",
            "-p",
            "18123:8123",
            "-e",
            "CLICKHOUSE_PASSWORD=bench",
            "--ulimit",
            "nofile=262144:262144",
            "clickhouse/clickhouse-server:25.6",
        ]);
    }
    assert!(
        ping(Duration::from_secs(90)),
        "clickhouse did not become reachable"
    );
    (host, port, creds.0, creds.1)
}

/// Run one SQL statement against ClickHouse over HTTP; returns the body.
pub fn clickhouse_sql(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
) -> std::io::Result<String> {
    let query: String = sql
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            b' ' => vec!['+'],
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect();
    crate::http_get(
        host,
        port,
        &format!("/?user={user}&password={password}&query={query}"),
    )
}
