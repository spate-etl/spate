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
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .is_some_and(|addr| {
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok()
        })
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

/// Run one SQL statement against ClickHouse over HTTP.
///
/// Uses POST: ClickHouse treats HTTP GET as readonly and silently rejects
/// DDL/inserts. Panics on a server exception so a misconfigured bench
/// fails loudly instead of producing a zero-row "result".
pub fn clickhouse_sql(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
) -> std::io::Result<String> {
    let body = crate::http_post(
        host,
        port,
        &format!("/?user={user}&password={password}"),
        sql,
    )?;
    if std::env::var("BENCH_SQL_DEBUG").is_ok() {
        eprintln!("SQL {sql:?} @ {host}:{port} -> {body:?}");
    }
    assert!(
        !body.contains("DB::Exception"),
        "clickhouse error for {sql:?}: {body}"
    );
    Ok(body)
}
