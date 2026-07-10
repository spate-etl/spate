//! Local benchmark infrastructure via the `docker` CLI.
//!
//! A reachable server (port/ping probe) is reused so repeated bench runs are
//! cheap — which means a server left RUNNING from a previous run is reused with
//! its OS page cache and ClickHouse query cache still warm. Only when nothing
//! answers is a **fresh** container started (any container of the same name is
//! force-removed first; see [`remove_container`]). Set `FRESH=1` to force that
//! remove+recreate even when a server already answers, restoring cold caches
//! for the server-CPU rig (`ch_native_format`).

use std::process::Command;
use std::time::{Duration, Instant};

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker CLI");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Force-remove any container of this name (running or exited), ignoring a
/// "no such container" miss.
///
/// Called before every `docker run`. A stopped/exited container of the same
/// name — the normal state after an interrupted or crashed run — would make
/// `docker run --name` fail with a name conflict. Starting fresh here (rather
/// than `docker start`-ing a stopped one) gives the new container cold OS and
/// query caches, which the server-CPU measurements in `ch_native_format` rely
/// on. This only applies on the fresh-start path, though: a server still
/// RUNNING from a previous run is reused as-is (warm caches) unless `FRESH=1`
/// forces a remove+recreate.
fn remove_container(name: &str) {
    let _ = docker(&["rm", "-f", name]);
}

/// `FRESH=1` forces a remove+recreate even when a server already answers,
/// giving the new container cold OS/page and query caches.
fn fresh_requested() -> bool {
    std::env::var("FRESH").is_ok_and(|v| v == "1")
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
    // FRESH=1 forces a cold container even when a broker already answers.
    if !fresh_requested() && tcp_open("localhost", 9092) {
        return bootstrap;
    }
    remove_container("etl-bench-kafka");
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
    // FRESH=1 forces a cold container even when a server already answers.
    if !fresh_requested() && ping(Duration::from_millis(600)) {
        return (host, port, creds.0, creds.1);
    }
    remove_container("etl-bench-clickhouse");
    eprintln!("starting etl-bench-clickhouse (clickhouse-server:25.6) ...");
    docker(&[
        "run",
        "-d",
        "--name",
        "etl-bench-clickhouse",
        "-p",
        "18123:8123", // HTTP
        "-p",
        "19000:9000", // native protocol
        "-e",
        "CLICKHOUSE_PASSWORD=bench",
        "--ulimit",
        "nofile=262144:262144",
        "clickhouse/clickhouse-server:25.6",
    ]);
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
