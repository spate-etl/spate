//! Local benchmark infrastructure via the `docker` CLI.
//!
//! A reachable server (port/ping probe) is reused so repeated bench runs are
//! cheap — which means a server left RUNNING from a previous run is reused with
//! its OS page cache and ClickHouse query cache still warm. Only when nothing
//! answers is a **fresh** container started (any container of the same name is
//! force-removed first; see [`remove_container`]). Set `FRESH=1` to force that
//! remove+recreate even when a server already answers, restoring cold caches
//! for the server-CPU rig (`ch_native_format`).
//!
//! The server image is `CLICKHOUSE_IMAGE` (default the 26.3 LTS line), and the
//! container is capped at `CLICKHOUSE_CPUS` cores (default 8) so a co-located
//! client and server share a host predictably (see `ch_sink_saturation`).
//! Because reuse keys only on the port, a running container of the *wrong*
//! version is reused silently — pass `FRESH=1` (or remove the container) when
//! you need a specific image; rigs that care record `SELECT version()`.

use std::process::Command;
use std::time::{Duration, Instant};

/// Run the `docker` CLI, returning trimmed stdout. Panics with the argv and
/// stderr on a non-zero exit: a failed `docker run` (e.g. a rejected `--cpus`)
/// must fail loudly here, not surface later as a misleading 90s ping timeout.
fn docker(args: &[&str]) -> String {
    docker_try(args).unwrap_or_else(|stderr| panic!("docker {args:?} failed: {stderr}"))
}

/// Like [`docker`] but returns the trimmed stderr as `Err` on a non-zero exit
/// instead of panicking, for callers that tolerate failure (a `rm -f` of a
/// container that isn't there).
fn docker_try(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker CLI");
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_owned())
    }
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
    let _ = docker_try(&["rm", "-f", name]);
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

/// Resolve the broker to use and a name for the `broker` variant key.
///
/// `BOOTSTRAP` points at an external broker, whose implementation the rig
/// cannot detect — set `BROKER` alongside it to label the records, or they
/// record `external`, which is at least honest about not knowing.
#[must_use]
pub fn resolve_broker() -> (String, String) {
    match std::env::var("BOOTSTRAP") {
        Ok(bootstrap) => (
            bootstrap,
            std::env::var("BROKER").unwrap_or_else(|_| "external".to_owned()),
        ),
        Err(_) => {
            let (bootstrap, broker) = ensure_broker();
            (bootstrap, broker.name().to_owned())
        }
    }
}

/// Whether a container of this name is currently running.
fn container_running(name: &str) -> bool {
    docker_try(&["inspect", "-f", "{{.State.Running}}", name]).is_ok_and(|s| s == "true")
}

/// The bench broker implementation, selected by `BROKER`.
///
/// Redpanda is the default because it is not a JVM process: a source-ceiling
/// measurement is a broker-latency measurement, and a JVM broker injects
/// stop-the-world GC pauses into exactly the quantity being measured. Apache
/// Kafka remains available for cross-checking a result against the reference
/// implementation.
///
/// The choice is identity-defining for any throughput number, so rigs record
/// it as a variant key — a Redpanda figure and a Kafka figure must never
/// aggregate into one median.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Broker {
    /// `redpandadata/redpanda` — the default. No JVM, so no GC pause lands in
    /// the middle of a fetch measurement.
    Redpanda,
    /// `apache/kafka` — the reference implementation, for cross-checking.
    Kafka,
}

impl Broker {
    /// The `broker` variant value recorded alongside every measurement.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Broker::Redpanda => "redpanda",
            Broker::Kafka => "kafka",
        }
    }

    fn container(self) -> &'static str {
        match self {
            Broker::Redpanda => "etl-bench-redpanda",
            Broker::Kafka => "etl-bench-kafka",
        }
    }

    fn image(self) -> &'static str {
        match self {
            Broker::Redpanda => "redpandadata/redpanda:v26.1.13",
            Broker::Kafka => "apache/kafka:4.1.0",
        }
    }

    fn from_env() -> Self {
        match std::env::var("BROKER").as_deref().unwrap_or("redpanda") {
            "redpanda" => Broker::Redpanda,
            "kafka" => Broker::Kafka,
            other => panic!("unknown BROKER {other} (redpanda|kafka)"),
        }
    }
}

/// Ensure the bench broker is reachable on `localhost:9092`, starting it if
/// nothing answers. Returns the bootstrap string and which broker it is.
///
/// When `KAFKA_CPUS` is set, a freshly started container is capped at that
/// many cores (`--cpus=$KAFKA_CPUS`), mirroring [`ensure_clickhouse`]'s
/// `CLICKHOUSE_CPUS` so a co-located client and broker share the host
/// predictably (the `kafka_sink_saturation` broker-headroom budget). Unset
/// leaves the broker uncapped — the pre-existing behaviour the consumer rigs
/// rely on. A broker already RUNNING is reused as-is and `KAFKA_CPUS` is
/// ignored for it; pass `FRESH=1` to force a remove+recreate at the new cap.
///
/// Unlike the ClickHouse path, reuse does **not** key on the port alone: it
/// requires the *expected* container to be the one running. A Kafka container
/// left over from an earlier run answers 9092 exactly as Redpanda does, and
/// reusing it would record the run against a broker it never touched.
pub fn ensure_broker() -> (String, Broker) {
    let bootstrap = "localhost:9092".to_owned();
    let broker = Broker::from_env();
    let running = container_running(broker.container());
    // FRESH=1 forces a cold container even when a broker already answers.
    if !fresh_requested() && running && tcp_open("localhost", 9092) {
        if std::env::var("KAFKA_CPUS").is_ok() {
            eprintln!(
                "reusing running {}; KAFKA_CPUS is ignored for a reused \
                 container (pass FRESH=1 to recreate at the new cap)",
                broker.container()
            );
        }
        return (bootstrap, broker);
    }
    assert!(
        running || !tcp_open("localhost", 9092),
        "something already answers localhost:9092 but it is not {}. Stop it, or \
         point the rig at it with BOOTSTRAP so the run is not recorded against \
         a broker it never used.",
        broker.container()
    );
    // Both names are cleared: switching BROKER between runs otherwise leaves
    // the other implementation holding the port.
    remove_container("etl-bench-redpanda");
    remove_container("etl-bench-kafka");

    let cpus = std::env::var("KAFKA_CPUS").ok();
    let cpus_arg = cpus.as_ref().map(|c| format!("--cpus={c}"));
    eprintln!(
        "starting {} ({}{}) ...",
        broker.container(),
        broker.image(),
        cpus_arg
            .as_ref()
            .map(|a| format!(", {a}"))
            .unwrap_or_default()
    );
    let mut args: Vec<&str> = vec!["run", "-d", "--name", broker.container(), "-p", "9092:9092"];
    if let Some(arg) = &cpus_arg {
        args.push(arg);
    }
    args.push(broker.image());
    // Redpanda needs its listeners named explicitly, and `--overprovisioned`
    // is load-bearing rather than cosmetic here: by default Redpanda busy-polls
    // one core per shard, which on a co-located rig burns CPU the client under
    // test needs and turns a consumer measurement into a scheduling contest.
    let smp = std::env::var("REDPANDA_SMP").unwrap_or_else(|_| "8".to_owned());
    let redpanda_args = [
        "redpanda",
        "start",
        "--node-id",
        "0",
        "--check=false",
        "--overprovisioned",
        "--kafka-addr",
        "PLAINTEXT://0.0.0.0:9092",
        "--advertise-kafka-addr",
        "PLAINTEXT://localhost:9092",
        "--smp",
        &smp,
        "--memory",
        "4G",
        "--reserve-memory",
        "0M",
    ];
    if broker == Broker::Redpanda {
        args.extend_from_slice(&redpanda_args);
    }
    docker(&args);
    let deadline = Instant::now() + Duration::from_secs(90);
    while !tcp_open("localhost", 9092) {
        assert!(Instant::now() < deadline, "broker did not become reachable");
        std::thread::sleep(Duration::from_millis(500));
    }
    // Port-open precedes broker readiness; give the listener a beat.
    std::thread::sleep(Duration::from_secs(2));
    (bootstrap, broker)
}

/// Ensure a ClickHouse server on `localhost:18123` (HTTP), starting a
/// `$CLICKHOUSE_IMAGE` container (default the 26.3 LTS line, capped at
/// `$CLICKHOUSE_CPUS` cores; `etl-bench-clickhouse`, password `bench`) if
/// nothing answers `/ping`. Returns (host, port, user, password).
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
        let version = clickhouse_sql(&host, port, &creds.0, &creds.1, "SELECT version()")
            .map(|v| v.trim().to_owned())
            .unwrap_or_default();
        eprintln!(
            "reusing running etl-bench-clickhouse (server version {version}); \
             CLICKHOUSE_IMAGE/CLICKHOUSE_CPUS are ignored for a reused container"
        );
        return (host, port, creds.0, creds.1);
    }
    remove_container("etl-bench-clickhouse");
    let image = std::env::var("CLICKHOUSE_IMAGE")
        .unwrap_or_else(|_| "clickhouse/clickhouse-server:26.3".to_owned());
    let cpus = std::env::var("CLICKHOUSE_CPUS").unwrap_or_else(|_| "8".to_owned());
    let cpus_arg = format!("--cpus={cpus}");
    eprintln!("starting etl-bench-clickhouse ({image}, --cpus={cpus}) ...");
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
        &cpus_arg,
        &image,
    ]);
    assert!(
        ping(Duration::from_secs(90)),
        "clickhouse did not become reachable"
    );
    (host, port, creds.0, creds.1)
}

/// Resolve a ClickHouse connection from `CLICKHOUSE_URL` (+ `CLICKHOUSE_USER` /
/// `CLICKHOUSE_PASSWORD`) or the local bench container, as
/// `(url, host, port, user, password)`. Only the `http://host:port` form is
/// supported — https and bare `host:port` fail fast with a clear message.
pub fn resolve_clickhouse() -> (String, String, u16, String, String) {
    match std::env::var("CLICKHOUSE_URL").ok() {
        Some(url) => {
            let rest = url.strip_prefix("http://").unwrap_or_else(|| {
                panic!(
                    "CLICKHOUSE_URL must be http://host:port (got {url:?}); \
                     https and bare host:port are unsupported"
                )
            });
            let (h, p) = rest
                .split_once(':')
                .unwrap_or_else(|| panic!("CLICKHOUSE_URL must be http://host:port (got {url:?})"));
            let port = p
                .parse::<u16>()
                .unwrap_or_else(|_| panic!("CLICKHOUSE_URL port not a u16 (got {p:?})"));
            (
                url.clone(),
                h.to_owned(),
                port,
                crate::env_str("CLICKHOUSE_USER", "default"),
                crate::env_str("CLICKHOUSE_PASSWORD", ""),
            )
        }
        None => {
            let (h, p, u, pw) = ensure_clickhouse();
            (format!("http://{h}:{p}"), h, p, u, pw)
        }
    }
}

/// Run one SQL statement against ClickHouse over HTTP, returning the raw
/// response body — which may itself carry a `DB::Exception`. Transport
/// failures are `Err`; keeps the `BENCH_SQL_DEBUG` trace hook. Best-effort
/// readers (chstats, log flushes) inspect the body themselves; callers that
/// must fail on a server exception use [`clickhouse_sql`].
///
/// Uses POST: ClickHouse treats HTTP GET as readonly and silently rejects
/// DDL/inserts.
pub fn try_clickhouse_sql(
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
    Ok(body)
}

/// Run one SQL statement, panicking on a server exception so a misconfigured
/// bench fails loudly instead of producing a zero-row "result".
pub fn clickhouse_sql(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
) -> std::io::Result<String> {
    let body = try_clickhouse_sql(host, port, user, password, sql)?;
    assert!(
        !body.contains("DB::Exception"),
        "clickhouse error for {sql:?}: {body}"
    );
    Ok(body)
}
