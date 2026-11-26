//! Admin HTTP server: `/metrics`, `/healthz`, and `/readyz`.
//!
//! One small hyper server per process, running on the I/O runtime. Probe
//! semantics:
//!
//! - **`/readyz`** — 200 once the source has received its assignment and
//!   the sinks are connected.
//! - **`/healthz`** — 200 while every pipeline thread's poll-loop heartbeat
//!   is fresh AND no checkpoint watermark has stalled behind a failed batch
//!   past the threshold while data flows. A stalled poll loop or a stalled
//!   watermark flips this to 503 so the kubelet restarts the pod.
//!
//! The server takes the metrics renderer as a plain closure
//! ([`RenderFn`]) and has no dependency on exporter internals. The probes
//! are served regardless of the exporter; `/metrics` exists only when a
//! renderer is supplied, and is a 404 otherwise.
//!
//! The port has no authentication, so every limit below holds against any
//! client that can reach it. A connection carries one request and closes
//! after [`CONNECTION_DEADLINE`] at the latest. The server serves at most
//! [`MAX_CONNECTIONS`] at once: a new connection past that closes the oldest, so a
//! probe is served while slow clients hold the rest. An error from
//! `accept` is logged and retried; it never stops the server.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::clock::tokio::{Clock as _, Sleep, SystemClock as RuntimeClock, before};
use crate::clock::{SharedClock, SystemClock};
use crate::telemetry::RateLimit;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{AbortHandle, Id, JoinSet};

/// Renders the current metrics exposition snapshot.
pub type RenderFn = Arc<dyn Fn() -> String + Send + Sync>;

/// The most connections the admin server serves at once, across all clients.
/// A connection accepted past this closes the oldest one it serves. Its socket
/// closes when the runtime next polls its task.
pub const MAX_CONNECTIONS: usize = 256;

/// The longest the admin server keeps one connection open, from accept to
/// close, whatever the client sends or reads in that time.
pub const CONNECTION_DEADLINE: Duration = Duration::from_secs(30);

/// How long the admin server waits before accepting again after `accept`
/// fails for a reason other than the one connection, such as the process
/// running out of descriptors.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// hyper's read buffer per connection, its minimum. A probe or scrape
/// request head fits many times over.
const READ_BUFFER: usize = 8 * 1024;

/// Staleness thresholds for the liveness probe.
#[derive(Clone, Copy, Debug)]
pub struct HealthThresholds {
    /// A pipeline thread whose heartbeat is older than this is considered
    /// stalled.
    pub heartbeat_stale: Duration,
    /// The watermark is considered stuck when it has stalled behind a failed
    /// batch for longer than this while data is flowing.
    pub watermark_stuck: Duration,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        HealthThresholds {
            heartbeat_stale: Duration::from_secs(30),
            watermark_stuck: Duration::from_secs(300),
        }
    }
}

/// Shared health state, updated by the pipeline runtime and read by the
/// probes. All methods are lock-free and callable from any thread.
pub struct HealthState {
    clock: SharedClock,
    epoch: Instant,
    thresholds: HealthThresholds,
    assignment_received: AtomicBool,
    sinks_connected: AtomicBool,
    /// Per-pipeline-thread heartbeat, as millis since `epoch`.
    heartbeats: Vec<AtomicU64>,
    /// How long the oldest watermark stalled behind a failed batch has
    /// waited, in millis. Zero when no watermark is stalled.
    watermark_age_ms: AtomicU64,
    data_flowing: AtomicBool,
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState")
            .field("epoch", &self.epoch)
            .field("thresholds", &self.thresholds)
            .field("assignment_received", &self.assignment_received)
            .field("sinks_connected", &self.sinks_connected)
            .field("heartbeats", &self.heartbeats)
            .field("watermark_age_ms", &self.watermark_age_ms)
            .field("data_flowing", &self.data_flowing)
            .finish_non_exhaustive()
    }
}

impl HealthState {
    /// Create state for `pipeline_threads` heartbeating threads. All
    /// heartbeats start "fresh" at creation time.
    #[must_use]
    pub fn new(pipeline_threads: usize, thresholds: HealthThresholds) -> Arc<Self> {
        Self::with_clock(pipeline_threads, thresholds, Arc::new(SystemClock))
    }

    /// [`new`](Self::new), reading heartbeats and liveness on `clock`.
    pub(crate) fn with_clock(
        pipeline_threads: usize,
        thresholds: HealthThresholds,
        clock: SharedClock,
    ) -> Arc<Self> {
        Arc::new(HealthState {
            epoch: clock.now(),
            clock,
            thresholds,
            assignment_received: AtomicBool::new(false),
            sinks_connected: AtomicBool::new(false),
            heartbeats: (0..pipeline_threads).map(|_| AtomicU64::new(0)).collect(),
            watermark_age_ms: AtomicU64::new(0),
            data_flowing: AtomicBool::new(false),
        })
    }

    fn millis_since_epoch(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.epoch).as_millis()).unwrap_or(u64::MAX)
    }

    /// Beat thread `thread`'s heart on the clock given at construction, the
    /// system clock from [`new`](Self::new). Called once per poll iteration;
    /// out of range indices are ignored.
    pub fn heartbeat(&self, thread: usize) {
        if let Some(hb) = self.heartbeats.get(thread) {
            hb.store(self.millis_since_epoch(self.clock.now()), Ordering::Relaxed);
        }
    }

    /// How long ago thread `thread` last beat, to the millisecond, on the
    /// clock given at construction. `None` for an index out of range.
    pub(crate) fn heartbeat_age(&self, thread: usize) -> Option<Duration> {
        self.heartbeat_age_at(thread, self.clock.now())
    }

    fn heartbeat_age_at(&self, thread: usize, now: Instant) -> Option<Duration> {
        let beat = self.heartbeats.get(thread)?.load(Ordering::Relaxed);
        Some(Duration::from_millis(
            self.millis_since_epoch(now).saturating_sub(beat),
        ))
    }

    /// Mark whether the source has received its (initial) assignment.
    pub fn set_assignment_received(&self, received: bool) {
        self.assignment_received.store(received, Ordering::Relaxed);
    }

    /// Mark whether all sinks report connectivity.
    pub fn set_sinks_connected(&self, connected: bool) {
        self.sinks_connected.store(connected, Ordering::Relaxed);
    }

    /// Report how long the longest-stalled watermark has waited behind a
    /// failed batch, and whether data is currently flowing (a stuck
    /// watermark only counts against liveness while records move).
    pub fn report_watermark(&self, age: Duration, data_flowing: bool) {
        self.watermark_age_ms.store(
            u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.data_flowing.store(data_flowing, Ordering::Relaxed);
    }

    /// Readiness: assignment received and sinks connected.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.assignment_received.load(Ordering::Relaxed)
            && self.sinks_connected.load(Ordering::Relaxed)
    }

    /// Liveness now, on the clock given at construction, the system clock
    /// from [`new`](Self::new).
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.healthy_at(self.clock.now())
    }

    /// Liveness as of `now` (injectable for tests).
    #[must_use]
    pub fn healthy_at(&self, now: Instant) -> bool {
        let hearts_fresh = (0..self.heartbeats.len()).all(|thread| {
            self.heartbeat_age_at(thread, now)
                .is_some_and(|age| age <= self.thresholds.heartbeat_stale)
        });

        let stuck_ms =
            u64::try_from(self.thresholds.watermark_stuck.as_millis()).unwrap_or(u64::MAX);
        let watermark_stuck = self.data_flowing.load(Ordering::Relaxed)
            && self.watermark_age_ms.load(Ordering::Relaxed) > stuck_ms;

        hearts_fresh && !watermark_stuck
    }
}

/// The admin server, bound and ready to run.
pub struct AdminServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    render: Option<RenderFn>,
    health: Arc<HealthState>,
}

impl std::fmt::Debug for AdminServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminServer")
            .field("local_addr", &self.local_addr)
            .field("health", &self.health)
            .finish_non_exhaustive()
    }
}

impl AdminServer {
    /// Bind the admin socket. Use port 0 to let the OS choose (tests).
    ///
    /// `render` of `None` serves the probes without `/metrics`.
    pub async fn bind(
        addr: SocketAddr,
        render: Option<RenderFn>,
        health: Arc<HealthState>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(AdminServer {
            listener,
            local_addr,
            render,
            health,
        })
    }

    /// The bound address (relevant with port 0).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and serve connections until `shutdown` observes `true`.
    /// Every open connection is closed when this returns.
    ///
    /// Runs on a tokio runtime with its time driver enabled, which the
    /// connection deadline and the accept backoff read.
    ///
    /// # Panics
    ///
    /// Panics on a failed accept when the runtime was built without its time
    /// driver, through `enable_time` or `enable_all`. On such a runtime each
    /// connection also closes unanswered, and its panic is logged.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut conns = JoinSet::new();
        let mut live = Live::new(MAX_CONNECTIONS);
        let evicted = RateLimit::hot_path();
        let refused = RateLimit::hot_path();
        let mut backoff: Option<Sleep> = None;
        loop {
            tokio::select! {
                accepted = self.listener.accept(), if backoff.is_none() => {
                    let stream = match accepted {
                        Ok((stream, _peer)) => stream,
                        Err(err) => {
                            tracing::debug!(error = %err, "admin accept failed");
                            if accept_error(&err) == AcceptError::Backoff {
                                crate::rate_limited_warn!(
                                    refused,
                                    error = %err,
                                    "the admin server cannot accept a connection; retrying"
                                );
                                backoff = Some(RuntimeClock.sleep_until(RuntimeClock.now() + ACCEPT_BACKOFF));
                            }
                            continue;
                        }
                    };
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "the set is local to `run`, so returning drops it and aborts every open connection"
                    )]
                    let handle = conns.spawn(serve(
                        hyper_util::rt::TokioIo::new(stream),
                        self.render.clone(),
                        Arc::clone(&self.health),
                    ));
                    if let Some(oldest) = live.admit(handle) {
                        oldest.abort();
                        crate::rate_limited_warn!(
                            evicted,
                            limit = MAX_CONNECTIONS,
                            "admin connections at the limit; closed the oldest"
                        );
                    }
                }
                () = async { backoff.as_mut().expect("guarded by the branch condition").await },
                    if backoff.is_some() =>
                {
                    backoff = None;
                }
                Some(joined) = conns.join_next_with_id(), if !conns.is_empty() => {
                    match joined {
                        Ok((id, ())) => live.finished(id),
                        Err(err) => {
                            live.finished(err.id());
                            if let Ok(payload) = err.try_into_panic() {
                                tracing::error!(
                                    panic = crate::task::panic_text(&*payload),
                                    "admin connection panicked"
                                );
                            }
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        // Aborting alone leaves each connection open until its task is next
        // polled; waiting here closes them before `run` returns.
        conns.shutdown().await;
    }
}

/// Serves one admin connection: one request, closed by
/// [`CONNECTION_DEADLINE`] at the latest.
pub(crate) async fn serve<IO>(io: IO, render: Option<RenderFn>, health: Arc<HealthState>)
where
    IO: hyper::rt::Read + hyper::rt::Write + Unpin,
{
    let service = service_fn(move |req| {
        let render = render.clone();
        let health = Arc::clone(&health);
        async move { respond(&req, render.as_ref(), &health) }
    });
    let mut builder = http1::Builder::new();
    builder.keep_alive(false).max_buf_size(READ_BUFFER);
    let conn = builder.serve_connection(io, service);
    let deadline = RuntimeClock.now() + CONNECTION_DEADLINE;
    match before(&RuntimeClock, deadline, conn).await {
        Some(Ok(())) => {}
        Some(Err(err)) => tracing::debug!(error = %err, "admin connection error"),
        None => tracing::debug!("admin connection closed at its deadline"),
    }
}

/// What the accept loop does after `accept` fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptError {
    /// The failure belongs to the one pending connection: accept the next.
    Connection,
    /// The failure is the process's, such as running out of descriptors:
    /// wait [`ACCEPT_BACKOFF`] before accepting again.
    Backoff,
}

fn accept_error(err: &io::Error) -> AcceptError {
    match err.kind() {
        io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset => AcceptError::Connection,
        _ => AcceptError::Backoff,
    }
}

/// The open connections, oldest first, held to a cap.
#[derive(Debug)]
struct Live {
    cap: usize,
    open: VecDeque<AbortHandle>,
}

impl Live {
    fn new(cap: usize) -> Self {
        Live {
            cap,
            open: VecDeque::with_capacity(cap),
        }
    }

    /// Records a newly accepted connection. At the cap, returns the oldest
    /// open connection, which the caller closes.
    fn admit(&mut self, handle: AbortHandle) -> Option<AbortHandle> {
        let oldest = (self.open.len() >= self.cap)
            .then(|| self.open.pop_front())
            .flatten();
        self.open.push_back(handle);
        oldest
    }

    /// Forgets a connection whose task has ended.
    fn finished(&mut self, id: Id) {
        if let Some(at) = self.open.iter().position(|h| h.id() == id) {
            self.open.remove(at);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.open.len()
    }
}

fn respond(
    req: &Request<hyper::body::Incoming>,
    render: Option<&RenderFn>,
    health: &HealthState,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if req.method() != Method::GET {
        return Ok(plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"));
    }
    Ok(match (req.uri().path(), render) {
        ("/metrics", Some(render)) => {
            let body = render();
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    hyper::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                .body(Full::new(Bytes::from(body)))
                .expect("static response parts are valid")
        }
        ("/healthz", _) => probe(health.healthy()),
        ("/readyz", _) => probe(health.ready()),
        _ => plain(StatusCode::NOT_FOUND, "not found"),
    })
}

fn probe(ok: bool) -> Response<Full<Bytes>> {
    if ok {
        plain(StatusCode::OK, "ok")
    } else {
        plain(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    }
}

fn plain(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("static response parts are valid")
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test task ends when the test's runtime drops"
)]
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    fn thresholds(stale_ms: u64, stuck_ms: u64) -> HealthThresholds {
        HealthThresholds {
            heartbeat_stale: Duration::from_millis(stale_ms),
            watermark_stuck: Duration::from_millis(stuck_ms),
        }
    }

    /// A heartbeat is stamped on the injected clock, and stays fresh up to
    /// and including the stale threshold after it.
    #[test]
    fn a_heartbeat_reads_the_injected_clock() {
        let clock = crate::clock::TestClock::frozen();
        let health = HealthState::with_clock(1, thresholds(1000, 60_000), clock.clone());
        clock.advance(Duration::from_millis(300));
        health.heartbeat(0);
        assert!(health.healthy_at(health.epoch + Duration::from_millis(1300)));
        assert!(!health.healthy_at(health.epoch + Duration::from_millis(1301)));
    }

    /// A thread's heartbeat age is the time since its last beat on the
    /// injected clock, and an index past the thread count has none.
    #[test]
    fn a_heartbeat_age_counts_from_the_last_beat() {
        let clock = crate::clock::TestClock::frozen();
        let health = HealthState::with_clock(2, thresholds(1000, 60_000), clock.clone());
        clock.advance(Duration::from_millis(300));
        health.heartbeat(1);
        clock.advance(Duration::from_millis(950));
        assert_eq!(health.heartbeat_age(0), Some(Duration::from_millis(1250)));
        assert_eq!(health.heartbeat_age(1), Some(Duration::from_millis(950)));
        assert_eq!(health.heartbeat_age(2), None);
    }

    /// Liveness reads the injected clock: a heartbeat goes stale once that
    /// clock moves past the threshold, with no real time passing.
    #[test]
    fn liveness_reads_the_injected_clock() {
        let clock = crate::clock::TestClock::frozen();
        let health = HealthState::with_clock(1, thresholds(1000, 60_000), clock.clone());
        health.heartbeat(0);
        assert!(health.healthy());
        clock.advance(Duration::from_millis(1001));
        assert!(!health.healthy());
    }

    /// The clock it holds leaves `HealthState` `UnwindSafe` and
    /// `RefUnwindSafe`.
    #[test]
    fn health_state_stays_unwind_safe() {
        fn unwind_safe<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        unwind_safe::<HealthState>();
    }

    /// The hand-written `Debug` names the type and its thresholds.
    #[test]
    fn debug_names_the_health_state_and_its_thresholds() {
        let text = format!("{:?}", HealthState::new(1, thresholds(1000, 2000)));
        assert!(text.starts_with("HealthState {"), "{text}");
        assert!(text.contains("heartbeat_stale: 1s"), "{text}");
    }

    #[test]
    fn readiness_requires_assignment_and_sinks() {
        let health = HealthState::new(2, HealthThresholds::default());
        assert!(!health.ready());
        health.set_assignment_received(true);
        assert!(!health.ready());
        health.set_sinks_connected(true);
        assert!(health.ready());
        health.set_sinks_connected(false);
        assert!(!health.ready());
    }

    #[test]
    fn liveness_detects_a_stale_heartbeat() {
        let health = HealthState::new(2, thresholds(1_000, 300_000));
        let start = health.epoch;
        // Fresh at creation.
        assert!(health.healthy_at(start + Duration::from_millis(500)));
        // Thread 1 beats, thread 0 does not: stale after the threshold.
        health.heartbeat(1);
        assert!(!health.healthy_at(start + Duration::from_millis(1_501)));
        // Both beating again: healthy shortly after.
        health.heartbeat(0);
        health.heartbeat(1);
        assert!(health.healthy_at(Instant::now()));
        // Out-of-range heartbeat is ignored, not a panic.
        health.heartbeat(99);
    }

    #[test]
    fn liveness_detects_a_stuck_watermark_only_while_data_flows() {
        let health = HealthState::new(0, thresholds(60_000, 1_000));
        let now = Instant::now();
        health.report_watermark(Duration::from_secs(5), false);
        assert!(health.healthy_at(now), "idle pipeline is not stuck");
        health.report_watermark(Duration::from_secs(5), true);
        assert!(!health.healthy_at(now), "wedged watermark under flow");
        health.report_watermark(Duration::from_millis(500), true);
        assert!(health.healthy_at(now));
    }

    /// Minimal HTTP/1.1 GET over the readiness API, on a real socket.
    async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let req = format!("GET {path} HTTP/1.1\r\nHost: admin\r\nConnection: close\r\n\r\n");
        let mut written = 0;
        while written < req.len() {
            stream.writable().await.expect("writable");
            match stream.try_write(&req.as_bytes()[written..]) {
                Ok(n) => written += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("write failed: {e}"),
            }
        }
        let mut buf = Vec::new();
        loop {
            stream.readable().await.expect("readable");
            let mut chunk = [0u8; 4096];
            match stream.try_read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("read failed: {e}"),
            }
        }
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("status code");
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_owned())
            .unwrap_or_default();
        (status, body)
    }

    /// Writes all of `bytes` to `stream`.
    async fn write_all(stream: &tokio::net::TcpStream, bytes: &[u8]) {
        let mut written = 0;
        while written < bytes.len() {
            stream.writable().await.expect("writable");
            match stream.try_write(&bytes[written..]) {
                Ok(n) => written += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("write failed: {e}"),
            }
        }
    }

    /// Reads from `stream` until `done` holds for what was read, or the peer
    /// closes. Returns what was read.
    async fn read_until(stream: &tokio::net::TcpStream, done: impl Fn(&[u8]) -> bool) -> Vec<u8> {
        let mut buf = Vec::new();
        while !done(&buf) {
            stream.readable().await.expect("readable");
            let mut chunk = [0u8; 4096];
            match stream.try_read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(e) => panic!("read failed: {e}"),
            }
        }
        buf
    }

    /// Pins that a connection still open is closed by the time `run`
    /// returns: the client reads end of stream and the connection releases
    /// the health state it held.
    #[tokio::test]
    async fn an_open_connection_ends_when_run_returns() {
        let health = HealthState::new(0, HealthThresholds::default());
        let server = AdminServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            None,
            Arc::clone(&health),
        )
        .await
        .expect("bind");
        let addr = server.local_addr();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (client_tx, client_rx) = oneshot::channel();
        let held = Arc::clone(&health);
        let _client = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
            // A head the server waits on: the connection stays open.
            write_all(&stream, b"GET /healthz HTTP/1.1\r\nHost: admin\r\n").await;
            // Held here, by the test and by the server: a fourth holder is
            // the connection's task, once accepted.
            while Arc::strong_count(&held) < 4 {
                tokio::task::yield_now().await;
            }
            drop(held);
            shutdown_tx.send(true).expect("signal shutdown");
            let _ = client_tx.send(stream);
        });

        // Awaited inline, so no other task runs between `run` returning and
        // the count below.
        server.run(shutdown_rx).await;
        assert_eq!(
            Arc::strong_count(&health),
            1,
            "the connection still holds the health state"
        );

        let stream = client_rx.await.expect("the client finished");
        let rest = tokio::time::timeout(Duration::from_secs(30), read_until(&stream, |_| false))
            .await
            .expect("the connection closes when run returns");
        assert_eq!(rest, b"", "no response to an unfinished head");
    }

    /// Pins that a connection whose request panics is logged once at
    /// `ERROR` with the panic's text, while the server keeps running.
    #[test]
    fn a_panicking_connection_is_logged() {
        #[derive(Clone)]
        struct Lines(mpsc::UnboundedSender<String>);
        impl std::io::Write for Lines {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let _ = self.0.send(String::from_utf8_lossy(bytes).into_owned());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (lines_tx, mut lines_rx) = mpsc::unbounded_channel();
        let writer = Lines(lines_tx);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_max_level(tracing::Level::ERROR)
            .finish();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let health = HealthState::new(0, HealthThresholds::default());
                let render: RenderFn = Arc::new(|| panic!("render boom"));
                let server =
                    AdminServer::bind("127.0.0.1:0".parse().expect("addr"), Some(render), health)
                        .await
                        .expect("bind");
                let addr = server.local_addr();
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                let running = tokio::spawn(server.run(shutdown_rx));

                let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
                write_all(&stream, b"GET /metrics HTTP/1.1\r\nHost: admin\r\n\r\n").await;
                let reply = read_until(&stream, |_| false).await;
                assert!(reply.is_empty(), "a panicking connection sends nothing");

                // The panicked connection ended before its client read the
                // close, and `run` reaps it before it serves the next
                // connection on this one thread, so the line is written by
                // the time this probe is answered.
                let (status, _) = get(addr, "/healthz").await;
                assert_eq!(
                    status, 200,
                    "the server serves on after a connection panics"
                );
                let line = lines_rx.try_recv().expect("the panic is logged");
                assert_eq!(
                    line.trim(),
                    r#"ERROR admin connection panicked panic="render boom""#
                );
                shutdown_tx.send(true).expect("signal shutdown");
                running.await.expect("server task joins");
                assert!(lines_rx.try_recv().is_err(), "one line only");
            });
        });
    }

    /// Serves one connection over an in-process pipe of `buffer` bytes, with
    /// `render` as the exposition. Returns the client's end and the task
    /// serving the server's end.
    fn serve_in_process(
        buffer: usize,
        render: Option<RenderFn>,
    ) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<()>) {
        let (client, server) = tokio::io::duplex(buffer);
        let health = HealthState::new(0, HealthThresholds::default());
        let task = tokio::spawn(serve(hyper_util::rt::TokioIo::new(server), render, health));
        (client, task)
    }

    /// Everything the server writes before it closes the pipe.
    async fn read_to_close(client: &mut tokio::io::DuplexStream) -> String {
        use tokio::io::AsyncReadExt as _;
        let mut out = Vec::new();
        client.read_to_end(&mut out).await.expect("read");
        String::from_utf8_lossy(&out).into_owned()
    }

    /// A client that never finishes its request head is closed at the
    /// deadline, and not before.
    #[tokio::test(start_paused = true)]
    async fn an_unfinished_head_is_closed_at_the_deadline() {
        use tokio::io::AsyncWriteExt as _;
        let start = tokio::time::Instant::now();
        let (mut client, task) = serve_in_process(64 * 1024, None);
        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: admin\r\n")
            .await
            .expect("write");
        tokio::time::sleep(CONNECTION_DEADLINE - Duration::from_millis(1)).await;
        assert!(!task.is_finished(), "closed before the deadline");
        // The paused clock runs on to the next timer while the test waits, so
        // the instant the connection ends is the deadline it was given.
        task.await.expect("the connection ends");
        assert_eq!(
            start.elapsed(),
            CONNECTION_DEADLINE,
            "closed at the deadline"
        );
        assert_eq!(read_to_close(&mut client).await, "");
    }

    /// A client that sends a whole request for a large exposition and never
    /// reads the reply is closed at the deadline.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_never_reads_is_closed_at_the_deadline() {
        use tokio::io::AsyncWriteExt as _;
        let render: RenderFn = Arc::new(|| "x".repeat(1024 * 1024));
        let start = tokio::time::Instant::now();
        let (mut client, task) = serve_in_process(1024, Some(render));
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: admin\r\n\r\n")
            .await
            .expect("write");
        tokio::time::sleep(CONNECTION_DEADLINE - Duration::from_millis(1)).await;
        assert!(!task.is_finished(), "closed before the deadline");
        task.await.expect("the connection ends");
        assert_eq!(
            start.elapsed(),
            CONNECTION_DEADLINE,
            "closed at the deadline"
        );
    }

    /// A connection carries one request: a second one sent on it gets no
    /// response, and the server closes the connection after the first.
    #[tokio::test(start_paused = true)]
    async fn a_connection_answers_one_request() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, task) = serve_in_process(64 * 1024, None);
        client
            .write_all(
                b"GET /healthz HTTP/1.1\r\nHost: admin\r\n\r\n\
                  GET /healthz HTTP/1.1\r\nHost: admin\r\n\r\n",
            )
            .await
            .expect("write");
        let text = read_to_close(&mut client).await;
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        assert_eq!(text.matches("HTTP/1.1 ").count(), 1, "{text}");
        task.await.expect("the connection ends");
    }

    /// A request head larger than the read buffer is refused, not buffered.
    #[tokio::test(start_paused = true)]
    async fn a_head_past_the_read_buffer_is_refused() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client, task) = serve_in_process(64 * 1024, None);
        let mut head = b"GET /healthz HTTP/1.1\r\nHost: admin\r\nX-Pad: ".to_vec();
        head.resize(READ_BUFFER + 1024, b'a');
        head.extend_from_slice(b"\r\n\r\n");
        client.write_all(&head).await.expect("write");
        let text = read_to_close(&mut client).await;
        assert!(!text.contains(" 200 "), "{text}");
        task.await.expect("the connection ends");
    }

    /// An exposition far larger than the read buffer is served whole.
    #[tokio::test(start_paused = true)]
    async fn an_exposition_past_the_read_buffer_is_served_whole() {
        use tokio::io::AsyncWriteExt as _;
        let body = "y".repeat(16 * READ_BUFFER);
        let expected = body.clone();
        let render: RenderFn = Arc::new(move || body.clone());
        let (mut client, task) = serve_in_process(4096, Some(render));
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: admin\r\n\r\n")
            .await
            .expect("write");
        let text = read_to_close(&mut client).await;
        let (head, got) = text.split_once("\r\n\r\n").expect("a response");
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert_eq!(got.len(), expected.len());
        task.await.expect("the connection ends");
    }

    /// A failure that belongs to the pending connection moves on to the next
    /// one; any other failure backs off.
    #[test]
    fn accept_errors_are_classed_by_kind() {
        for kind in [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
        ] {
            assert_eq!(
                accept_error(&io::Error::from(kind)),
                AcceptError::Connection,
                "{kind:?}"
            );
        }
        // EMFILE: the process is out of descriptors.
        assert_eq!(
            accept_error(&io::Error::from_raw_os_error(24)),
            AcceptError::Backoff
        );
        assert_eq!(
            accept_error(&io::Error::from(io::ErrorKind::OutOfMemory)),
            AcceptError::Backoff
        );
    }

    /// `n` handles to tasks that never end, in spawn order.
    fn pending_handles(set: &mut JoinSet<()>, n: usize) -> Vec<AbortHandle> {
        (0..n)
            .map(|_| set.spawn(std::future::pending::<()>()))
            .collect()
    }

    /// At the cap, a new connection displaces the oldest still open, and the
    /// count never passes the cap.
    #[tokio::test]
    async fn the_oldest_connection_makes_room_at_the_cap() {
        let mut set = JoinSet::new();
        let handles = pending_handles(&mut set, 5);
        let mut live = Live::new(3);
        for h in &handles[..3] {
            assert!(live.admit(h.clone()).is_none(), "room below the cap");
        }
        let first = live.admit(handles[3].clone()).expect("the cap is reached");
        assert_eq!(first.id(), handles[0].id());
        let second = live.admit(handles[4].clone()).expect("still at the cap");
        assert_eq!(second.id(), handles[1].id());
        assert_eq!(live.len(), 3);
        set.shutdown().await;
    }

    /// A connection whose task ended frees its place: after as many endings
    /// as admissions, the next admission closes nothing.
    #[tokio::test]
    async fn an_ended_connection_frees_its_place() {
        let mut set = JoinSet::new();
        let handles = pending_handles(&mut set, 4);
        let mut live = Live::new(3);
        for h in &handles[..3] {
            assert!(live.admit(h.clone()).is_none());
        }
        for h in &handles[..3] {
            live.finished(h.id());
        }
        assert_eq!(live.len(), 0);
        assert!(
            live.admit(handles[3].clone()).is_none(),
            "room after endings"
        );
        set.shutdown().await;
    }

    #[tokio::test]
    async fn serves_probes_and_metrics_and_shuts_down() {
        let health = HealthState::new(0, HealthThresholds::default());
        let render: RenderFn = Arc::new(|| "spate_pipeline_info 1\n".to_owned());
        let server = AdminServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            Some(render),
            Arc::clone(&health),
        )
        .await
        .expect("bind");
        let addr = server.local_addr();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn(server.run(shutdown_rx));

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(body.contains("spate_pipeline_info"));

        let (status, _) = get(addr, "/readyz").await;
        assert_eq!(status, 503, "not ready before assignment + sinks");
        health.set_assignment_received(true);
        health.set_sinks_connected(true);
        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok");

        let (status, _) = get(addr, "/healthz").await;
        assert_eq!(status, 200);

        let (status, _) = get(addr, "/nope").await;
        assert_eq!(status, 404);

        shutdown_tx.send(true).expect("signal shutdown");
        running.await.expect("server task joins");
    }

    /// With no exporter there is no exposition, and `/metrics` says so. A 200
    /// carrying an empty body would read to a scraper as a healthy target
    /// delivering no series.
    #[tokio::test]
    async fn probes_serve_without_an_exporter_and_metrics_does_not() {
        let health = HealthState::new(0, HealthThresholds::default());
        let server = AdminServer::bind("127.0.0.1:0".parse().expect("addr"), None, health)
            .await
            .expect("bind");
        let addr = server.local_addr();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn(server.run(shutdown_rx));

        let (status, _) = get(addr, "/metrics").await;
        assert_eq!(status, 404, "no exporter, no exposition");

        let (status, _) = get(addr, "/healthz").await;
        assert_eq!(status, 200, "liveness does not depend on the exporter");
        let (status, _) = get(addr, "/readyz").await;
        assert_eq!(status, 503, "readiness does not depend on the exporter");

        shutdown_tx.send(true).expect("signal shutdown");
        running.await.expect("server task joins");
    }
}
