//! Admin HTTP server: `/metrics`, `/healthz`, and `/readyz`.
//!
//! One small hyper server per process, running on the I/O runtime. Probe
//! semantics (see `docs/DESIGN.md` § Errors, panics, shutdown, health):
//!
//! - **`/readyz`** — 200 once the source has received its assignment and
//!   the sinks are connected.
//! - **`/healthz`** — 200 while every pipeline thread's poll-loop heartbeat
//!   is fresh AND the checkpoint watermark is not stuck while data flows.
//!   A stalled poll loop or a wedged watermark flips this to 503 so the
//!   kubelet restarts the pod.
//!
//! The server takes the metrics renderer as a plain closure
//! ([`RenderFn`]) so it has no dependency on exporter internals.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::sync::watch;

/// Renders the current metrics exposition snapshot.
pub type RenderFn = Arc<dyn Fn() -> String + Send + Sync>;

/// Staleness thresholds for the liveness probe.
#[derive(Clone, Copy, Debug)]
pub struct HealthThresholds {
    /// A pipeline thread whose heartbeat is older than this is considered
    /// stalled.
    pub heartbeat_stale: Duration,
    /// The watermark is considered stuck when its age exceeds this while
    /// data is flowing.
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
#[derive(Debug)]
pub struct HealthState {
    epoch: Instant,
    thresholds: HealthThresholds,
    assignment_received: AtomicBool,
    sinks_connected: AtomicBool,
    /// Per-pipeline-thread heartbeat, as millis since `epoch`.
    heartbeats: Vec<AtomicU64>,
    /// Watermark age reported by the checkpointer, in millis.
    watermark_age_ms: AtomicU64,
    data_flowing: AtomicBool,
}

impl HealthState {
    /// Create state for `pipeline_threads` heartbeating threads. All
    /// heartbeats start "fresh" at creation time.
    #[must_use]
    pub fn new(pipeline_threads: usize, thresholds: HealthThresholds) -> Arc<Self> {
        Arc::new(HealthState {
            epoch: Instant::now(),
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

    /// Beat thread `thread`'s heart. Called once per poll iteration; out of
    /// range indices are ignored (defensive — sizing is fixed at startup).
    pub fn heartbeat(&self, thread: usize) {
        if let Some(hb) = self.heartbeats.get(thread) {
            hb.store(self.millis_since_epoch(Instant::now()), Ordering::Relaxed);
        }
    }

    /// Mark whether the source has received its (initial) assignment.
    pub fn set_assignment_received(&self, received: bool) {
        self.assignment_received.store(received, Ordering::Relaxed);
    }

    /// Mark whether all sinks report connectivity.
    pub fn set_sinks_connected(&self, connected: bool) {
        self.sinks_connected.store(connected, Ordering::Relaxed);
    }

    /// Report the checkpointer's oldest-unacknowledged-batch age and
    /// whether data is currently flowing (a stuck watermark only counts
    /// against liveness while records move).
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

    /// Liveness now.
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.healthy_at(Instant::now())
    }

    /// Liveness as of `now` (injectable for tests).
    #[must_use]
    pub fn healthy_at(&self, now: Instant) -> bool {
        let now_ms = self.millis_since_epoch(now);
        let stale = u64::try_from(self.thresholds.heartbeat_stale.as_millis()).unwrap_or(u64::MAX);
        let hearts_fresh = self
            .heartbeats
            .iter()
            .all(|hb| now_ms.saturating_sub(hb.load(Ordering::Relaxed)) <= stale);

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
    render: RenderFn,
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
    pub async fn bind(
        addr: SocketAddr,
        render: RenderFn,
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
    /// Probe connections are short-lived; in-flight requests finish on
    /// their detached tasks after this returns.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> io::Result<()> {
        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (stream, _peer) = accepted?;
                    let render = Arc::clone(&self.render);
                    let health = Arc::clone(&self.health);
                    tokio::spawn(async move {
                        let io = hyper_util::rt::TokioIo::new(stream);
                        let service = service_fn(move |req| {
                            let render = Arc::clone(&render);
                            let health = Arc::clone(&health);
                            async move { respond(&req, &render, &health) }
                        });
                        if let Err(err) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .await
                        {
                            tracing::debug!(error = %err, "admin connection error");
                        }
                    });
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn respond(
    req: &Request<hyper::body::Incoming>,
    render: &RenderFn,
    health: &HealthState,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if req.method() != Method::GET {
        return Ok(plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"));
    }
    Ok(match req.uri().path() {
        "/metrics" => {
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
        "/healthz" => probe(health.healthy()),
        "/readyz" => probe(health.ready()),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds(stale_ms: u64, stuck_ms: u64) -> HealthThresholds {
        HealthThresholds {
            heartbeat_stale: Duration::from_millis(stale_ms),
            watermark_stuck: Duration::from_millis(stuck_ms),
        }
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

    /// Minimal HTTP/1.1 GET over the readiness API — avoids requiring
    /// tokio's `io-util` feature just for this test.
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

    #[tokio::test]
    async fn serves_probes_and_metrics_and_shuts_down() {
        let health = HealthState::new(0, HealthThresholds::default());
        let render: RenderFn = Arc::new(|| "etl_pipeline_info 1\n".to_owned());
        let server = AdminServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            render,
            Arc::clone(&health),
        )
        .await
        .expect("bind");
        let addr = server.local_addr();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn(server.run(shutdown_rx));

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(body.contains("etl_pipeline_info"));

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
        running
            .await
            .expect("server task joins")
            .expect("clean shutdown");
    }
}
