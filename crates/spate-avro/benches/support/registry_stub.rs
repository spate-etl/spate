//! A loopback stub of the Confluent registry's `GET /schemas/ids/{id}`, and
//! the warm-up that drives a Confluent deserializer against it.
//!
//! # Why this exists
//!
//! The Confluent cases need a schema cache that already holds a compiled
//! schema (and, for the poison case, a negative entry). Both states are
//! reachable only through a registry fetch: `insert_ready` and
//! `insert_failed` are private to the crate and the fetcher task is the only
//! caller. The public API can express it by pointing `registry.url` at a
//! socket this process owns, which is what `tests/registry_flow.rs` already
//! does, so the benches stay on the public API rather than growing a seam.
//!
//! Nothing here runs inside a measured region. The fetch happens in the
//! `#[bench]` argument expression, the stub is shut down and joined before
//! the rig is handed to the benchmark function, and the deserializer's
//! runtime is not driven again, so the measured walk resolves every payload
//! out of the local cache snapshot.
//!
//! It is deliberately a hand-framed HTTP/1.1 responder rather than a hyper
//! service: the whole thing runs under valgrind, and a canned response over
//! a blocking socket is the smallest thing that answers the one request
//! shape the fetcher makes.

use spate_avro::AvroValueDeserializer;
use spate_core::checkpoint::AckRef;
use spate_core::deser::Deserializer;
use spate_core::error::DeserError;
use spate_core::record::{PartitionId, RawPayload};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How long the accept loop parks between polls when no connection is
/// waiting. Short enough that a fetch is not delayed behind it.
const ACCEPT_POLL: Duration = Duration::from_millis(1);

/// How long the warm-up parks between replays while a fetch is in flight.
const WARM_POLL: Duration = Duration::from_millis(2);

/// Replays the warm-up will make before giving up. Generous, because it is
/// only reached when something is wrong: the failure is a panic naming the
/// id, not a silent fallback to an unwarmed cache.
const WARM_ATTEMPTS: usize = 500;

/// A registry that answers one schema id and 404s everything else.
pub(crate) struct StubRegistry {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl StubRegistry {
    /// Bind an ephemeral loopback port and serve `schema` under `id`.
    pub(crate) fn start(id: u32, schema: &str) -> StubRegistry {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
        let addr = listener.local_addr().expect("the bound address");
        listener
            .set_nonblocking(true)
            .expect("the listener polls rather than blocking");

        let body = serde_json::json!({ "schema": schema, "id": id }).to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        // The fetcher asks for `/schemas/ids/{id}?deleted=true`; matching on
        // the id segment is what makes the poison id a 404 rather than a
        // second copy of the schema.
        let ready_segment = format!("/schemas/ids/{id}");

        let thread = std::thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => serve(stream, &ready_segment, &body),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if thread_stop.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(ACCEPT_POLL);
                    }
                    Err(_) => return,
                }
            }
        });

        StubRegistry {
            addr,
            stop,
            thread: Some(thread),
        }
    }

    /// The base URL to configure the registry section with.
    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stop serving and join the thread, so no socket and no thread outlive
    /// the setup that created them.
    pub(crate) fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the stub registry thread exits");
        }
    }
}

impl Drop for StubRegistry {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl std::fmt::Debug for StubRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StubRegistry")
            .field("addr", &self.addr)
            .finish()
    }
}

/// Read one request and write one response. `Connection: close` on every
/// reply, so a connection serves exactly one fetch and the accept loop stays
/// the only place that waits.
fn serve(mut stream: TcpStream, ready_segment: &str, body: &str) {
    stream
        .set_nonblocking(false)
        .expect("an accepted stream blocks");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("the read is bounded");

    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    // Headers only: the fetcher issues a GET, which carries no body. The scan
    // resumes three bytes behind what it has already seen rather than
    // restarting, so a terminator split across two reads is still found
    // without re-walking the whole buffer each time.
    let mut scanned = 0usize;
    while !request[scanned..].windows(4).any(|w| w == b"\r\n\r\n") {
        scanned = request.len().saturating_sub(3);
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&request);
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();

    // The id segment must end where the path does rather than only start
    // there: `starts_with` alone would serve the ready schema for id 42110 as
    // well as for 4211, and a case that silently stopped 404ing would measure
    // the decode it is named for the absence of.
    let id_segment = path.split('?').next().unwrap_or_default();
    let (status, payload) = if id_segment == ready_segment {
        ("200 OK", body.to_owned())
    } else {
        (
            "404 Not Found",
            r#"{"error_code":40403,"message":"Schema not found"}"#.to_owned(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         content-type: application/vnd.schemaregistry.v1+json\r\n\
         content-length: {}\r\n\
         connection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// What a warm-up is waiting for the cache to reach.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Warm {
    /// The id compiles and the payload decodes.
    Ready,
    /// The registry answered 404, so the id is negatively cached and the
    /// payload resolves to `SchemaUnavailable`.
    Poisoned,
}

/// Replay one payload through `deser` until the registry fetch it triggers
/// lands, driving the fetcher on `rt` between attempts.
///
/// This is the only place the runtime is driven. Once it returns, the
/// deserializer's memo holds the answer and the measured walk never reaches
/// the shared cache lock for a `Ready` id at all.
pub(crate) fn warm(
    deser: &mut AvroValueDeserializer,
    rt: &tokio::runtime::Runtime,
    ack: &AckRef,
    sink: &mut crate::orders::Sink,
    payload: &[u8],
    want: Warm,
) {
    let raw = RawPayload {
        bytes: payload,
        key: None,
        partition: PartitionId(0),
        offset: 0,
        timestamp_ms: 0,
    };
    for _ in 0..WARM_ATTEMPTS {
        let outcome = deser.deserialize(&raw, ack, sink);
        match (&outcome, want) {
            (Ok(()), Warm::Ready) => return,
            (Err(DeserError::SchemaUnavailable { .. }), Warm::Poisoned) => return,
            (Err(DeserError::NotReady { .. }), _) => {
                rt.block_on(async { tokio::time::sleep(WARM_POLL).await });
            }
            _ => panic!("warm-up wanted {want:?} but the payload resolved to {outcome:?}"),
        }
    }
    panic!("the stub registry never brought the schema to {want:?} — {WARM_ATTEMPTS} replays");
}
