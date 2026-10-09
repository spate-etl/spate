//! An HTTP/1.1 proxy in front of a DynamoDB endpoint that answers each call
//! as a script decides: forwarded, delayed, refused with an error, or
//! forwarded with its reply replaced or dropped.

use std::fmt;
use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

/// The 5xx statuses the AWS SDK retries.
pub const RETRIED_STATUSES: [u16; 4] = [500, 502, 503, 504];

/// One request the proxy received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    /// The operation from `X-Amz-Target`, such as `UpdateItem`.
    pub op: String,
    /// The call's index among every call this proxy received, from 0.
    pub seq: u64,
    /// The item key's `pk` and `sk`, when the body names one under `Key`.
    pub key: Option<(String, String)>,
    /// The SDK attempt from the `amz-sdk-request` header, 1 when absent.
    pub attempt: u32,
}

/// How the proxy answers one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Forward the call and its reply unchanged.
    Pass,
    /// Answer 400 `ThrottlingException` without forwarding.
    Throttle,
    /// Answer 400 `ProvisionedThroughputExceededException` without forwarding.
    ThroughputExceeded,
    /// Answer this status without forwarding.
    ServerError(u16),
    /// Forward the call, wait for its reply, and answer this status instead.
    ErrorAfterLand(u16),
    /// Forward the call, wait for its reply, and close the connection without
    /// a status line.
    DropAfterLand,
    /// Wait this long, then forward as [`Fault::Pass`] does.
    Delay(Duration),
}

/// The status of [`RETRIED_STATUSES`] that `draw` selects.
#[must_use]
pub fn retried_status(draw: u64) -> u16 {
    RETRIED_STATUSES[(draw % RETRIED_STATUSES.len() as u64) as usize]
}

/// `pass`, `throttle`, `throughput_exceeded`, `server_error(503)`,
/// `error_after_land(503)`, `drop_after_land` or `delay(40ms)`.
impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Pass => f.write_str("pass"),
            Fault::Throttle => f.write_str("throttle"),
            Fault::ThroughputExceeded => f.write_str("throughput_exceeded"),
            Fault::ServerError(s) => write!(f, "server_error({s})"),
            Fault::ErrorAfterLand(s) => write!(f, "error_after_land({s})"),
            Fault::DropAfterLand => f.write_str("drop_after_land"),
            Fault::Delay(d) => write!(f, "delay({}ms)", d.as_millis()),
        }
    }
}

type Script = dyn Fn(&Call) -> Fault + Send + Sync;

/// A running proxy. Dropping it stops accepting connections; open
/// connections end when their client closes them.
pub struct DynamoDbFaultProxy {
    addr: SocketAddr,
    closed: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl fmt::Debug for DynamoDbFaultProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoDbFaultProxy")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl DynamoDbFaultProxy {
    /// Listens on a free loopback port and serves each connection on its own
    /// thread, asking `script` how to answer every request and forwarding
    /// each request it forwards on a new connection to `upstream`.
    ///
    /// Requests and replies must carry `Content-Length`. A connection whose
    /// upstream exchange fails is closed without a reply.
    ///
    /// # Errors
    ///
    /// Fails when the listener cannot be bound.
    pub fn start(
        upstream: SocketAddr,
        script: impl Fn(&Call) -> Fault + Send + Sync + 'static,
    ) -> io::Result<DynamoDbFaultProxy> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let closed = Arc::new(AtomicBool::new(false));
        let script: Arc<Script> = Arc::new(script);
        let seq = Arc::new(AtomicU64::new(0));
        let accept = {
            let closed = Arc::clone(&closed);
            std::thread::spawn(move || {
                for client in listener.incoming() {
                    if closed.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(client) = client else { continue };
                    let (script, seq) = (Arc::clone(&script), Arc::clone(&seq));
                    std::thread::spawn(move || {
                        let _ = serve(client, upstream, &*script, &seq);
                    });
                }
            })
        };
        Ok(DynamoDbFaultProxy {
            addr,
            closed,
            accept: Some(accept),
        })
    }

    /// The address clients connect to.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for DynamoDbFaultProxy {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        // Wakes the blocked `accept` so the thread sees the flag.
        let _ = TcpStream::connect(self.addr);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }
}

/// One HTTP message read off a stream: its raw bytes, and its lowercased
/// header names with their values.
struct Message {
    raw: Vec<u8>,
    headers: Vec<(String, String)>,
    body_at: usize,
}

impl Message {
    /// Reads one message, or `None` at a clean end of stream.
    fn read(reader: &mut BufReader<TcpStream>) -> io::Result<Option<Message>> {
        let mut raw = Vec::new();
        let mut headers = Vec::new();
        loop {
            let start = raw.len();
            if reader.read_until(b'\n', &mut raw)? == 0 {
                return if raw.is_empty() {
                    Ok(None)
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                };
            }
            let line = String::from_utf8_lossy(&raw[start..]).trim_end().to_owned();
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
            }
        }
        let body_at = raw.len();
        let length = headers
            .iter()
            .find(|(n, _)| n == "content-length")
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no Content-Length"))?;
        raw.resize(body_at + length, 0);
        reader.read_exact(&mut raw[body_at..])?;
        Ok(Some(Message {
            raw,
            headers,
            body_at,
        }))
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn call(&self, seq: u64) -> Call {
        let op = self
            .header("x-amz-target")
            .and_then(|t| t.rsplit('.').next())
            .unwrap_or_default()
            .to_owned();
        let attempt = self
            .header("amz-sdk-request")
            .and_then(|v| {
                v.split(';')
                    .find_map(|kv| kv.trim().strip_prefix("attempt="))
                    .and_then(|n| n.parse().ok())
            })
            .unwrap_or(1);
        let body: serde_json::Value =
            serde_json::from_slice(&self.raw[self.body_at..]).unwrap_or_default();
        let part = |name: &str| body["Key"][name]["S"].as_str().map(str::to_owned);
        let key = part("pk").zip(part("sk"));
        Call {
            op,
            seq,
            key,
            attempt,
        }
    }
}

fn serve(
    client: TcpStream,
    upstream: SocketAddr,
    script: &Script,
    seq: &AtomicU64,
) -> io::Result<()> {
    let mut requests = BufReader::new(client.try_clone()?);
    let mut client = client;
    while let Some(request) = Message::read(&mut requests)? {
        let fault = script(&request.call(seq.fetch_add(1, Ordering::SeqCst)));
        let reply = match fault {
            Fault::Throttle => error_reply(400, "ThrottlingException"),
            Fault::ThroughputExceeded => error_reply(400, "ProvisionedThroughputExceededException"),
            Fault::ServerError(status) => error_reply(status, "InternalServerError"),
            Fault::Pass | Fault::Delay(_) | Fault::ErrorAfterLand(_) | Fault::DropAfterLand => {
                if let Fault::Delay(d) = fault {
                    std::thread::sleep(d);
                }
                let landed = match forward(upstream, &request.raw) {
                    Ok(reply) => reply,
                    Err(e) => {
                        let _ = client.shutdown(Shutdown::Both);
                        return Err(e);
                    }
                };
                match fault {
                    Fault::ErrorAfterLand(status) => error_reply(status, "InternalServerError"),
                    Fault::DropAfterLand => return client.shutdown(Shutdown::Both),
                    _ => landed,
                }
            }
        };
        client.write_all(&reply)?;
    }
    Ok(())
}

/// Sends `request` to `upstream` on a new connection and returns the reply's
/// raw bytes. A connection kept between calls could be one the upstream has
/// closed while idle, which loses the next call with no reply.
fn forward(upstream: SocketAddr, request: &[u8]) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(upstream)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    stream.write_all(request)?;
    Message::read(&mut reader)?
        .map(|message| message.raw)
        .ok_or_else(|| io::ErrorKind::UnexpectedEof.into())
}

/// A DynamoDB JSON error reply with `status` and error type `code`.
fn error_reply(status: u16, code: &str) -> Vec<u8> {
    let body = format!(
        r#"{{"__type":"com.amazonaws.dynamodb.v20120810#{code}","message":"injected by the fault proxy"}}"#
    );
    format!(
        "HTTP/1.1 {status} Injected\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::time::Instant;

    const REPLY: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nx-amz-crc32: 1\r\nContent-Length: 11\r\n\r\n{\"Item\":{}}";

    /// An upstream that answers every request with [`REPLY`] on keep-alive
    /// connections and reports each request's raw bytes and arrival time.
    fn upstream() -> (SocketAddr, mpsc::Receiver<(Vec<u8>, Instant)>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let (mut stream, tx) = (stream.unwrap(), tx.clone());
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    while let Ok(Some(request)) = Message::read(&mut reader) {
                        let _ = tx.send((request.raw, Instant::now()));
                        stream.write_all(REPLY.as_bytes()).unwrap();
                    }
                });
            }
        });
        (addr, rx)
    }

    fn request(target: &str, attempt: u32, body: &str) -> String {
        format!(
            "POST / HTTP/1.1\r\nHost: dynamodb\r\nX-Amz-Target: DynamoDB_20120810.{target}\r\namz-sdk-request: attempt={attempt}; max=3\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    const UPDATE: &str = r#"{"TableName":"t","Key":{"pk":{"S":"job#d"},"sk":{"S":"split.a"}}}"#;

    /// Sends `request` on `stream` and returns the reply's raw bytes, empty
    /// when the proxy closed the connection without one.
    fn exchange(stream: &mut TcpStream, request: &str) -> Vec<u8> {
        stream.write_all(request.as_bytes()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        Message::read(&mut reader)
            .unwrap()
            .map(|m| m.raw)
            .unwrap_or_default()
    }

    fn proxy(
        script: impl Fn(&Call) -> Fault + Send + Sync + 'static,
    ) -> (DynamoDbFaultProxy, mpsc::Receiver<(Vec<u8>, Instant)>) {
        let (addr, received) = upstream();
        (DynamoDbFaultProxy::start(addr, script).unwrap(), received)
    }

    fn connect(proxy: &DynamoDbFaultProxy) -> TcpStream {
        TcpStream::connect(proxy.addr()).unwrap()
    }

    /// A throttle or a throughput refusal is answered 400 with its DynamoDB
    /// error type, and the call never reaches the upstream.
    #[test]
    fn throttle_answers_without_forwarding() {
        for (fault, code) in [
            (Fault::Throttle, "ThrottlingException"),
            (
                Fault::ThroughputExceeded,
                "ProvisionedThroughputExceededException",
            ),
        ] {
            let (proxy, received) = proxy(move |_| fault);
            let reply = String::from_utf8(exchange(
                &mut connect(&proxy),
                &request("UpdateItem", 1, UPDATE),
            ))
            .unwrap();
            assert!(reply.starts_with("HTTP/1.1 400 "), "{reply}");
            assert!(reply.contains(&format!("#{code}\"")), "{reply}");
            drop(proxy);
            assert!(received.try_recv().is_err(), "{fault} forwarded the call");
        }
    }

    /// A drop after land forwards the call, waits for its reply, and closes
    /// the connection with no status line.
    #[test]
    fn drop_after_land_forwards_then_closes() {
        let (proxy, received) = proxy(|_| Fault::DropAfterLand);
        let reply = exchange(&mut connect(&proxy), &request("UpdateItem", 1, UPDATE));
        assert_eq!(reply, b"");
        let (raw, _) = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(raw, request("UpdateItem", 1, UPDATE).into_bytes());
    }

    /// A drop on the first attempt and a pass on the second: both reach the
    /// upstream, the first answer is a closed connection, and the second
    /// reply reaches the client unchanged.
    #[test]
    fn drop_after_land_then_pass_forwards_the_retry() {
        let (proxy, received) = proxy(|call| {
            if call.attempt == 1 {
                Fault::DropAfterLand
            } else {
                Fault::Pass
            }
        });
        assert_eq!(
            exchange(&mut connect(&proxy), &request("UpdateItem", 1, UPDATE)),
            b""
        );
        let retry = exchange(&mut connect(&proxy), &request("UpdateItem", 2, UPDATE));
        assert_eq!(retry, REPLY.as_bytes());
        let arrived: Vec<_> = (0..2)
            .map(|_| received.recv_timeout(Duration::from_secs(5)).unwrap().0)
            .collect();
        assert_eq!(
            arrived,
            [1, 2].map(|a| request("UpdateItem", a, UPDATE).into_bytes())
        );
    }

    /// A server error is drawn only from the statuses the SDK retries, each
    /// of them is drawn, and it is answered without forwarding.
    #[test]
    fn server_error_draws_only_retried_statuses() {
        let drawn: std::collections::BTreeSet<_> = (0..64).map(retried_status).collect();
        assert_eq!(drawn.into_iter().collect::<Vec<_>>(), [500, 502, 503, 504]);
        let (proxy, received) = proxy(|_| Fault::ServerError(503));
        let reply = String::from_utf8(exchange(
            &mut connect(&proxy),
            &request("GetItem", 1, UPDATE),
        ))
        .unwrap();
        assert!(reply.starts_with("HTTP/1.1 503 "), "{reply}");
        drop(proxy);
        assert!(
            received.try_recv().is_err(),
            "a server error forwarded the call"
        );
    }

    /// An error after land forwards the call and answers the status in place
    /// of the upstream's reply.
    #[test]
    fn error_after_land_forwards_then_answers_the_status() {
        let (proxy, received) = proxy(|_| Fault::ErrorAfterLand(500));
        let reply = String::from_utf8(exchange(
            &mut connect(&proxy),
            &request("UpdateItem", 1, UPDATE),
        ))
        .unwrap();
        assert!(reply.starts_with("HTTP/1.1 500 "), "{reply}");
        received.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    /// A delay holds the call for at least its length before the upstream
    /// receives it.
    #[test]
    fn delay_applies_before_forwarding() {
        let delay = Duration::from_millis(200);
        let (proxy, received) = proxy(move |_| Fault::Delay(delay));
        let sent = Instant::now();
        let reply = exchange(&mut connect(&proxy), &request("GetItem", 1, UPDATE));
        assert_eq!(reply, REPLY.as_bytes());
        let (_, arrived) = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(arrived - sent >= delay, "{:?}", arrived - sent);
    }

    /// The script sees each call's operation, its order, the item key from
    /// the body and the SDK attempt; a call with no key or attempt header
    /// reads as keyless, attempt 1.
    #[test]
    fn call_carries_the_item_key_and_attempt() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&calls);
        let (proxy, _received) = proxy(move |call| {
            seen.lock().unwrap().push(call.clone());
            Fault::Pass
        });
        let mut stream = connect(&proxy);
        exchange(&mut stream, &request("UpdateItem", 2, UPDATE));
        let bare = "POST / HTTP/1.1\r\nX-Amz-Target: DynamoDB_20120810.Query\r\nContent-Length: 2\r\n\r\n{}";
        exchange(&mut stream, bare);
        assert_eq!(
            *calls.lock().unwrap(),
            [
                Call {
                    op: "UpdateItem".to_owned(),
                    seq: 0,
                    key: Some(("job#d".to_owned(), "split.a".to_owned())),
                    attempt: 2,
                },
                Call {
                    op: "Query".to_owned(),
                    seq: 1,
                    key: None,
                    attempt: 1,
                },
            ]
        );
    }

    /// Requests that share one client connection are each forwarded, and
    /// each reply comes back on it.
    #[test]
    fn pass_forwards_keep_alive_requests() {
        let (proxy, received) = proxy(|_| Fault::Pass);
        let mut stream = connect(&proxy);
        for attempt in 1..=3 {
            assert_eq!(
                exchange(&mut stream, &request("GetItem", attempt, UPDATE)),
                REPLY.as_bytes()
            );
        }
        for _ in 0..3 {
            received.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    /// A pass after the upstream closed its connection is forwarded on a new
    /// one, and its reply reaches the client.
    #[test]
    fn pass_reconnects_after_the_upstream_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                if let Ok(Some(_)) = Message::read(&mut reader) {
                    stream.write_all(REPLY.as_bytes()).unwrap();
                }
            }
        });
        let proxy = DynamoDbFaultProxy::start(addr, |_| Fault::Pass).unwrap();
        let mut stream = connect(&proxy);
        for attempt in 1..=2 {
            assert_eq!(
                exchange(&mut stream, &request("GetItem", attempt, UPDATE)),
                REPLY.as_bytes()
            );
        }
    }

    /// Faults are named as the fault journal records them.
    #[test]
    fn faults_display_as_journalled() {
        let names: Vec<String> = [
            Fault::Pass,
            Fault::Throttle,
            Fault::ThroughputExceeded,
            Fault::ServerError(503),
            Fault::ErrorAfterLand(500),
            Fault::DropAfterLand,
            Fault::Delay(Duration::from_millis(40)),
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(
            names,
            [
                "pass",
                "throttle",
                "throughput_exceeded",
                "server_error(503)",
                "error_after_land(500)",
                "drop_after_land",
                "delay(40ms)"
            ]
        );
    }
}
