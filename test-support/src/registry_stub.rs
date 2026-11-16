//! A Confluent schema-registry stub on `127.0.0.1`, serving until the process
//! exits.

use std::collections::BTreeMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::serve::{read_head, respond, target};
use crate::serve_each;

/// The content type of every response, as the registry sends it.
const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// The registry's answer to an id or subject it does not hold.
const NOT_FOUND: &str = r#"{"error_code":40403,"message":"Schema not found"}"#;

/// A schema-registry stub that answers each request path from a script.
///
/// A path with no script left gets a 404 with the registry's 40403 body. Each
/// response closes its connection. Clones share the server and its state.
#[derive(Clone)]
pub struct RegistryStub {
    addr: SocketAddr,
    state: Arc<State>,
}

/// One request the stub received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryRequest {
    /// The request target: the path and its query.
    pub target: String,
    /// The value of each `Authorization` header, in order.
    pub authorization: Vec<String>,
}

#[derive(Default)]
struct State {
    routes: Mutex<BTreeMap<String, Vec<Scripted>>>,
    requests: Mutex<Vec<RegistryRequest>>,
    gate: Mutex<Gate>,
    changed: Condvar,
}

/// The held path, and how many requests for it are waiting.
#[derive(Default)]
struct Gate {
    held: Option<String>,
    waiting: usize,
}

/// A response served for the next `times` requests, or for every request
/// when `times` is 0.
struct Scripted {
    status: u16,
    body: String,
    times: usize,
}

impl std::fmt::Debug for RegistryStub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryStub")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl RegistryStub {
    /// Binds a port on `127.0.0.1` and serves it with nothing scripted.
    ///
    /// # Panics
    ///
    /// Panics when no local port can be bound.
    #[must_use]
    pub fn start() -> Self {
        let state = Arc::new(State::default());
        let serving = Arc::clone(&state);
        let addr = serve_each(move |mut stream| {
            let _ = serving.serve(&mut stream);
        });
        Self { addr, state }
    }

    /// The address the stub listens on.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The stub's `http://` URL.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Answers requests for `path`, whatever their query, with `status` and
    /// `body` for the next `times` of them, or for all of them when `times`
    /// is 0. Entries for one path are served in the order they were added.
    pub fn script(&self, path: &str, status: u16, body: impl Into<String>, times: usize) {
        lock(&self.state.routes)
            .entry(path.to_owned())
            .or_default()
            .push(Scripted {
                status,
                body: body.into(),
                times,
            });
    }

    /// Holds each request for `path` until [`release`](Self::release). A held
    /// request is recorded once it is let through.
    pub fn hold(&self, path: &str) {
        lock(&self.state.gate).held = Some(path.to_owned());
    }

    /// Lets the held requests through.
    pub fn release(&self) {
        lock(&self.state.gate).held = None;
        self.state.changed.notify_all();
    }

    /// Every request received, in the order they arrived.
    #[must_use]
    pub fn requests(&self) -> Vec<RegistryRequest> {
        lock(&self.state.requests).clone()
    }

    /// How many requests the stub received.
    #[must_use]
    pub fn hits(&self) -> usize {
        lock(&self.state.requests).len()
    }

    /// How many requests the stub received for `path`, whatever their query.
    #[must_use]
    pub fn path_hits(&self, path: &str) -> usize {
        lock(&self.state.requests)
            .iter()
            .filter(|r| r.target.split('?').next() == Some(path))
            .count()
    }
}

impl State {
    fn serve(&self, stream: &mut TcpStream) -> io::Result<()> {
        let request = read_head(stream)?;
        if request.is_empty() {
            return Ok(());
        }
        let (path, query) = target(&request);
        self.pass_gate(&path);
        let target = if query.is_empty() {
            path.clone()
        } else {
            format!("{path}?{query}")
        };
        lock(&self.requests).push(RegistryRequest {
            target,
            authorization: authorization(&request),
        });
        let (status, body) = self.answer(&path);
        // The client reads the status code alone, so the reason is left empty.
        respond(
            stream,
            &format!("{status} "),
            CONTENT_TYPE,
            "connection: close\r\n",
            body.as_bytes(),
        )
    }

    /// Waits while `path` is held.
    fn pass_gate(&self, path: &str) {
        let mut gate = lock(&self.gate);
        if gate.held.as_deref() != Some(path) {
            return;
        }
        gate.waiting += 1;
        self.changed.notify_all();
        let mut gate = self
            .changed
            .wait_while(gate, |g| g.held.as_deref() == Some(path))
            .expect("registry stub gate");
        gate.waiting -= 1;
    }

    fn answer(&self, path: &str) -> (u16, String) {
        let mut routes = lock(&self.routes);
        let Some(first) = routes.get_mut(path).and_then(|queue| queue.first_mut()) else {
            return (404, NOT_FOUND.to_owned());
        };
        let answer = (first.status, first.body.clone());
        if first.times > 0 {
            first.times -= 1;
            if first.times == 0 {
                routes.get_mut(path).expect("the path's queue").remove(0);
            }
        }
        answer
    }
}

/// The value of each `Authorization` header in `request`, whatever its case.
fn authorization(request: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(request)
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.trim().eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_owned())
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("registry stub state")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::time::Duration;

    /// A socket wait that takes this long has hung.
    const HANG: Duration = Duration::from_secs(60);

    /// Sends a GET for `target` with `headers` (each ending `\r\n`) and
    /// returns the whole response.
    fn get(addr: SocketAddr, target: &str, headers: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(HANG)).unwrap();
        stream.set_write_timeout(Some(HANG)).unwrap();
        write!(
            stream,
            "GET {target} HTTP/1.1\r\nhost: stub\r\n{headers}\r\n"
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    /// The response body.
    fn body(response: &str) -> &str {
        response.split_once("\r\n\r\n").unwrap().1
    }

    /// A script entry is served `times` times and then gives way to the next,
    /// which with `times` 0 is served for good.
    #[test]
    fn a_script_entry_repeats_then_gives_way() {
        let stub = RegistryStub::start();
        stub.script("/schemas/ids/9", 503, "warming up", 2);
        stub.script("/schemas/ids/9", 200, "ready", 0);
        let bodies: Vec<String> = (0..4)
            .map(|_| body(&get(stub.addr(), "/schemas/ids/9", "")).to_owned())
            .collect();
        assert_eq!(bodies, ["warming up", "warming up", "ready", "ready"]);
    }

    /// A response is framed for one request on its connection, and an
    /// unscripted path gets the registry's 404.
    #[test]
    fn a_response_is_framed_for_one_request() {
        let stub = RegistryStub::start();
        assert_eq!(stub.url(), format!("http://{}", stub.addr()));
        assert_eq!(
            format!("{stub:?}"),
            format!("RegistryStub {{ addr: {}, .. }}", stub.addr())
        );
        stub.script("/schemas/ids/1", 200, "{}", 0);
        assert_eq!(
            get(stub.addr(), "/schemas/ids/1", ""),
            "HTTP/1.1 200 \r\ncontent-type: application/vnd.schemaregistry.v1+json\r\n\
             connection: close\r\ncontent-length: 2\r\n\r\n{}"
        );
        assert_eq!(
            get(stub.addr(), "/schemas/ids/2", ""),
            format!(
                "HTTP/1.1 404 \r\ncontent-type: application/vnd.schemaregistry.v1+json\r\n\
                 connection: close\r\ncontent-length: {}\r\n\r\n{NOT_FOUND}",
                NOT_FOUND.len()
            )
        );
    }

    /// The query does not choose the script entry or the path a hit counts
    /// against, and the recorded target keeps it.
    #[test]
    fn the_query_does_not_choose_the_entry() {
        let stub = RegistryStub::start();
        stub.script("/schemas/ids/5", 200, "five", 0);
        assert_eq!(
            body(&get(stub.addr(), "/schemas/ids/5?deleted=true", "")),
            "five"
        );
        get(stub.addr(), "/schemas/ids/6", "");
        assert_eq!(stub.requests()[0].target, "/schemas/ids/5?deleted=true");
        assert_eq!(stub.path_hits("/schemas/ids/5"), 1);
        assert_eq!(stub.path_hits("/schemas/ids/6"), 1);
        assert_eq!(stub.path_hits("/schemas/ids/7"), 0);
        assert_eq!(stub.hits(), 2);
    }

    /// Requests are recorded in order, each with its `Authorization` values
    /// whatever the header's case.
    #[test]
    fn requests_are_recorded_in_order_with_their_authorization() {
        let stub = RegistryStub::start();
        get(stub.addr(), "/a", "authorization: Basic c3Zj\r\n");
        get(stub.addr(), "/b", "");
        assert_eq!(
            stub.requests(),
            [
                RegistryRequest {
                    target: "/a".to_owned(),
                    authorization: vec!["Basic c3Zj".to_owned()],
                },
                RegistryRequest {
                    target: "/b".to_owned(),
                    authorization: Vec::new(),
                },
            ]
        );
    }

    /// A held request waits unrecorded while another path answers, and is
    /// answered and recorded once released.
    #[test]
    fn a_held_request_waits_until_release() {
        let stub = RegistryStub::start();
        stub.script("/a", 200, "a", 0);
        stub.script("/b", 200, "b", 0);
        stub.hold("/a");
        std::thread::scope(|scope| {
            // A failed assertion lets the held request through, so the scope's
            // join does not wait for its read to time out.
            let _release = Release(&stub);
            let held = scope.spawn(|| get(stub.addr(), "/a", ""));
            let gate = stub
                .state
                .changed
                .wait_timeout_while(lock(&stub.state.gate), HANG, |g| g.waiting == 0)
                .unwrap();
            assert_eq!(gate.0.waiting, 1, "the request for /a reaches the gate");
            drop(gate);
            assert_eq!(body(&get(stub.addr(), "/b", "")), "b");
            assert_eq!(stub.requests().len(), 1, "{:?}", stub.requests());
            stub.release();
            assert_eq!(body(&held.join().unwrap()), "a");
        });
        assert_eq!(stub.path_hits("/a"), 1);
        assert_eq!(lock(&stub.state.gate).waiting, 0);
    }

    /// Releases the stub's gate when dropped.
    struct Release<'a>(&'a RegistryStub);

    impl Drop for Release<'_> {
        fn drop(&mut self) {
            self.0.release();
        }
    }
}
