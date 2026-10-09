//! A Toxiproxy server in a container on a named Docker network, and the
//! calls to its HTTP API that create proxies and add, remove and disable
//! faults on them.

use std::fmt;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::{container_image, http, http_with_body};

/// The container port of Toxiproxy's HTTP API.
pub const API_PORT: u16 = 8474;

/// The container ports a proxy may listen on, each published to the host.
pub const LISTEN_PORTS: RangeInclusive<u16> = 21000..=21015;

const READY: Duration = Duration::from_secs(30);

/// The direction of a connection a toxic acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    /// Client to server.
    Upstream,
    /// Server to client.
    Downstream,
}

impl Stream {
    fn as_str(self) -> &'static str {
        match self {
            Stream::Upstream => "upstream",
            Stream::Downstream => "downstream",
        }
    }
}

/// A toxic's type and attributes. Every toxic is added with toxicity 1, so it
/// applies to every connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Toxic {
    /// Holds each chunk this long before passing it on, with no jitter.
    /// Removing it passes held data on at once.
    Latency(Duration),
    /// Drops everything sent and closes the connection after this long, or
    /// never when zero. Removing it closes the connection.
    Timeout(Duration),
    /// Closes the connection once this many bytes have passed.
    LimitData(u64),
}

impl Toxic {
    fn kind(self) -> &'static str {
        match self {
            Toxic::Latency(_) => "latency",
            Toxic::Timeout(_) => "timeout",
            Toxic::LimitData(_) => "limit_data",
        }
    }

    fn attributes(self) -> Value {
        match self {
            Toxic::Latency(d) => json!({ "latency": millis(d), "jitter": 0 }),
            Toxic::Timeout(d) => json!({ "timeout": millis(d) }),
            Toxic::LimitData(bytes) => json!({ "bytes": bytes }),
        }
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A running Toxiproxy container. Dropping it removes the container.
pub struct Toxiproxy {
    container: Container<GenericImage>,
    api: SocketAddr,
}

impl fmt::Debug for Toxiproxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Toxiproxy")
            .field("container", &self.container.id())
            .field("api", &self.api)
            .finish()
    }
}

impl Toxiproxy {
    /// Starts the image `ci/toxiproxy/` pins, pulled by digest, on `network`,
    /// and waits until its API answers. Proxies on it reach other containers
    /// on `network` by container name.
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or its API does not answer
    /// within 30 seconds.
    ///
    /// # Panics
    ///
    /// Panics as [`container_image`] does.
    pub fn start(network: &str) -> Result<Toxiproxy, String> {
        let (image, tag) = container_image(&["--pull", "toxiproxy"]);
        let mut image = GenericImage::new(image, tag)
            .with_exposed_port(API_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Starting Toxiproxy HTTP server"));
        for port in LISTEN_PORTS {
            image = image.with_exposed_port(port.tcp());
        }
        let container = image
            .with_network(network)
            .start()
            .map_err(|e| format!("start Toxiproxy: {e}"))?;
        let port = container
            .get_host_port_ipv4(API_PORT)
            .map_err(|e| format!("Toxiproxy API port: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], port));
        // The server logs its start line before it listens.
        let until = Instant::now() + READY;
        loop {
            match http(api, "GET", "/version") {
                Ok((200, _)) => break,
                failure if Instant::now() >= until => {
                    return Err(format!(
                        "the Toxiproxy API did not answer within {READY:?}: {failure:?}"
                    ));
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        Ok(Toxiproxy { container, api })
    }

    /// The host address of the HTTP API.
    #[must_use]
    pub fn api(&self) -> SocketAddr {
        self.api
    }

    /// Creates the enabled proxy `name`, listening on container port `listen`
    /// and forwarding to `upstream` as `host:port`, and returns the host
    /// address clients connect to. Toxiproxy resolves `upstream` on each
    /// connection.
    ///
    /// # Errors
    ///
    /// Fails when `listen` is outside [`LISTEN_PORTS`] or the API refuses the
    /// proxy.
    pub fn create_proxy(
        &self,
        name: &str,
        listen: u16,
        upstream: &str,
    ) -> Result<SocketAddr, String> {
        if !LISTEN_PORTS.contains(&listen) {
            return Err(format!("listen port {listen} is outside {LISTEN_PORTS:?}"));
        }
        create_proxy(self.api, name, listen, upstream)?;
        let port = self
            .container
            .get_host_port_ipv4(listen)
            .map_err(|e| format!("Toxiproxy port {listen}: {e}"))?;
        Ok(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    /// Adds `toxic` as `name` on `stream` of `proxy`.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses it, as it does for a name already in use.
    pub fn add_toxic(
        &self,
        proxy: &str,
        name: &str,
        stream: Stream,
        toxic: Toxic,
    ) -> Result<(), String> {
        add_toxic(self.api, proxy, name, stream, toxic)
    }

    /// Removes the toxic `name` from `proxy`.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses it, as it does for an unknown name.
    pub fn remove_toxic(&self, proxy: &str, name: &str) -> Result<(), String> {
        remove_toxic(self.api, proxy, name)
    }

    /// Enables or disables `proxy`. A disabled proxy closes its open
    /// connections and stops listening. With Docker's default port forwarding,
    /// a new connection to the address [`Toxiproxy::create_proxy`] returns is
    /// still accepted and then closed, so the client sees a reset or an end of
    /// stream.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses it.
    pub fn set_enabled(&self, proxy: &str, enabled: bool) -> Result<(), String> {
        set_enabled(self.api, proxy, enabled)
    }
}

fn create_proxy(api: SocketAddr, name: &str, listen: u16, upstream: &str) -> Result<(), String> {
    let body = json!({
        "name": name,
        "listen": format!("0.0.0.0:{listen}"),
        "upstream": upstream,
        "enabled": true,
    });
    call(api, "POST", "/proxies", &body.to_string())
}

fn add_toxic(
    api: SocketAddr,
    proxy: &str,
    name: &str,
    stream: Stream,
    toxic: Toxic,
) -> Result<(), String> {
    let body = json!({
        "name": name,
        "type": toxic.kind(),
        "stream": stream.as_str(),
        "toxicity": 1,
        "attributes": toxic.attributes(),
    });
    call(
        api,
        "POST",
        &format!("/proxies/{proxy}/toxics"),
        &body.to_string(),
    )
}

fn remove_toxic(api: SocketAddr, proxy: &str, name: &str) -> Result<(), String> {
    call(
        api,
        "DELETE",
        &format!("/proxies/{proxy}/toxics/{name}"),
        "",
    )
}

fn set_enabled(api: SocketAddr, proxy: &str, enabled: bool) -> Result<(), String> {
    let body = json!({ "enabled": enabled });
    call(
        api,
        "PATCH",
        &format!("/proxies/{proxy}"),
        &body.to_string(),
    )
}

/// Sends one API request and fails on any status outside 2xx, with the body.
fn call(api: SocketAddr, method: &str, path: &str, body: &str) -> Result<(), String> {
    match http_with_body(api, method, path, body) {
        Ok((status, _)) if (200..300).contains(&status) => Ok(()),
        Ok((status, reply)) => Err(format!("Toxiproxy {method} {path}: {status} {reply}")),
        Err(e) => Err(format!("Toxiproxy {method} {path}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_once;

    const OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

    fn json_of(body: &str) -> Value {
        serde_json::from_str(body).unwrap()
    }

    /// A proxy listens on every interface at its container port and starts
    /// enabled.
    #[test]
    fn create_proxy_sends_listen_upstream_and_enabled() {
        let (addr, server) = serve_once("HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n");
        create_proxy(addr, "nats-0", 21003, "nats-a:4222").unwrap();
        let request = server.join().unwrap();
        assert_eq!(request.line, "POST /proxies HTTP/1.1");
        assert_eq!(
            json_of(&request.body),
            json!({
                "name": "nats-0",
                "listen": "0.0.0.0:21003",
                "upstream": "nats-a:4222",
                "enabled": true,
            })
        );
    }

    /// A latency toxic names its stream, applies to every connection, and has
    /// no jitter.
    #[test]
    fn latency_names_its_stream_with_no_jitter_and_toxicity_1() {
        let (addr, server) = serve_once(OK);
        let latency = Toxic::Latency(Duration::from_millis(250));
        add_toxic(addr, "nats-0", "slow", Stream::Upstream, latency).unwrap();
        let request = server.join().unwrap();
        assert_eq!(request.line, "POST /proxies/nats-0/toxics HTTP/1.1");
        assert_eq!(
            json_of(&request.body),
            json!({
                "name": "slow",
                "type": "latency",
                "stream": "upstream",
                "toxicity": 1,
                "attributes": { "latency": 250, "jitter": 0 },
            })
        );
    }

    /// Timeout and data-limit toxics carry their own attribute and the stream
    /// they were given.
    #[test]
    fn timeout_and_limit_data_carry_their_attribute_and_stream() {
        let cases = [
            (
                Toxic::Timeout(Duration::ZERO),
                Stream::Downstream,
                json!({
                    "name": "t",
                    "type": "timeout",
                    "stream": "downstream",
                    "toxicity": 1,
                    "attributes": { "timeout": 0 },
                }),
            ),
            (
                Toxic::Timeout(Duration::from_millis(1500)),
                Stream::Upstream,
                json!({
                    "name": "t",
                    "type": "timeout",
                    "stream": "upstream",
                    "toxicity": 1,
                    "attributes": { "timeout": 1500 },
                }),
            ),
            (
                Toxic::LimitData(4096),
                Stream::Upstream,
                json!({
                    "name": "t",
                    "type": "limit_data",
                    "stream": "upstream",
                    "toxicity": 1,
                    "attributes": { "bytes": 4096 },
                }),
            ),
        ];
        for (toxic, stream, expected) in cases {
            let (addr, server) = serve_once(OK);
            add_toxic(addr, "p", "t", stream, toxic).unwrap();
            assert_eq!(json_of(&server.join().unwrap().body), expected, "{toxic:?}");
        }
    }

    /// Removing a toxic is a `DELETE` of its path with an empty body.
    #[test]
    fn remove_toxic_deletes_its_path() {
        let (addr, server) = serve_once("HTTP/1.1 204 No Content\r\n\r\n");
        remove_toxic(addr, "nats-0", "slow").unwrap();
        let request = server.join().unwrap();
        assert_eq!(request.line, "DELETE /proxies/nats-0/toxics/slow HTTP/1.1");
        assert_eq!(request.body, "");
    }

    /// Disabling a proxy is a `PATCH` of its path with `enabled: false`.
    #[test]
    fn set_enabled_patches_the_flag() {
        let (addr, server) = serve_once(OK);
        set_enabled(addr, "nats-0", false).unwrap();
        let request = server.join().unwrap();
        assert_eq!(request.line, "PATCH /proxies/nats-0 HTTP/1.1");
        assert_eq!(json_of(&request.body), json!({ "enabled": false }));
    }

    /// A status outside 2xx is an error carrying the status and the reply.
    #[test]
    fn a_refused_call_is_an_error_with_the_reply() {
        let (addr, server) =
            serve_once("HTTP/1.1 409 Conflict\r\nContent-Length: 14\r\n\r\ntoxic exists\r\n");
        let err = add_toxic(addr, "p", "t", Stream::Upstream, Toxic::LimitData(1)).unwrap_err();
        assert!(err.contains("409") && err.contains("toxic exists"), "{err}");
        server.join().unwrap();
    }
}
