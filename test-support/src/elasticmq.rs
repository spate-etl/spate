//! An ElasticMQ queue server in a container, and the SQS query API calls that
//! create a queue and send, receive and delete its messages.
//!
//! Each call is a `GET` with its parameters in the query string. The server
//! reads a `POST` body only with a form `Content-Type`, which
//! [`http_with_body`](crate::http_with_body) does not send.

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage};

use crate::{container_image, http};

/// The container port of the SQS query API.
pub const QUERY_PORT: u16 = 9324;

const READY: Duration = Duration::from_secs(30);

/// A message as a receive returned it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    /// The body, unescaped.
    pub body: String,
    /// The handle that deletes this receipt of the message.
    pub receipt_handle: String,
}

/// A running ElasticMQ container. Dropping it removes the container.
pub struct ElasticMq {
    container: Container<GenericImage>,
    api: SocketAddr,
}

impl fmt::Debug for ElasticMq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ElasticMq")
            .field("container", &self.container.id())
            .field("api", &self.api)
            .finish()
    }
}

impl ElasticMq {
    /// Starts the image `ci/elasticmq/` pins, pulled by digest, and waits
    /// until the query API answers.
    ///
    /// # Errors
    ///
    /// Fails when the container does not start or its API does not answer
    /// within 30 seconds.
    ///
    /// # Panics
    ///
    /// Panics as [`container_image`] does.
    pub fn start() -> Result<ElasticMq, String> {
        let (image, tag) = container_image(&["--pull", "elasticmq"]);
        let container = GenericImage::new(image, tag)
            .with_exposed_port(QUERY_PORT.tcp())
            .start()
            .map_err(|e| format!("start ElasticMQ: {e}"))?;
        let port = container
            .get_host_port_ipv4(QUERY_PORT)
            .map_err(|e| format!("ElasticMQ port: {e}"))?;
        let api = SocketAddr::from(([127, 0, 0, 1], port));
        let until = Instant::now() + READY;
        loop {
            match call(api, "/", &[("Action", "ListQueues")]) {
                Ok(_) => break,
                Err(failure) if Instant::now() >= until => {
                    return Err(format!(
                        "the ElasticMQ API did not answer within {READY:?}: {failure}"
                    ));
                }
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        Ok(ElasticMq { container, api })
    }

    /// The endpoint URL a client on the host reaches the query API at.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.api)
    }

    /// Creates the queue `name` and returns its path on this server, such as
    /// `/000000000000/name`. The queue URL the server reports names its own
    /// address, which the host does not reach.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call or its reply carries no queue URL.
    pub fn create_queue(&self, name: &str) -> Result<String, String> {
        self.create_queue_with_attributes(name, &[])
    }

    /// Creates the queue `name` with each `(name, value)` queue attribute,
    /// such as `("RedrivePolicy", …)`, and returns its path as
    /// [`ElasticMq::create_queue`] does.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call, as it does for a redrive policy
    /// naming a queue that does not exist, or its reply carries no queue URL.
    pub fn create_queue_with_attributes(
        &self,
        name: &str,
        attributes: &[(&str, &str)],
    ) -> Result<String, String> {
        let keys: Vec<(String, String)> = (1..=attributes.len())
            .map(|n| {
                (
                    format!("Attribute.{n}.Name"),
                    format!("Attribute.{n}.Value"),
                )
            })
            .collect();
        let mut params = vec![("Action", "CreateQueue"), ("QueueName", name)];
        for ((name_key, value_key), (attribute, value)) in keys.iter().zip(attributes) {
            params.push((name_key.as_str(), attribute));
            params.push((value_key.as_str(), value));
        }
        let reply = call(self.api, "/", &params)?;
        let url = element(&reply, "QueueUrl")
            .ok_or_else(|| format!("no QueueUrl in CreateQueue reply: {reply}"))?;
        let path = url
            .split_once("://")
            .and_then(|(_, rest)| rest.find('/').map(|at| &rest[at..]))
            .ok_or_else(|| format!("no path in queue URL {url}"))?;
        Ok(path.to_owned())
    }

    /// Deletes the queue at `queue`, a path [`ElasticMq::create_queue`]
    /// returned.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call.
    pub fn delete_queue(&self, queue: &str) -> Result<(), String> {
        call(self.api, queue, &[("Action", "DeleteQueue")]).map(drop)
    }

    /// The value of the queue attribute `name` on `queue`, unescaped, or
    /// `None` when the server does not report it.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call.
    pub fn queue_attribute(&self, queue: &str, name: &str) -> Result<Option<String>, String> {
        let reply = call(
            self.api,
            queue,
            &[("Action", "GetQueueAttributes"), ("AttributeName.1", name)],
        )?;
        Ok(reply
            .split("<Attribute>")
            .find(|a| element(a, "Name") == Some(name))
            .and_then(|a| element(a, "Value"))
            .map(unescape))
    }

    /// The ARN of `queue`, which a redrive policy names it by.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call or reports no ARN.
    pub fn queue_arn(&self, queue: &str) -> Result<String, String> {
        self.queue_attribute(queue, "QueueArn")?
            .ok_or_else(|| format!("no QueueArn for {queue}"))
    }

    /// Sends `body` to the queue at `queue`, a path [`ElasticMq::create_queue`]
    /// returned.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call.
    pub fn send_message(&self, queue: &str, body: &str) -> Result<(), String> {
        call(
            self.api,
            queue,
            &[("Action", "SendMessage"), ("MessageBody", body)],
        )
        .map(drop)
    }

    /// Receives up to ten messages from `queue` without waiting. Each stays
    /// in flight, and invisible to other receives, until deleted or until the
    /// queue's visibility timeout passes.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call or a message lacks a body or a
    /// receipt handle.
    pub fn receive_messages(&self, queue: &str) -> Result<Vec<Message>, String> {
        let reply = call(
            self.api,
            queue,
            &[
                ("Action", "ReceiveMessage"),
                ("MaxNumberOfMessages", "10"),
                ("WaitTimeSeconds", "0"),
            ],
        )?;
        reply
            .split("<Message>")
            .skip(1)
            .map(|message| {
                let body = element(message, "Body");
                let handle = element(message, "ReceiptHandle");
                match (body, handle) {
                    (Some(body), Some(handle)) => Ok(Message {
                        body: unescape(body),
                        receipt_handle: unescape(handle),
                    }),
                    _ => Err(format!("a message without a body or handle: {message}")),
                }
            })
            .collect()
    }

    /// Deletes the receipt `receipt_handle` names from `queue`.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call.
    pub fn delete_message(&self, queue: &str, receipt_handle: &str) -> Result<(), String> {
        call(
            self.api,
            queue,
            &[
                ("Action", "DeleteMessage"),
                ("ReceiptHandle", receipt_handle),
            ],
        )
        .map(drop)
    }

    /// The approximate number of visible and of in-flight messages on
    /// `queue`, in that order.
    ///
    /// # Errors
    ///
    /// Fails when the API refuses the call or the reply lacks either count.
    pub fn approximate_counts(&self, queue: &str) -> Result<(u64, u64), String> {
        let reply = call(
            self.api,
            queue,
            &[("Action", "GetQueueAttributes"), ("AttributeName.1", "All")],
        )?;
        let count = |name: &str| {
            reply
                .split("<Attribute>")
                .find(|a| element(a, "Name") == Some(name))
                .and_then(|a| element(a, "Value"))
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| format!("no {name} in GetQueueAttributes reply: {reply}"))
        };
        Ok((
            count("ApproximateNumberOfMessages")?,
            count("ApproximateNumberOfMessagesNotVisible")?,
        ))
    }
}

/// Sends one query API call to `path` and returns the reply body, failing on
/// any status but `200`.
fn call(api: SocketAddr, path: &str, params: &[(&str, &str)]) -> Result<String, String> {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
        .collect();
    let target = format!("{path}?{}", query.join("&"));
    match http(api, "GET", &target) {
        Ok((200, reply)) => Ok(reply),
        Ok((status, reply)) => Err(format!("ElasticMQ GET {target}: {status} {reply}")),
        Err(e) => Err(format!("ElasticMQ GET {target}: {e}")),
    }
}

/// Percent-encodes everything but the URI unreserved characters.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The text of the first `<tag>` element in `xml`, still escaped.
fn element<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let len = xml[start..].find(&format!("</{tag}>"))?;
    Some(&xml[start..start + len])
}

/// Undoes XML's five predefined entity escapes and `&#13;`, the numeric
/// reference the server sends for a carriage return.
fn unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#13;", "\r")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_once;

    /// Unreserved characters pass through and every other byte, multibyte
    /// UTF-8 included, is percent-encoded.
    #[test]
    fn encode_keeps_unreserved_characters_alone() {
        assert_eq!(encode("aZ09-._~"), "aZ09-._~");
        assert_eq!(encode("a b/c#d&e=é"), "a%20b%2Fc%23d%26e%3D%C3%A9");
        assert_eq!(encode("a+b"), "a%2Bb");
    }

    /// A call is a `GET` of the path with each parameter encoded.
    #[test]
    fn a_call_is_a_get_with_an_encoded_query() {
        let (addr, server) = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        let reply = call(
            addr,
            "/000000000000/q",
            &[("Action", "DeleteMessage"), ("ReceiptHandle", "id#1")],
        )
        .unwrap();
        assert_eq!(reply, "ok");
        let request = server.join().unwrap();
        assert_eq!(
            request.line,
            "GET /000000000000/q?Action=DeleteMessage&ReceiptHandle=id%231 HTTP/1.1"
        );
    }

    /// A status other than `200` is an error carrying the status and the reply.
    #[test]
    fn a_refused_call_is_an_error_with_the_reply() {
        let (addr, server) =
            serve_once("HTTP/1.1 400 Bad Request\r\nContent-Length: 13\r\n\r\nMissingAction");
        let err = call(addr, "/", &[]).unwrap_err();
        assert!(
            err.contains("400") && err.contains("MissingAction"),
            "{err}"
        );
        server.join().unwrap();
    }

    /// An element's text comes back escaped, and unescaping undoes `&amp;`
    /// last so an escaped entity or reference stays literal.
    #[test]
    fn element_text_unescapes_once() {
        let xml = "<Message><Body>a &amp;lt; b &lt; c</Body></Message>";
        assert_eq!(element(xml, "Body"), Some("a &amp;lt; b &lt; c"));
        assert_eq!(unescape(element(xml, "Body").unwrap()), "a &lt; b < c");
        assert_eq!(element(xml, "ReceiptHandle"), None);
        assert_eq!(unescape("a&#13;\nb &amp;#13;"), "a\r\nb &#13;");
    }
}
