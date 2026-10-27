//! Ceph RGW: the bootstrap that runs a one-node cluster and its gateway in
//! the image's container, its readiness path, and the persistent Kafka topic
//! and notification configuration a bucket's events go through.

use testcontainers::{ContainerRequest, GenericImage, ImageExt};

use super::{ACCESS_KEY, BROKER_FROM_INSIDE, SECRET_KEY};

/// The container port of the S3 API, as the bootstrap's beast frontend sets
/// it. It is clear of the broker's listeners when RGW joins the broker's
/// network namespace.
pub(super) const PORT: u16 = 7480;

/// A path RGW answers `200` without credentials once it listens. The
/// bootstrap creates the user before radosgw starts, so a signed call
/// succeeds from the first `200`.
pub(super) const READY_PATH: &str = "/";

/// The `OpaqueData` every topic carries, which RGW copies into each event.
pub(crate) const OPAQUE_DATA: &str = "spate-test-opaque";

/// Where the bootstrap is copied in the container.
const BOOTSTRAP_PATH: &str = "/spate/rgw.sh";

const BOOTSTRAP: &str = include_str!("rgw.sh");

/// The image running the bootstrap with the fixed key pair.
pub(super) fn configure(image: GenericImage) -> ContainerRequest<GenericImage> {
    image
        .with_entrypoint("/bin/bash")
        .with_copy_to(BOOTSTRAP_PATH, BOOTSTRAP.as_bytes().to_vec())
        .with_cmd([BOOTSTRAP_PATH, ACCESS_KEY, SECRET_KEY])
}

/// The image as [`configure`] runs it. RGW takes its Kafka target through
/// [`create_topic_body`] once it serves requests.
pub(super) fn notifying(image: GenericImage) -> ContainerRequest<GenericImage> {
    configure(image)
}

/// The form body of the `CreateTopic` call for a persistent topic that
/// produces each event to the Kafka topic `topic`, acknowledged by the
/// broker, carrying `opaque` as its `OpaqueData`.
pub(super) fn create_topic_body(topic: &str, opaque: &str) -> String {
    let endpoint = format!("kafka://{BROKER_FROM_INSIDE}");
    let attributes = [
        ("push-endpoint", endpoint.as_str()),
        ("persistent", "true"),
        ("kafka-ack-level", "broker"),
        ("OpaqueData", opaque),
    ];
    let mut pairs = vec![
        ("Action".to_owned(), "CreateTopic"),
        ("Name".to_owned(), topic),
    ];
    for (i, (key, value)) in attributes.into_iter().enumerate() {
        let n = i + 1;
        pairs.push((format!("Attributes.entry.{n}.key"), key));
        pairs.push((format!("Attributes.entry.{n}.value"), value));
    }
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", form_encoded(key), form_encoded(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The `TopicArn` a `CreateTopic` reply names.
///
/// # Errors
///
/// Fails, carrying the reply, when it names none.
pub(super) fn topic_arn(reply: &str) -> Result<String, String> {
    let arn = reply
        .split_once("<TopicArn>")
        .and_then(|(_, rest)| rest.split_once("</TopicArn>"))
        .map(|(arn, _)| arn.trim());
    match arn {
        Some(arn) if !arn.is_empty() => Ok(arn.to_owned()),
        _ => Err(format!("the CreateTopic reply names no TopicArn: {reply}")),
    }
}

/// The `PUT ?notification` body sending every `s3:ObjectCreated:*` event to
/// the topic `arn`.
pub(super) fn notification_configuration(arn: &str) -> String {
    format!(
        "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <TopicConfiguration><Id>spate-test</Id><Topic>{arn}</Topic>\
         <Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>"
    )
}

/// `s` encoded for an `application/x-www-form-urlencoded` body: every byte
/// other than an ASCII letter, a digit, `-`, `.`, `_` and `*` is written
/// `%HH`.
fn form_encoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'*') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
