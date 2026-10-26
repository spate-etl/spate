//! The queue the feed talks to: the calls it makes, as plain data, and
//! [`SdkQueue`], which makes them through the AWS SDK.
//!
//! The feed's tasks see only [`Queue`], so a scripted queue in the unit tests
//! stands in for SQS.

use crate::client;
use crate::error::{CallError, Class, classify};
use crate::startup::QueueAttributes;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::retry::RetryConfig;
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
    MessageAttributeValue, MessageSystemAttributeName, QueueAttributeName,
    SendMessageBatchRequestEntry,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// The String message attribute that carries a unit's id.
pub(crate) const UNIT_ATTRIBUTE: &str = "spate.unit";

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One `ReceiveMessage` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReceiveRequest {
    pub(crate) max: i32,
    pub(crate) visibility: i32,
    pub(crate) wait: i32,
}

/// A message as a receive returned it, its attributes still unparsed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Received {
    pub(crate) message_id: String,
    pub(crate) handle: String,
    pub(crate) body: String,
    /// The `spate.unit` attribute.
    pub(crate) unit_id: Option<String>,
    /// `ApproximateReceiveCount`.
    pub(crate) receive_count: Option<String>,
}

/// One entry of a `SendMessageBatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SendEntry {
    pub(crate) id: String,
    pub(crate) body: String,
    pub(crate) unit_id: String,
}

/// One entry of a `ChangeMessageVisibilityBatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VisibilityEntry {
    pub(crate) id: String,
    pub(crate) handle: String,
    pub(crate) seconds: i32,
}

/// One entry of a `DeleteMessageBatch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeleteEntry {
    pub(crate) id: String,
    pub(crate) handle: String,
}

/// A batch call that SQS answered: the entries it applied and those it
/// failed. An entry in neither list is the caller's to treat as failed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BatchOutcome {
    pub(crate) successful: Vec<String>,
    pub(crate) failed: Vec<EntryFailure>,
}

/// One failed entry of a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EntryFailure {
    pub(crate) id: String,
    pub(crate) sender_fault: bool,
    pub(crate) code: String,
    pub(crate) message: String,
}

/// The calls the feed makes. Every error is already classified.
pub(crate) trait Queue: Send + Sync + std::fmt::Debug + 'static {
    fn receive(&self, request: ReceiveRequest) -> BoxFuture<'_, Result<Vec<Received>, CallError>>;

    fn send_batch(&self, entries: Vec<SendEntry>)
    -> BoxFuture<'_, Result<BatchOutcome, CallError>>;

    /// A `ChangeMessageVisibilityBatch`. With `within` set, the call and
    /// its retries end after that long, as a Retryable timeout.
    fn change_visibility_batch(
        &self,
        entries: Vec<VisibilityEntry>,
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>>;

    /// A `DeleteMessageBatch`, bounded by `within` as
    /// [`change_visibility_batch`](Queue::change_visibility_batch) is.
    fn delete_batch(
        &self,
        entries: Vec<DeleteEntry>,
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>>;
}

/// [`Queue`] over the AWS SDK's client.
#[derive(Debug)]
pub(crate) struct SdkQueue {
    pub(crate) client: Client,
    pub(crate) queue_url: String,
    /// Timeouts for `ReceiveMessage`, whose long poll outlasts the client's
    /// read timeout.
    pub(crate) receive_timeouts: aws_sdk_sqs::config::Builder,
}

impl SdkQueue {
    /// The attributes `names` of the queue at `queue_url`, which may be
    /// another queue than the one this reads, keyed by name.
    pub(crate) async fn attributes(
        &self,
        queue_url: &str,
        names: &[&str],
    ) -> Result<HashMap<String, String>, CallError> {
        let out = self
            .client
            .get_queue_attributes()
            .queue_url(queue_url)
            .set_attribute_names(Some(
                names.iter().map(|n| QueueAttributeName::from(*n)).collect(),
            ))
            .send()
            .await
            .map_err(|e| classify("GetQueueAttributes", &e))?;
        Ok(out
            .attributes()
            .into_iter()
            .flatten()
            .map(|(name, value)| (name.as_str().to_owned(), value.clone()))
            .collect())
    }
}

impl QueueAttributes for SdkQueue {
    fn attributes<'a>(
        &'a self,
        queue_url: &'a str,
        names: &'static [&'static str],
    ) -> BoxFuture<'a, Result<HashMap<String, String>, CallError>> {
        Box::pin(SdkQueue::attributes(self, queue_url, names))
    }
}

/// An SDK builder refused an entry the feed built: a batcher bug.
fn built<T>(entry: Result<T, aws_sdk_sqs::error::BuildError>) -> Result<T, CallError> {
    entry.map_err(|e| CallError {
        class: Class::Fatal,
        batch_shape: true,
        message: format!("building a batch entry: {e}"),
    })
}

/// The timeouts that end a call and its retries after `within`. They set
/// only timeouts, so the client's retry config, and with it the attempt
/// count, still applies.
fn bounded(within: Option<Duration>) -> Option<aws_sdk_sqs::config::Builder> {
    within.map(|within| {
        aws_sdk_sqs::config::Builder::default().timeout_config(client::timeouts(
            client::READ_TIMEOUT,
            within.min(client::OPERATION_TIMEOUT),
        ))
    })
}

fn failures(failed: &[aws_sdk_sqs::types::BatchResultErrorEntry]) -> Vec<EntryFailure> {
    failed
        .iter()
        .map(|f| EntryFailure {
            id: f.id().to_owned(),
            sender_fault: f.sender_fault(),
            code: f.code().to_owned(),
            message: f.message().unwrap_or_default().to_owned(),
        })
        .collect()
}

impl Queue for SdkQueue {
    fn receive(&self, request: ReceiveRequest) -> BoxFuture<'_, Result<Vec<Received>, CallError>> {
        Box::pin(async move {
            let out = self
                .client
                .receive_message()
                .queue_url(&self.queue_url)
                .max_number_of_messages(request.max)
                .visibility_timeout(request.visibility)
                .wait_time_seconds(request.wait)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names(UNIT_ATTRIBUTE)
                .customize()
                .config_override(self.receive_timeouts.clone())
                .send()
                .await
                .map_err(|e| classify("ReceiveMessage", &e))?;
            Ok(out
                .messages()
                .iter()
                .map(|m| {
                    let system = |name: &MessageSystemAttributeName| {
                        m.attributes().and_then(|a| a.get(name)).cloned()
                    };
                    Received {
                        message_id: m.message_id().unwrap_or_default().to_owned(),
                        handle: m.receipt_handle().unwrap_or_default().to_owned(),
                        body: m.body().unwrap_or_default().to_owned(),
                        unit_id: m
                            .message_attributes()
                            .and_then(|a| a.get(UNIT_ATTRIBUTE))
                            .and_then(|v| v.string_value())
                            .map(str::to_owned),
                        receive_count: system(&MessageSystemAttributeName::ApproximateReceiveCount),
                    }
                })
                .collect())
        })
    }

    fn send_batch(
        &self,
        entries: Vec<SendEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut call = self.client.send_message_batch().queue_url(&self.queue_url);
            for entry in entries {
                let mut message = SendMessageBatchRequestEntry::builder()
                    .id(entry.id)
                    .message_body(entry.body);
                if !entry.unit_id.is_empty() {
                    let id = built(
                        MessageAttributeValue::builder()
                            .data_type("String")
                            .string_value(entry.unit_id)
                            .build(),
                    )?;
                    message = message.message_attributes(UNIT_ATTRIBUTE, id);
                }
                call = call.entries(built(message.build())?);
            }
            // One attempt per call: the feed's own loop retries, and it must
            // see every failed attempt to know that SQS may hold the unit.
            let out = call
                .customize()
                .config_override(
                    aws_sdk_sqs::config::Builder::default()
                        .retry_config(RetryConfig::standard().with_max_attempts(1)),
                )
                .send()
                .await
                .map_err(|e| classify("SendMessageBatch", &e))?;
            Ok(BatchOutcome {
                successful: out.successful().iter().map(|s| s.id().to_owned()).collect(),
                failed: failures(out.failed()),
            })
        })
    }

    fn change_visibility_batch(
        &self,
        entries: Vec<VisibilityEntry>,
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut call = self
                .client
                .change_message_visibility_batch()
                .queue_url(&self.queue_url);
            for entry in entries {
                call = call.entries(built(
                    ChangeMessageVisibilityBatchRequestEntry::builder()
                        .id(entry.id)
                        .receipt_handle(entry.handle)
                        .visibility_timeout(entry.seconds)
                        .build(),
                )?);
            }
            let call = call.customize();
            let call = match bounded(within) {
                Some(timeouts) => call.config_override(timeouts),
                None => call,
            };
            let out = call
                .send()
                .await
                .map_err(|e| classify("ChangeMessageVisibilityBatch", &e))?;
            Ok(BatchOutcome {
                successful: out.successful().iter().map(|s| s.id().to_owned()).collect(),
                failed: failures(out.failed()),
            })
        })
    }

    fn delete_batch(
        &self,
        entries: Vec<DeleteEntry>,
        within: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut call = self
                .client
                .delete_message_batch()
                .queue_url(&self.queue_url);
            for entry in entries {
                call = call.entries(built(
                    DeleteMessageBatchRequestEntry::builder()
                        .id(entry.id)
                        .receipt_handle(entry.handle)
                        .build(),
                )?);
            }
            let call = call.customize();
            let call = match bounded(within) {
                Some(timeouts) => call.config_override(timeouts),
                None => call,
            };
            let out = call
                .send()
                .await
                .map_err(|e| classify("DeleteMessageBatch", &e))?;
            Ok(BatchOutcome {
                successful: out.successful().iter().map(|s| s.id().to_owned()).collect(),
                failed: failures(out.failed()),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqsFeedConfig;
    use crate::config::tests::{PARTITION_REGIONS, sdk_host};
    use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};
    use spate_test_support::{TestCa, native_certs, serve_tls};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    /// An [`SdkQueue`] over `config`, with a static access key.
    async fn queue_for(config: &SqsFeedConfig) -> SdkQueue {
        let credentials = Credentials::new("x", "x", None, None, "test");
        let step = std::sync::Mutex::new("starting");
        crate::client::sdk_queue(
            config,
            Some(SharedCredentialsProvider::new(credentials)),
            &step,
        )
        .await
        .unwrap()
    }

    /// An [`SdkQueue`] over `endpoint`, with a static access key.
    async fn queue_at(endpoint: &str) -> SdkQueue {
        queue_for(&SqsFeedConfig {
            queue_url: format!("{endpoint}/000000000000/jobs"),
            visibility: Duration::from_secs(60),
            region: Some("eu-west-2".to_owned()),
            endpoint: Some(endpoint.to_owned()),
        })
        .await
    }

    /// Pins that the client takes its region from the queue URL's host, in
    /// one region per partition the SDK lists, when `region` is unset.
    #[tokio::test]
    async fn the_client_takes_its_region_from_the_queue_url_host() {
        for region in PARTITION_REGIONS {
            let host = sdk_host(region, false, false).await;
            let queue = queue_for(&SqsFeedConfig {
                queue_url: format!("https://{host}/000000000000/jobs"),
                visibility: Duration::from_secs(60),
                region: None,
                endpoint: Some("http://127.0.0.1:1".to_owned()),
            })
            .await;
            assert_eq!(
                queue.client.config().region(),
                Some(&Region::new(region)),
                "{host}"
            );
        }
    }

    /// Pins that a private CA the system trust store yields is trusted: a
    /// `GetQueueAttributes` call to a server it signed succeeds.
    #[tokio::test]
    async fn a_private_ca_in_the_system_store_is_trusted() {
        let ca = TestCa::new("private");
        let addr = serve_tls(ca.server_config(None), |mut tls| async move {
            let mut buf = vec![0u8; 65536];
            let _ = tls.read(&mut buf).await;
            let body = r#"{"Attributes":{"QueueArn":"arn:aws:sqs:eu-west-2:000000000000:jobs"}}"#;
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = tls.write_all(reply.as_bytes()).await;
            let _ = tokio::io::copy(&mut tls, &mut tokio::io::sink()).await;
        })
        .await;
        let der = ca.der();
        let http = crate::client::http_client(move || native_certs(vec![der]))
            .await
            .unwrap();
        let endpoint = format!("https://{addr}");
        let conf = aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .http_client(http)
            .endpoint_url(&endpoint)
            .region(Region::new("eu-west-2"))
            .credentials_provider(Credentials::new("x", "x", None, None, "test"))
            .build();
        let queue = SdkQueue {
            client: Client::from_conf(conf),
            queue_url: format!("{endpoint}/000000000000/jobs"),
            receive_timeouts: aws_sdk_sqs::config::Builder::default(),
        };
        let attributes = queue
            .attributes(&queue.queue_url, &["QueueArn"])
            .await
            .unwrap();
        assert_eq!(
            attributes.get("QueueArn").map(String::as_str),
            Some("arn:aws:sqs:eu-west-2:000000000000:jobs")
        );
    }

    /// Pins that a server whose certificate the client does not trust fails
    /// the call as Fatal, with the TLS error in its message.
    #[tokio::test]
    async fn a_rejected_tls_handshake_is_fatal() {
        let ca = TestCa::new("untrusted");
        let addr = serve_tls(ca.server_config(None), |_| async {}).await;
        let queue = queue_at(&format!("https://{addr}")).await;
        let e = queue
            .attributes(&queue.queue_url, &["QueueArn"])
            .await
            .unwrap_err();
        assert_eq!(e.class, Class::Fatal, "{}", e.message);
        assert!(e.message.contains("UnknownIssuer"), "{}", e.message);
    }

    /// An [`SdkQueue`] over a listener that reads every request and never
    /// answers, with tokio time paused.
    async fn silent_queue() -> SdkQueue {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut tcp, &mut tokio::io::sink()).await;
                });
            }
        });
        let queue = queue_at(&format!("http://{addr}")).await;
        tokio::time::pause();
        queue
    }

    /// Pins that `within` bounds a `ChangeMessageVisibilityBatch` and its
    /// retries: against a server that never answers, the call fails as
    /// Retryable after `within` and before twice `within`.
    #[tokio::test]
    async fn a_visibility_call_ends_within_its_bound() {
        let queue = silent_queue().await;
        let within = Duration::from_secs(1);
        let started = tokio::time::Instant::now();
        let entry = VisibilityEntry {
            id: "0".to_owned(),
            handle: "h".to_owned(),
            seconds: 0,
        };
        let e = queue
            .change_visibility_batch(vec![entry], Some(within))
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert_eq!(e.class, Class::Retryable, "{}", e.message);
        assert!(
            elapsed >= within && elapsed < within * 2,
            "ended after {elapsed:?}: {}",
            e.message
        );
    }

    /// Pins that `within` bounds a `DeleteMessageBatch` and its retries, as
    /// it does a visibility change.
    #[tokio::test]
    async fn a_delete_call_ends_within_its_bound() {
        let queue = silent_queue().await;
        let within = Duration::from_secs(1);
        let started = tokio::time::Instant::now();
        let entry = DeleteEntry {
            id: "0".to_owned(),
            handle: "h".to_owned(),
        };
        let e = queue
            .delete_batch(vec![entry], Some(within))
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert_eq!(e.class, Class::Retryable, "{}", e.message);
        assert!(
            elapsed >= within && elapsed < within * 2,
            "ended after {elapsed:?}: {}",
            e.message
        );
    }
}
