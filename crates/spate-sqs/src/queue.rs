//! The queue the feed talks to: the calls it makes, as plain data, and
//! [`SdkQueue`], which makes them through the AWS SDK.
//!
//! The feed's tasks see only [`Queue`], so a scripted queue in the unit tests
//! stands in for SQS.

use crate::error::{CallError, Class, classify};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
    MessageAttributeValue, MessageSystemAttributeName, QueueAttributeName,
    SendMessageBatchRequestEntry,
};
use std::future::Future;
use std::pin::Pin;

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
    /// `ApproximateFirstReceiveTimestamp`, epoch milliseconds.
    pub(crate) first_receive_ms: Option<String>,
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

    fn change_visibility_batch(
        &self,
        entries: Vec<VisibilityEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>>;

    fn delete_batch(
        &self,
        entries: Vec<DeleteEntry>,
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
    /// The queue's ARN: one read that proves the queue exists and the
    /// credentials reach it.
    pub(crate) async fn queue_arn(&self) -> Result<String, CallError> {
        let out = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| classify("GetQueueAttributes", &e))?;
        Ok(out
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::QueueArn))
            .cloned()
            .unwrap_or_default())
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
                .message_system_attribute_names(
                    MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
                )
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
                        first_receive_ms: system(
                            &MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
                        ),
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
            let out = call
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
