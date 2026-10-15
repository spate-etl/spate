//! The crate's public error, and SDK errors classified as Fatal or Retryable.
//!
//! A credential, permission or TLS rejection, a KMS key the queue cannot
//! use, a missing queue and a malformed batch are Fatal: retrying cannot
//! succeed. Everything else, throttling, 5xx, timeouts and a failure to load
//! credentials included, is Retryable.

use aws_sdk_sqs::config::http::HttpResponse;
use aws_sdk_sqs::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_sqs::operation::change_message_visibility_batch::ChangeMessageVisibilityBatchError;
use aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError;
use aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError;
use aws_sdk_sqs::operation::receive_message::ReceiveMessageError;
use aws_sdk_sqs::operation::send_message_batch::SendMessageBatchError;
use spate_core::config::ConfigError;
use std::error::Error;
use std::fmt::Write as _;

/// Why [`SqsFeed::connect`](crate::SqsFeed::connect) could not build a feed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqsError {
    /// The `feed:` section is not one the SQS feed can read.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// SQS refused the request, and sending it again fails the same way: a
    /// rejected credential, a missing permission or queue, a rejected TLS
    /// handshake.
    #[error("SQS refused the feed: {0}")]
    Refused(String),
    /// SQS did not answer in time, or kept failing transiently. Starting
    /// again later may succeed.
    #[error("SQS is unavailable: {0}")]
    Unavailable(String),
}

/// The prefix SQS puts on its own error codes in the query protocol, which
/// the JSON protocol reports through its compatibility header.
const PREFIX: &str = "AWS.SimpleQueueService.";

/// Codes that mean the queue's URL names nothing.
const MISSING_QUEUE: &[&str] = &["QueueDoesNotExist", "NonExistentQueue"];

/// Codes for a batch the feed built wrongly. Sending it again fails the same
/// way.
pub(crate) const BATCH_SHAPE: &[&str] = &[
    "BatchRequestTooLong",
    "TooManyEntriesInBatchRequest",
    "BatchEntryIdsNotDistinct",
    "EmptyBatchRequest",
    "InvalidBatchEntryId",
];

/// Codes for a rejected credential, signature or request.
const REJECTED: &[&str] = &[
    "AccessDenied",
    "InvalidClientTokenId",
    "SignatureDoesNotMatch",
    "ExpiredToken",
    "InvalidAddress",
    "InvalidSecurity",
];

/// Codes for a queue's KMS key that SQS cannot use until an operator changes
/// the key or its policy. `KMS.ThrottlingException` stays Retryable.
const KMS_KEY: &[&str] = &[
    "KMS.AccessDeniedException",
    "KMS.DisabledException",
    "KMS.NotFoundException",
    "KMS.InvalidStateException",
    "KMS.OptInRequired",
    "KMS.InvalidKeyUsageException",
];

/// Whether `code` is `name`, bare or with SQS's `AWS.SimpleQueueService.`
/// prefix.
pub(crate) fn code_is(code: &str, name: &str) -> bool {
    code.strip_prefix(PREFIX).unwrap_or(code) == name
}

fn code_in(code: &str, names: &[&str]) -> bool {
    names.iter().any(|name| code_is(code, name))
}

/// How a caller reacts to a failed call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Fatal,
    Retryable,
}

/// A failed call, classified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CallError {
    pub(crate) class: Class,
    /// The call failed on a batch-shape code: a batcher bug.
    pub(crate) batch_shape: bool,
    pub(crate) message: String,
}

impl CallError {
    #[cfg(test)]
    pub(crate) fn retryable(message: &str) -> CallError {
        CallError {
            class: Class::Retryable,
            batch_shape: false,
            message: message.to_owned(),
        }
    }

    #[cfg(test)]
    pub(crate) fn fatal(message: &str) -> CallError {
        CallError {
            class: Class::Fatal,
            batch_shape: false,
            message: message.to_owned(),
        }
    }

    #[cfg(test)]
    pub(crate) fn batch_shape(message: &str) -> CallError {
        CallError {
            class: Class::Fatal,
            batch_shape: true,
            message: message.to_owned(),
        }
    }
}

/// The typed variants an operation's error has for the Fatal codes. The SDK
/// maps only some spellings of a code to a typed variant, so [`classify`]
/// also matches the code itself.
pub(crate) trait TypedFatal {
    fn typed_fatal(&self) -> bool;
    fn typed_batch_shape(&self) -> bool {
        false
    }
}

impl TypedFatal for ReceiveMessageError {
    fn typed_fatal(&self) -> bool {
        self.is_queue_does_not_exist()
            || self.is_invalid_address()
            || self.is_invalid_security()
            || self.is_kms_access_denied()
            || self.is_kms_disabled()
            || self.is_kms_not_found()
            || self.is_kms_invalid_state()
            || self.is_kms_opt_in_required()
            || self.is_kms_invalid_key_usage()
    }
}

impl TypedFatal for GetQueueAttributesError {
    fn typed_fatal(&self) -> bool {
        self.is_queue_does_not_exist() || self.is_invalid_address() || self.is_invalid_security()
    }
}

impl TypedFatal for SendMessageBatchError {
    fn typed_fatal(&self) -> bool {
        self.is_queue_does_not_exist()
            || self.is_invalid_address()
            || self.is_invalid_security()
            || self.is_kms_access_denied()
            || self.is_kms_disabled()
            || self.is_kms_not_found()
            || self.is_kms_invalid_state()
            || self.is_kms_opt_in_required()
            || self.is_kms_invalid_key_usage()
            || self.typed_batch_shape()
    }

    fn typed_batch_shape(&self) -> bool {
        self.is_batch_request_too_long()
            || self.is_too_many_entries_in_batch_request()
            || self.is_batch_entry_ids_not_distinct()
            || self.is_empty_batch_request()
            || self.is_invalid_batch_entry_id()
    }
}

impl TypedFatal for ChangeMessageVisibilityBatchError {
    fn typed_fatal(&self) -> bool {
        self.is_queue_does_not_exist()
            || self.is_invalid_address()
            || self.is_invalid_security()
            || self.typed_batch_shape()
    }

    fn typed_batch_shape(&self) -> bool {
        self.is_too_many_entries_in_batch_request()
            || self.is_batch_entry_ids_not_distinct()
            || self.is_empty_batch_request()
            || self.is_invalid_batch_entry_id()
    }
}

impl TypedFatal for DeleteMessageBatchError {
    fn typed_fatal(&self) -> bool {
        self.is_queue_does_not_exist()
            || self.is_invalid_address()
            || self.is_invalid_security()
            || self.typed_batch_shape()
    }

    fn typed_batch_shape(&self) -> bool {
        self.is_too_many_entries_in_batch_request()
            || self.is_batch_entry_ids_not_distinct()
            || self.is_empty_batch_request()
            || self.is_invalid_batch_entry_id()
    }
}

/// Fatal for a TLS rejection, a 401 or 403 the service answered, or a Fatal
/// code in either spelling; Retryable for everything else. The message
/// carries the error's source chain.
pub(crate) fn classify<E>(op: &str, e: &SdkError<E, HttpResponse>) -> CallError
where
    E: ProvideErrorMetadata + TypedFatal + Error + Send + Sync + 'static,
{
    let mut message = format!("SQS {op}: {}", chain(e));
    let fatal = |message, batch_shape| CallError {
        class: Class::Fatal,
        batch_shape,
        message,
    };
    if let Some(tls) = spate_core::tls_rejection!(rustls, e) {
        let tls = tls.to_string();
        if !message.contains(&tls) {
            let _ = write!(message, ": {tls}");
        }
        return fatal(message, false);
    }
    let code = e.code().unwrap_or_default();
    let typed = e.as_service_error();
    let batch_shape =
        code_in(code, BATCH_SHAPE) || typed.is_some_and(TypedFatal::typed_batch_shape);
    let rejected = e
        .raw_response()
        .is_some_and(|r| matches!(r.status().as_u16(), 401 | 403));
    if batch_shape
        || rejected
        || code_in(code, MISSING_QUEUE)
        || code_in(code, REJECTED)
        || code_in(code, KMS_KEY)
        || typed.is_some_and(TypedFatal::typed_fatal)
    {
        fatal(message, batch_shape)
    } else {
        CallError {
            class: Class::Retryable,
            batch_shape: false,
            message,
        }
    }
}

/// `e` and each of its sources, joined by `: `.
pub(crate) fn chain(e: &(dyn Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut next = e.source();
    while let Some(source) = next {
        let _ = write!(out, ": {source}");
        next = source.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sqs::error::ErrorMetadata;

    fn service<E>(err: E, status: u16) -> SdkError<E, HttpResponse> {
        let raw = HttpResponse::new(status.try_into().unwrap(), "".into());
        SdkError::service_error(err, raw)
    }

    fn receive_error(code: &str) -> SdkError<ReceiveMessageError, HttpResponse> {
        let meta = ErrorMetadata::builder().code(code).build();
        service(ReceiveMessageError::generic(meta), 400)
    }

    fn batch_error(code: &str) -> SdkError<SendMessageBatchError, HttpResponse> {
        let meta = ErrorMetadata::builder().code(code).build();
        service(SendMessageBatchError::generic(meta), 400)
    }

    /// Pins that a missing queue and a batch-shape code are Fatal as a
    /// generic error, bare and with the `AWS.SimpleQueueService.` prefix,
    /// since the SDK maps only one spelling of each to its typed variant.
    #[test]
    fn queue_and_batch_shape_codes_are_fatal_in_either_form() {
        for code in [
            "QueueDoesNotExist",
            "AWS.SimpleQueueService.NonExistentQueue",
        ] {
            let e = classify("ReceiveMessage", &receive_error(code));
            assert_eq!(e.class, Class::Fatal, "{code}");
            assert!(!e.batch_shape, "{code}");
        }
        for code in [
            "TooManyEntriesInBatchRequest",
            "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
        ] {
            let e = classify("SendMessageBatch", &batch_error(code));
            assert_eq!(e.class, Class::Fatal, "{code}");
            assert!(e.batch_shape, "{code}");
        }
    }

    /// Pins the other classes: a rejected credential or a 403 is Fatal, and
    /// throttling or a 500 is Retryable.
    #[test]
    fn rejections_are_fatal_and_throttling_is_retryable() {
        assert_eq!(
            classify("ReceiveMessage", &receive_error("InvalidClientTokenId")).class,
            Class::Fatal
        );
        let meta = ErrorMetadata::builder().code("SomethingElse").build();
        let forbidden = service(ReceiveMessageError::generic(meta), 403);
        assert_eq!(classify("ReceiveMessage", &forbidden).class, Class::Fatal);
        assert_eq!(
            classify(
                "ReceiveMessage",
                &receive_error("AWS.SimpleQueueService.RequestThrottled")
            )
            .class,
            Class::Retryable
        );
        let meta = ErrorMetadata::builder().code("InternalError").build();
        let failed = service(ReceiveMessageError::generic(meta), 500);
        assert_eq!(classify("ReceiveMessage", &failed).class, Class::Retryable);
    }

    /// Pins that each KMS error an operator must fix is Fatal, as the typed
    /// variant and as a generic code on `ReceiveMessage` and
    /// `SendMessageBatch`, and that KMS throttling is Retryable.
    #[test]
    fn a_kms_key_the_queue_cannot_use_is_fatal() {
        use aws_sdk_sqs::types::error::{
            KmsAccessDenied, KmsDisabled, KmsInvalidKeyUsage, KmsInvalidState, KmsNotFound,
            KmsOptInRequired, KmsThrottled,
        };
        // Without a code in the metadata, so only the typed check can match.
        macro_rules! typed {
            ($ty:ident) => {{
                let meta = || ErrorMetadata::builder().message("scripted").build();
                (
                    ReceiveMessageError::$ty($ty::builder().meta(meta()).build()),
                    SendMessageBatchError::$ty($ty::builder().meta(meta()).build()),
                )
            }};
        }
        let cases = [
            ("KMS.AccessDeniedException", typed!(KmsAccessDenied)),
            ("KMS.DisabledException", typed!(KmsDisabled)),
            ("KMS.NotFoundException", typed!(KmsNotFound)),
            ("KMS.InvalidStateException", typed!(KmsInvalidState)),
            ("KMS.OptInRequired", typed!(KmsOptInRequired)),
            ("KMS.InvalidKeyUsageException", typed!(KmsInvalidKeyUsage)),
        ];
        for (code, (receive, send)) in cases {
            assert_eq!(
                classify("ReceiveMessage", &service(receive, 400)).class,
                Class::Fatal,
                "typed {code}"
            );
            assert_eq!(
                classify("SendMessageBatch", &service(send, 400)).class,
                Class::Fatal,
                "typed {code}"
            );
            assert_eq!(
                classify("ReceiveMessage", &receive_error(code)).class,
                Class::Fatal,
                "{code}"
            );
            assert_eq!(
                classify("SendMessageBatch", &batch_error(code)).class,
                Class::Fatal,
                "{code}"
            );
        }
        let (receive, send) = typed!(KmsThrottled);
        assert_eq!(
            classify("ReceiveMessage", &service(receive, 400)).class,
            Class::Retryable
        );
        assert_eq!(
            classify("SendMessageBatch", &service(send, 400)).class,
            Class::Retryable
        );
        assert_eq!(
            classify("SendMessageBatch", &batch_error("KMS.ThrottlingException")).class,
            Class::Retryable
        );
    }

    /// Pins the matcher: a code matches bare or prefixed, never by suffix.
    #[test]
    fn a_code_matches_bare_or_prefixed() {
        assert!(code_is("MessageNotInflight", "MessageNotInflight"));
        assert!(code_is(
            "AWS.SimpleQueueService.MessageNotInflight",
            "MessageNotInflight"
        ));
        assert!(!code_is("XMessageNotInflight", "MessageNotInflight"));
        assert!(!code_is("Other.MessageNotInflight", "MessageNotInflight"));
    }
}
