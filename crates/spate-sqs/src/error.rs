//! The crate's public error, and SDK errors classified as Fatal or Retryable.
//!
//! A credential, permission or TLS rejection, a KMS key the queue cannot
//! use, a missing queue, a malformed batch and any other 400 answer that
//! carries an error code are Fatal: retrying cannot succeed. Throttling, the
//! in-flight message limit and a request timeout are Retryable whatever
//! status carries them, as is everything else: 5xx, timeouts, an answer with
//! no code and a failure to load credentials.

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
    /// The feed cannot start, and starting again fails the same way: SQS
    /// refused a request, for a rejected credential, a missing permission or
    /// queue, a rejected TLS handshake, or a request it rejects as one no
    /// retry can fix; the TLS context could not be
    /// built; or the queue failed one of the feed's startup checks.
    #[error("the SQS feed cannot start: {0}")]
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
    "InvalidAttributeName",
    "UnsupportedOperation",
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

/// Codes for a limit or a timeout that clears without an operator, whatever
/// status carries them: the SDK's own throttling and transient codes, plus
/// SQS's KMS throttling and in-flight message limit.
const RETRYABLE: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
    "RequestTimeout",
    "RequestTimeoutException",
    "KMS.ThrottlingException",
    "OverLimit",
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
        self.is_queue_does_not_exist()
            || self.is_invalid_address()
            || self.is_invalid_security()
            || self.is_invalid_attribute_name()
            || self.is_unsupported_operation()
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

/// Retryable for a code in [`RETRYABLE`] at any status; otherwise Fatal for a
/// TLS rejection, a batch-shape code, a 401 or 403 the service answered, a
/// Fatal code in either spelling, or a 400 that carries a code; Retryable for
/// everything else. The message carries the error's source chain.
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
    let retryable = |message| CallError {
        class: Class::Retryable,
        batch_shape: false,
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
    // Checked before the status: SQS documents its throttling codes as 403
    // as well as 400.
    if !batch_shape && code_in(code, RETRYABLE) {
        return retryable(message);
    }
    let status = e.raw_response().map(|r| r.status().as_u16());
    let rejected = matches!(status, Some(401 | 403));
    let sender = status == Some(400) && !code.is_empty();
    if batch_shape
        || rejected
        || sender
        || code_in(code, MISSING_QUEUE)
        || code_in(code, REJECTED)
        || code_in(code, KMS_KEY)
        || typed.is_some_and(TypedFatal::typed_fatal)
    {
        fatal(message, batch_shape)
    } else {
        retryable(message)
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

    /// Pins that each typed `GetQueueAttributes` error no retry can fix is
    /// Fatal with no code in its metadata, and that an untyped 500 stays
    /// Retryable.
    #[test]
    fn a_typed_attribute_read_error_is_fatal() {
        use aws_sdk_sqs::types::error::{
            InvalidAddress, InvalidAttributeName, InvalidSecurity, QueueDoesNotExist,
            UnsupportedOperation,
        };
        let meta = || ErrorMetadata::builder().message("scripted").build();
        let cases = [
            (
                "QueueDoesNotExist",
                GetQueueAttributesError::QueueDoesNotExist(
                    QueueDoesNotExist::builder().meta(meta()).build(),
                ),
            ),
            (
                "InvalidAddress",
                GetQueueAttributesError::InvalidAddress(
                    InvalidAddress::builder().meta(meta()).build(),
                ),
            ),
            (
                "InvalidSecurity",
                GetQueueAttributesError::InvalidSecurity(
                    InvalidSecurity::builder().meta(meta()).build(),
                ),
            ),
            (
                "InvalidAttributeName",
                GetQueueAttributesError::InvalidAttributeName(
                    InvalidAttributeName::builder().meta(meta()).build(),
                ),
            ),
            (
                "UnsupportedOperation",
                GetQueueAttributesError::UnsupportedOperation(
                    UnsupportedOperation::builder().meta(meta()).build(),
                ),
            ),
        ];
        for (name, e) in cases {
            assert_eq!(
                classify("GetQueueAttributes", &service(e, 400)).class,
                Class::Fatal,
                "{name}"
            );
        }
        let untyped = GetQueueAttributesError::generic(meta());
        assert_eq!(
            classify("GetQueueAttributes", &service(untyped, 500)).class,
            Class::Retryable
        );
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

    /// Pins that a 400 carrying a code no list names is Fatal, whatever its
    /// fault type, and that the message names the code. Regression for #1188.
    #[test]
    fn an_unlisted_sender_error_is_fatal() {
        for code in [
            "InvalidAction",
            "AWS.SimpleQueueService.InvalidAction",
            "AccessDeniedException",
            "NotAuthorized",
            "IncompleteSignature",
            "InvalidParameterCombination",
            "InvalidParameterValue",
            "InvalidQueryParameter",
            "MissingAction",
            "MissingParameter",
            "ValidationError",
            "RequestExpired",
            "SomeNewSenderError",
        ] {
            let e = classify("ReceiveMessage", &receive_error(code));
            assert_eq!(e.class, Class::Fatal, "{code}");
            assert!(e.message.contains(code), "{code}: {}", e.message);
        }
        for fault in ["Sender", "Receiver"] {
            let meta = ErrorMetadata::builder()
                .code("InvalidAction")
                .custom("type", fault)
                .build();
            let e = service(ReceiveMessageError::generic(meta), 400);
            assert_eq!(
                classify("ReceiveMessage", &e).class,
                Class::Fatal,
                "{fault}"
            );
        }
    }

    /// Pins that a coded 400 is Fatal on every call the feed makes, and is
    /// never read as a batch-shape fault.
    #[test]
    fn a_sender_error_on_any_call_is_fatal_and_not_batch_shape() {
        let meta = || ErrorMetadata::builder().code("InvalidAction").build();
        let cases = [
            classify(
                "ReceiveMessage",
                &service(ReceiveMessageError::generic(meta()), 400),
            ),
            classify(
                "GetQueueAttributes",
                &service(GetQueueAttributesError::generic(meta()), 400),
            ),
            classify(
                "SendMessageBatch",
                &service(SendMessageBatchError::generic(meta()), 400),
            ),
            classify(
                "ChangeMessageVisibilityBatch",
                &service(ChangeMessageVisibilityBatchError::generic(meta()), 400),
            ),
            classify(
                "DeleteMessageBatch",
                &service(DeleteMessageBatchError::generic(meta()), 400),
            ),
        ];
        for e in cases {
            assert_eq!(e.class, Class::Fatal, "{}", e.message);
            assert!(!e.batch_shape, "{}", e.message);
        }
    }

    /// Pins that throttling, the in-flight limit and a request timeout are
    /// Retryable at 400 and at 403, bare, prefixed or typed.
    #[test]
    fn throttling_and_the_in_flight_limit_are_retryable_at_400_and_403() {
        use aws_sdk_sqs::types::error::{OverLimit, RequestThrottled};
        for status in [400, 403] {
            for code in [
                "RequestThrottled",
                "AWS.SimpleQueueService.RequestThrottled",
                "ThrottlingException",
                "KMS.ThrottlingException",
                "OverLimit",
                "AWS.SimpleQueueService.OverLimit",
                "RequestTimeout",
                "TooManyRequestsException",
            ] {
                let meta = || {
                    ErrorMetadata::builder()
                        .code(code)
                        .custom("type", "Sender")
                        .build()
                };
                let receive = service(ReceiveMessageError::generic(meta()), status);
                assert_eq!(
                    classify("ReceiveMessage", &receive).class,
                    Class::Retryable,
                    "{code} at {status}"
                );
                let send = service(SendMessageBatchError::generic(meta()), status);
                assert_eq!(
                    classify("SendMessageBatch", &send).class,
                    Class::Retryable,
                    "{code} at {status}"
                );
            }
        }
        let meta = || ErrorMetadata::builder().message("scripted").build();
        let receive =
            ReceiveMessageError::RequestThrottled(RequestThrottled::builder().meta(meta()).build());
        let attributes = GetQueueAttributesError::RequestThrottled(
            RequestThrottled::builder().meta(meta()).build(),
        );
        let over = ReceiveMessageError::OverLimit(OverLimit::builder().meta(meta()).build());
        assert_eq!(
            classify("ReceiveMessage", &service(receive, 400)).class,
            Class::Retryable
        );
        assert_eq!(
            classify("GetQueueAttributes", &service(attributes, 400)).class,
            Class::Retryable
        );
        assert_eq!(
            classify("ReceiveMessage", &service(over, 400)).class,
            Class::Retryable
        );
    }

    /// Pins each code on the Retryable list as Retryable at 400 and 403, so
    /// dropping one turns that throttle into a sticky Fatal.
    #[test]
    fn every_retryable_code_is_retryable_at_400_and_403() {
        for status in [400, 403] {
            for code in [
                "Throttling",
                "ThrottlingException",
                "ThrottledException",
                "RequestThrottledException",
                "TooManyRequestsException",
                "ProvisionedThroughputExceededException",
                "TransactionInProgressException",
                "RequestLimitExceeded",
                "BandwidthLimitExceeded",
                "LimitExceededException",
                "RequestThrottled",
                "SlowDown",
                "PriorRequestNotComplete",
                "EC2ThrottledException",
                "RequestTimeout",
                "RequestTimeoutException",
                "KMS.ThrottlingException",
                "OverLimit",
            ] {
                let meta = ErrorMetadata::builder().code(code).build();
                let e = service(ReceiveMessageError::generic(meta), status);
                assert_eq!(
                    classify("ReceiveMessage", &e).class,
                    Class::Retryable,
                    "{code} at {status}"
                );
            }
        }
    }

    /// Pins that a 401 the service answered is Fatal with a code no list
    /// names and with no code.
    #[test]
    fn a_401_is_fatal_whatever_its_code() {
        for meta in [
            ErrorMetadata::builder().code("SomethingElse").build(),
            ErrorMetadata::builder().build(),
        ] {
            let e = service(ReceiveMessageError::generic(meta), 401);
            assert_eq!(classify("ReceiveMessage", &e).class, Class::Fatal);
        }
    }

    /// Pins that the sender rule needs a 400 the service answered with a
    /// code: a codeless 400, another 4xx, a 5xx and a timeout stay Retryable.
    #[test]
    fn the_sender_rule_needs_a_400_with_a_code() {
        let codeless = ReceiveMessageError::generic(ErrorMetadata::builder().build());
        assert_eq!(
            classify("ReceiveMessage", &service(codeless, 400)).class,
            Class::Retryable
        );
        let raw = HttpResponse::new(400.try_into().unwrap(), "".into());
        let unparsed: SdkError<ReceiveMessageError, HttpResponse> =
            SdkError::response_error("unparsed", raw);
        assert_eq!(
            classify("ReceiveMessage", &unparsed).class,
            Class::Retryable
        );
        for status in [404, 408, 429] {
            let meta = ErrorMetadata::builder().code("InvalidAction").build();
            let e = service(ReceiveMessageError::generic(meta), status);
            assert_eq!(
                classify("ReceiveMessage", &e).class,
                Class::Retryable,
                "{status}"
            );
        }
        let meta = ErrorMetadata::builder()
            .code("InternalError")
            .custom("type", "Receiver")
            .build();
        let failed = service(ReceiveMessageError::generic(meta), 500);
        assert_eq!(classify("ReceiveMessage", &failed).class, Class::Retryable);
        let timed_out: SdkError<ReceiveMessageError, HttpResponse> =
            SdkError::timeout_error("timed out");
        assert_eq!(
            classify("ReceiveMessage", &timed_out).class,
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
