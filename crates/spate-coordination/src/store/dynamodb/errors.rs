//! SDK errors as [`StoreError`]s, classified per ADR-0047.

use crate::store::StoreError;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use std::error::Error;
use std::fmt::Write as _;

/// Codes the service answers when retrying cannot succeed: a rejected
/// credential or signature, a missing table, or a request it refuses.
const FATAL_CODES: &[&str] = &[
    "AccessDeniedException",
    "UnrecognizedClientException",
    "InvalidSignatureException",
    "MissingAuthenticationTokenException",
    "IncompleteSignatureException",
    "ExpiredTokenException",
    "ResourceNotFoundException",
    "ValidationException",
    "ItemCollectionSizeLimitExceededException",
];

/// Fatal for a TLS rejection, a fatal service code, or a 401 or 403, and
/// Retryable for everything else, including throttling, 5xx, timeouts, IO
/// and a failure to load credentials. The message carries the error's
/// source chain.
pub(super) fn classify<E>(op: &str, e: &SdkError<E, HttpResponse>) -> StoreError
where
    E: ProvideErrorMetadata + Error + Send + Sync + 'static,
{
    let mut message = format!("DynamoDB {op}: {}", chain(e));
    if let Some(tls) = spate_core::tls_rejection!(rustls, e) {
        let tls = tls.to_string();
        if !message.contains(&tls) {
            let _ = write!(message, ": {tls}");
        }
        return StoreError::Fatal(message);
    }
    let fatal_code = e.code().is_some_and(|c| FATAL_CODES.contains(&c));
    let rejected = e
        .raw_response()
        .is_some_and(|r| matches!(r.status().as_u16(), 401 | 403));
    if fatal_code || rejected {
        StoreError::Fatal(message)
    } else {
        StoreError::Retryable(message)
    }
}

/// `e` and each of its sources, joined by `: `.
pub(super) fn chain(e: &(dyn Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut next = e.source();
    while let Some(source) = next {
        let _ = write!(out, ": {source}");
        next = source.source();
    }
    out
}
