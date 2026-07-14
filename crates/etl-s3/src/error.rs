//! Mapping `object_store` failures into the framework's error taxonomy.

use etl_core::error::{ErrorClass, SourceError};

/// Classify an `object_store` error for a **data** operation (list / GET).
///
/// - `NotFound` / `Precondition` on data mean the frozen key set changed
///   underneath the backfill (a deleted key, an overwritten in-progress
///   object failing its `if_match`) — fatal, replaying would skip or
///   corrupt.
/// - Authentication, permission, and configuration errors are fatal: no
///   retry can fix them.
/// - Everything else (`Generic` transport failures, timeouts, 5xx) is
///   retryable.
pub(crate) fn classify(e: &object_store::Error) -> ErrorClass {
    use object_store::Error as E;
    match e {
        E::NotFound { .. }
        | E::Precondition { .. }
        | E::NotModified { .. }
        | E::AlreadyExists { .. }
        | E::InvalidPath { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::UnknownConfigurationKey { .. } => ErrorClass::Fatal,
        _ => ErrorClass::Retryable,
    }
}

/// Wrap an `object_store` error as the framework's source error, with the
/// operation context in the reason.
pub(crate) fn source_error(context: &str, e: &object_store::Error) -> SourceError {
    SourceError::Client {
        class: classify(e),
        reason: format!("{context}: {e}"),
    }
}
