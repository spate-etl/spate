//! Error construction and classification helpers.

use crate::store::StoreError;
use spate_core::coordination::{CoordinationError, CoordinationErrorKind};

/// A [`Fatal`](CoordinationErrorKind::Fatal) coordination error.
pub(crate) fn fatal(reason: impl Into<String>) -> CoordinationError {
    CoordinationError::new(CoordinationErrorKind::Fatal, reason)
}

/// A [`Retryable`](CoordinationErrorKind::Retryable) coordination error.
pub(crate) fn retryable(reason: impl Into<String>) -> CoordinationError {
    CoordinationError::new(CoordinationErrorKind::Retryable, reason)
}

/// Map a store failure into the seam taxonomy, with context.
pub(crate) fn store_error(context: &str, e: &StoreError) -> CoordinationError {
    match e {
        StoreError::Retryable(reason) => retryable(format!("{context}: {reason}")),
        StoreError::Fatal(reason) => fatal(format!("{context}: {reason}")),
    }
}

/// `Err` for a fatal store failure, so the task stops; `Ok` for a retryable
/// one, which the next tick retries.
pub(crate) fn fatal_only(context: &str, e: &StoreError) -> Result<(), CoordinationError> {
    match e {
        StoreError::Fatal(_) => Err(store_error(context, e)),
        StoreError::Retryable(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_errors_keep_their_class_and_gain_context() {
        let r = store_error("renewing lease", &StoreError::Retryable("timeout".into()));
        assert_eq!(r.kind, CoordinationErrorKind::Retryable);
        assert!(r.to_string().contains("renewing lease"));
        assert!(r.to_string().contains("timeout"));
        let f = store_error("probing store", &StoreError::Fatal("no CAS".into()));
        assert_eq!(f.kind, CoordinationErrorKind::Fatal);
    }
}
