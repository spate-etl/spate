//! Mapping `object_store` failures into the framework's error taxonomy.

use spate_core::error::ErrorClass;

/// Classify an `object_store` error for a **data** operation (list / GET).
///
/// [`ErrorClass::Fatal`] here means *not worth retrying in place*, which is
/// a narrower claim than "kill the pipeline". How far a non-retryable
/// failure reaches is [`is_pipeline_fatal`]'s decision, by scope:
///
/// - `NotFound` / `Precondition` and friends mean the planned key set moved
///   under the backfill (a deleted key, an overwritten object failing its
///   `if_match`). Retrying the same read cannot help, but the fact is about
///   **one object**, so it becomes split poison rather than a fleet-wide
///   failure.
/// - Authentication, permission, and configuration errors hold for every
///   object on every instance: non-retryable *and* pipeline-fatal.
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

/// Whether a non-retryable **data-read** failure condemns the whole
/// pipeline rather than just the object being read.
///
/// Credentials, permissions, and client misconfiguration hold for every
/// object on every instance, and no peer will fare better, so they stay
/// pipeline-fatal. The rest of the non-retryable classes (`NotFound`,
/// `Precondition`, `NotModified`, `AlreadyExists`) are facts about **one
/// object** (deleted after planning, overwritten under its ETag pin) and
/// are handled as split poison: the split is handed back, retried
/// elsewhere, and quarantined at the attempt cap instead of killing a
/// fleet-wide backfill.
pub(crate) fn is_pipeline_fatal(e: &object_store::Error) -> bool {
    use object_store::Error as E;
    matches!(
        e,
        E::InvalidPath { .. }
            | E::NotSupported { .. }
            | E::NotImplemented { .. }
            | E::PermissionDenied { .. }
            | E::Unauthenticated { .. }
            | E::UnknownConfigurationKey { .. }
    )
}

/// Classify an object-level (non-pipeline-fatal, non-retryable) failure
/// into the bounded poison taxonomy.
pub(crate) fn poison_kind(e: &object_store::Error) -> crate::split_ctx::PoisonKind {
    use crate::split_ctx::PoisonKind;
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => PoisonKind::NotFound,
        // Conditional-request failures: the content moved under its pin.
        E::Precondition { .. } | E::NotModified { .. } | E::AlreadyExists { .. } => {
            PoisonKind::EtagDrift
        }
        _ => PoisonKind::Undecodable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_level_failures_classify_as_poison_not_pipeline_fatal() {
        use object_store::Error as E;
        let src = || "boom".into();
        // (error, non-retryable?, pipeline-fatal?)
        let table: Vec<(E, bool, bool)> = vec![
            // One object's fact: deleted after planning / overwritten
            // under the pin. Poison, never pipeline-fatal.
            (
                E::NotFound {
                    path: "k".into(),
                    source: src(),
                },
                true,
                false,
            ),
            (
                E::Precondition {
                    path: "k".into(),
                    source: src(),
                },
                true,
                false,
            ),
            // Holds for every object on every instance: pipeline-fatal.
            (
                E::PermissionDenied {
                    path: "k".into(),
                    source: src(),
                },
                true,
                true,
            ),
            (
                E::Unauthenticated {
                    path: "k".into(),
                    source: src(),
                },
                true,
                true,
            ),
            (
                E::UnknownConfigurationKey {
                    store: "s3",
                    key: "nope".into(),
                },
                true,
                true,
            ),
            // Transient transport trouble: retried in place, escalating
            // to poison only when the budget runs out.
            (
                E::Generic {
                    store: "s3",
                    source: src(),
                },
                false,
                false,
            ),
        ];
        for (e, non_retryable, fatal) in &table {
            assert_eq!(
                classify(e) != crate::error::ErrorClass::Retryable,
                *non_retryable,
                "classify({e})"
            );
            assert_eq!(is_pipeline_fatal(e), *fatal, "is_pipeline_fatal({e})");
        }
    }
}
