//! A coordinator described by the pipeline's `coordination:` section.

use crate::config::CoordinationConfig;
use spate_core::config::{ConfigError, CoordinationSection};
use spate_core::coordination::{CoordinationError, SplitCoordinator};
use spate_core::metrics::CoordinationMetrics;
use std::fmt;

type Build = Box<
    dyn FnOnce(
            tokio::runtime::Handle,
            Option<CoordinationMetrics>,
        ) -> Result<Box<dyn SplitCoordinator>, CoordinationError>
        + Send,
>;

/// A validated `coordination:` section, ready to become a coordinator.
///
/// [`from_section`](Self::from_section) decodes and checks everything without
/// I/O, so a source can reject a bad section before the pipeline starts;
/// [`build`](Self::build) runs later, once the source has its metrics scope.
/// The store's lease TTL and the coordinator's `lease_duration` are the same
/// value by construction.
pub struct CoordinatorSpec {
    build: Build,
}

impl fmt::Debug for CoordinatorSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatorSpec").finish_non_exhaustive()
    }
}

impl CoordinatorSpec {
    /// Decode and validate the section's tuning and store.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] naming the offending `coordination.*` key, an unknown
    /// store, or a store this build was compiled without.
    pub fn from_section(section: &CoordinationSection) -> Result<Self, ConfigError> {
        let tuning: CoordinationConfig = section.deserialize_tuning()?;
        tuning.validate().map_err(|e| ConfigError::Component {
            context: "coordination".into(),
            message: e.reason,
        })?;
        match section.store().type_tag() {
            #[cfg(feature = "nats")]
            "nats" => {
                use crate::store::nats::{NatsConfig, NatsStore};
                let config: NatsConfig = section.store().deserialize_into()?;
                let store = NatsStore::new(config, tuning.lease_duration).map_err(|e| {
                    ConfigError::Component {
                        context: "coordination.store".into(),
                        message: e.to_string(),
                    }
                })?;
                Ok(CoordinatorSpec {
                    build: Box::new(move |io, metrics| {
                        let coordinator = crate::StoreCoordinator::new(store, tuning, io, metrics)?;
                        Ok(Box::new(coordinator) as Box<dyn SplitCoordinator>)
                    }),
                })
            }
            #[cfg(not(feature = "nats"))]
            "nats" => Err(nats_not_compiled()),
            #[cfg(feature = "dynamodb")]
            "dynamodb" => {
                use crate::store::dynamodb::{DynamoDbConfig, DynamoDbStore};
                let config: DynamoDbConfig = section.store().deserialize_into()?;
                let store = DynamoDbStore::new(config, tuning.lease_duration, tuning.op_timeout)
                    .map_err(|e| ConfigError::Component {
                        context: "coordination.store".into(),
                        message: e.to_string(),
                    })?;
                if tuning.reconcile_interval < RECONCILE_FLOOR {
                    tracing::warn!(
                        reconcile_interval = ?tuning.reconcile_interval,
                        "the leader's reconcile on the DynamoDB store reads every split record; \
                         an interval below {RECONCILE_FLOOR:?} raises its read cost"
                    );
                }
                Ok(CoordinatorSpec {
                    build: Box::new(move |io, metrics| {
                        let coordinator = crate::StoreCoordinator::new(store, tuning, io, metrics)?;
                        Ok(Box::new(coordinator) as Box<dyn SplitCoordinator>)
                    }),
                })
            }
            #[cfg(not(feature = "dynamodb"))]
            "dynamodb" => Err(dynamodb_not_compiled()),
            other => Err(ConfigError::Component {
                context: "coordination.store".into(),
                message: format!(
                    "unknown store `{other}`; the known stores are `nats` and `dynamodb`"
                ),
            }),
        }
    }

    /// Build the coordinator on `io`, which must be a multi-thread runtime.
    ///
    /// # Errors
    ///
    /// As [`StoreCoordinator::new`](crate::StoreCoordinator::new).
    pub fn build(
        self,
        io: tokio::runtime::Handle,
        metrics: Option<CoordinationMetrics>,
    ) -> Result<Box<dyn SplitCoordinator>, CoordinationError> {
        (self.build)(io, metrics)
    }
}

/// The reconcile interval below which the DynamoDB store warns.
#[cfg(feature = "dynamodb")]
const RECONCILE_FLOOR: std::time::Duration = std::time::Duration::from_secs(60);

#[cfg_attr(feature = "nats", allow(dead_code))]
fn nats_not_compiled() -> ConfigError {
    ConfigError::Component {
        context: "coordination.store.nats".into(),
        message: "this build has no NATS store; enable the `coordination-nats` feature of \
                  `spate`, or `nats` on `spate-coordination`"
            .into(),
    }
}

#[cfg_attr(feature = "dynamodb", allow(dead_code))]
fn dynamodb_not_compiled() -> ConfigError {
    ConfigError::Component {
        context: "coordination.store.dynamodb".into(),
        message: "this build has no DynamoDB store; enable the `coordination-dynamodb` feature \
                  of `spate`, or `dynamodb` on `spate-coordination`"
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_core::config::PipelineConfig;

    fn section(body: &str) -> CoordinationSection {
        let yaml = format!(
            "pipeline: {{ name: t }}\nsource: {{ memory: {{}} }}\nsink: {{ memory: {{}} }}\n\
             coordination:\n{body}"
        );
        PipelineConfig::from_str(&yaml)
            .expect("config")
            .coordination
            .expect("section")
    }

    fn error(body: &str) -> String {
        CoordinatorSpec::from_section(&section(body))
            .unwrap_err()
            .to_string()
    }

    const NATS: &str = "  store:\n    nats: { servers: [\"nats://127.0.0.1:1\"], job: j }\n";

    #[test]
    fn builds_a_nats_coordinator_without_connecting() {
        let spec =
            CoordinatorSpec::from_section(&section(&format!("  lease_duration: 20s\n{NATS}")))
                .unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        // StoreCoordinator::new rejects a store TTL that differs from
        // lease_duration, so a successful build pins the shared value.
        spec.build(rt.handle().clone(), None).unwrap();
    }

    #[test]
    fn unknown_store_is_rejected() {
        let err = error("  store: { etcd: {} }\n");
        assert!(err.starts_with("coordination.store"), "{err}");
        assert!(err.contains("etcd"), "{err}");
    }

    #[test]
    fn tuning_errors_name_the_key() {
        let err = error(&format!("  lease_duraton: 5s\n{NATS}"));
        assert!(err.starts_with("coordination"), "{err}");
        assert!(err.contains("lease_duraton"), "{err}");
        let err = error(&format!("  lease_duration: 1ms\n{NATS}"));
        assert!(err.contains("lease_duration"), "{err}");
    }

    #[test]
    fn empty_instance_id_is_rejected() {
        let err = error(&format!("  instance_id: \"\"\n{NATS}"));
        assert!(err.starts_with("coordination"), "{err}");
    }

    #[test]
    fn nats_errors_carry_the_store_path() {
        let err = error("  store: { nats: { job: j } }\n");
        assert!(err.starts_with("coordination.store.nats"), "{err}");
        assert!(err.contains("servers"), "{err}");
        let err = error("  store: { nats: { servers: [], job: j } }\n");
        assert!(err.starts_with("coordination.store"), "{err}");
        assert!(err.contains("servers"), "{err}");
    }

    /// The `credentials` spellings the NATS page documents, plus the tagged
    /// forms.
    #[test]
    fn documented_credential_spellings_parse() {
        for credentials in [
            "none",
            "{ user_password: { username: u, password: p } }",
            "{ token: t }",
            "{ creds_file: /etc/nats/worker.creds }",
            "!none",
            "!user_password { username: u, password: p }",
            "!token t",
            "!creds_file /etc/nats/worker.creds",
        ] {
            let body = format!(
                "  store:\n    nats: {{ servers: [\"nats://n:4222\"], job: j, credentials: {credentials} }}\n"
            );
            CoordinatorSpec::from_section(&section(&body))
                .unwrap_or_else(|e| panic!("{credentials}: {e}"));
        }
    }

    #[test]
    fn malformed_credentials_are_rejected_without_echoing_the_value() {
        for (credentials, expect) in [
            ("hunter2", "expected `none`"),
            ("{ password: hunter2 }", "password"),
            ("{ token: a, creds_file: b }", "exactly one"),
        ] {
            let body = format!(
                "  store:\n    nats: {{ servers: [\"nats://n:4222\"], job: j, credentials: {credentials} }}\n"
            );
            let err = error(&body);
            assert!(err.starts_with("coordination.store.nats"), "{err}");
            assert!(err.contains(expect), "{credentials}: {err}");
            assert!(!err.contains("hunter2"), "{err}");
        }
    }

    #[cfg(feature = "dynamodb")]
    const DYNAMODB: &str = "  store:\n    dynamodb: { table: spate-coordination, job: j, region: \
                            eu-west-1, endpoint: \"http://127.0.0.1:1\" }\n";

    #[test]
    #[cfg(feature = "dynamodb")]
    fn builds_a_dynamodb_coordinator_without_connecting() {
        let spec = CoordinatorSpec::from_section(&section(DYNAMODB)).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        // StoreCoordinator::new rejects a store whose TTL or op_timeout
        // differs from the tuning's.
        spec.build(rt.handle().clone(), None).unwrap();
    }

    /// A reconcile interval under a minute on the DynamoDB store is logged
    /// at WARN.
    #[test]
    #[cfg(feature = "dynamodb")]
    fn a_short_reconcile_interval_warns() {
        let warned = |interval: &str| {
            let body = format!("{DYNAMODB}  reconcile_interval: {interval}\n");
            let lines = spate_test::capture_logs(tracing::Level::WARN, || {
                CoordinatorSpec::from_section(&section(&body)).unwrap();
            });
            lines.iter().any(|l| l.contains("reconcile"))
        };
        assert!(warned("30s"));
        assert!(!warned("5m"));
    }

    #[test]
    #[cfg(feature = "dynamodb")]
    fn dynamodb_errors_carry_the_store_path() {
        let err = error("  store: { dynamodb: { job: j } }\n");
        assert!(err.starts_with("coordination.store.dynamodb"), "{err}");
        assert!(err.contains("table"), "{err}");
        let err = error("  store: { dynamodb: { table: t1234, job: j, endpoint: \"ftp://h\" } }\n");
        assert!(err.starts_with("coordination.store"), "{err}");
        assert!(err.contains("dynamodb.endpoint"), "{err}");
    }

    #[test]
    fn a_missing_dynamodb_feature_names_both_spellings() {
        let err = dynamodb_not_compiled().to_string();
        assert!(err.contains("coordination-dynamodb"), "{err}");
        assert!(err.contains("`dynamodb` on `spate-coordination`"), "{err}");
    }

    #[test]
    fn a_missing_nats_feature_names_both_spellings() {
        let err = nats_not_compiled().to_string();
        assert!(err.contains("coordination-nats"), "{err}");
        assert!(err.contains("`nats` on `spate-coordination`"), "{err}");
    }
}
