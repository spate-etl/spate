//! Coordinator configuration and its mechanical floors.

use crate::error::fatal;
use crate::records::validate_instance_id;
use etl_core::coordination::CoordinationError;
use serde::Deserialize;
use std::time::Duration;

/// Tuning for one worker's coordinator; embed under a source's
/// `coordination:` config section.
///
/// The mechanical floors here (`validate`) keep the protocol sound in
/// tests as well as production — user-facing floors (e.g. "a lease below
/// 15s churns takeovers under routine GC pauses") belong to the embedding
/// connector's config layer, which knows its deployment story.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CoordinationConfig {
    /// Takeover-latency ceiling: a dead worker's splits flow back to the
    /// fleet one lease after its last heartbeat. Default 30s.
    #[serde(with = "humantime_serde")]
    pub lease_duration: Duration,
    /// Per-store-operation deadline. Default 10s.
    #[serde(with = "humantime_serde")]
    pub op_timeout: Duration,
    /// Stable identity for fast reclaim after a restart (e.g. the pod
    /// name — must be UNIQUE per live worker; two live processes sharing
    /// an id is detected and Fatal). Default: a random id per run.
    pub instance_id: Option<String>,
    /// Delivery attempts before a split is quarantined. Default 4.
    pub max_attempts: u32,
    /// Working-set bound: how many splits this worker holds at once
    /// (also its data-plane lane count). Default 8.
    pub max_in_flight: u32,
    /// How often the leader re-runs the planner while the plan is open.
    /// Default 60s.
    #[serde(with = "humantime_serde")]
    pub replan_interval: Duration,
    /// How often every worker reconciles its watch-fed view against a
    /// full listing (the missed-event backstop). Default 30s.
    #[serde(with = "humantime_serde")]
    pub reconcile_interval: Duration,
    /// Startup retry budget (store probe, join, seeding) before giving
    /// up; steady-state operations are not budgeted — they retry on the
    /// next tick and escalate through lease expiry. Default 8.
    pub startup_max_attempts: u32,
}

impl Default for CoordinationConfig {
    fn default() -> CoordinationConfig {
        CoordinationConfig {
            lease_duration: Duration::from_secs(30),
            op_timeout: Duration::from_secs(10),
            instance_id: None,
            max_attempts: 4,
            max_in_flight: 8,
            replan_interval: Duration::from_secs(60),
            reconcile_interval: Duration::from_secs(30),
            startup_max_attempts: 8,
        }
    }
}

impl CoordinationConfig {
    /// Enforce the mechanical floors.
    ///
    /// # Errors
    ///
    /// Fatal on any violated floor, with the rule spelled out.
    pub fn validate(&self) -> Result<(), CoordinationError> {
        if self.op_timeout < Duration::from_millis(50) {
            return Err(fatal(format!(
                "op_timeout must be >= 50ms, got {:?}",
                self.op_timeout
            )));
        }
        if self.lease_duration < Duration::from_millis(300) {
            return Err(fatal(format!(
                "lease_duration must be >= 300ms, got {:?}",
                self.lease_duration
            )));
        }
        // A single slow store write must not outlive the lease it renews:
        // with renewals at a third of the lease, two op_timeouts must fit.
        if self.lease_duration < self.op_timeout * 2 {
            return Err(fatal(format!(
                "lease_duration ({:?}) must be >= 2 x op_timeout ({:?}): a single slow \
                 store write must not outlive the lease it renews",
                self.lease_duration, self.op_timeout
            )));
        }
        if self.max_attempts == 0 {
            return Err(fatal("max_attempts must be >= 1"));
        }
        if self.max_in_flight == 0 {
            return Err(fatal("max_in_flight must be >= 1"));
        }
        if self.replan_interval < self.lease_duration {
            return Err(fatal(format!(
                "replan_interval ({:?}) must be >= lease_duration ({:?}): replanning \
                 faster than leadership can be observed to fail is churn, not freshness",
                self.replan_interval, self.lease_duration
            )));
        }
        if let Some(id) = &self.instance_id {
            validate_instance_id(id)?;
        }
        if self.startup_max_attempts == 0 {
            return Err(fatal("startup_max_attempts must be >= 1"));
        }
        Ok(())
    }

    /// Renewal cadence: a third of the lease, the KCL/Kafka ratio — three
    /// renewal opportunities fit in every lease.
    #[must_use]
    pub fn renew_interval(&self) -> Duration {
        self.lease_duration / 3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate_and_follow_the_documented_ratios() {
        let config = CoordinationConfig::default();
        config.validate().unwrap();
        assert_eq!(config.lease_duration, Duration::from_secs(30));
        assert_eq!(config.renew_interval(), Duration::from_secs(10));
        assert_eq!(config.max_attempts, 4);
        assert_eq!(config.max_in_flight, 8);
    }

    #[test]
    fn floors_reject_with_the_rule_spelled_out() {
        let cases: Vec<(CoordinationConfig, &str)> = vec![
            (
                CoordinationConfig {
                    op_timeout: Duration::from_millis(10),
                    ..Default::default()
                },
                "op_timeout",
            ),
            (
                CoordinationConfig {
                    lease_duration: Duration::from_millis(100),
                    op_timeout: Duration::from_millis(50),
                    ..Default::default()
                },
                "lease_duration",
            ),
            (
                CoordinationConfig {
                    lease_duration: Duration::from_secs(15),
                    op_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
                "2 x op_timeout",
            ),
            (
                CoordinationConfig {
                    max_attempts: 0,
                    ..Default::default()
                },
                "max_attempts",
            ),
            (
                CoordinationConfig {
                    max_in_flight: 0,
                    ..Default::default()
                },
                "max_in_flight",
            ),
            (
                CoordinationConfig {
                    replan_interval: Duration::from_secs(5),
                    ..Default::default()
                },
                "replan_interval",
            ),
            (
                CoordinationConfig {
                    instance_id: Some("a.b".into()),
                    ..Default::default()
                },
                "instance_id",
            ),
        ];
        for (config, needle) in cases {
            let err = config.validate().unwrap_err();
            assert!(err.to_string().contains(needle), "{err}");
        }
    }

    #[test]
    fn yaml_round_trip_with_humantime_and_unknown_field_rejection() {
        let config: CoordinationConfig =
            serde_yaml::from_str("lease_duration: 45s\nmax_in_flight: 4\ninstance_id: pod-3\n")
                .unwrap();
        assert_eq!(config.lease_duration, Duration::from_secs(45));
        assert_eq!(config.max_in_flight, 4);
        assert_eq!(config.instance_id.as_deref(), Some("pod-3"));
        config.validate().unwrap();

        let err = serde_yaml::from_str::<CoordinationConfig>("lease_secs: 45\n").unwrap_err();
        assert!(err.to_string().contains("lease_secs"), "{err}");
    }
}
