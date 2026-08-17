//! Coordinator configuration and its mechanical floors.

use crate::error::fatal;
use crate::records::validate_instance_id;
use serde::Deserialize;
use spate_core::coordination::CoordinationError;
use std::time::Duration;

/// Tuning for one worker's coordinator; embed under a source's
/// `coordination:` config section.
///
/// The mechanical floors here (`validate`) keep the protocol sound in
/// tests as well as production. User-facing floors (e.g. "a lease below
/// 15s churns takeovers under routine GC pauses") belong to the embedding
/// connector's config layer.
///
/// Construct with [`CoordinationConfig::default`] and set the fields. The
/// struct is `#[non_exhaustive]` so new knobs can be added without
/// breaking callers.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct CoordinationConfig {
    /// Takeover-latency ceiling: a dead worker's splits flow back to the
    /// fleet one lease after its last heartbeat. Default 30s.
    #[serde(with = "humantime_serde")]
    pub lease_duration: Duration,
    /// Per-store-operation deadline. Default 10s.
    #[serde(with = "humantime_serde")]
    pub op_timeout: Duration,
    /// Stable identity for fast reclaim after a restart (e.g. the pod
    /// name). It must be UNIQUE per live worker; two live processes sharing
    /// an id is detected and Fatal. Default: a random id per run.
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
    /// up; steady-state operations are not budgeted, and retry on the
    /// next tick and escalate through lease expiry. Default 8.
    pub startup_max_attempts: u32,
    /// How long the leader withholds a departed instance's splits before
    /// reassigning them, so a restarting worker reclaims its own work
    /// instead of the fleet churning around a bounce. Default 20s.
    ///
    /// The window is cancelled early when the instance comes back, so a
    /// fast restart costs nothing at all. Setting it to zero reassigns
    /// immediately, which is a supported configuration.
    ///
    /// The default is short. A long window suits systems whose work units are
    /// expensive to start, whereas a split starts by reading a descriptor
    /// and spawning a fetcher. When starting is cheap, idle work costs more
    /// than movement does.
    #[serde(with = "humantime_serde")]
    pub rebalance_delay: Duration,
    /// How long a revoked split may keep draining before the release is
    /// forced. Default 10s.
    ///
    /// Revocation is cooperative first: the owner stops intake at a safe
    /// boundary, chases its tail to a final fenced commit, and releases,
    /// replaying nothing. A source that declines, or one whose drain
    /// outruns this deadline, is revoked outright instead and its
    /// uncommitted tail replays under the new owner. Without the deadline,
    /// one wedged drain pins a rebalance open forever.
    ///
    /// It may exceed `lease_duration`. A draining split is still owned and
    /// still heartbeated, so a long drain does not race its own lease; it
    /// makes the rebalance slow. Size it against how long the source
    /// needs to flush a split's tail through its sink, which for a paced
    /// sink can be minutes.
    #[serde(with = "humantime_serde")]
    pub drain_deadline: Duration,
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
            rebalance_delay: Duration::from_secs(20),
            drain_deadline: Duration::from_secs(10),
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
        // Zero is legal and means "reassign immediately"; see the field
        // docs. There is no floor here, and no upper bound against
        // `lease_duration`. A draining split is still owned and still
        // renewed, so a drain does not race its own lease however long it
        // takes; a slow drain is a slow rebalance, not a correctness
        // problem. Sources with paced sinks want deadlines far above the
        // lease.
        if self.drain_deadline < self.op_timeout {
            return Err(fatal(format!(
                "drain_deadline ({:?}) must be >= op_timeout ({:?}): a deadline shorter \
                 than one store round-trip forces every revocation before its final \
                 commit can land, so no drain could ever finish cooperatively",
                self.drain_deadline, self.op_timeout
            )));
        }
        Ok(())
    }

    /// Renewal cadence: a third of the lease, so three renewal
    /// opportunities fit in every lease and two may be lost before it
    /// expires.
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
        assert_eq!(
            config.rebalance_delay,
            Duration::from_secs(20),
            "sized for a pod bounce, not for expensive task startup"
        );
        assert!(
            config.drain_deadline >= config.op_timeout,
            "a drain deadline below one store round-trip forces every revocation"
        );
    }

    #[test]
    fn a_zero_rebalance_delay_is_legal() {
        // Zero is a case in its own right rather than a value flowing
        // through the delay path, so it means what it says: reassign
        // immediately.
        CoordinationConfig {
            rebalance_delay: Duration::ZERO,
            ..Default::default()
        }
        .validate()
        .expect("zero delay is a supported configuration");
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
            (
                CoordinationConfig {
                    drain_deadline: Duration::from_millis(1),
                    ..Default::default()
                },
                "drain_deadline",
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
