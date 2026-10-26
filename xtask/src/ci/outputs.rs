//! The `$GITHUB_OUTPUT` block the `changes` job writes.

use std::collections::BTreeSet;
use std::fmt;

use serde::Serialize;

use crate::checks::container_image::Capability;

/// One classification result, in the order the workflow reads it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Outputs {
    pub(crate) rust: bool,
    pub(crate) site: bool,
    pub(crate) fuzz: bool,
    pub(crate) bench: bool,
    pub(crate) manifests: bool,
    pub(crate) advisories: bool,
    pub(crate) release_tooling: bool,
    pub(crate) macos: bool,
    pub(crate) container_pkgs: BTreeSet<String>,
    pub(crate) semver_pkgs: BTreeSet<String>,
    pub(crate) bench_shards: Vec<Shard>,
    pub(crate) clickhouse_lanes: Vec<Lane>,
    pub(crate) s3_lanes: Vec<StoreLane>,
}

/// One `strategy.matrix.include` entry of the counter tier.
///
/// `arch` names the architecture the shard measures and `runner` the runner
/// label `runs-on:` takes for it.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Shard {
    pub(crate) package: String,
    pub(crate) arm: String,
    pub(crate) cargo_features: String,
    pub(crate) arch: String,
    pub(crate) runner: String,
}

/// One `strategy.matrix.include` entry of the ClickHouse version tier.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Lane {
    pub(crate) lane: String,
}

/// One `strategy.matrix.include` entry of the S3 store tier: a lane, and the
/// nextest filterset selecting the store tests its store supports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StoreLane {
    pub(crate) lane: String,
    pub(crate) filter: String,
}

impl StoreLane {
    /// The entry for `lane`, whose store has `capabilities`.
    pub(crate) fn new(lane: &str, capabilities: &[Capability]) -> StoreLane {
        let mut filter = String::from(
            "package(spate-s3) | (package(spate-test-support) & binary(containers) \
             & test(=s3_store_round_trips_an_object))",
        );
        if capabilities.contains(&Capability::Notifications) {
            filter.push_str(" | (package(spate-kafka) & binary(feed_notifications))");
        }
        StoreLane {
            lane: lane.to_owned(),
            filter,
        }
    }
}

impl Outputs {
    /// The `-p` arguments naming every selected container suite.
    fn container_args(&self) -> String {
        self.container_pkgs
            .iter()
            .map(|p| format!("-p {p}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

impl fmt::Display for Outputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let container_args = self.container_args();
        writeln!(f, "rust={}", self.rust)?;
        writeln!(f, "site={}", self.site)?;
        writeln!(f, "fuzz={}", self.fuzz)?;
        writeln!(f, "containers={}", !container_args.is_empty())?;
        writeln!(f, "container-args={container_args}")?;
        writeln!(f, "semver={}", !self.semver_pkgs.is_empty())?;
        writeln!(
            f,
            "semver-pkgs={}",
            self.semver_pkgs
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(" ")
        )?;
        writeln!(f, "bench={}", self.bench)?;
        writeln!(f, "manifests={}", self.manifests)?;
        writeln!(f, "advisories={}", self.advisories)?;
        writeln!(f, "release-tooling={}", self.release_tooling)?;
        writeln!(f, "macos={}", self.macos)?;
        // One line each. `$GITHUB_OUTPUT` is a key=value file, so a multi-line
        // value needs heredoc delimiters that the value can itself contain.
        writeln!(f, "bench-shards={}", compact_json(&self.bench_shards))?;
        writeln!(
            f,
            "clickhouse-lanes={}",
            compact_json(&self.clickhouse_lanes)
        )?;
        writeln!(f, "s3-lanes={}", compact_json(&self.s3_lanes))
    }
}

/// Serialises to one line. Package and lane names come from directory names a
/// branch chooses, so every value is escaped.
fn compact_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("matrix entries are plain strings")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store without notifications drops the notification suite from its
    /// lane, so the lane reports no failure for a store that cannot publish.
    #[test]
    fn a_store_lane_filter_runs_the_notification_suite_only_with_notifications() {
        let base = "package(spate-s3) | (package(spate-test-support) & binary(containers) \
                    & test(=s3_store_round_trips_an_object))";
        assert_eq!(StoreLane::new("plain", &[]).filter, base);
        assert_eq!(
            StoreLane::new("notifying", &[Capability::Notifications]).filter,
            format!("{base} | (package(spate-kafka) & binary(feed_notifications))")
        );
    }
}
