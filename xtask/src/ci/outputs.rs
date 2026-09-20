//! The `$GITHUB_OUTPUT` block the `changes` job writes.

use std::collections::BTreeSet;
use std::fmt;

use serde::Serialize;

/// One classification result, in the order the workflow reads it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Outputs {
    pub(crate) rust: bool,
    pub(crate) site: bool,
    pub(crate) fuzz: bool,
    pub(crate) bench: bool,
    pub(crate) manifests: bool,
    pub(crate) container_pkgs: BTreeSet<String>,
    pub(crate) semver_pkgs: BTreeSet<String>,
    pub(crate) bench_shards: Vec<Shard>,
    pub(crate) clickhouse_lanes: Vec<Lane>,
}

/// One `strategy.matrix.include` entry of the counter tier.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Shard {
    pub(crate) package: String,
    pub(crate) arm: String,
    pub(crate) cargo_features: String,
}

/// One `strategy.matrix.include` entry of the ClickHouse version tier.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct Lane {
    pub(crate) lane: String,
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
        // One line each. `$GITHUB_OUTPUT` is a key=value file, so a multi-line
        // value needs heredoc delimiters that the value can itself contain.
        writeln!(f, "bench-shards={}", compact_json(&self.bench_shards))?;
        writeln!(
            f,
            "clickhouse-lanes={}",
            compact_json(&self.clickhouse_lanes)
        )
    }
}

/// Serialises to one line. Package and lane names come from directory names a
/// branch chooses, so every value is escaped.
fn compact_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("matrix entries are plain strings")
}
