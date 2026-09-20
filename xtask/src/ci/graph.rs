//! The workspace crate graph, read from `cargo metadata`.
//!
//! Two reverse-dependency closures are derived here. They differ by which
//! edges they follow, so a crate can reach another in one and not the other.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

use serde::Deserialize;

/// The wall-clock benchmark harness. Crates dev-depend on it for their
/// `benches/*_wall.rs` targets, and cargo builds no bench target for an
/// `#[ignore]`d test, so the edge reaches no container suite.
const WALL_BENCH_HARNESS: &str = "spate-bench";

/// The libFuzzer harness. It sits outside `crates/` and owns no `#[ignore]`d
/// test, so both closures intersect it away and neither set gains a member.
const FUZZ_HARNESS: &str = "spate-fuzz";

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<MetaPackage>,
    workspace_members: Vec<String>,
}

#[derive(Deserialize)]
struct MetaPackage {
    id: String,
    name: String,
    manifest_path: String,
    #[serde(default)]
    publish: Option<Vec<String>>,
    dependencies: Vec<MetaDependency>,
    // Read by the test holding the bench arm table to the features each
    // package declares.
    #[cfg_attr(not(test), allow(dead_code))]
    #[serde(default)]
    features: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct MetaDependency {
    name: String,
    /// `None` for a normal dependency, `"dev"` or `"build"` otherwise.
    kind: Option<String>,
}

/// The workspace graph and the sets derived from it.
#[derive(Debug)]
pub(crate) struct Graph {
    /// Reverse edges including dev-dependencies, minus the wall-bench harness.
    container_rdeps: BTreeMap<String, BTreeSet<String>>,
    /// Reverse edges over normal dependencies alone.
    semver_rdeps: BTreeMap<String, BTreeSet<String>>,
    /// Workspace members under `crates/` that own `#[ignore]`d container tests.
    container_pkgs: BTreeSet<String>,
    /// Publishable crates under `crates/`.
    semver_pkgs: BTreeSet<String>,
    /// Workspace crates the fuzz harness depends on.
    fuzz_pkgs: BTreeSet<String>,
    /// Crates owning at least one gungraun bench.
    bench_pkgs: BTreeSet<String>,
    /// Each workspace member's declared features, for holding the bench
    /// arm table to them.
    #[cfg(test)]
    features: BTreeMap<String, BTreeSet<String>>,
}

impl Graph {
    /// Reads `cargo metadata` and the bench targets on disk, which no manifest
    /// graph reaches.
    pub(crate) fn load(root: &Path) -> Result<Self, String> {
        let meta = cargo_metadata(root)?;
        let members: BTreeSet<&str> = meta
            .packages
            .iter()
            .filter(|p| meta.workspace_members.contains(&p.id))
            .map(|p| p.name.as_str())
            .collect();

        let mut container_rdeps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut semver_rdeps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for pkg in meta
            .packages
            .iter()
            .filter(|p| members.contains(p.name.as_str()))
        {
            for dep in &pkg.dependencies {
                if !members.contains(dep.name.as_str()) {
                    continue;
                }
                // The edge runs crate -> harness, so skipping it by the
                // dependency name keeps the harness from reaching its dependents.
                if dep.name != WALL_BENCH_HARNESS {
                    container_rdeps
                        .entry(dep.name.clone())
                        .or_default()
                        .insert(pkg.name.clone());
                }
                // Build-dependencies included: generated code reaches a
                // public signature the same way a normal dependency does.
                if dep.kind.as_deref().is_none_or(|k| k == "build") {
                    semver_rdeps
                        .entry(dep.name.clone())
                        .or_default()
                        .insert(pkg.name.clone());
                }
            }
        }

        let in_crates = |p: &MetaPackage| p.manifest_path.contains("/crates/");
        let publishable =
            |p: &MetaPackage| p.publish.as_ref().is_none_or(|allow| !allow.is_empty());

        let semver_pkgs = meta
            .packages
            .iter()
            .filter(|p| members.contains(p.name.as_str()) && in_crates(p) && publishable(p))
            .map(|p| p.name.clone())
            .collect();

        Ok(Self {
            container_rdeps,
            semver_rdeps,
            container_pkgs: container_test_owners(root, &members)?,
            semver_pkgs,
            fuzz_pkgs: fuzz_dependencies(&meta, &members),
            bench_pkgs: crate::checks::gungraun::owners(root),
            #[cfg(test)]
            features: meta
                .packages
                .iter()
                .filter(|p| members.contains(p.name.as_str()))
                .map(|p| (p.name.clone(), p.features.keys().cloned().collect()))
                .collect(),
        })
    }
}

fn cargo_metadata(root: &Path) -> Result<Metadata, String> {
    let out = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cargo metadata: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "cargo metadata exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("cargo metadata output: {e}"))
}

/// Workspace members under `crates/` that own at least one `#[ignore]`d test.
fn container_test_owners(
    root: &Path,
    members: &BTreeSet<&str>,
) -> Result<BTreeSet<String>, String> {
    let mut owners = BTreeSet::new();
    for member in members {
        let dir = root.join("crates").join(member);
        if !dir.is_dir() {
            continue;
        }
        // Matches `#[ignore]` and `#[ignore = "needs Docker"]` alike.
        if tree_contains(&dir, "#[ignore")? {
            owners.insert((*member).to_string());
        }
    }
    Ok(owners)
}

/// The workspace crates the fuzz harness depends on. Empty when the harness is
/// absent, which `a_fuzz_dependency_builds_the_harness` is the guard against.
fn fuzz_dependencies(meta: &Metadata, members: &BTreeSet<&str>) -> BTreeSet<String> {
    meta.packages
        .iter()
        .filter(|p| p.name == FUZZ_HARNESS)
        .flat_map(|p| &p.dependencies)
        .filter(|d| members.contains(d.name.as_str()))
        .map(|d| d.name.clone())
        .collect()
}

/// Whether any `.rs` file under a directory contains a needle.
fn tree_contains(dir: &Path, needle: &str) -> Result<bool, String> {
    for entry in std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))? {
        let path = entry.map_err(|e| format!("{}: {e}", dir.display()))?.path();
        if path.is_dir() {
            if tree_contains(&path, needle)? {
                return Ok(true);
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            if text.contains(needle) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

impl Graph {
    /// Which container suites can a change to `crate_name` reach? The
    /// transitive dependents over dev edges too, intersected with the crates
    /// that own container tests.
    ///
    /// The question is reachability: a change confined to one connector still
    /// boots the others through the facade.
    pub(crate) fn container_suites_for(&self, crate_name: &str) -> BTreeSet<String> {
        closure(&self.container_rdeps, crate_name)
            .intersection(&self.container_pkgs)
            .cloned()
            .collect()
    }

    /// Whose published API can a change to `crate_name` move? Dependents over
    /// normal edges alone, since a dev-dependency reaches no public signature.
    pub(crate) fn semver_closure_for(&self, crate_name: &str) -> BTreeSet<String> {
        closure(&self.semver_rdeps, crate_name)
            .intersection(&self.semver_pkgs)
            .cloned()
            .collect()
    }

    /// Whose instruction counts can a change to `crate_name` move? The unit is
    /// the whole crate, because codegen is crate-global.
    pub(crate) fn bench_pkgs_for(&self, crate_name: &str) -> BTreeSet<String> {
        // Every crate depends on the core, so a change there is measured
        // everywhere a bench exists.
        if crate_name == "spate-core" {
            return self.bench_pkgs.clone();
        }
        self.bench_pkgs
            .get(crate_name)
            .cloned()
            .into_iter()
            .collect()
    }

    pub(crate) fn all_container_pkgs(&self) -> &BTreeSet<String> {
        &self.container_pkgs
    }

    pub(crate) fn all_semver_pkgs(&self) -> &BTreeSet<String> {
        &self.semver_pkgs
    }

    /// The features a package declares.
    #[cfg(test)]
    pub(crate) fn features_of(&self, pkg: &str) -> Option<&BTreeSet<String>> {
        self.features.get(pkg)
    }

    pub(crate) fn all_bench_pkgs(&self) -> &BTreeSet<String> {
        &self.bench_pkgs
    }

    pub(crate) fn is_fuzz_dependency(&self, crate_name: &str) -> bool {
        self.fuzz_pkgs.contains(crate_name)
    }
}

/// Every crate reachable backwards from `start`, `start` included.
fn closure(rdeps: &BTreeMap<String, BTreeSet<String>>, start: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut queue = vec![start.to_string()];
    while let Some(name) = queue.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(parents) = rdeps.get(&name) {
            queue.extend(parents.iter().cloned());
        }
    }
    seen
}
