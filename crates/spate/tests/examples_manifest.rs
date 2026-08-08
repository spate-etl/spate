//! Holds the examples to their declarations.
//!
//! An example is the one target shape cargo will drop on the floor without
//! saying so. A `[[example]]` whose `required-features` names a feature the
//! package does not declare is not an error — the target is skipped, and
//! `cargo check --examples` reports success having built one fewer thing than
//! the manifest asked for. A typo there removes an example from every job in
//! this repository, permanently, with the build green. `--all-features` is the
//! only reason that is survivable, which makes it load-bearing in a way
//! nothing else states.
//!
//! Two questions, both answered by `cargo metadata`, which reads manifests and
//! resolves nothing:
//!
//! 1. Does every `required-features` entry name a feature that exists?
//! 2. Do an example's runner block and its `test = true` agree, in both
//!    directions?
//!
//! `test = true` **is** the declaration that an example runs on the
//! pull-request tier, so anything recording that a second time is a second
//! place to forget it. The pair that can genuinely disagree is the stanza and
//! the source: a runner nothing collects, or a collected target with nothing
//! in it. An example that needs servers declares neither and is driven by
//! `tests/e2e_examples.rs`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

#[derive(serde::Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(serde::Deserialize)]
struct Package {
    name: String,
    features: BTreeMap<String, Vec<String>>,
    targets: Vec<Target>,
}

#[derive(serde::Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
    /// Whether cargo collects this target as a test — `[[example]] test = true`.
    test: bool,
    /// Note the hyphen: the JSON key is `required-features`, and reading it as
    /// `required_features` yields an empty list for every target, which would
    /// make the assertion below vacuously true.
    #[serde(rename = "required-features", default)]
    required_features: Vec<String>,
    src_path: PathBuf,
}

fn spate_package() -> Package {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let out = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--locked",
            "--manifest-path",
            manifest,
        ])
        .output()
        .expect("cargo metadata");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: Metadata = serde_json::from_slice(&out.stdout).expect("parse cargo metadata");
    meta.packages
        .into_iter()
        .find(|p| p.name == "spate")
        .expect("the spate package is in its own metadata")
}

fn examples(pkg: &Package) -> Vec<&Target> {
    pkg.targets
        .iter()
        .filter(|t| t.kind.iter().any(|k| k == "example"))
        .collect()
}

/// Guards every assertion below: a filter that matched nothing would make them
/// all pass while testing nothing, which is how a check quietly stops checking.
#[test]
fn the_examples_are_discovered() {
    let pkg = spate_package();
    let found = examples(&pkg).len();
    assert!(
        found >= 15,
        "only {found} example target(s) in cargo metadata; the filter has stopped \
         matching, so every other assertion in this file is vacuous"
    );
}

/// The assertion this file exists for. `cargo check --examples --all-features`
/// exits 0 with a typo here.
#[test]
fn every_required_feature_is_declared() {
    let pkg = spate_package();
    let declared: BTreeSet<&str> = pkg.features.keys().map(String::as_str).collect();
    assert!(
        !declared.is_empty(),
        "cargo metadata reported no features for spate"
    );

    let mut bad = Vec::new();
    for target in examples(&pkg) {
        for feature in &target.required_features {
            if !declared.contains(feature.as_str()) {
                bad.push(format!("{}: requires `{feature}`", target.name));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "these examples require features spate does not declare, so cargo skips \
         them silently and no build ever fails:\n  {}",
        bad.join("\n  ")
    );
}

/// A `#[cfg(test)]` runner and `test = true` have to agree. One without the
/// other is a runner nothing collects, or a test binary with nothing in it.
#[test]
fn a_runner_and_test_true_agree() {
    let pkg = spate_package();
    let mut bad = Vec::new();

    for target in examples(&pkg) {
        let src = std::fs::read_to_string(&target.src_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", target.src_path.display()));
        let has_runner = src.contains("#[cfg(test)]") && src.contains("mod tests");

        match (has_runner, target.test) {
            (true, false) => bad.push(format!(
                "{}: has a `#[cfg(test)]` runner, but its stanza has no `test = true`, \
                 so nothing runs it",
                target.name
            )),
            (false, true) => bad.push(format!(
                "{}: has `test = true`, but no `#[cfg(test)]` runner, so cargo \
                 collects an empty test binary",
                target.name
            )),
            _ => {}
        }
    }

    assert!(
        bad.is_empty(),
        "an example's runner and its `test = true` disagree:\n  {}\n\n\
         An example that needs servers carries neither, and is driven by \
         tests/e2e_examples.rs instead.",
        bad.join("\n  ")
    );
}
