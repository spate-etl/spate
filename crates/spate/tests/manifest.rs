//! Holds the examples, the integration tests and the rustdoc feature table to
//! what the manifest declares.
//!
//! `autoexamples` and `autotests` are both left at their default, so a file
//! nothing declares is collected anyway, carrying no `required-features`. What
//! that costs differs by kind.
//!
//! An example is the target shape cargo will drop on the floor without saying
//! so. A `[[example]]` whose `required-features` names a feature the package
//! does not declare is not an error: the target is skipped, and
//! `cargo check --examples` reports success having built one fewer thing than
//! the manifest asked for. A typo there removes an example from every job in
//! this repository, permanently, with the build green.
//!
//! A test divides the other way. A declared target with unmet features errors
//! when it is named, so `cargo test -p spate --test e2e_happy` says what is
//! wrong; what no unnamed run does is build it, which is how a typo retires a
//! suite quietly. An *undeclared* test file is the reverse: every run builds
//! it, including under feature sets its imports do not exist in, and there it
//! fails to compile and takes the package's test run with it.
//!
//! `--all-features` meets every `required-features` entry that names a real
//! feature, so once question 2 holds, a run under it builds every test target.
//!
//! The questions, none of which builds anything, since `cargo metadata` reads
//! manifests and resolves nothing:
//!
//! 1. Is every example declared rather than auto-discovered?
//! 2. Does every `required-features` entry, on either kind, name a feature that
//!    exists?
//! 3. Do an example's runner block and its `test = true` agree, in both
//!    directions?
//! 4. Does every test target require exactly the features that its source,
//!    the shared end-to-end harness and the examples it runs reach?
//! 5. Does every declared feature have a row in the rustdoc feature table, and
//!    does every row name a feature that exists?
//!
//! Question 1 has no counterpart for tests. Counting `[[test]]` stanzas against
//! test targets would demand a stanza for the two that need no features, which
//! declare nothing on purpose; question 4 asks what that count was proxying for.
//!
//! `test = true` **is** the declaration that an example runs on the
//! pull-request tier, so anything recording that a second time is a second
//! place to forget it. The pair that can disagree is the stanza and the
//! source: a runner nothing collects, or a collected target with nothing
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
    /// Whether cargo collects this target as a test, from `[[example]] test = true`.
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

/// Filter on `kind`, not on `test`: the library target reports `test = true`
/// as well, and an integration test reports it unconditionally, so the flag
/// separates nothing here.
fn tests(pkg: &Package) -> Vec<&Target> {
    pkg.targets
        .iter()
        .filter(|t| t.kind.iter().any(|k| k == "test"))
        .collect()
}

/// The attribute a scenario includes the shared harness with.
const HARNESS: &str = r#"#[path = "e2e_support/mod.rs"]"#;

/// The header row of the rustdoc feature table, and the prefix its lines carry.
const FEATURE_TABLE: &str = "| Feature | Enables |";
const DOC: &str = "//!";

/// The feature names backticked in the first cell of each feature-table row,
/// and any first cell carrying text outside the backticks. Both are empty when
/// the header row is absent.
///
/// A first cell is a list of feature names and separators. Prose there, such
/// as a cross-reference to a feature whose row sits elsewhere, would otherwise
/// read as that feature's own row.
fn table_features(src: &str) -> (BTreeSet<String>, Vec<String>) {
    let mut lines = src
        .lines()
        .map(|l| l.trim_start().trim_start_matches(DOC).trim())
        .skip_while(|l| *l != FEATURE_TABLE)
        .skip(2);
    let (mut names, mut malformed) = (BTreeSet::new(), Vec::new());
    for row in lines.by_ref().take_while(|l| l.starts_with('|')) {
        let cell = row.trim_matches('|').split('|').next().unwrap_or_default();
        // Backticked runs sit at the odd indices of a split on the delimiter,
        // so one row can carry several names. The even indices hold the
        // separators.
        let parts = cell.split('`');
        if parts
            .clone()
            .step_by(2)
            .any(|gap| gap.chars().any(|c| c != ',' && !c.is_whitespace()))
        {
            malformed.push(cell.trim().to_owned());
            continue;
        }
        names.extend(parts.skip(1).step_by(2).map(str::to_owned));
    }
    (names, malformed)
}

/// Guards every assertion below: a filter that matched nothing would make
/// them all pass while testing nothing.
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

/// Every example file is declared, not auto-discovered. `autoexamples` is left
/// at its default, so a file added under `examples/` without a `[[example]]`
/// stanza becomes a target anyway, carrying no `required-features` and no
/// `test = true`, which is the one shape nothing else here reports. Counting
/// rather than matching names is enough in both directions: a stanza naming a
/// file that does not exist fails `cargo metadata` outright, so equal counts
/// mean equal sets.
#[test]
fn every_example_file_has_a_stanza() {
    let pkg = spate_package();
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read the spate manifest");
    let stanzas = manifest.matches("[[example]]").count();
    assert_eq!(
        stanzas,
        examples(&pkg).len(),
        "the manifest declares {stanzas} `[[example]]` stanza(s) against {} example \
         target(s); an undeclared file is auto-discovered with no \
         `required-features` and nothing runs it",
        examples(&pkg).len()
    );
}

/// The assertion this file exists for. `cargo check --examples --all-features`
/// exits 0 with a typo here, and so does every test job, which selects no
/// target by name.
#[test]
fn every_required_feature_is_declared() {
    let pkg = spate_package();
    let declared: BTreeSet<&str> = pkg.features.keys().map(String::as_str).collect();
    assert!(
        !declared.is_empty(),
        "cargo metadata reported no features for spate"
    );

    let mut bad = Vec::new();
    for target in examples(&pkg).into_iter().chain(tests(&pkg)) {
        for feature in &target.required_features {
            if !declared.contains(feature.as_str()) {
                bad.push(format!("{}: requires `{feature}`", target.name));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "these targets require features spate does not declare, so cargo skips \
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

/// Guards the two assertions below, as `the_examples_are_discovered` guards
/// the ones above. A floor rather than a count, so adding a suite needs no
/// edit here; retiring one does.
#[test]
fn the_tests_are_discovered() {
    let pkg = spate_package();
    let found = tests(&pkg).len();
    assert!(
        found >= 7,
        "only {found} test target(s) in cargo metadata; the filter has stopped \
         matching, so the assertions keyed on it are vacuous"
    );
}

/// The declared features a source reaches through `spate::<module>` paths,
/// counting each member of a `spate::{…}` group.
///
/// A path through an alias (`use spate as s`) is not seen. Panics on
/// `spate::coordination`, which also resolves without its feature.
fn connector_features(src: &str, declared: &BTreeSet<&str>) -> BTreeSet<String> {
    fn ident(s: &str) -> &str {
        let s = s.trim_start();
        let end = s
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(s.len());
        &s[..end]
    }

    let mut found = BTreeSet::new();
    for (at, needle) in src.match_indices("spate::") {
        if src[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            continue;
        }
        let rest = &src[at + needle.len()..];
        let mut names = Vec::new();
        match rest.strip_prefix('{') {
            Some(group) => {
                let (mut depth, mut item) = (0, 0);
                for (i, c) in group.char_indices() {
                    match c {
                        '{' => depth += 1,
                        '}' if depth == 0 => {
                            names.push(ident(&group[item..i]));
                            break;
                        }
                        '}' => depth -= 1,
                        ',' if depth == 0 => {
                            names.push(ident(&group[item..i]));
                            item = i + 1;
                        }
                        _ => {}
                    }
                }
            }
            None => names.push(ident(rest)),
        }
        for name in names {
            assert_ne!(
                name, "coordination",
                "`spate::coordination` resolves with or without the `coordination` \
                 feature, so whether a target needs it is decided by hand"
            );
            if declared.contains(name) {
                found.insert(name.to_owned());
            }
        }
    }
    found
}

/// Every test target requires exactly the features it reaches: the connector
/// modules its source names and, for a scenario including the shared harness,
/// the harness's modules plus the `required-features` of each example it names
/// as a quoted literal.
///
/// Requiring less fails to compile under the declared set; requiring more
/// builds connectors the test never touches.
#[test]
fn test_targets_require_what_they_reach() {
    let pkg = spate_package();
    let declared: BTreeSet<&str> = pkg.features.keys().map(String::as_str).collect();
    let harness_src = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/e2e_support/mod.rs"
    ))
    .expect("read the end-to-end harness");
    let harness = connector_features(&harness_src, &declared);

    // This file names the attribute it searches for, so it matches itself.
    // `file!()` tracks the path through a rename where a written-out constant
    // would go stale.
    let mut skipped = 0;
    let mut includes = 0;
    let mut bad = Vec::new();

    for target in tests(&pkg) {
        if target.src_path.ends_with(file!()) {
            skipped += 1;
            continue;
        }
        let src = std::fs::read_to_string(&target.src_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", target.src_path.display()));
        let mut needs = connector_features(&src, &declared);
        if src.contains(HARNESS) {
            includes += 1;
            needs.extend(harness.iter().cloned());
            for example in examples(&pkg) {
                if src.contains(&format!("\"{}\"", example.name)) {
                    needs.extend(example.required_features.iter().cloned());
                }
            }
        }
        let requires: BTreeSet<String> = target.required_features.iter().cloned().collect();
        if requires != needs {
            bad.push(format!(
                "{}: requires {requires:?}, reaches {needs:?}",
                target.name
            ));
        }
    }

    assert_eq!(
        skipped, 1,
        "this file excludes itself by `file!()` and matched {skipped} target(s); \
         at 0 it fails on its own source, above 1 it is excluding a scenario"
    );
    assert!(
        !harness.is_empty() && includes >= 6,
        "the harness reaches {harness:?} and {includes} test target(s) include it; \
         a needle has stopped matching, so the check below is vacuous"
    );
    assert!(
        bad.is_empty(),
        "a test target's stanza disagrees with what its source reaches:\n  {}",
        bad.join("\n  ")
    );
}

/// Every declared feature has a row in the rustdoc feature table, and every
/// row names a feature that exists.
///
/// docs.rs renders that table and nothing reads it back, so a feature added
/// without a row is documented nowhere and every gate stays green.
#[test]
fn every_feature_has_a_table_row() {
    let pkg = spate_package();
    let declared: BTreeSet<String> = pkg
        .features
        .keys()
        // `default` is empty and enables nothing, so it has no row.
        .filter(|f| *f != "default")
        .cloned()
        .collect();
    let src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read the spate crate root");
    let (documented, malformed) = table_features(&src);

    assert!(
        malformed.is_empty(),
        "a feature-table row's first cell carries text outside the backticks:\n  {}\n\n\
         A cell lists the row's own feature names, separated by commas. Prose there \
         reads the same as a name documented elsewhere, so the shape is refused.",
        malformed.join("\n  ")
    );
    assert!(
        !documented.is_empty(),
        "no rows under `{FEATURE_TABLE}` in src/lib.rs; the header has moved, and \
         the comparison below would pass on an empty manifest"
    );
    let undocumented: Vec<&String> = declared.difference(&documented).collect();
    let stale: Vec<&String> = documented.difference(&declared).collect();
    assert!(
        undocumented.is_empty() && stale.is_empty(),
        "the rustdoc feature table and the manifest disagree:\n  \
         declared with no row: {undocumented:?}\n  \
         row naming no feature: {stale:?}"
    );
}
