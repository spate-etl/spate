//! Talking to cargo: discovering wall-clock targets, building them, and
//! learning what the toolchain is.
//!
//! Everything here runs cargo as a subprocess in a named directory, because the
//! two legs of an A/B run are two different checkouts. A leg's targets, its
//! resolved features and its toolchain are all properties of *its* tree, and a
//! `rust-toolchain.toml` can differ between them, so nothing is read from this
//! process's own environment.
//!
//! # Discovery is by naming convention
//!
//! A wall-clock target is `benches/<name>_wall.rs` in any workspace member.
//! The suffix is what keeps three tiers of benchmark from colliding:
//! `*_gungraun.rs` belongs to the instruction-count tier, `*_alloc.rs` to the
//! allocation snapshots, and this one globs for `_wall`. A target that forgets
//! `harness = false` is invisible to this
//! module, since `cargo metadata` reports no harness setting, and is refused later,
//! at the runner protocol's handshake, with an error naming the stanza to add.
//! Both legs are built by the time it arrives.
//!
//! # Why not the `cargo_metadata` crate
//!
//! A few fields are read. A dependency for a few fields would be a dependency
//! this crate has to keep in step with cargo's output across two checkouts that
//! may be months apart, which is more risk than the parsing it saves.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

use crate::record::CaseId;

/// The suffix that marks a bench target as belonging to this tier.
pub const WALL_SUFFIX: &str = "_wall";

/// One wall-clock bench target.
///
/// The package and the name together identify it. Two packages can each
/// declare a target of one name, as the counted tier's targets already do.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BenchTarget {
    /// The package that declares it.
    pub package: String,
    /// The target's name, ending in [`WALL_SUFFIX`].
    pub target: String,
}

impl From<&CaseId> for BenchTarget {
    fn from(case: &CaseId) -> Self {
        Self {
            package: case.krate.clone(),
            target: case.target.clone(),
        }
    }
}

impl std::fmt::Display for BenchTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.package, self.target)
    }
}

/// The packages a run is narrowed to, by exact name.
///
/// `packages` keeps only those packages' targets, and an empty list keeps every
/// one. `excluded` then drops those packages' targets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Packages whose targets to keep. Empty keeps every package.
    pub packages: Vec<String>,
    /// Packages whose targets to drop.
    pub excluded: Vec<String>,
}

impl Selection {
    /// Whether this selection keeps every target.
    #[must_use]
    pub fn is_everything(&self) -> bool {
        self.packages.is_empty() && self.excluded.is_empty()
    }
}

/// What one tree offers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovery {
    /// Every wall-clock target, sorted.
    pub targets: Vec<BenchTarget>,
    /// `package/feature` for every feature cargo resolved for a package that
    /// owns one, sorted.
    ///
    /// Part of the build fingerprint: two legs whose manifests differ can
    /// resolve differently from identical flags, and that makes them
    /// incomparable however the flags read.
    pub features: Vec<String>,
    /// Each owning package's cargo package id, by package name.
    ///
    /// A build message names its package by id only, and the id leaves the
    /// name out when the package's directory carries it, so the name is
    /// looked up here rather than parsed out of the id.
    pub package_ids: BTreeMap<String, String>,
}

impl Discovery {
    /// Narrows [`targets`](Discovery::targets) to a [`Selection`].
    ///
    /// Names are exact, and an empty selection keeps everything. `whose` names
    /// the tree the names were looked for in, since the two legs of a
    /// comparison are two trees and only one of them may be missing a package.
    ///
    /// [`features`](Discovery::features) is left whole. A selected package's
    /// bench binary links whatever it depends on, `spate-bench` among them, so
    /// a set narrowed to the selection would stop guarding packages the leg
    /// compiled. Held whole it can refuse a narrowed run over a feature neither
    /// leg built, which `--allow features` waives and the report's header names.
    ///
    /// An excluded name this tree does not declare drops nothing. A base leg
    /// can predate a package the working tree has, and [`check`](Self::check)
    /// has already refused a misspelt one there.
    ///
    /// # Errors
    ///
    /// When a kept name owns no wall-clock target here. Every such name is
    /// named.
    pub fn select(&mut self, selection: &Selection, whose: &str) -> Result<(), String> {
        if selection.is_everything() {
            return Ok(());
        }
        self.refuse_unknown(&selection.packages, whose)?;
        let wanted: BTreeSet<&str> = selection.packages.iter().map(String::as_str).collect();
        let dropped: BTreeSet<&str> = selection.excluded.iter().map(String::as_str).collect();
        self.targets.retain(|target| {
            let package = target.package.as_str();
            (wanted.is_empty() || wanted.contains(package)) && !dropped.contains(package)
        });
        Ok(())
    }

    /// Refuses a kept or excluded name that owns no wall-clock target here.
    ///
    /// Run against the working tree before anything is built. An exclusion that
    /// matches nothing would measure the package it meant to drop, so a
    /// misspelt one is refused like a misspelt narrowing.
    ///
    /// # Errors
    ///
    /// When any name owns no wall-clock target here. Every such name is named.
    pub fn check(&self, selection: &Selection, whose: &str) -> Result<(), String> {
        let named: Vec<String> = selection
            .packages
            .iter()
            .chain(&selection.excluded)
            .cloned()
            .collect();
        self.refuse_unknown(&named, whose)
    }

    fn refuse_unknown(&self, names: &[String], whose: &str) -> Result<(), String> {
        let owners: BTreeSet<&str> = self.targets.iter().map(|t| t.package.as_str()).collect();
        let unknown: BTreeSet<&str> = names
            .iter()
            .map(String::as_str)
            .filter(|name| !owners.contains(name))
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        Err(format!(
            "no wall-clock bench target in {whose} belongs to {}. The packages that \
             declare one there are {}. A package that declares none and a name that \
             is no package at all both look like this.",
            crate::quoted(unknown),
            crate::quoted(owners.iter().copied())
        ))
    }
}

// --- the four fields read out of `cargo metadata` ---------------------------

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<MetaPackage>,
    /// Package ids of the workspace's own members.
    ///
    /// Read so discovery cannot wander into the dependency graph: this tree
    /// resolves ~500 packages carrying ~300 bench targets between them, and a
    /// dependency that happened to ship a `*_wall` bench would otherwise be
    /// discovered, selected with `-p`, and built.
    #[serde(default)]
    workspace_members: Vec<String>,
    #[serde(default)]
    resolve: Option<MetaResolve>,
}

#[derive(Debug, Deserialize)]
struct MetaPackage {
    id: String,
    name: String,
    targets: Vec<MetaTarget>,
}

#[derive(Debug, Deserialize)]
struct MetaTarget {
    name: String,
    kind: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct MetaResolve {
    nodes: Vec<MetaNode>,
}

#[derive(Debug, Deserialize)]
struct MetaNode {
    id: String,
    #[serde(default)]
    features: Vec<String>,
}

/// Reads a tree's wall-clock targets and resolved features.
///
/// # Errors
///
/// When cargo fails, when its metadata has no `resolve` section, or when a
/// target's name is not a valid Rust identifier.
pub fn discover(dir: &Path, feature_args: &[String]) -> Result<Discovery, String> {
    let raw = run(
        dir,
        "cargo",
        &{
            let mut args = vec![
                "metadata".to_owned(),
                "--format-version".to_owned(),
                "1".to_owned(),
                "--locked".to_owned(),
            ];
            args.extend_from_slice(feature_args);
            args
        },
        &[],
    )?;

    let metadata: Metadata =
        serde_json::from_str(&raw).map_err(|e| format!("cargo metadata did not parse: {e}"))?;
    discovery_from(&metadata)
}

/// The discovery rule, over parsed metadata.
///
/// Split from the subprocess so it can be tested against a fixture: a test that
/// ran `cargo metadata` would assert what this tree happens to contain today,
/// which is not the property worth pinning.
fn discovery_from(metadata: &Metadata) -> Result<Discovery, String> {
    let mut targets = Vec::new();
    let mut package_ids = BTreeMap::new();

    let members: BTreeSet<&str> = metadata
        .workspace_members
        .iter()
        .map(String::as_str)
        .collect();

    for package in &metadata.packages {
        if !members.contains(package.id.as_str()) {
            continue;
        }
        for target in &package.targets {
            if !target.kind.iter().any(|k| k == "bench") || !target.name.ends_with(WALL_SUFFIX) {
                continue;
            }
            // A hyphen survives into the cargo target name but not into
            // `CARGO_CRATE_NAME`, which the binary stamps into every record, so
            // the two would disagree and the driver would have no runner for
            // the case it just listed. Refused here, before either leg builds.
            if target.name.contains('-') {
                return Err(format!(
                    "'{}' declares a bench target named '{}'; a wall-clock target name \
                     must be a valid Rust identifier, because the record carries the \
                     crate name cargo compiles it under and a hyphen becomes an \
                     underscore there",
                    package.name, target.name
                ));
            }
            package_ids.insert(package.name.clone(), package.id.clone());
            targets.push(BenchTarget {
                package: package.name.clone(),
                target: target.name.clone(),
            });
        }
    }
    targets.sort();

    let by_id: BTreeMap<&str, &str> = metadata
        .packages
        .iter()
        .map(|p| (p.id.as_str(), p.name.as_str()))
        .collect();
    // Absent `resolve` is an error, not an empty feature set. Empty on both
    // legs compares equal, so the resolved-feature guard would silently stop
    // guarding while the header still reported that every guard passed.
    let resolve = metadata.resolve.as_ref().ok_or_else(|| {
        "cargo metadata carried no `resolve` section, so the resolved features cannot be \
         read — and a comparison that cannot read them cannot claim the two legs \
         compiled the same code"
            .to_owned()
    })?;
    let mut features = BTreeSet::new();
    let owning_ids: BTreeSet<&str> = package_ids.values().map(String::as_str).collect();
    for node in &resolve.nodes {
        if !owning_ids.contains(node.id.as_str()) {
            continue;
        }
        let name = by_id.get(node.id.as_str()).copied().unwrap_or("?");
        for feature in &node.features {
            features.insert(format!("{name}/{feature}"));
        }
    }

    Ok(Discovery {
        targets,
        features: features.into_iter().collect(),
        package_ids,
    })
}

// --- the fields read out of `cargo bench --message-format=json` --------------

#[derive(Debug, Deserialize)]
struct Artifact {
    reason: String,
    #[serde(default)]
    package_id: Option<String>,
    #[serde(default)]
    target: Option<MetaTarget>,
    #[serde(default)]
    executable: Option<PathBuf>,
}

/// Builds every target `discovery` holds and returns the executable each
/// produced.
///
/// The **bench** profile, not release: `[profile.bench] debug = true` in this
/// workspace makes them different profiles, and a comparison across two
/// profiles is not a comparison.
///
/// # Errors
///
/// When cargo fails, or when a named target produced no executable.
pub fn build(
    dir: &Path,
    target_dir: &Path,
    discovery: &Discovery,
    feature_args: &[String],
) -> Result<BTreeMap<BenchTarget, PathBuf>, String> {
    let targets = &discovery.targets;
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut args = vec![
        "bench".to_owned(),
        "--no-run".to_owned(),
        "--locked".to_owned(),
        "--message-format".to_owned(),
        "json-render-diagnostics".to_owned(),
    ];
    for package in targets.iter().map(|t| &t.package).collect::<BTreeSet<_>>() {
        args.push("-p".to_owned());
        args.push(package.clone());
    }
    // A name two packages share is passed twice, which cargo accepts. Each
    // `--bench` selects that target in every package `-p` named.
    for target in targets {
        args.push("--bench".to_owned());
        args.push(target.target.clone());
    }
    args.extend_from_slice(feature_args);

    let stdout = run(
        dir,
        "cargo",
        &args,
        &[("CARGO_TARGET_DIR", target_dir.display().to_string())],
    )?;

    let wanted: BTreeSet<&BenchTarget> = targets.iter().collect();
    let built = executables_in(&stdout, &wanted, &discovery.package_ids);

    for target in targets {
        if !built.contains_key(target) {
            return Err(format!("cargo built no executable for '{target}'"));
        }
    }
    Ok(built)
}

/// The bench executables cargo reported for the wanted targets.
///
/// An artifact is matched on its package and its target name together, so two
/// packages' targets of one name each get their own executable.
///
/// Split from the subprocess so a test can drive it with a fixture of the lines
/// cargo emits, including the ones this crate does not model, which a
/// future cargo will add and which must be skipped rather than fatal.
fn executables_in(
    stdout: &str,
    wanted: &BTreeSet<&BenchTarget>,
    package_ids: &BTreeMap<String, String>,
) -> BTreeMap<BenchTarget, PathBuf> {
    let names: BTreeMap<&str, &str> = package_ids
        .iter()
        .map(|(name, id)| (id.as_str(), name.as_str()))
        .collect();
    let mut built = BTreeMap::new();
    for line in stdout.lines() {
        let Ok(artifact) = serde_json::from_str::<Artifact>(line) else {
            continue;
        };
        if artifact.reason != "compiler-artifact" {
            continue;
        }
        let (Some(id), Some(target), Some(executable)) =
            (artifact.package_id, artifact.target, artifact.executable)
        else {
            continue;
        };
        let Some(&package) = names.get(id.as_str()) else {
            continue;
        };
        let key = BenchTarget {
            package: package.to_owned(),
            target: target.name,
        };
        if target.kind.iter().any(|k| k == "bench") && wanted.contains(&key) {
            built.insert(key, executable);
        }
    }
    built
}

/// A digest over everything that decides how a leg's code is generated but is
/// not a source file.
///
/// The profile *name* is not this. Both legs are built with `cargo bench`, so
/// the name is `bench` on each by construction and a guard on it can never
/// fire, while the settings behind that name are what a performance
/// change is likely to touch. A head that adds `lto = "fat"` to
/// `[profile.bench]` is a real and plausible pull request, and without this the
/// two legs would fingerprint identically and the win would be attributed to
/// the code.
///
/// Three inputs, each read from the leg's own tree or environment:
///
/// - every `[profile...]` table in the root manifest,
/// - every distinct `.cargo/config.toml` from the leg's directory upward, whole,
///   since all of it is build configuration and cargo reads all of them, except
///   `$CARGO_HOME`'s, which cargo reads for every invocation whatever the
///   directory, so it applies to both legs and cannot separate them,
/// - `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS`, read from the driver's own
///   environment, so they are the same for both legs and can never *separate*
///   them. They are here so a leg written under one setting and compared later
///   against a leg written under another is refused.
///
/// Absent inputs contribute their absence, so a leg that gains a distinct
/// `.cargo/config.toml` differs from one without.
#[must_use]
pub fn codegen_digest(dir: &Path) -> String {
    use std::hash::Hasher as _;

    let mut hasher = twox_hash::XxHash64::with_seed(0);
    let mut absorb = |label: &str, text: &str| {
        hasher.write(label.as_bytes());
        hasher.write(&(text.len() as u64).to_le_bytes());
        hasher.write(text.as_bytes());
    };

    absorb(
        "profiles",
        &profile_tables(&std::fs::read_to_string(dir.join("Cargo.toml")).unwrap_or_default()),
    );
    // Every distinct ancestor config, nearest first, contents only. Cargo reads
    // them from the invocation's directory upward, and the two legs have different
    // ancestors by construction (the head is the repository, the base a
    // worktree under the cache root). Hashing the *contents* rather than the
    // paths is what makes that difference visible: a `.cargo/config.toml` above
    // the repository that differs from the leg's own applies to the head build
    // and not the base one, so the two digests differ and the guard fires.
    // Hashing the paths as well would make them differ always.
    for text in ancestor_cargo_configs(dir) {
        absorb("cargo-config", &text);
    }
    for key in ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"] {
        absorb(key, &std::env::var(key).unwrap_or_default());
    }

    format!("{:016x}", hasher.finish())
}

/// The distinct contents of every `.cargo/config.toml` (or `.cargo/config`) from
/// `dir` upward, nearest first.
///
/// A file identical to a nearer one sets no new key and only repeats its array
/// entries, and a worktree nested in the repository has the same config above
/// it.
fn ancestor_cargo_configs(dir: &Path) -> Vec<String> {
    let home = cargo_home();
    let mut out = Vec::new();
    for ancestor in dir.ancestors() {
        let candidate = ancestor.join(".cargo");
        // `$CARGO_HOME/config.toml` is skipped. Cargo reads it for *every*
        // invocation whatever the working directory, so it applies to both legs
        // equally, but it is a path ancestor of a repository under `$HOME` and
        // not of a worktree under the cache root, so hashing it would make the
        // two digests differ over a file that changed nothing. One inert alias
        // in `~/.cargo/config.toml` was enough to abort a whole A/B run.
        if home.as_ref().is_some_and(|home| *home == candidate) {
            continue;
        }
        for name in ["config.toml", "config"] {
            if let Ok(text) = std::fs::read_to_string(candidate.join(name))
                && !out.contains(&text)
            {
                out.push(text);
            }
        }
    }
    out
}

/// Where cargo keeps its own configuration, as cargo resolves it.
fn cargo_home() -> Option<PathBuf> {
    std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
}

/// Every `[profile...]` table in a manifest, concatenated in file order.
///
/// A line scan rather than a TOML parse; a table header in this repository is
/// always a `[` in the first column.
fn profile_tables(manifest: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in manifest.lines() {
        // Trimmed before the test: TOML permits leading whitespace before a
        // table header, and reading only column 0 would let an indented
        // `[profile.bench]` past, or, worse, let an indented `[dependencies]`
        // *fail* to close the profile table and drag itself into the digest.
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            inside = trimmed.starts_with("[profile");
        }
        // `profile.bench.lto = "fat"` is a dotted key, valid at the top level
        // and outside any `[profile...]` header. Over-capturing a key that
        // that only starts with `profile.` is safe here; missing one is not.
        if inside || trimmed.starts_with("profile.") {
            out.push_str(trimmed);
            out.push('\n');
        }
    }
    out
}

/// `rustc --version` and the host triple, for the tree at `dir`.
///
/// Asked of the tree rather than of this process, because a leg may pin a
/// different toolchain in its own `rust-toolchain.toml`.
///
/// # Errors
///
/// When `rustc -vV` cannot be run or does not carry a `host:` line.
pub fn toolchain(dir: &Path) -> Result<(String, String), String> {
    let verbose = run(dir, "rustc", &["-vV".to_owned()], &[])?;
    let version = verbose
        .lines()
        .next()
        .ok_or_else(|| "rustc -vV printed nothing".to_owned())?
        .trim()
        .to_owned();
    let host = verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| "rustc -vV carries no host: line".to_owned())?
        .trim()
        .to_owned();
    Ok((version, host))
}

/// Runs a command in `dir` and returns its stdout.
///
/// Stderr is inherited rather than captured: cargo's progress and any compiler
/// diagnostic belong on the terminal in real time, and a build that fails
/// should say why where the person watching can see it.
fn run(
    dir: &Path,
    program: &str,
    args: &[String],
    env: &[(&str, String)],
) -> Result<String, String> {
    let mut command = Command::new(program);
    command
        .current_dir(dir)
        .args(args)
        // Inherited rather than captured, so a ten-minute build's progress and
        // any compiler diagnostic reach the terminal as they happen. Only
        // stdout is piped, because only stdout is parsed.
        .stderr(Stdio::inherit())
        .stdout(Stdio::piped())
        // rustup's proxy exports the toolchain it resolved for *this* process
        // into every child. The two legs of a comparison are two checkouts that
        // may pin different toolchains in their own `rust-toolchain.toml`, and
        // an inherited override would silently build both with one of them,
        // which the build fingerprint would then record as agreement.
        .env_remove("RUSTUP_TOOLCHAIN");
    for (key, value) in env {
        command.env(key, value);
    }

    let output = command
        .spawn()
        .map_err(|e| format!("could not run {program}: {e}"))?
        .wait_with_output()
        .map_err(|e| format!("{program} could not be waited on: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "{program} {} failed with {}",
            args.join(" "),
            output.status
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| format!("{program} printed invalid UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::Metadata;

    const FIXTURE: &str = r#"{
        "workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0"],
        "packages": [
            {
                "id": "path+file:///w/crates/spate-json#spate-json@0.1.0",
                "name": "spate-json",
                "targets": [
                    {"name": "spate-json", "kind": ["lib"]},
                    {"name": "decode_wall", "kind": ["bench"]},
                    {"name": "decode_gungraun", "kind": ["bench"]},
                    {"name": "decode", "kind": ["bench"]},
                    {"name": "wall_helper", "kind": ["test"]}
                ]
            },
            {
                "id": "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0",
                "name": "other",
                "targets": [{"name": "someone_elses_wall", "kind": ["bench"]}]
            }
        ],
        "resolve": {"nodes": [{
            "id": "path+file:///w/crates/spate-json#spate-json@0.1.0",
            "features": ["simd", "default"]
        }]}
    }"#;

    /// The discovery rule itself, not a re-implementation of it:
    /// `discovery_from` is what `discover` calls once cargo has answered.
    #[test]
    fn discovery_takes_wall_targets_and_leaves_everything_else_alone() {
        let metadata: Metadata = serde_json::from_str(FIXTURE).expect("parses");
        let found = super::discovery_from(&metadata).expect("discovers");

        // `decode_gungraun` belongs to the counter tier, `decode` carries no
        // tier suffix at all, `wall_helper` is not a bench, and
        // `someone_elses_wall` belongs to a dependency rather than to this
        // workspace.
        assert_eq!(
            found.targets,
            [super::BenchTarget {
                package: "spate-json".to_owned(),
                target: "decode_wall".to_owned(),
            }]
        );
        assert_eq!(found.features, ["spate-json/default", "spate-json/simd"]);
    }

    /// A hyphen survives into the cargo target name but not into
    /// `CARGO_CRATE_NAME`, so the record and the driver would disagree about
    /// which case they were talking about.
    #[test]
    fn a_hyphenated_target_name_is_refused_by_name() {
        let metadata: Metadata =
            serde_json::from_str(&FIXTURE.replace("decode_wall", "decode-json_wall"))
                .expect("parses");
        let err = super::discovery_from(&metadata).expect_err("refused");
        assert!(err.contains("valid Rust identifier"), "{err}");
    }

    const JSON_ID: &str = "path+file:///w/crates/spate-json#spate-json@0.1.0";
    const OTHER_ID: &str = "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0";

    /// Two packages may each declare a target of one name. Each is discovered
    /// under its own package, with the id its build messages will carry.
    #[test]
    fn a_target_name_shared_by_two_packages_discovers_both() {
        let both_members = FIXTURE
            .replace(
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0"]"#,
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0", "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0"]"#,
            )
            .replace("someone_elses_wall", "decode_wall");
        let metadata: Metadata = serde_json::from_str(&both_members).expect("parses");
        let found = super::discovery_from(&metadata).expect("discovers");

        assert_eq!(
            found.targets,
            [
                target("other", "decode_wall"),
                target("spate-json", "decode_wall")
            ]
        );
        assert_eq!(
            found.package_ids,
            BTreeMap::from([
                ("other".to_owned(), OTHER_ID.to_owned()),
                ("spate-json".to_owned(), JSON_ID.to_owned()),
            ])
        );
    }

    fn target(package: &str, name: &str) -> super::BenchTarget {
        super::BenchTarget {
            package: package.to_owned(),
            target: name.to_owned(),
        }
    }

    /// One `compiler-artifact` line for a bench target.
    fn artifact(id: &str, name: &str, executable: &str) -> String {
        format!(
            r#"{{"reason":"compiler-artifact","package_id":"{id}","target":{{"name":"{name}","kind":["bench"]}},"executable":"{executable}"}}"#
        )
    }

    fn both_ids() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("other".to_owned(), OTHER_ID.to_owned()),
            ("spate-json".to_owned(), JSON_ID.to_owned()),
        ])
    }

    /// A selection that keeps only the named packages.
    fn keep(names: &[&str]) -> super::Selection {
        super::Selection {
            packages: names.iter().map(|&n| n.to_owned()).collect(),
            excluded: Vec::new(),
        }
    }

    /// A selection that drops the named packages and keeps the rest.
    fn without(names: &[&str]) -> super::Selection {
        super::Selection {
            packages: Vec::new(),
            excluded: names.iter().map(|&n| n.to_owned()).collect(),
        }
    }

    /// Only a selection that names nothing keeps every target, so only it
    /// skips the check and the narrowing.
    #[test]
    fn only_an_empty_selection_keeps_everything() {
        assert!(super::Selection::default().is_everything());
        assert!(!keep(&["spate-json"]).is_everything());
        assert!(!without(&["spate-json"]).is_everything());
    }

    /// Two owning packages, so a selection has something to leave out.
    /// `other` is promoted to a workspace member and given a resolve node of
    /// its own, and keeps its own target name.
    fn two_owners() -> super::Discovery {
        let both = FIXTURE
            .replace(
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0"]"#,
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0", "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0"]"#,
            )
            .replace(
                r#""resolve": {"nodes": [{"#,
                r#""resolve": {"nodes": [{"id": "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0", "features": ["std"]}, {"#,
            );
        let metadata: Metadata = serde_json::from_str(&both).expect("parses");
        super::discovery_from(&metadata).expect("discovers")
    }

    /// A selection narrows the targets and leaves the resolved features whole.
    /// `other/std` surviving is the guard staying in force over a package the
    /// selected one links but does not name.
    #[test]
    fn a_selection_narrows_the_targets_and_leaves_the_features_whole() {
        let mut found = two_owners();
        found
            .select(&keep(&["spate-json"]), "the tree")
            .expect("selects");

        assert_eq!(
            found.targets,
            [super::BenchTarget {
                package: "spate-json".to_owned(),
                target: "decode_wall".to_owned(),
            }]
        );
        assert_eq!(
            found.features,
            ["other/std", "spate-json/default", "spate-json/simd"]
        );
    }

    /// A name that owns no target is a typo or a crate with no wall target, and
    /// the reader cannot tell which from the name alone, so the message names
    /// the set that does work.
    #[test]
    fn an_unknown_package_is_refused_and_names_the_ones_that_declare_a_target() {
        let mut found = two_owners();
        let err = found
            .select(&keep(&["spate-jsn"]), "the tree")
            .expect_err("refused");

        assert!(err.contains("'spate-jsn'"), "{err}");
        assert!(
            err.contains("'other'") && err.contains("'spate-json'"),
            "{err}"
        );
        assert_eq!(found, two_owners(), "a refusal narrowed the discovery");
    }

    /// Measuring a subset of what was asked for would report a scope the flags
    /// do not describe.
    #[test]
    fn an_unknown_name_beside_a_known_one_is_still_refused() {
        let mut found = two_owners();
        let err = found
            .select(&keep(&["spate-json", "nope"]), "the tree")
            .expect_err("refused");

        // The known name appears in the second clause, which lists what does
        // work, so the first clause is what has to be checked.
        let (named, _) = err.split_once(". The packages").expect("two clauses");
        assert!(named.contains("'nope'"), "{err}");
        assert!(
            !named.contains("'spate-json'"),
            "a known name was reported as unknown: {err}"
        );
    }

    /// An exclusion drops that package's targets and keeps every other one,
    /// with the resolved features whole as for a narrowing.
    #[test]
    fn an_exclusion_drops_that_packages_targets_and_keeps_the_rest() {
        let mut found = two_owners();
        found
            .select(&without(&["spate-json"]), "the tree")
            .expect("selects");

        assert_eq!(
            found.targets,
            [super::BenchTarget {
                package: "other".to_owned(),
                target: "someone_elses_wall".to_owned(),
            }]
        );
        assert_eq!(found.features, two_owners().features);
    }

    /// An exclusion that matches nothing would measure the package it meant to
    /// drop, so the working tree's check refuses a misspelt one like a
    /// misspelt narrowing.
    #[test]
    fn the_check_refuses_an_unknown_excluded_package() {
        let err = two_owners()
            .check(&without(&["spate-jsn"]), "the tree")
            .expect_err("refused");
        assert!(err.contains("'spate-jsn'"), "{err}");

        two_owners()
            .check(&without(&["spate-json"]), "the tree")
            .expect("a declared package passes");
        let err = two_owners()
            .check(&keep(&["nope"]), "the tree")
            .expect_err("refused");
        assert!(err.contains("'nope'"), "{err}");
    }

    /// A base leg can predate a package the working tree has, so an excluded
    /// name this tree does not declare drops nothing and refuses nothing.
    #[test]
    fn a_leg_without_the_excluded_package_keeps_everything() {
        let mut found = two_owners();
        found
            .select(&without(&["only-in-the-head"]), "the base leg")
            .expect("selects");
        assert_eq!(found, two_owners());
    }

    /// The default path, which every unnarrowed run takes.
    #[test]
    fn an_empty_selection_changes_nothing() {
        let mut found = two_owners();
        found
            .select(&super::Selection::default(), "the tree")
            .expect("selects");
        assert_eq!(found, two_owners());
    }

    /// `executables_in` itself, over the lines cargo emits, including a
    /// `reason` this crate does not model, which a future cargo will add and
    /// which must be skipped rather than fatal.
    #[test]
    fn only_bench_artifacts_with_an_executable_are_taken() {
        const LINES: &str = concat!(
            r#"{"reason":"compiler-artifact","package_id":"path+file:///w/crates/spate-json#spate-json@0.1.0","target":{"name":"decode_wall","kind":["bench"]},"executable":"/t/decode_wall-abc"}"#,
            "\n",
            r#"{"reason":"compiler-artifact","package_id":"path+file:///w/crates/spate-json#spate-json@0.1.0","target":{"name":"spate-json","kind":["lib"]},"executable":null}"#,
            "\n",
            r#"{"reason":"compiler-artifact","package_id":"path+file:///w/crates/spate-json#spate-json@0.1.0","target":{"name":"other_wall","kind":["bench"]},"executable":"/t/other_wall-def"}"#,
            "\n",
            r#"{"reason":"something-new","extra":{"a":1}}"#,
            "\n",
            "not json at all\n",
        );

        let wanted = target("spate-json", "decode_wall");
        let built = super::executables_in(LINES, &BTreeSet::from([&wanted]), &both_ids());
        assert_eq!(
            built,
            BTreeMap::from([(wanted, std::path::PathBuf::from("/t/decode_wall-abc"))])
        );
    }

    /// Two packages' targets of one name each get their own executable.
    #[test]
    fn a_shared_target_name_maps_each_package_to_its_own_executable() {
        let lines = [
            artifact(JSON_ID, "decode_wall", "/t/decode_wall-abc"),
            artifact(OTHER_ID, "decode_wall", "/t/decode_wall-def"),
        ]
        .join("\n");
        let json = target("spate-json", "decode_wall");
        let other = target("other", "decode_wall");

        let built = super::executables_in(&lines, &BTreeSet::from([&json, &other]), &both_ids());

        assert_eq!(
            built,
            BTreeMap::from([
                (json, std::path::PathBuf::from("/t/decode_wall-abc")),
                (other, std::path::PathBuf::from("/t/decode_wall-def")),
            ])
        );
    }

    /// An artifact from a package discovery did not record is skipped, even
    /// when its target name is wanted under another package.
    #[test]
    fn an_artifact_from_an_unknown_package_is_skipped() {
        let lines = artifact(
            "path+file:///elsewhere#3.0.0",
            "decode_wall",
            "/t/decode_wall-zzz",
        );
        let wanted = target("spate-json", "decode_wall");

        let built = super::executables_in(&lines, &BTreeSet::from([&wanted]), &both_ids());

        assert_eq!(built, BTreeMap::new());
    }

    /// The package and the name are matched as a pair. Each artifact here has
    /// a wanted package and a wanted name, but not a wanted pair.
    #[test]
    fn a_wanted_package_and_a_wanted_name_do_not_make_a_wanted_pair() {
        let lines = [
            artifact(JSON_ID, "y_wall", "/t/y_wall-abc"),
            artifact(OTHER_ID, "x_wall", "/t/x_wall-def"),
        ]
        .join("\n");
        let json = target("spate-json", "x_wall");
        let other = target("other", "y_wall");

        let built = super::executables_in(&lines, &BTreeSet::from([&json, &other]), &both_ids());

        assert_eq!(built, BTreeMap::new());
    }

    /// The digest has to move with a codegen setting and stand still for
    /// anything else in the manifest.
    #[test]
    fn the_profile_extract_takes_the_profile_tables_and_nothing_else() {
        const BASE: &str = "[workspace]\nmembers = [\"a\"]\n\n\
                            [profile.bench]\ndebug = true\n\n\
                            [workspace.dependencies]\nserde = \"1\"\n";
        let tables = super::profile_tables(BASE);
        assert!(tables.contains("[profile.bench]"), "{tables}");
        assert!(tables.contains("debug = true"), "{tables}");
        assert!(!tables.contains("serde"), "{tables}");
        assert!(!tables.contains("[workspace]"), "{tables}");

        // A dependency added elsewhere leaves it alone; a codegen setting moves
        // it.
        let unrelated = BASE.replace("serde = \"1\"", "serde = \"1\"\nbytes = \"1\"");
        assert_eq!(super::profile_tables(&unrelated), tables);
        let relto = BASE.replace("debug = true", "debug = true\nlto = \"fat\"");
        assert_ne!(super::profile_tables(&relto), tables);

        // The two forms cargo accepts that a column-0 header scan misses: a
        // dotted key with no header at all, and an indented header.
        let dotted = format!("profile.bench.lto = \"fat\"\n{BASE}");
        assert!(super::profile_tables(&dotted).contains("lto"), "dotted key");
        let indented = BASE.replace("[profile.bench]", "  [profile.bench]");
        assert!(
            super::profile_tables(&indented).contains("debug = true"),
            "indented header"
        );
    }

    /// A nested worktree's digest matches a leg with the same config once, and
    /// a config that differs above it still separates them. Regression for
    /// #1132.
    #[test]
    fn a_config_repeated_above_a_leg_counts_once() {
        fn config(dir: &std::path::Path, text: &str) {
            std::fs::create_dir_all(dir.join(".cargo")).expect("a config directory");
            std::fs::write(dir.join(".cargo/config.toml"), text).expect("a config");
        }

        let root = tempfile::tempdir().expect("scratch directory");
        let base = root.path().join("cache/base");
        let repo = root.path().join("repo");
        let nested = repo.join("wt/aa");
        config(&base, "[build]\njobs = 4\n");
        config(&repo, "[build]\njobs = 4\n");
        config(&nested, "[build]\njobs = 4\n");
        assert_eq!(super::codegen_digest(&nested), super::codegen_digest(&base));

        config(&repo, "[build]\nrustflags = [\"-Ctarget-cpu=native\"]\n");
        assert_ne!(super::codegen_digest(&nested), super::codegen_digest(&base));
    }
}
