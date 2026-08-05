//! Talking to cargo: discovering wall-clock targets, building them, and
//! learning what the toolchain is.
//!
//! Everything here runs cargo as a subprocess in a named directory, because the
//! two legs of an A/B run are two different checkouts. A leg's targets, its
//! resolved features and its toolchain are all properties of *its* tree — a
//! `rust-toolchain.toml` can differ between them — so nothing is read from this
//! process's own environment.
//!
//! # Discovery is by naming convention
//!
//! A wall-clock target is `crates/<pkg>/benches/<name>_wall.rs`. The suffix is
//! what keeps three tiers of benchmark from colliding: `*_gungraun.rs` belongs
//! to the instruction-count tier and its discovery script globs for exactly
//! that, criterion and divan targets carry neither suffix, and this one globs
//! for `_wall`. A target that forgets `harness = false` is caught by the
//! protocol at list time rather than by a manifest lint, because the error it
//! produces there can say what to do about it.
//!
//! # Why not the `cargo_metadata` crate
//!
//! Four fields are read. A dependency for four fields would be a dependency
//! this crate has to keep in step with cargo's output across two checkouts that
//! may be months apart, which is more risk than the parsing it saves.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

/// The suffix that marks a bench target as belonging to this tier.
pub const WALL_SUFFIX: &str = "_wall";

/// One wall-clock bench target.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BenchTarget {
    /// The package that declares it.
    pub package: String,
    /// The target's name, ending in [`WALL_SUFFIX`].
    pub target: String,
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
/// When cargo fails, or when two packages declare a target of the same name —
/// the driver keys build artifacts by target name, and two of them would make
/// one leg's binary stand in for the other's.
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
    let mut owners: BTreeMap<String, String> = BTreeMap::new();
    let mut owning_ids = BTreeSet::new();

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
            if let Some(other) = owners.insert(target.name.clone(), package.name.clone()) {
                return Err(format!(
                    "both '{other}' and '{}' declare a bench target named '{}'; \
                     wall-clock target names must be unique across the workspace",
                    package.name, target.name
                ));
            }
            owning_ids.insert(package.id.clone());
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
    for node in &resolve.nodes {
        if !owning_ids.contains(&node.id) {
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
    })
}

// --- the two fields read out of `cargo bench --message-format=json` ---------

#[derive(Debug, Deserialize)]
struct Artifact {
    reason: String,
    #[serde(default)]
    target: Option<MetaTarget>,
    #[serde(default)]
    executable: Option<PathBuf>,
}

/// Builds every named target and returns the executable each produced, keyed by
/// target name.
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
    targets: &[BenchTarget],
    feature_args: &[String],
) -> Result<BTreeMap<String, PathBuf>, String> {
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

    let wanted: BTreeSet<&str> = targets.iter().map(|t| t.target.as_str()).collect();
    let built = executables_in(&stdout, &wanted);

    for target in targets {
        if !built.contains_key(&target.target) {
            return Err(format!(
                "cargo built no executable for '{}' in '{}'",
                target.target, target.package
            ));
        }
    }
    Ok(built)
}

/// The bench executables cargo reported, keyed by target name.
///
/// Split from the subprocess so a test can drive it with a fixture of the lines
/// cargo actually emits — including the ones this crate does not model, which a
/// future cargo will add and which must be skipped rather than fatal.
fn executables_in(stdout: &str, wanted: &BTreeSet<&str>) -> BTreeMap<String, PathBuf> {
    let mut built = BTreeMap::new();
    for line in stdout.lines() {
        let Ok(artifact) = serde_json::from_str::<Artifact>(line) else {
            continue;
        };
        if artifact.reason != "compiler-artifact" {
            continue;
        }
        let (Some(target), Some(executable)) = (artifact.target, artifact.executable) else {
            continue;
        };
        if target.kind.iter().any(|k| k == "bench") && wanted.contains(target.name.as_str()) {
            built.insert(target.name, executable);
        }
    }
    built
}

/// A digest over everything that decides how a leg's code is generated but is
/// not a source file.
///
/// The profile *name* is not this. Both legs are built with `cargo bench`, so
/// the name is `bench` on each by construction and a guard on it can never
/// fire — while the settings behind that name are exactly what a performance
/// change is likely to touch. A head that adds `lto = "fat"` to
/// `[profile.bench]` is a real and plausible pull request, and without this the
/// two legs would fingerprint identically and the win would be attributed to
/// the code.
///
/// Three inputs, each read from the leg's own tree or environment:
///
/// - every `[profile...]` table in the root manifest,
/// - every `.cargo/config.toml` from the leg's directory upward, whole, since
///   all of it is build configuration and cargo reads all of them — except
///   `$CARGO_HOME`'s, which cargo reads for every invocation whatever the
///   directory, so it applies to both legs and cannot separate them,
/// - `RUSTFLAGS` and `CARGO_ENCODED_RUSTFLAGS` — read from the driver's own
///   environment, so they are the same for both legs and can never *separate*
///   them. They are here so a leg written under one setting and compared later
///   against a leg written under another is refused.
///
/// Absent inputs contribute their absence, so a leg that gains a
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
    // Every ancestor's config, nearest first, contents only — cargo reads them
    // from the invocation's directory upward, and the two legs have different
    // ancestors by construction (the head is the repository, the base a
    // worktree under the cache root). Hashing the *contents* rather than the
    // paths is what makes that difference visible: a `.cargo/config.toml` above
    // the repository applies to the head build and not the base one, so the two
    // digests differ and the guard fires. Hashing the paths as well would make
    // them differ always.
    for text in ancestor_cargo_configs(dir) {
        absorb("cargo-config", &text);
    }
    for key in ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"] {
        absorb(key, &std::env::var(key).unwrap_or_default());
    }

    format!("{:016x}", hasher.finish())
}

/// The contents of every `.cargo/config.toml` (or `.cargo/config`) from `dir`
/// upward, nearest first.
fn ancestor_cargo_configs(dir: &Path) -> Vec<String> {
    let home = cargo_home();
    let mut out = Vec::new();
    for ancestor in dir.ancestors() {
        let candidate = ancestor.join(".cargo");
        // `$CARGO_HOME/config.toml` is skipped. Cargo reads it for *every*
        // invocation whatever the working directory, so it applies to both legs
        // equally — but it is a path ancestor of a repository under `$HOME` and
        // not of a worktree under the cache root, so hashing it would make the
        // two digests differ over a file that changed nothing. One inert alias
        // in `~/.cargo/config.toml` was enough to abort a whole A/B run.
        if home.as_ref().is_some_and(|home| *home == candidate) {
            continue;
        }
        for name in ["config.toml", "config"] {
            if let Ok(text) = std::fs::read_to_string(candidate.join(name)) {
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
/// A line scan rather than a TOML parse: the alternative is a dependency whose
/// job would be to read four tables, and a table header in this repository is
/// always a `[` in the first column.
fn profile_tables(manifest: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in manifest.lines() {
        // Trimmed before the test: TOML permits leading whitespace before a
        // table header, and reading only column 0 would let an indented
        // `[profile.bench]` past — or, worse, let an indented `[dependencies]`
        // *fail* to close the profile table and drag itself into the digest.
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            inside = trimmed.starts_with("[profile");
        }
        // `profile.bench.lto = "fat"` is a dotted key, valid at the top level
        // and outside any `[profile...]` header. Over-capturing a key that
        // merely starts with `profile.` is safe here; missing one is not.
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
        // an inherited override would silently build both with one of them —
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

        // `decode_gungraun` belongs to the counter tier, `decode` to criterion,
        // `wall_helper` is not a bench at all, and `someone_elses_wall` belongs
        // to a dependency rather than to this workspace.
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

    /// Artifacts are keyed by target name, so two packages sharing one would
    /// make a leg run the wrong binary.
    #[test]
    fn a_duplicate_target_name_across_packages_is_refused() {
        let both_members = FIXTURE
            .replace(
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0"]"#,
                r#""workspace_members": ["path+file:///w/crates/spate-json#spate-json@0.1.0", "registry+https://github.com/rust-lang/crates.io-index#other@1.0.0"]"#,
            )
            .replace("someone_elses_wall", "decode_wall");
        let metadata: Metadata = serde_json::from_str(&both_members).expect("parses");
        let err = super::discovery_from(&metadata).expect_err("refused");
        assert!(err.contains("must be unique"), "{err}");
    }

    /// `executables_in` itself, over the lines cargo emits — including a
    /// `reason` this crate does not model, which a future cargo will add and
    /// which must be skipped rather than fatal.
    #[test]
    fn only_bench_artifacts_with_an_executable_are_taken() {
        const LINES: &str = concat!(
            r#"{"reason":"compiler-artifact","target":{"name":"decode_wall","kind":["bench"]},"executable":"/t/decode_wall-abc"}"#,
            "\n",
            r#"{"reason":"compiler-artifact","target":{"name":"spate-json","kind":["lib"]},"executable":null}"#,
            "\n",
            r#"{"reason":"compiler-artifact","target":{"name":"other_wall","kind":["bench"]},"executable":"/t/other_wall-def"}"#,
            "\n",
            r#"{"reason":"something-new","extra":{"a":1}}"#,
            "\n",
            "not json at all\n",
        );

        let wanted = BTreeSet::from(["decode_wall"]);
        let built = super::executables_in(LINES, &wanted);
        assert_eq!(
            built,
            BTreeMap::from([(
                "decode_wall".to_owned(),
                std::path::PathBuf::from("/t/decode_wall-abc")
            )])
        );
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
}
