//! The workspace's instruction-count bench targets: which ones exist, the
//! cargo invocation each takes, and the status a run reports.
//!
//! Discovery is by naming convention, `crates/<pkg>/benches/<name>_gungraun.rs`,
//! sorted bytewise so every consumer sees one order on every machine.
//!
//! [`run`] answers 0 when at least one bench ran and all succeeded, 1 when any
//! failed, and 2 when the selection matched nothing. CI's merge-base leg
//! branches on those three to decide whether it has a baseline, so the
//! trichotomy is a contract.
//!
//! [`check`] holds every discovered target to a `[[bench]]` stanza naming it
//! with `harness = false`. Without one, cargo auto-discovers the file under the
//! default libtest harness, and the bench reports 0 measured and exits 0.

use std::collections::BTreeSet;
use std::path::Path;

use crate::run::{self, Error, Outcome, Step};

/// The suffix a bench source carries to be discovered.
const SUFFIX: &str = "_gungraun.rs";

/// What a selected target does.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    /// Build and run it.
    Run,
    /// Build it and run nothing.
    Check,
}

/// One bench target: the crate owning it and the target's name.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Target {
    pkg: String,
    bench: String,
}

impl Target {
    /// The `pkg bench` line bare discovery prints, and the sort key.
    fn line(&self) -> String {
        format!("{} {}", self.pkg, self.bench)
    }
}

/// Runs or builds every discovered target the filter selects, in order.
///
/// A failing target is reported and the rest still run, so one broken bench
/// does not hide the state of the others.
pub(crate) fn run(
    root: &Path,
    explain: bool,
    mode: Mode,
    filter: &[String],
    features: &str,
) -> Outcome {
    let mut ran = 0usize;
    let mut failed = false;
    for target in discover(root) {
        if !selected(&target.pkg, filter) {
            continue;
        }
        let step = Step::new("cargo", cargo_args(mode, &target, features));
        if explain {
            println!("{}", step.display());
            ran += 1;
            continue;
        }
        // The shell form on stderr, ahead of the child's own output, so a log
        // says which target produced what follows.
        eprintln!("+ {}", step.display());
        let ok = run::succeeded(root, &step).unwrap_or_else(|e| {
            eprintln!("gungraun-benches: {}", e.message);
            false
        });
        if ok {
            ran += 1;
        } else {
            eprintln!(
                "gungraun-benches: {} --bench {} {}",
                target.pkg,
                target.bench,
                mode.failure()
            );
            failed = true;
        }
    }
    match verdict(ran, failed) {
        None => Ok(()),
        Some(code) => {
            if code == 2 {
                eprintln!("{}", no_match(filter));
            }
            Err(Error::status(code))
        }
    }
}

/// Prints one `pkg bench` line per discovered target.
pub(crate) fn print_listing(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads crates/*/benches/*{SUFFIX})");
        return Ok(());
    }
    for target in discover(root) {
        println!("{}", target.line());
    }
    Ok(())
}

/// Prints the crates owning a discovered target as a JSON array.
pub(crate) fn print_pkgs_json(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads crates/*/benches/*{SUFFIX})");
        return Ok(());
    }
    println!("{}", pkgs_json(&owners(root)));
    Ok(())
}

/// Holds every discovered target to its `harness = false` stanza.
pub(crate) fn check(root: &Path, explain: bool) -> Outcome {
    if explain {
        println!("(reads crates/*/benches/*{SUFFIX} against each crate's Cargo.toml)");
        return Ok(());
    }
    let read =
        |pkg: &str| std::fs::read_to_string(root.join("crates").join(pkg).join("Cargo.toml")).ok();
    let checked = verify(&discover(root), &read)?;
    println!("gungraun-benches: {checked} bench target(s) are declared correctly");
    Ok(())
}

/// The crates owning at least one discovered target.
pub(crate) fn owners(root: &Path) -> BTreeSet<String> {
    discover(root).into_iter().map(|t| t.pkg).collect()
}

/// Every bench target under `crates/`, sorted bytewise by its `pkg bench` line.
///
/// A tree with no `crates/` yields nothing.
fn discover(root: &Path) -> Vec<Target> {
    let mut out = Vec::new();
    for pkg in entries(&root.join("crates")) {
        for file in entries(&root.join("crates").join(&pkg).join("benches")) {
            if file.ends_with(SUFFIX) {
                out.push(Target {
                    pkg: pkg.clone(),
                    bench: file[..file.len() - ".rs".len()].to_owned(),
                });
            }
        }
    }
    out.sort_by_key(Target::line);
    out
}

/// The names in a directory, excluding dotfiles. An unreadable directory yields
/// nothing.
fn entries(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with('.'))
        .collect()
}

/// Whether the filter selects a crate. An empty filter selects every crate, and
/// a name matching none is ignored.
fn selected(pkg: &str, filter: &[String]) -> bool {
    filter.is_empty() || filter.iter().any(|w| w == pkg)
}

/// The cargo argv one target takes. An empty feature list is the crate's
/// default features and passes no flag.
fn cargo_args(mode: Mode, target: &Target, features: &str) -> Vec<String> {
    let mut args = vec!["bench".to_owned()];
    if mode == Mode::Check {
        args.push("--no-run".to_owned());
    }
    args.extend(
        ["-p", &target.pkg, "--locked", "--bench", &target.bench]
            .into_iter()
            .map(str::to_owned),
    );
    if !features.is_empty() {
        args.push("--features".to_owned());
        args.push(features.to_owned());
    }
    args
}

/// The exit status a completed pass reports, or `None` for success.
fn verdict(ran: usize, failed: bool) -> Option<i32> {
    if failed {
        return Some(1);
    }
    if ran == 0 {
        return Some(2);
    }
    None
}

/// What an empty selection reports on stderr.
fn no_match(filter: &[String]) -> String {
    if filter.is_empty() {
        return "gungraun-benches: no bench target was discovered".to_owned();
    }
    format!(
        "gungraun-benches: no bench target belongs to {}",
        filter.join(" ")
    )
}

/// The crates as a JSON array, in the order given.
fn pkgs_json(pkgs: &BTreeSet<String>) -> String {
    let quoted: Vec<String> = pkgs.iter().map(|p| format!("\"{p}\"")).collect();
    format!("[{}]", quoted.join(","))
}

/// Holds each target to its declaration, reading a crate's manifest through
/// `manifest`, and returns how many were checked.
fn verify(targets: &[Target], manifest: &dyn Fn(&str) -> Option<String>) -> Result<usize, Error> {
    if targets.is_empty() {
        return Err(Error::msg(format!(
            "no gungraun bench was discovered at all. The naming convention \
             crates/*/benches/*{SUFFIX} or the tree under it has changed, and both CI \
             legs are measuring nothing."
        )));
    }
    for target in targets {
        let pkg = &target.pkg;
        let bench = &target.bench;
        let Some(text) = manifest(pkg) else {
            return Err(Error::msg(format!(
                "crates/{pkg}/Cargo.toml not found for bench {bench}"
            )));
        };
        if !declares_target(&text, bench) {
            return Err(Error::msg(format!(
                "crates/{pkg}/benches/{bench}.rs has no '[[bench]] name = \"{bench}\"' with\n  \
                 harness = false in crates/{pkg}/Cargo.toml. Cargo would auto-discover it under\n  \
                 the default libtest harness, which reports 0 measured and exits 0."
            )));
        }
    }
    Ok(targets.len())
}

/// Whether a manifest carries a `[[bench]]` block naming `bench` with
/// `harness = false`.
///
/// Key order inside a block is free, so a block is judged at its end: the next
/// table header, or the end of the file.
fn declares_target(manifest: &str, bench: &str) -> bool {
    let mut in_bench = false;
    let mut name = String::new();
    let mut harness_false = false;
    for line in manifest.lines() {
        let trimmed = trim(line);
        if trimmed.starts_with('[') {
            if in_bench && name == bench {
                return harness_false;
            }
            in_bench = trimmed.starts_with("[[bench]]");
            name = String::new();
            harness_false = false;
            continue;
        }
        if let Some(value) = key(trimmed, "name")
            && let Some(quoted) = quoted(value)
        {
            name = quoted.to_owned();
        }
        if key(trimmed, "harness").is_some_and(|v| trim(v).starts_with("false")) {
            harness_false = true;
        }
    }
    in_bench && name == bench && harness_false
}

/// What follows `key =` on a line, where the line opens with that key.
fn key<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    trim(line.strip_prefix(key)?).strip_prefix('=')
}

/// The first double-quoted run on a line, without its quotes.
fn quoted(line: &str) -> Option<&str> {
    let rest = line.split_once('"')?.1;
    rest.split_once('"').map(|(inside, _)| inside)
}

/// Drops leading ASCII whitespace, the class the manifest scan treats as
/// indentation.
fn trim(line: &str) -> &str {
    line.trim_start_matches(|c: char| c.is_ascii_whitespace())
}

impl Mode {
    /// How a failing target is described.
    fn failure(self) -> &'static str {
        match self {
            Self::Run => "failed",
            Self::Check => "failed to build",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(pkg: &str, bench: &str) -> Target {
        Target {
            pkg: pkg.to_owned(),
            bench: bench.to_owned(),
        }
    }

    fn names(pkgs: &[&str]) -> Vec<String> {
        pkgs.iter().map(|p| (*p).to_owned()).collect()
    }

    // --- discovery ---------------------------------------------------------

    /// Discovery over the repository's own tree.
    #[test]
    fn the_tree_yields_one_target_per_bench_source() {
        let root = crate::repo_root().unwrap();
        let found = discover(&root);
        let mut expected: Vec<Target> = Vec::new();
        for entry in std::fs::read_dir(root.join("crates")).unwrap() {
            let pkg = entry.unwrap().file_name().to_string_lossy().into_owned();
            let Ok(benches) = std::fs::read_dir(root.join("crates").join(&pkg).join("benches"))
            else {
                continue;
            };
            for bench in benches {
                let name = bench.unwrap().file_name().to_string_lossy().into_owned();
                if let Some(stem) = name.strip_suffix(".rs")
                    && stem.ends_with("_gungraun")
                {
                    expected.push(target(&pkg, stem));
                }
            }
        }
        expected.sort_by_key(Target::line);
        assert_eq!(found, expected);
        assert!(!found.is_empty(), "the tree holds no gungraun bench");
    }

    /// Bytewise, so glob order and the host's collation change nothing.
    #[test]
    fn discovery_is_sorted_bytewise_by_its_printed_line() {
        let root = crate::repo_root().unwrap();
        let lines: Vec<String> = discover(&root).iter().map(Target::line).collect();
        let mut sorted = lines.clone();
        sorted.sort();
        assert_eq!(lines, sorted);
    }

    #[test]
    fn a_tree_with_no_crates_directory_discovers_nothing() {
        assert!(discover(Path::new("/nonexistent-spate-root")).is_empty());
    }

    /// The name in front of the suffix may be empty.
    #[test]
    fn the_suffix_alone_is_a_bench_source() {
        let scratch = Scratch::new("suffix-only");
        scratch.bench("spate-x", "_gungraun.rs");
        scratch.bench("spate-x", "a_gungraun.rs");
        assert_eq!(
            discover(&scratch.0),
            vec![
                target("spate-x", "_gungraun"),
                target("spate-x", "a_gungraun")
            ]
        );
    }

    #[test]
    fn a_source_without_the_suffix_is_not_discovered() {
        let scratch = Scratch::new("no-suffix");
        scratch.bench("spate-x", "throughput.rs");
        scratch.bench("spate-x", "a_gungraun.rs.bak");
        scratch.bench("spate-x", "a_gungraun");
        assert_eq!(discover(&scratch.0), vec![]);
    }

    #[test]
    fn a_dotfile_is_not_discovered() {
        let scratch = Scratch::new("dotfile");
        scratch.bench("spate-x", ".hidden_gungraun.rs");
        scratch.bench(".spate-hidden", "a_gungraun.rs");
        assert!(discover(&scratch.0).is_empty());
    }

    #[test]
    fn a_crate_without_a_benches_directory_contributes_nothing() {
        let scratch = Scratch::new("no-benches");
        std::fs::create_dir_all(scratch.0.join("crates/spate-x/src")).unwrap();
        assert!(discover(&scratch.0).is_empty());
    }

    #[test]
    fn the_owners_are_the_crates_that_own_a_target_without_repeats() {
        let scratch = Scratch::new("owners");
        scratch.bench("spate-b", "one_gungraun.rs");
        scratch.bench("spate-b", "two_gungraun.rs");
        scratch.bench("spate-a", "one_gungraun.rs");
        assert_eq!(
            owners(&scratch.0).into_iter().collect::<Vec<_>>(),
            names(&["spate-a", "spate-b"])
        );
    }

    // --- selection ---------------------------------------------------------

    #[test]
    fn an_empty_filter_selects_every_crate() {
        assert!(selected("spate-core", &[]));
    }

    #[test]
    fn a_filter_selects_the_crates_it_names_and_no_others() {
        let filter = names(&["spate-core", "spate-json"]);
        assert!(selected("spate-core", &filter));
        assert!(selected("spate-json", &filter));
        assert!(!selected("spate-s3", &filter));
    }

    /// A filter entry is compared as data, so a wildcard selects nothing.
    #[test]
    fn a_filter_entry_is_not_a_pattern() {
        assert!(!selected("spate-core", &names(&["spate-*"])));
        assert!(!selected("spate-core", &names(&["spate-cor"])));
    }

    // --- the cargo argv ----------------------------------------------------

    #[test]
    fn a_run_names_the_target_and_locks_the_graph() {
        assert_eq!(
            cargo_args(Mode::Run, &target("spate-core", "chain_gungraun"), ""),
            names(&[
                "bench",
                "-p",
                "spate-core",
                "--locked",
                "--bench",
                "chain_gungraun"
            ])
        );
    }

    #[test]
    fn a_check_builds_the_target_and_runs_nothing() {
        assert_eq!(
            cargo_args(Mode::Check, &target("spate-core", "chain_gungraun"), ""),
            names(&[
                "bench",
                "--no-run",
                "-p",
                "spate-core",
                "--locked",
                "--bench",
                "chain_gungraun"
            ])
        );
    }

    /// The feature list reaches cargo as one argument whatever it holds.
    #[test]
    fn a_feature_arm_is_forwarded_as_one_argument() {
        for features in ["simd", "simd,other", "a b"] {
            let args = cargo_args(
                Mode::Run,
                &target("spate-json", "decode_gungraun"),
                features,
            );
            assert_eq!(&args[args.len() - 2..], &["--features", features]);
        }
    }

    #[test]
    fn the_default_arm_passes_no_feature_flag() {
        for mode in [Mode::Run, Mode::Check] {
            let args = cargo_args(mode, &target("spate-json", "decode_gungraun"), "");
            assert!(!args.iter().any(|a| a == "--features"), "{args:?}");
        }
    }

    // --- the exit-status contract ------------------------------------------

    #[test]
    fn every_bench_succeeding_is_success() {
        assert_eq!(verdict(3, false), None);
    }

    #[test]
    fn a_failing_bench_is_one() {
        assert_eq!(verdict(3, true), Some(1));
        assert_eq!(verdict(0, true), Some(1));
    }

    /// An empty selection and a failure must not look alike: a merge base with
    /// no bench legitimately measures nothing, a failure means discard the
    /// measurement.
    #[test]
    fn an_empty_selection_is_two() {
        assert_eq!(verdict(0, false), Some(2));
    }

    #[test]
    fn an_empty_selection_names_the_filter_it_was_given() {
        assert_eq!(
            no_match(&names(&["spate-a", "spate-b"])),
            "gungraun-benches: no bench target belongs to spate-a spate-b"
        );
    }

    #[test]
    fn an_empty_selection_under_no_filter_reports_discovery() {
        assert_eq!(
            no_match(&[]),
            "gungraun-benches: no bench target was discovered"
        );
    }

    // --- the JSON array ----------------------------------------------------

    #[test]
    fn the_package_array_is_json() {
        assert_eq!(
            pkgs_json(&names(&["spate-avro", "spate-core"]).into_iter().collect()),
            r#"["spate-avro","spate-core"]"#
        );
    }

    #[test]
    fn an_empty_package_array_is_json_too() {
        assert_eq!(pkgs_json(&BTreeSet::new()), "[]");
    }

    // --- the declaration gate ----------------------------------------------

    const MANIFEST: &str = r#"
[package]
name = "spate-x"

[[bench]]
name = "a_gungraun"
harness = false

[[bench]]
name = "b_gungraun"
harness = false

[dependencies]
serde = "1"
"#;

    #[test]
    fn a_declared_target_is_recognised() {
        assert!(declares_target(MANIFEST, "a_gungraun"));
        assert!(declares_target(MANIFEST, "b_gungraun"));
    }

    #[test]
    fn a_target_no_block_names_is_not_declared() {
        assert!(!declares_target(MANIFEST, "c_gungraun"));
    }

    #[test]
    fn a_block_without_the_harness_key_is_not_a_declaration() {
        let text = MANIFEST.replacen("harness = false\n", "", 1);
        assert!(!declares_target(&text, "a_gungraun"));
        assert!(declares_target(&text, "b_gungraun"));
    }

    #[test]
    fn a_harness_left_on_is_not_a_declaration() {
        let text = MANIFEST.replacen("harness = false", "harness = true", 1);
        assert!(!declares_target(&text, "a_gungraun"));
    }

    /// The last block in a file is judged at the end of the file, where no
    /// table header follows it.
    #[test]
    fn the_final_block_is_judged_at_the_end_of_the_file() {
        let text = "[[bench]]\nname = \"a_gungraun\"\nharness = false\n";
        assert!(declares_target(text, "a_gungraun"));
        assert!(!declares_target(
            "[[bench]]\nname = \"a_gungraun\"\n",
            "a_gungraun"
        ));
    }

    /// A verdict reached at a block's end is not revisited by a later block.
    #[test]
    fn a_later_block_does_not_overturn_an_earlier_verdict() {
        let text = "[[bench]]\nname = \"a_gungraun\"\n\n[[bench]]\nname = \"a_gungraun\"\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    #[test]
    fn keys_may_appear_in_either_order() {
        let text = "[[bench]]\nharness = false\nname = \"a_gungraun\"\n";
        assert!(declares_target(text, "a_gungraun"));
    }

    #[test]
    fn indentation_and_spacing_around_the_keys_are_free() {
        let text = "  [[bench]]\n\tname   =   \"a_gungraun\"\n  harness\t=\tfalse\n";
        assert!(declares_target(text, "a_gungraun"));
    }

    /// A key opening with the same letters is a different key.
    #[test]
    fn a_longer_key_is_not_the_one_being_read() {
        let text = "[[bench]]\nnames = \"a_gungraun\"\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
        let text = "[[bench]]\nname = \"a_gungraun\"\nharnessed = false\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    /// A key outside a `[[bench]]` block belongs to the table it sits in.
    #[test]
    fn keys_under_another_table_are_not_a_declaration() {
        let text = "[[bench]]\nname = \"a_gungraun\"\n\n[lib]\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
        let text = "[lib]\nname = \"a_gungraun\"\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    /// The crate's own name sits in `[package]`, and every block starts from
    /// nothing.
    #[test]
    fn a_name_under_another_table_does_not_carry_into_a_block() {
        let text = "[package]\nname = \"a_gungraun\"\n\n[[bench]]\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    #[test]
    fn a_harness_under_another_table_does_not_carry_into_a_block() {
        let text = "[lib]\nharness = false\n\n[[bench]]\nname = \"a_gungraun\"\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    #[test]
    fn an_unnamed_block_declares_no_named_target() {
        let text = "[[bench]]\nharness = false\n";
        assert!(!declares_target(text, "a_gungraun"));
    }

    #[test]
    fn a_name_is_read_from_its_quotes() {
        assert_eq!(quoted(r#" "a_gungraun" # "trailing""#), Some("a_gungraun"));
        assert_eq!(quoted(r#" "" "#), Some(""));
        assert_eq!(quoted(" a_gungraun"), None);
        assert_eq!(quoted(r#" "unterminated"#), None);
    }

    #[test]
    fn a_key_is_read_up_to_its_equals_sign() {
        assert_eq!(key("name = \"x\"", "name"), Some(" \"x\""));
        assert_eq!(key("name=\"x\"", "name"), Some("\"x\""));
        assert_eq!(key("name\t= \"x\"", "name"), Some(" \"x\""));
        assert_eq!(key("names = \"x\"", "name"), None);
        assert_eq!(key("# name = \"x\"", "name"), None);
    }

    // --- the gate ----------------------------------------------------------

    /// Every manifest reads from `MANIFEST`, so a fixture declares whatever it
    /// is asked about unless a case removes the stanza.
    fn manifests(text: &str) -> impl Fn(&str) -> Option<String> + '_ {
        move |_pkg: &str| Some(text.to_owned())
    }

    #[test]
    fn the_repository_declares_every_bench_it_owns() {
        let root = crate::repo_root().unwrap();
        let targets = discover(&root);
        let read = |pkg: &str| {
            std::fs::read_to_string(root.join("crates").join(pkg).join("Cargo.toml")).ok()
        };
        assert_eq!(verify(&targets, &read).unwrap(), targets.len());
    }

    /// The gate's reason for existing: a source with no stanza fails, naming
    /// the file, the stanza and the consequence.
    #[test]
    fn a_bench_whose_stanza_was_removed_is_caught() {
        let stripped = MANIFEST.replacen("harness = false\n", "", 1);
        let targets = vec![target("spate-x", "a_gungraun")];
        let e = verify(&targets, &manifests(&stripped)).unwrap_err();
        assert_eq!(
            e.message,
            "crates/spate-x/benches/a_gungraun.rs has no '[[bench]] name = \"a_gungraun\"' with\n  \
             harness = false in crates/spate-x/Cargo.toml. Cargo would auto-discover it under\n  \
             the default libtest harness, which reports 0 measured and exits 0."
        );
    }

    #[test]
    fn a_declared_bench_passes_the_gate() {
        let targets = vec![target("spate-x", "a_gungraun")];
        assert_eq!(verify(&targets, &manifests(MANIFEST)).unwrap(), 1);
    }

    #[test]
    fn a_crate_with_no_manifest_is_caught() {
        let targets = vec![target("spate-x", "a_gungraun")];
        let e = verify(&targets, &|_| None).unwrap_err();
        assert_eq!(
            e.message,
            "crates/spate-x/Cargo.toml not found for bench a_gungraun"
        );
    }

    /// Everything the gate asserts is about discovered benches, so discovering
    /// none is the failure that hides the rest.
    #[test]
    fn discovering_nothing_is_caught() {
        let e = verify(&[], &manifests(MANIFEST)).unwrap_err();
        assert!(
            e.message
                .starts_with("no gungraun bench was discovered at all."),
            "{}",
            e.message
        );
        assert!(e.message.contains("_gungraun.rs"), "{}", e.message);
    }

    #[test]
    fn the_gate_passes_on_the_repository() {
        assert!(check(&crate::repo_root().unwrap(), false).is_ok());
    }

    #[test]
    fn the_gate_fails_on_a_tree_with_no_bench() {
        let scratch = Scratch::new("gate-empty");
        std::fs::create_dir_all(scratch.0.join("crates")).unwrap();
        let e = check(&scratch.0, false).unwrap_err();
        assert!(
            e.message
                .starts_with("no gungraun bench was discovered at all."),
            "{}",
            e.message
        );
    }

    #[test]
    fn the_gate_fails_on_an_undeclared_target() {
        let scratch = Scratch::new("gate-undeclared");
        scratch.bench("spate-x", "a_gungraun.rs");
        std::fs::write(
            scratch.0.join("crates/spate-x/Cargo.toml"),
            "[package]\nname = \"spate-x\"\n",
        )
        .unwrap();
        let e = check(&scratch.0, false).unwrap_err();
        assert!(e.message.contains("harness = false"), "{}", e.message);
    }

    #[test]
    fn the_gate_reads_nothing_under_explain() {
        assert!(check(Path::new("/nonexistent-spate-root"), true).is_ok());
    }

    #[test]
    fn a_failure_message_names_what_the_mode_did() {
        assert_eq!(Mode::Run.failure(), "failed");
        assert_eq!(Mode::Check.failure(), "failed to build");
    }

    /// A directory under the system temporary directory, removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("spate-xtask-gg-{}-{name}", std::process::id()));
            drop(std::fs::remove_dir_all(&dir));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn bench(&self, pkg: &str, file: &str) {
            let dir = self.0.join("crates").join(pkg).join("benches");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(file), "").unwrap();
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }
}
