//! The process-level contract of `cargo xtask ci-changes`: the event each run
//! classifies under, the note it prints when a diff cannot be resolved, the
//! bytes it appends to `$GITHUB_OUTPUT`, and the keys `ci.yml` reads back.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The variables the classifier and its `git` invocations read, cleared so
/// nothing on the host reaches the child.
const READS: [&str; 10] = [
    "EVENT_NAME",
    "BASE_SHA",
    "HEAD_SHA",
    "MERGE_BASE_SHA",
    "MERGE_HEAD_SHA",
    "EVENT_BEFORE",
    "PR_AUTHOR",
    "PR_LABELS",
    "GITHUB_OUTPUT",
    "GITHUB_ACTIONS",
];

/// The keys the workflow reads, in the order the block writes them.
const KEYS: [&str; 11] = [
    "rust",
    "site",
    "fuzz",
    "containers",
    "container-args",
    "semver",
    "semver-pkgs",
    "bench",
    "manifests",
    "bench-shards",
    "clickhouse-lanes",
];

/// The block one documentation page selects. Nothing here builds or tests
/// Rust.
const SITE_ONLY: &str = "rust=false\n\
     site=true\n\
     fuzz=false\n\
     containers=false\n\
     container-args=\n\
     semver=false\n\
     semver-pkgs=\n\
     bench=false\n\
     manifests=false\n\
     bench-shards=[]\n\
     clickhouse-lanes=[]\n";

fn xtask(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command.args(args);
    for name in READS {
        command.env_remove(name);
    }
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().unwrap()
}

/// The exit status, stdout and stderr of one run, all three held at once.
#[track_caller]
fn held(args: &[&str], env: &[(&str, &str)], code: i32, out: &str, err: &str) {
    let got = xtask(args, env);
    let stdout = String::from_utf8_lossy(&got.stdout);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert_eq!(stdout, out, "stdout");
    assert_eq!(stderr, err, "stderr");
    assert_eq!(got.status.code(), Some(code), "{stdout}{stderr}");
}

/// One key's value in a classification block.
#[track_caller]
fn field<'a>(block: &'a str, key: &str) -> &'a str {
    block
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("no '{key}=' line in:\n{block}"))
}

/// The block a run with no diff to reason about writes, held to the keys the
/// workflow reads and to the jobs it turns on.
fn force_all() -> String {
    let got = xtask(&["ci-changes"], &[("EVENT_NAME", "workflow_dispatch")]);
    assert_eq!(String::from_utf8_lossy(&got.stderr), "", "stderr");
    assert_eq!(got.status.code(), Some(0));
    let block = String::from_utf8(got.stdout).unwrap();
    let keys: Vec<&str> = block
        .lines()
        .map(|line| line.split_once('=').expect("key=value").0)
        .collect();
    assert_eq!(keys, KEYS);
    assert_eq!(field(&block, "rust"), "true");
    assert_eq!(field(&block, "manifests"), "true");
    block
}

/// A pull request whose merge base cannot be resolved says so and classifies
/// as the event that runs everything.
#[test]
fn an_unresolvable_diff_reports_the_fallback_and_runs_everything() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "pull_request")],
        0,
        &format!("note: no usable diff; running everything.\n{}", force_all()),
        "",
    );
}

/// A push with no `before..HEAD` to read says so and leaves the manifest gate
/// on. Nothing else in a push run checks the manifests.
#[test]
fn a_push_with_no_before_fails_the_manifest_gate_closed() {
    held(
        &["ci-changes"],
        &[("EVENT_NAME", "push")],
        0,
        &format!(
            "note: no usable before..HEAD diff; the manifest gate fails closed.\n{}",
            force_all()
        ),
        "",
    );
}

/// A path list is classified as a pull request whatever the environment names,
/// so a push run over a documentation page selects the site alone.
#[test]
fn a_path_list_classifies_as_a_pull_request_under_any_event() {
    let dir = Dir::new("a_path_list_classifies_as_a_pull_request_under_any_event");
    let list = dir.write("paths", b"docs/a.md\0");
    held(
        &["ci-changes", "--classify-paths", &list],
        &[("EVENT_NAME", "push")],
        0,
        SITE_ONLY,
        "",
    );
}

/// A path list that cannot be read names the file and the reason, and writes
/// no block a job could act on.
#[test]
fn a_path_list_that_cannot_be_read_is_refused() {
    let dir = Dir::new("a_path_list_that_cannot_be_read_is_refused");
    let missing = dir.path().join("absent").to_string_lossy().into_owned();
    held(
        &["ci-changes", "--classify-paths", &missing],
        &[("EVENT_NAME", "push")],
        1,
        "",
        &format!("xtask: {missing}: No such file or directory (os error 2)\n"),
    );
}

/// `$GITHUB_OUTPUT` receives the block stdout carried, appended to what the
/// steps before this one wrote.
#[test]
fn the_github_output_file_gains_the_block_that_was_printed() {
    let dir = Dir::new("the_github_output_file_gains_the_block_that_was_printed");
    let list = dir.write("paths", b"docs/a.md\0");
    let earlier = "an-earlier-step=value\n";
    let output = dir.write("github-output", earlier.as_bytes());

    held(
        &["ci-changes", "--classify-paths", &list],
        &[("EVENT_NAME", "push"), ("GITHUB_OUTPUT", &output)],
        0,
        SITE_ONLY,
        "",
    );
    assert_eq!(
        std::fs::read_to_string(&output).unwrap(),
        format!("{earlier}{SITE_ONLY}")
    );
}

/// The `changes` outputs `ci.yml` reads and the keys the classifier writes are
/// the same set. A job gated on a key nothing writes compares the empty string
/// to `'true'` and skips every run; a key nothing reads selects nothing.
///
/// The key runs to the end of its identifier, `_` and digits included, so a
/// `bench_shards` typo fails here rather than truncating to a `bench` that is
/// in `KEYS`.
#[test]
fn the_workflow_and_the_classifier_agree_on_the_output_keys() {
    let workflow = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join(".github/workflows/ci.yml");
    let text = std::fs::read_to_string(&workflow).unwrap();

    let mut seen: Vec<&str> = Vec::new();
    for prefix in ["needs.changes.outputs.", "steps.classify.outputs."] {
        for (at, _) in text.match_indices(prefix) {
            let rest = &text[at + prefix.len()..];
            let end = rest
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
                .unwrap_or(rest.len());
            let key = &rest[..end];
            assert!(
                KEYS.contains(&key),
                "{prefix}{key} is read by ci.yml and written by nothing"
            );
            seen.push(key);
        }
    }
    assert!(!seen.is_empty(), "ci.yml reads no classifier output");
    for key in KEYS {
        assert!(
            seen.contains(&key),
            "the classifier writes {key} and ci.yml reads it nowhere"
        );
    }
}

/// A directory of this run's own, removed with its contents.
struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-ci-changes-{name}.{}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Writes one file, answering the path to give the child.
    fn write(&self, name: &str, body: &[u8]) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, body).unwrap();
        path.to_string_lossy().into_owned()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}
