//! The process-level contract of `cargo xtask tidy changelog`, `cargo xtask
//! changelog new` and the two release commands: the exit status each outcome
//! reports, which stream carries what, and the bytes of every diagnostic.

use std::path::Path;
use std::process::{Command, Output};

/// The variables the gate reads, and the two its `git` invocations read,
/// cleared so nothing on the host reaches the child.
const READS: [&str; 8] = [
    "EVENT_NAME",
    "BASE_SHA",
    "HEAD_SHA",
    "PR_BODY",
    "GITHUB_ACTIONS",
    "GITHUB_EVENT_NAME",
    "GIT_DIR",
    "GIT_WORK_TREE",
];

/// What the refusal prints under the shipped paths.
const GUIDANCE: &str = "
  Add one with:

      cargo xtask changelog new fixed short-description

  and write what the change means for somebody upgrading, not what moved.
  changelog.d/README.md has the format and the conventions.

  If nobody upgrading would notice, as with a refactor, a test, a doc comment
  or a fix to a bug that was never released, put this line in the pull
  request body:

      Changelog: none

  A 'Changelog: none' trailer on a commit of the branch does the same before
  the pull request exists.
";

/// The whole refusal naming the shipped paths.
fn refusal(paths: &[&str]) -> String {
    let listed: String = paths.iter().map(|p| format!("    {p}\n")).collect();
    format!(
        "changelog: this change touches what a crate ships, and no fragment was\n  \
         added under changelog.d/:\n\n{listed}{GUIDANCE}"
    )
}

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

/// An event the pull request gate did not raise reports that it evaluated no
/// fragment requirement, and says which event that was.
#[test]
fn an_event_with_no_base_reports_that_it_evaluated_nothing() {
    held(
        &["tidy", "changelog"],
        &[("EVENT_NAME", "merge_group")],
        0,
        "changelog: changelog.d/ is present with its README; no base to compare against,\n  \
         so no fragment requirement was evaluated (EVENT_NAME='merge_group').\n",
        "",
    );
}

/// A pull request inside GitHub Actions that reached structure-only is refused,
/// because it would otherwise report success having checked nothing.
#[test]
fn a_pull_request_that_evaluated_nothing_is_refused() {
    held(
        &["tidy", "changelog"],
        &[
            ("EVENT_NAME", "push"),
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_EVENT_NAME", "pull_request"),
        ],
        1,
        "",
        "::error::xtask: running on a pull request inside GitHub Actions with no EVENT_NAME, so this\n  \
         would have checked nothing and passed.\n\n  \
         The job that runs this has to pass EVENT_NAME, BASE_SHA, HEAD_SHA and PR_BODY\n  \
         through `env:`, and its checkout needs `fetch-depth: 0`. See the `changelog`\n  \
         job in .github/workflows/ci.yml.\n",
    );
}

/// The same run outside Actions passes, so the guard turns on nothing but the
/// runner.
#[test]
fn the_same_run_outside_actions_evaluates_nothing_and_passes() {
    held(
        &["tidy", "changelog"],
        &[
            ("EVENT_NAME", "push"),
            ("GITHUB_EVENT_NAME", "pull_request"),
        ],
        0,
        "changelog: changelog.d/ is present with its README; no base to compare against,\n  \
         so no fragment requirement was evaluated (EVENT_NAME='push').\n",
        "",
    );
}

/// The guard reads the event the runner raised, so a run inside Actions that is
/// not a pull request evaluates nothing and still passes.
#[test]
fn a_run_inside_actions_that_is_not_a_pull_request_passes() {
    held(
        &["tidy", "changelog"],
        &[
            ("EVENT_NAME", "merge_group"),
            ("GITHUB_ACTIONS", "true"),
            ("GITHUB_EVENT_NAME", "merge_group"),
        ],
        0,
        "changelog: changelog.d/ is present with its README; no base to compare against,\n  \
         so no fragment requirement was evaluated (EVENT_NAME='merge_group').\n",
        "",
    );
}

/// A pull request with no merge base is a refusal, and the diagnostic names the
/// depth the checkout needs.
#[test]
fn a_pull_request_with_no_merge_base_is_refused() {
    held(
        &["tidy", "changelog"],
        &[
            ("EVENT_NAME", "pull_request"),
            ("BASE_SHA", "0000000000000000000000000000000000000000"),
            ("HEAD_SHA", "HEAD"),
        ],
        1,
        "",
        "xtask: no merge base for 0000000000000000000000000000000000000000..HEAD. \
         Does the checkout still set fetch-depth: 0?\n",
    );
    held(
        &["tidy", "changelog"],
        &[("EVENT_NAME", "pull_request")],
        1,
        "",
        "xtask: no merge base for ?..?. Does the checkout still set fetch-depth: 0?\n",
    );
}

/// The sha of the tip, which a pull request run is pointed at both ends of so
/// the range changes nothing.
fn head() -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim_end().to_owned()
}

/// A range that changes nothing passes, and says which range it read.
#[test]
fn a_range_shipping_nothing_passes() {
    let sha = head();
    held(
        &["tidy", "changelog"],
        &[
            ("EVENT_NAME", "pull_request"),
            ("BASE_SHA", &sha),
            ("HEAD_SHA", &sha),
        ],
        0,
        &format!("changelog: nothing in {sha}..{sha} changes what a crate ships.\n"),
        "",
    );
}

/// A repository of this run's own, removed with its contents. `GIT_DIR` and
/// `GIT_WORK_TREE` point the gate's `git` invocations at it, so the range it
/// reads is the one built here and no test depends on this repository's
/// history.
struct Repo {
    git_dir: String,
    work_tree: String,
}

impl Repo {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "spate-xtask-changelog-{name}.{}",
            std::process::id()
        ));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(&dir).unwrap();
        let repo = Self {
            git_dir: dir.join(".git").to_string_lossy().into_owned(),
            work_tree: dir.to_string_lossy().into_owned(),
        };
        repo.git(&["init", "--quiet", "-b", "main", "."]);
        repo.write("changelog.d/README.md", "the conventions\n");
        repo.commit("workspace: the first commit");
        repo
    }

    fn path(&self) -> &Path {
        Path::new(&self.work_tree)
    }

    fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .current_dir(self.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn write(&self, rel: &str, body: &str) {
        let path = self.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// Commits everything in the tree, answering the new commit's sha and the
    /// short form `git log` prints for it.
    fn commit(&self, message: &str) -> (String, String) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "--quiet", "--allow-empty", "-m", message]);
        (
            self.git(&["rev-parse", "HEAD"]),
            self.git(&["log", "-1", "--format=%h"]),
        )
    }
}

impl Drop for Repo {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(self.path()));
    }
}

/// The environment a pull request run reads, over a range in `repo`.
fn over<'a>(
    repo: &'a Repo,
    base: &'a str,
    head: &'a str,
    body: &'a str,
) -> Vec<(&'a str, &'a str)> {
    vec![
        ("EVENT_NAME", "pull_request"),
        ("BASE_SHA", base),
        ("HEAD_SHA", head),
        ("PR_BODY", body),
        ("GIT_DIR", &repo.git_dir),
        ("GIT_WORK_TREE", &repo.work_tree),
    ]
}

/// A change to what a crate ships with no fragment fails, and the failure
/// names the paths and how to satisfy it.
#[test]
fn a_shipped_change_with_no_fragment_fails_with_the_guidance() {
    let repo = Repo::new("a_shipped_change_with_no_fragment_fails_with_the_guidance");
    let (base, _) = repo.commit("workspace: a base");
    repo.write("crates/spate-kafka/src/revoke.rs", "pub fn revoke() {}\n");
    repo.write("crates/spate-kafka/tests/revoke.rs", "#[test] fn t() {}\n");
    let (head, _) = repo.commit("kafka: stop dropping offsets on revoke");

    held(
        &["tidy", "changelog"],
        &over(&repo, &base, &head, ""),
        1,
        "",
        &refusal(&["crates/spate-kafka/src/revoke.rs"]),
    );
}

/// A line reading `Changelog: none` anywhere in the body is taken at its word,
/// including above the template's own sections.
#[test]
fn a_body_line_excuses_the_pull_request() {
    let repo = Repo::new("a_body_line_excuses_the_pull_request");
    let (base, _) = repo.commit("workspace: a base");
    repo.write("crates/spate-core/src/window.rs", "pub struct Window;\n");
    let (head, _) = repo.commit("core: move the window");

    held(
        &["tidy", "changelog"],
        &over(
            &repo,
            &base,
            &head,
            "A refactor.\n\nChangelog: none\n\n## Checks\n\n- [x] ci\n",
        ),
        0,
        "changelog: the pull request body says 'Changelog: none'. Taken at its word.\n",
        "",
    );
}

/// A commit of the branch carrying a `Changelog: none` trailer excuses the
/// change, and the run names the commit.
#[test]
fn a_commit_trailer_excuses_the_change() {
    let repo = Repo::new("a_commit_trailer_excuses_the_change");
    let (base, _) = repo.commit("workspace: a base");
    repo.write("crates/spate-core/src/window.rs", "pub struct Window;\n");
    let (head, short) = repo.commit("core: never released\n\nChangelog: none\n");

    held(
        &["tidy", "changelog"],
        &over(&repo, &base, &head, ""),
        0,
        &format!(
            "changelog: commit {short} carries a 'Changelog: none' trailer. Taken at its word.\n"
        ),
        "",
    );
}

/// A fragment added in the range satisfies the requirement, and the run says
/// how many of each it counted.
#[test]
fn an_added_fragment_satisfies_the_requirement() {
    let repo = Repo::new("an_added_fragment_satisfies_the_requirement");
    let (base, _) = repo.commit("workspace: a base");
    repo.write("changelog.d/a-windowed-operator.added.md", "A real note.\n");
    repo.write("crates/spate-core/src/window.rs", "pub struct Window;\n");
    let (head, _) = repo.commit("core: a windowed operator");

    held(
        &["tidy", "changelog"],
        &over(&repo, &base, &head, ""),
        0,
        "changelog: 1 shipped file(s) changed, 1 fragment(s) added.\n",
        "",
    );
}

/// `changelog breaking` answers `breaking` once a committed fragment opens with
/// the marker, and `none` before.
#[test]
fn breaking_answers_from_the_committed_fragments() {
    let repo = Repo::new("breaking_answers_from_the_committed_fragments");
    repo.write("Cargo.toml", "[workspace.package]\nversion = \"0.2.0\"\n");
    repo.write("CHANGELOG.md", "# Changelog\n");
    repo.commit("workspace: a manifest");
    repo.git(&["tag", "v0.2.0"]);
    let env = [
        ("GIT_DIR", repo.git_dir.as_str()),
        ("GIT_WORK_TREE", repo.work_tree.as_str()),
    ];
    held(&["changelog", "breaking"], &env, 0, "none\n", "");

    repo.write(
        "changelog.d/moved.changed.md",
        "**Breaking:** **A move** (`spate-core`)\n",
    );
    repo.commit("core: a move");
    held(&["changelog", "breaking"], &env, 0, "breaking\n", "");
}

/// A type the Keep a Changelog six do not name is refused, and so is a slug
/// that would not make a filename.
#[test]
fn a_type_and_a_slug_are_both_checked() {
    held(
        &["--explain", "changelog", "new", "fix", "a-thing"],
        &[],
        1,
        "",
        "xtask: 'fix' is not a fragment type. The Keep a Changelog six are: \
         added changed deprecated removed fixed security\n",
    );
    held(
        &["--explain", "changelog", "new", "fixed", "BarUpper"],
        &[],
        1,
        "",
        "xtask: 'BarUpper' should be lowercase letters, digits and hyphens, starting and ending\n  \
         with one of the first two. It becomes a filename.\n",
    );
}

/// `--explain` answers with the path it would write, and writes nothing.
#[test]
fn explain_names_the_path_it_would_write() {
    held(
        &["--explain", "changelog", "new", "fixed", "a-thing"],
        &[],
        0,
        "(writes changelog.d/a-thing.fixed.md)\n",
        "",
    );
    assert!(
        !Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../changelog.d/a-thing.fixed.md")
            .exists()
    );
    held(
        &["--explain", "tidy", "changelog"],
        &[],
        0,
        "(reads the paths this branch changes and changelog.d/)\n",
        "",
    );
}

/// A rejection under `--explain` is the rejection without it.
#[test]
fn explain_refuses_what_a_write_would_refuse() {
    held(
        &["--explain", "changelog", "new", "fixed", "Nope"],
        &[],
        1,
        "",
        "xtask: 'Nope' should be lowercase letters, digits and hyphens, starting and ending\n  \
         with one of the first two. It becomes a filename.\n",
    );
}

/// This repository's own changelog, which the release notes are read out of.
fn changelog() -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("CHANGELOG.md"),
    )
    .unwrap()
}

/// The notes are the section's own bytes and nothing else, so what a release
/// body carries is what the changelog says.
#[test]
fn the_notes_are_a_verbatim_slice_of_the_changelog() {
    let got = xtask(&["changelog", "notes", "0.2.0"], &[]);
    let stdout = String::from_utf8(got.stdout).unwrap();
    assert_eq!(String::from_utf8_lossy(&got.stderr), "", "stderr");
    assert_eq!(got.status.code(), Some(0));
    assert!(changelog().contains(&stdout), "{stdout}");
    assert!(!stdout.starts_with('\n'), "{stdout}");
    assert!(
        stdout.ends_with('\n') && !stdout.ends_with("\n\n"),
        "{stdout}"
    );
    assert!(
        !stdout.split('\n').any(|line| line.starts_with("## ")),
        "{stdout}"
    );
}

/// A version the changelog does not carry is a refusal on stderr with nothing
/// on stdout, so a release body is never assembled out of a diagnostic.
#[test]
fn notes_for_a_version_that_is_not_there_writes_nothing_to_stdout() {
    held(
        &["changelog", "notes", "9.9.9"],
        &[],
        1,
        "",
        "xtask: no '## [9.9.9]' section in CHANGELOG.md. The notes read what the assembly wrote,\n  \
         so the release is assembled first.\n",
    );
}

/// `--explain` answers with what each release command would do, and leaves the
/// changelog and the fragments alone.
#[test]
fn explain_names_what_the_release_commands_would_do() {
    let before = changelog();
    held(
        &["--explain", "changelog", "build", "0.3.0"],
        &[],
        0,
        "(writes ## [0.3.0] into CHANGELOG.md and consumes changelog.d/)\n",
        "",
    );
    held(
        &["--explain", "changelog", "notes", "0.3.0"],
        &[],
        0,
        "(prints the ## [0.3.0] section of CHANGELOG.md)\n",
        "",
    );
    assert_eq!(changelog(), before);
}
