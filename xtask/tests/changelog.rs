//! The process-level contract of `cargo xtask tidy changelog` and `cargo xtask
//! changelog new`: the exit status each outcome reports, which stream carries
//! what, and the bytes of every diagnostic.

use std::process::{Command, Output};

/// The environment variables the gate reads, cleared so nothing on the host
/// reaches the child.
const READS: [&str; 7] = [
    "EVENT_NAME",
    "BASE_SHA",
    "HEAD_SHA",
    "PR_TITLE",
    "PR_BODY",
    "GITHUB_ACTIONS",
    "GITHUB_EVENT_NAME",
];

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
         The job that runs this has to pass EVENT_NAME, BASE_SHA, HEAD_SHA, PR_TITLE and\n  \
         PR_BODY through `env:`, and its checkout needs `fetch-depth: 0`. See the\n  \
         `changelog` job in .github/workflows/ci.yml.\n",
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
/// the range holds no commits and the title is the only subject.
fn head() -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim_end().to_owned()
}

/// The environment a pull request run reads, pointed at one commit.
fn pull_request<'a>(sha: &'a str, title: &'a str, body: &'a str) -> Vec<(&'a str, &'a str)> {
    vec![
        ("EVENT_NAME", "pull_request"),
        ("BASE_SHA", sha),
        ("HEAD_SHA", sha),
        ("PR_TITLE", title),
        ("PR_BODY", body),
    ]
}

/// A title requiring a fragment with none added fails, and the failure names
/// the subject, where it came from and how to satisfy it.
#[test]
fn a_title_requiring_a_fragment_fails_with_the_guidance() {
    let sha = head();
    let title = "feat(spate-core): a windowed operator";
    held(
        &["tidy", "changelog"],
        &pull_request(&sha, title, ""),
        1,
        "",
        &format!(
            "changelog: these subject(s) say this change is visible to somebody\n  \
             upgrading, and no fragment was added under changelog.d/:\n\n    \
             {title}{} (pull request title)\n\n  \
             Add one with:\n\n      \
             cargo xtask changelog new fixed short-description\n\n  \
             and write what the change means for somebody upgrading, not what moved.\n  \
             changelog.d/README.md has the format and the conventions.\n\n  \
             If it is not user-visible, say so in the subject. There is no label and\n  \
             no opt-out checkbox for this, and .github/labels.yml says why. The exemption\n  \
             is derived from the type and scope you write:\n\n      \
             feat(spate-core): ...  ->  refactor(spate-core): ...  nothing user-facing moved\n      \
             fix(spate-core): ...   ->  test(spate-core): ...      it only touched tests\n      \
             feat(spate-core): ...  ->  feat(docs): ...            it only touched docs\n\n  \
             For a fix to a bug that was never released, put a 'Changelog: none'\n  \
             trailer on the commit.\n\n  \
             The pull request title is what lands on main, since this repository squashes\n  \
             with the title as the subject, so the title is the one that has to be right.\n",
            " ".repeat(70 - title.len())
        ),
    );
}

/// A title the classifier exempts passes, and says which range it read.
#[test]
fn an_exempt_title_passes() {
    let sha = head();
    held(
        &["tidy", "changelog"],
        &pull_request(&sha, "docs(ci): a page", ""),
        0,
        &format!("changelog: nothing in {sha}..{sha} requires a changelog fragment.\n"),
        "",
    );
}

/// A `Changelog: none` trailer in the body is taken at its word for the whole
/// pull request, because the body is what the squash commit carries.
#[test]
fn a_body_trailer_excuses_the_pull_request() {
    let sha = head();
    held(
        &["tidy", "changelog"],
        &pull_request(
            &sha,
            "feat(spate-core): a windowed operator",
            "never released\n\nChangelog: none",
        ),
        0,
        "changelog: the pull request body carries a 'Changelog: none' trailer, which\n  \
         is what the squash commit will carry. Taken at its word for this pull request.\n",
        "",
    );
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
        !std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../changelog.d/a-thing.fixed.md")
            .exists()
    );
    held(
        &["--explain", "tidy", "changelog"],
        &[],
        0,
        "(classifies this branch's subjects and reads changelog.d/)\n",
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
