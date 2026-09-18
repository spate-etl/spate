//! The process-level contract of `cargo xtask semver-checks`: the exit status
//! each outcome reports, which stream carries what, the argv it hands `cargo`
//! and `git`, and the step output the cache key lands in.
//!
//! `curl`, `cargo`, `git`, `rustc` and `uname` are recording shims on `PATH`,
//! so nothing here reaches the network or builds a rustdoc tree. The shims are
//! shell scripts, so this binary is empty off unix.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The version `Cargo.toml` declares, which a baseline is compared against.
const TREE: &str = "0.2.0";

/// The newest release tag the `git` shim reports.
const TAG: &str = "v0.2.0";

/// One live index entry per version, newest published last.
fn index(versions: &[(&str, bool)]) -> String {
    versions
        .iter()
        .map(|(v, yanked)| format!(r#"{{"name":"spate-core","vers":"{v}","yanked":{yanked}}}"#))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

/// A directory holding the shims, their fixtures and their recordings.
struct Shim(PathBuf);

impl Shim {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("spate-xtask-semver-{}-{name}", std::process::id()));
        drop(std::fs::remove_dir_all(&dir));
        std::fs::create_dir_all(dir.join("bin")).unwrap();
        std::fs::create_dir_all(dir.join("fix")).unwrap();
        let me = Self(dir);
        for (program, body) in [
            ("curl", CURL),
            ("cargo", CARGO),
            ("git", GIT),
            ("rustc", RUSTC),
            ("uname", UNAME),
        ] {
            let path = me.0.join("bin").join(program);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        me.fixture("git.tags", &format!("{TAG}\nv0.1.0\n"));
        me
    }

    /// Writes one fixture the shims read.
    fn fixture(&self, name: &str, body: &str) -> &Self {
        std::fs::write(self.0.join("fix").join(name), body).unwrap();
        self
    }

    /// Serves one crate's index entries under a 200.
    fn published(&self, crate_name: &str, versions: &[(&str, bool)]) -> &Self {
        self.fixture(&format!("curl.{crate_name}.body"), &index(versions))
    }

    /// Serves one crate's index request a status code and an empty body.
    fn answers(&self, crate_name: &str, code: &str) -> &Self {
        self.fixture(&format!("curl.{crate_name}.body"), "");
        self.fixture(&format!("curl.{crate_name}.code"), code)
    }

    /// Every invocation of one shim so far, in order.
    fn calls(&self, program: &str) -> Vec<Vec<String>> {
        let path = self.0.join(format!("argv.{program}"));
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Vec::new();
        };
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|line| {
                line.split('\0')
                    .filter(|a| !a.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .collect()
    }

    fn step_output(&self) -> Option<String> {
        std::fs::read_to_string(self.0.join("step_output")).ok()
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

const CURL: &str = r#"#!/bin/sh
for a in "$@"; do printf '%s\0' "$a" >>"$SPATE_LOG.curl"; done
printf '\n' >>"$SPATE_LOG.curl"
last=
for a in "$@"; do last=$a; done
crate=${last##*/}
if [ -f "$SPATE_FIX/curl.$crate.fail" ]; then
  printf 'curl: (6) Could not resolve host\n' >&2
  exit 6
fi
if [ -f "$SPATE_FIX/curl.$crate.body" ]; then cat "$SPATE_FIX/curl.$crate.body"; fi
if [ -f "$SPATE_FIX/curl.$crate.code" ]; then
  printf '\n%s' "$(cat "$SPATE_FIX/curl.$crate.code")"
else
  printf '\n200'
fi
"#;

const CARGO: &str = r#"#!/bin/sh
for a in "$@"; do printf '%s\0' "$a" >>"$SPATE_LOG.cargo"; done
printf '\n' >>"$SPATE_LOG.cargo"
if [ "$2" = "--version" ]; then echo "cargo-semver-checks 0.45.2"; exit 0; fi
n=0
if [ -f "$SPATE_LOG.n" ]; then n=$(cat "$SPATE_LOG.n"); fi
n=$((n + 1))
echo "$n" >"$SPATE_LOG.n"
printf 'findings %s\n' "$n"
set -- $SPATE_CARGO_CODES
eval "code=\${$n:-0}"
exit "$code"
"#;

const GIT: &str = r#"#!/bin/sh
for a in "$@"; do printf '%s\0' "$a" >>"$SPATE_LOG.git"; done
printf '\n' >>"$SPATE_LOG.git"
case "$1" in
tag) if [ -f "$SPATE_FIX/git.tags" ]; then cat "$SPATE_FIX/git.tags"; fi; exit 0 ;;
ls-tree)
  if [ -f "$SPATE_FIX/git.lstree" ]; then cat "$SPATE_FIX/git.lstree"; fi
  if [ -f "$SPATE_FIX/git.lstree.ok" ]; then exit 0; fi
  printf 'fatal: not a tree object\n' >&2
  exit 128 ;;
log) if [ -f "$SPATE_FIX/git.log" ]; then cat "$SPATE_FIX/git.log"; fi; exit 0 ;;
esac
exit 0
"#;

const RUSTC: &str = "#!/bin/sh\necho \"rustc 1.96.0 (1159e78c4 2026-09-14)\"\n";

const UNAME: &str = "#!/bin/sh\necho TestOS\n";

/// One run of the gate with the shims ahead of everything on `PATH`.
fn xtask(shim: &Shim, args: &[&str], env: &[(&str, &str)]) -> Output {
    let path = match std::env::var_os("PATH") {
        Some(p) => {
            let mut dirs = vec![shim.0.join("bin")];
            dirs.extend(std::env::split_paths(&p));
            std::env::join_paths(dirs).unwrap()
        }
        None => shim.0.join("bin").into_os_string(),
    };
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command
        .arg("semver-checks")
        .args(args)
        .env("PATH", path)
        .env("SPATE_LOG", shim.0.join("argv"))
        .env("SPATE_FIX", shim.0.join("fix"))
        // The annotation prefix would otherwise depend on the host, and the
        // marker scan on whatever the caller's own event set.
        .env_remove("GITHUB_ACTIONS")
        .env_remove("GITHUB_OUTPUT")
        .env_remove("PR_TITLE")
        .env_remove("BASE_SHA")
        .env_remove("SPATE_CARGO_CODES");
    for (k, v) in env {
        command.env(k, v);
    }
    command.output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The packages one `cargo semver-checks` invocation names, and the release
/// type it pins.
fn group(call: &[String]) -> (Vec<&str>, Option<&str>) {
    let pkgs = call
        .windows(2)
        .filter(|w| w[0] == "--package")
        .map(|w| w[1].as_str())
        .collect();
    let rt = call
        .windows(2)
        .find(|w| w[0] == "--release-type")
        .map(|w| w[1].as_str());
    (pkgs, rt)
}

// ── Which baseline a crate is compared against ─────────────────────────

/// A registry serving the tree's own version runs the crate with no release
/// type.
#[test]
fn a_baseline_matching_the_tree_carries_no_release_type() {
    let shim = Shim::new("plain");
    shim.published("spate-core", &[("0.1.0", false), (TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "semver-checks: checking 1 crate(s) against their published baseline\n\
         findings 1\n\
         semver-checks: 1 crate(s) hold their published API surface.\n"
    );
    let calls = shim.calls("cargo");
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(group(&calls[0]), (vec!["spate-core"], None));
}

/// A registry behind the tree pins the expectation to a minor, so the
/// major-breaking lints keep running between a release merge and its publish.
#[test]
fn a_baseline_behind_the_tree_is_pinned_to_a_minor() {
    let shim = Shim::new("pinned");
    shim.published("spate-core", &[("0.1.0", false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let calls = shim.calls("cargo");
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(group(&calls[0]), (vec!["spate-core"], Some("minor")));
}

/// The two groups are two invocations, the unpinned one first, and every
/// package of a group rides one invocation.
#[test]
fn each_group_is_one_invocation() {
    let shim = Shim::new("groups");
    shim.published("spate-core", &[(TREE, false)]);
    shim.published("spate-s3", &[("0.1.0", false)]);
    shim.published("spate-json", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &[
            "--against-registry",
            "--packages",
            "spate-core spate-s3 spate-json",
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let calls = shim.calls("cargo");
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(group(&calls[0]), (vec!["spate-core", "spate-json"], None));
    assert_eq!(group(&calls[1]), (vec!["spate-s3"], Some("minor")));
    assert_eq!(
        stdout(&out)
            .lines()
            .filter(|l| l.starts_with("semver-checks: checking"))
            .collect::<Vec<_>>(),
        [
            "semver-checks: checking 2 crate(s) against their published baseline",
            "semver-checks: checking 1 crate(s) against their published baseline"
        ]
    );
}

/// The index is ordered by publish time, so the last live entry is the
/// baseline. A higher version published earlier does not win, and the run that
/// would compare against it pins a release type this one does not.
#[test]
fn the_baseline_is_the_last_live_entry_in_publish_order() {
    let shim = Shim::new("publish-order");
    shim.published("spate-core", &[("0.9.0", false), (TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(group(&shim.calls("cargo")[0]).1, None);
}

/// A yanked newest version is not a baseline, so the live version before it is.
#[test]
fn a_yanked_version_is_not_a_baseline() {
    let shim = Shim::new("yanked");
    shim.published("spate-core", &[(TREE, false), ("0.3.0", true)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(group(&shim.calls("cargo")[0]).1, None);
}

/// A crate whose every version is yanked has nothing to diff against, so it
/// counts as skipped and no comparison runs.
#[test]
fn a_crate_with_every_version_yanked_is_skipped() {
    let shim = Shim::new("all-yanked");
    shim.published("spate-core", &[("0.1.0", true), (TREE, true)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "::notice::every published version of spate-core is yanked; nothing to diff against.\n\
         semver-checks: every crate was skipped for want of a baseline; the check is silent\n  \
         until the first release publishes one.\n"
    );
    assert!(shim.calls("cargo").is_empty());
}

/// A crate the registry does not know is skipped, and the run still passes.
#[test]
fn a_crate_with_no_published_baseline_is_skipped() {
    let shim = Shim::new("unpublished");
    shim.answers("spate-core", "404");
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        stdout(&out).starts_with(
            "::notice::spate-core has no published baseline, so there is nothing to diff against.\n  \
             Its name is claimed by hand at the first release including it; see RELEASING.md.\n"
        ),
        "{}",
        stdout(&out)
    );
    assert!(shim.calls("cargo").is_empty());
}

/// Only 200 and 404 are answers, so any other code fails before the run can
/// claim a pass it did not evaluate.
#[test]
fn an_index_answer_that_is_neither_200_nor_404_fails() {
    for code in ["403", "500", "000"] {
        let shim = Shim::new(&format!("code-{code}"));
        shim.answers("spate-core", code);
        let out = xtask(
            &shim,
            &["--against-registry", "--packages", "spate-core"],
            &[],
        );
        assert_eq!(out.status.code(), Some(1), "{code}");
        assert_eq!(
            stderr(&out),
            format!(
                "xtask: the sparse index answered {code} for spate-core; refusing to report a pass it cannot evaluate\n"
            )
        );
        assert!(shim.calls("cargo").is_empty(), "{code}");
    }
}

/// A transport failure fails the run, since nothing was learned about the
/// published surface.
#[test]
fn an_index_request_that_fails_outright_fails_the_run() {
    let shim = Shim::new("transport");
    shim.fixture("curl.spate-core.fail", "");
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).ends_with(
            "xtask: the index request for spate-core failed outright; the check cannot evaluate\n"
        ),
        "{}",
        stderr(&out)
    );
    assert!(shim.calls("cargo").is_empty());
}

/// The index request names the crate's path in the sparse index, identifies
/// itself, retries, and bounds its own wait. The tag listing is ordered newest
/// first, since the first line is taken as the release.
#[test]
fn the_children_carry_the_arguments_the_gate_depends_on() {
    let shim = Shim::new("argv");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        shim.calls("curl"),
        [[
            "-sS",
            "--retry",
            "3",
            "--max-time",
            "30",
            "-w",
            r"\n%{http_code}",
            "-H",
            "User-Agent: spate-release (github.com/spate-etl/spate)",
            "https://index.crates.io/sp/at/spate-core",
        ]]
    );
    let tag = shim
        .calls("git")
        .into_iter()
        .find(|c| c.first().is_some_and(|a| a == "tag"))
        .unwrap();
    assert_eq!(tag, ["tag", "--list", "v[0-9]*", "--sort=-v:refname"]);
}

// ── Reading the tool's verdict ─────────────────────────────────────────

/// A broken batch re-runs one crate at a time to name which crates broke, and
/// the re-run carries the group's own release type.
#[test]
fn a_broken_batch_is_attributed_crate_by_crate() {
    let shim = Shim::new("attribute");
    shim.published("spate-core", &[("0.1.0", false)]);
    shim.published("spate-s3", &[("0.1.0", false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core spate-s3"],
        &[
            ("SPATE_CARGO_CODES", "100 0 100"),
            ("PR_TITLE", "feat!: an announced break"),
        ],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let calls = shim.calls("cargo");
    assert_eq!(calls.len(), 3, "{calls:?}");
    assert_eq!(
        group(&calls[0]),
        (vec!["spate-core", "spate-s3"], Some("minor"))
    );
    assert_eq!(group(&calls[1]), (vec!["spate-core"], Some("minor")));
    assert_eq!(group(&calls[2]), (vec!["spate-s3"], Some("minor")));
    assert!(
        stdout(&out).contains("semver-checks: breaking against the registry: spate-s3\n"),
        "{}",
        stdout(&out)
    );
}

/// The attribution run discards its output, since the batch already reported
/// the findings.
#[test]
fn an_attribution_run_reports_only_its_verdict() {
    let shim = Shim::new("attribute-quiet");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[
            ("SPATE_CARGO_CODES", "100 100"),
            ("PR_TITLE", "feat!: an announced break"),
        ],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out).matches("findings ").count(),
        1,
        "{}",
        stdout(&out)
    );
}

/// A batch that reports no verdict is the tool failing to complete, so the run
/// fails whatever the code.
#[test]
fn a_batch_without_a_verdict_fails_the_run() {
    for code in ["1", "101", "137"] {
        let shim = Shim::new(&format!("batch-{code}"));
        shim.published("spate-core", &[(TREE, false)]);
        let out = xtask(
            &shim,
            &["--against-registry", "--packages", "spate-core"],
            &[("SPATE_CARGO_CODES", code)],
        );
        assert_eq!(out.status.code(), Some(1), "{code}");
        assert_eq!(
            stderr(&out),
            format!(
                "xtask: cargo semver-checks exited {code} without a verdict. That is the\n  \
                 tool failing to complete, not an API judgement; read its output above.\n"
            ),
            "{code}"
        );
    }
}

/// An error while attributing fails the run, so a break is never reported
/// against a set the tool could not finish judging.
#[test]
fn an_error_while_attributing_fails_the_run() {
    let shim = Shim::new("attribute-error");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[("SPATE_CARGO_CODES", "100 101")],
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "xtask: cargo semver-checks exited 101 for spate-core while attributing a break.\n  \
         Read its output above; the batch already reported the findings.\n"
    );
}

// ── Removals ───────────────────────────────────────────────────────────

/// A crate the last tag published and the tree no longer holds is breaking,
/// whatever the tool says about what remains.
#[test]
fn a_crate_the_tree_lost_is_breaking() {
    let shim = Shim::new("removed");
    shim.published("spate-core", &[(TREE, false)]);
    shim.fixture("git.lstree", "spate-core\nspate-ghost\n");
    shim.fixture("git.lstree.ok", "");
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[("PR_TITLE", "feat!: drop a crate")],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        stdout(&out)
            .contains("semver-checks: breaking against the registry: spate-ghost(removed)\n"),
        "{}",
        stdout(&out)
    );
    let ls = shim
        .calls("git")
        .into_iter()
        .find(|c| c.first().is_some_and(|a| a == "ls-tree"))
        .unwrap();
    assert_eq!(ls, ["ls-tree", "--name-only", &format!("{TAG}:crates")]);
}

/// A tag that names no tree leaves nothing removed, so a clean run stays clean.
#[test]
fn a_tag_naming_no_crate_tree_reports_no_removal() {
    let shim = Shim::new("no-tree");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(stderr(&out), "");
    assert!(
        stdout(&out).ends_with("semver-checks: 1 crate(s) hold their published API surface.\n"),
        "{}",
        stdout(&out)
    );
}

/// The listing is read whatever status `git` reports, so a partial one still
/// names the crates it did print.
#[test]
fn a_partial_crate_listing_is_read() {
    let shim = Shim::new("partial-tree");
    shim.published("spate-core", &[(TREE, false)]);
    shim.fixture("git.lstree", "spate-ghost\n");
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[("PR_TITLE", "feat!: drop a crate")],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        stdout(&out)
            .contains("semver-checks: breaking against the registry: spate-ghost(removed)\n"),
        "{}",
        stdout(&out)
    );
    // `git`'s own diagnostic stays off the gate's stderr.
    assert_eq!(stderr(&out), "");
}

// ── The marker excuse ──────────────────────────────────────────────────

/// The title alone announces this pull request's own break, and the log is not
/// read once it does.
#[test]
fn the_title_marker_excuses_a_break() {
    let shim = Shim::new("title-marker");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[
            ("SPATE_CARGO_CODES", "100 100"),
            ("PR_TITLE", "fix(spate-core)!: drop a method"),
        ],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        stdout(&out).ends_with(
            "semver-checks: breaking against the registry: spate-core\n  \
             The title carries the marker; the next release derives as a minor.\n"
        ),
        "{}",
        stdout(&out)
    );
    assert!(
        !shim
            .calls("git")
            .iter()
            .any(|c| c.first().is_some_and(|a| a == "log")),
        "the log was scanned although the title announced the break"
    );
}

/// A marker anywhere in the log since the last tag excuses a break, and the
/// scan ends at the base tip. Scanning to `HEAD` would read this branch's own
/// subjects, which squash into body lines the release derivation reads as
/// plain.
#[test]
fn a_marker_since_the_last_tag_excuses_a_break() {
    let shim = Shim::new("log-marker");
    shim.published("spate-core", &[(TREE, false)]);
    shim.fixture(
        "git.log",
        "chore: a subject\n\nfeat(spate)!: an earlier break\n",
    );
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[
            ("SPATE_CARGO_CODES", "100 100"),
            ("PR_TITLE", "fix(spate-core): drop a method"),
            ("BASE_SHA", "0123456789ab"),
        ],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        stdout(&out).ends_with(&format!(
            "  A marker since {TAG} already announces it; the next release derives as a minor.\n"
        )),
        "{}",
        stdout(&out)
    );
    let log = shim
        .calls("git")
        .into_iter()
        .find(|c| c.first().is_some_and(|a| a == "log"))
        .unwrap();
    assert_eq!(
        log,
        [
            "log",
            "--no-merges",
            "--format=%B",
            &format!("{TAG}..0123456789ab")
        ]
    );
}

/// With no base tip the scan runs to `HEAD`, so a run off a pull request still
/// reads a range.
#[test]
fn an_absent_base_tip_scans_to_head() {
    for base in [None, Some("")] {
        let shim = Shim::new("head-scan");
        shim.published("spate-core", &[(TREE, false)]);
        let mut env = vec![("SPATE_CARGO_CODES", "100 100")];
        if let Some(base) = base {
            env.push(("BASE_SHA", base));
        }
        let out = xtask(
            &shim,
            &["--against-registry", "--packages", "spate-core"],
            &env,
        );
        assert_eq!(out.status.code(), Some(1), "{base:?}");
        let log = shim
            .calls("git")
            .into_iter()
            .find(|c| c.first().is_some_and(|a| a == "log"))
            .unwrap();
        assert_eq!(log[3], format!("{TAG}..HEAD"), "{base:?}");
    }
}

/// A break nobody announced fails, and both lines reach stdout as workflow
/// annotations.
#[test]
fn a_break_with_no_marker_anywhere_fails() {
    let shim = Shim::new("no-marker");
    shim.published("spate-core", &[(TREE, false)]);
    shim.fixture("git.log", "chore: nothing announced\n");
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[
            ("SPATE_CARGO_CODES", "100 100"),
            ("PR_TITLE", "fix(spate-core): drop a method"),
        ],
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(stderr(&out), "");
    assert!(
        stdout(&out).ends_with(&format!(
            "::error::Breaking against the registry with no marker since {TAG}: spate-core\n\
             ::error::The release derivation reads the log for the marker, so this break would \
             under-bump the next version. Land a commit carrying `!` in its subject and a \
             changelog fragment opening with **Breaking:**.\n"
        )),
        "{}",
        stdout(&out)
    );
}

/// A title is matched against and never evaluated, so shell syntax inside one
/// is text.
#[test]
fn a_title_is_matched_and_never_evaluated() {
    let shim = Shim::new("title-text");
    shim.published("spate-core", &[(TREE, false)]);
    shim.fixture("git.log", "chore: nothing announced\n");
    let canary = shim.0.join("canary");
    let title = format!("$(touch {}) `id` feat!: x", canary.display());
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[("SPATE_CARGO_CODES", "100 100"), ("PR_TITLE", &title)],
    );
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(!canary.exists(), "the title was evaluated");
}

/// A break with no release tag leaves nothing to scan, so the run fails.
#[test]
fn a_break_with_no_tag_to_scan_fails() {
    let shim = Shim::new("no-tag-break");
    shim.fixture("git.tags", "");
    shim.published("spate-core", &[(TREE, false)]);
    let out = xtask(
        &shim,
        &["--against-registry", "--packages", "spate-core"],
        &[("SPATE_CARGO_CODES", "100 100")],
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "xtask: breaking changes found but no vX.Y.Z tag to scan for their markers\n"
    );
}

// ── The selection ──────────────────────────────────────────────────────

/// A value that names no crate directory is a wiring fault in the caller, so
/// the gate fails before it reaches the index.
#[test]
fn an_unusable_package_list_is_rejected() {
    for list in [
        "",
        " ",
        "spate-*",
        "../etc",
        "crates/spate",
        "spate;rm -rf /",
        "spate ghost-crate",
    ] {
        let shim = Shim::new("reject");
        let out = xtask(&shim, &["--against-registry", "--packages", list], &[]);
        assert_eq!(out.status.code(), Some(1), "{list:?}");
        assert_eq!(
            stderr(&out),
            format!(
                "xtask: --packages needs a space-separated list of crate directory names under crates/,\n  \
                 and got '{list}'. An empty or unrecognized value is a wiring fault in the\n  \
                 caller, and passing it would check the wrong set or nothing at all.\n"
            ),
            "{list:?}"
        );
        assert!(shim.calls("curl").is_empty(), "{list:?}");
    }
}

/// A selection naming no crate at all evaluated nothing, so it must not report
/// a pass.
#[test]
fn a_selection_naming_no_crate_reports_that_nothing_was_evaluated() {
    let shim = Shim::new("empty-selection");
    let out = xtask(&shim, &["--against-registry", "--packages", "\t"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "xtask: the selection named no crate to check, so the check evaluated nothing\n"
    );
    assert!(shim.calls("cargo").is_empty());
}

/// Naming no mode reports the usage line.
#[test]
fn an_invocation_naming_no_mode_reports_the_usage_line() {
    let shim = Shim::new("no-mode");
    let out = xtask(&shim, &[], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "xtask: usage: cargo xtask semver-checks --against-registry [--packages \"a b\"] | --cache-key\n"
    );
}

// ── The baseline cache key ─────────────────────────────────────────────

/// The key the cache restores under is a step output, since the workflow reads
/// it by name. It reaches stderr either way, so a log says what was keyed on.
#[test]
fn the_cache_key_is_a_step_output_when_a_runner_names_one() {
    let shim = Shim::new("cache-key-output");
    let file = shim.0.join("step_output");
    // A step earlier in the job has already written to it.
    std::fs::write(&file, "earlier=kept\n").unwrap();
    let out = xtask(
        &shim,
        &["--cache-key"],
        &[("GITHUB_OUTPUT", &file.display().to_string())],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let key = format!(
        "semver-baseline-TestOS-{TAG}-rustc-1.96.0--1159e78c4-2026-09-14--cargo-semver-checks-0.45.2"
    );
    assert_eq!(stderr(&out), format!("semver-checks: {key}\n"));
    assert_eq!(stdout(&out), "");
    assert_eq!(
        shim.step_output().as_deref(),
        Some(&*format!("earlier=kept\nkey={key}\n"))
    );
}

/// Off a runner the key goes to stdout, so a contributor can read it.
#[test]
fn the_cache_key_goes_to_stdout_when_no_runner_names_a_file() {
    for value in [None, Some("")] {
        let shim = Shim::new("cache-key-stdout");
        let env: Vec<(&str, &str)> = value
            .map(|v| vec![("GITHUB_OUTPUT", v)])
            .unwrap_or_default();
        let out = xtask(&shim, &["--cache-key"], &env);
        assert_eq!(out.status.code(), Some(0), "{value:?}: {}", stderr(&out));
        let key = format!(
            "semver-baseline-TestOS-{TAG}-rustc-1.96.0--1159e78c4-2026-09-14--cargo-semver-checks-0.45.2"
        );
        assert_eq!(stdout(&out), format!("{key}\n"), "{value:?}");
        assert_eq!(stderr(&out), format!("semver-checks: {key}\n"), "{value:?}");
        assert_eq!(shim.step_output(), None, "{value:?}");
    }
}

/// With no release tag there is no published baseline to key on, so the key is
/// an error and the cache step restores nothing.
#[test]
fn a_tree_with_no_release_tag_has_no_cache_key() {
    let shim = Shim::new("cache-key-no-tag");
    shim.fixture("git.tags", "");
    let out = xtask(&shim, &["--cache-key"], &[]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "xtask: no vX.Y.Z tag, so there is no published baseline to key on\n"
    );
    assert_eq!(stdout(&out), "");
}

/// The workflow reads the key by the name this writes, so the two are held
/// together.
#[test]
fn the_workflow_reads_the_step_output_this_writes() {
    let workflow = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(".github/workflows/ci.yml"),
    )
    .unwrap();
    let readers = workflow
        .lines()
        .filter(|l| l.contains("steps.baseline-key.outputs."))
        .count();
    assert!(readers > 0, "no job restores the baseline cache");
    assert_eq!(
        workflow
            .lines()
            .filter(|l| l.contains("steps.baseline-key.outputs.key"))
            .count(),
        readers,
        "a job reads a step output this command does not write"
    );
}

/// On a runner a failure carries the annotation prefix, so a swallowed step
/// still shows up.
#[test]
fn a_failure_on_a_runner_carries_the_annotation_prefix() {
    let shim = Shim::new("annotation");
    shim.fixture("git.tags", "");
    let out = xtask(&shim, &["--cache-key"], &[("GITHUB_ACTIONS", "true")]);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        stderr(&out),
        "::error::xtask: no vX.Y.Z tag, so there is no published baseline to key on\n"
    );
}

// ── Explaining ─────────────────────────────────────────────────────────

/// `--explain` names what each mode would read and spawns nothing.
#[test]
fn explain_names_what_each_mode_reads() {
    let shim = Shim::new("explain");
    let out = xtask(
        &shim,
        &[
            "--against-registry",
            "--packages",
            "spate-core spate-s3",
            "--explain",
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "(reads the sparse index for 2 crate(s), then runs cargo semver-checks)\n"
    );
    assert!(shim.calls("curl").is_empty());
    assert!(shim.calls("cargo").is_empty());

    let out = xtask(&shim, &["--cache-key", "--explain"], &[]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "(reads the newest release tag, and the rustc and cargo-semver-checks versions)\n"
    );
}
