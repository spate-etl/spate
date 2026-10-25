//! The process-level contract of `cargo xtask deny`: the cargo-deny command
//! each form runs, and what the advisories check reports and exits with, read
//! from cargo-deny output recorded under `fixtures/deny/`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const CARGO_DENY: &str = "cargo deny --all-features --locked check";
const ADVISORIES: &str = "cargo deny --format json --color never --all-features --locked check advisories --hide-inclusion-graph";

fn xtask(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command
        .args(args)
        .env_remove("GITHUB_STEP_SUMMARY")
        .env_remove("GITHUB_ACTIONS");
    for (name, value) in env {
        command.env(name, value);
    }
    command.output().unwrap()
}

/// `--explain` prints each form's cargo-deny command and runs nothing. Only
/// `--dependency-change false` lets a finding pass, and an empty value blocks
/// as a dropped workflow input reads.
#[test]
fn deny_runs_each_check_it_names() {
    for (args, line) in [
        (&["deny"][..], format!("{CARGO_DENY} all")),
        (
            &["deny", "policy"],
            format!("{CARGO_DENY} bans licenses sources"),
        ),
        (
            &["deny", "advisories", "--dependency-change", "false"],
            format!("{ADVISORIES} --warn denied"),
        ),
        (
            &["deny", "advisories", "--dependency-change", "true"],
            ADVISORIES.to_owned(),
        ),
        (
            &["deny", "advisories", "--dependency-change", ""],
            ADVISORIES.to_owned(),
        ),
        (&["deny", "advisories"], ADVISORIES.to_owned()),
    ] {
        let got = xtask(&[&["--explain"][..], args].concat(), &[]);
        assert_eq!(got.status.code(), Some(0), "{args:?}");
        assert_eq!(
            String::from_utf8_lossy(&got.stdout),
            format!("{line}\n"),
            "{args:?}"
        );
    }
}

/// `--dependency-change` names the advisories check's rule and is refused
/// beside any other.
#[test]
fn the_dependency_change_belongs_to_the_advisories_check() {
    let got = xtask(
        &[
            "--explain",
            "deny",
            "policy",
            "--dependency-change",
            "false",
        ],
        &[],
    );
    assert_eq!(got.status.code(), Some(1));
    assert_eq!(String::from_utf8_lossy(&got.stdout), "");
}

/// The local bar runs the blocking half first and reports an advisory without
/// failing on it.
#[test]
fn the_local_bar_runs_every_deny_check() {
    let got = xtask(&["--explain", "ci"], &[]);
    assert_eq!(got.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&got.stdout);
    let deny: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("cargo deny"))
        .collect();
    assert_eq!(
        deny,
        [
            format!("{CARGO_DENY} bans licenses sources"),
            format!("{ADVISORIES} --warn denied"),
        ]
    );
}

/// A scratch directory holding a `cargo` that records its arguments, writes
/// a recorded cargo-deny stderr and exits as cargo-deny did.
struct Shim {
    dir: PathBuf,
}

impl Shim {
    fn new(name: &str, fixture: &str, code: i32) -> Self {
        let dir = std::env::temp_dir().join(format!("spate-deny-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/deny")
            .join(fixture);
        let argv = dir.join("argv");
        // Anything but cargo-deny exits 97, so a second cargo call cannot pass
        // as the recorded one.
        let script = format!(
            "#!/bin/sh\n\
             [ \"$1\" = deny ] || {{ echo \"unexpected: cargo $*\" >&2; exit 97; }}\n\
             printf '%s\\n' \"$@\" > '{}'\n\
             cat '{}' >&2\n\
             exit {code}\n",
            argv.display(),
            fixture.display()
        );
        let cargo = dir.join("cargo");
        std::fs::write(&cargo, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self { dir }
    }

    /// Runs `deny advisories --dependency-change <value>` against the shim,
    /// with the job summary in the scratch directory.
    fn run(&self, value: &str) -> (Output, Option<String>) {
        let path = std::env::join_paths(std::iter::once(self.dir.clone()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .unwrap();
        let summary = self.dir.join("summary.md");
        let got = xtask(
            &["deny", "advisories", "--dependency-change", value],
            &[
                ("PATH", path.to_str().unwrap()),
                ("GITHUB_STEP_SUMMARY", summary.to_str().unwrap()),
            ],
        );
        (got, std::fs::read_to_string(summary).ok())
    }

    fn argv(&self) -> String {
        std::fs::read_to_string(self.dir.join("argv")).expect("cargo ran")
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

const SMALLVEC: &str = "```\n\
    vulnerability: Buffer overflow in SmallVec::insert_many\n  \
    smallvec 1.6.0 registry+https://github.com/rust-lang/crates.io-index\n  \
    RUSTSEC-2021-0003\n\
    ```\n";

/// On a change that leaves the dependency inputs alone, an advisory reaches
/// the job summary and the run passes.
#[cfg(unix)]
#[test]
fn a_report_only_advisory_reaches_the_summary_and_passes() {
    let shim = Shim::new("reported", "reported.stderr", 0);
    let (got, summary) = shim.run("false");
    assert_eq!(
        got.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&got.stderr)
    );
    assert_eq!(
        summary.as_deref(),
        Some(
            format!(
                "## Advisories\n\n\
                 This change leaves the dependency inputs alone, so no finding fails it. \
                 The nightly scan raises them against `main`.\n\n{SMALLVEC}"
            )
            .as_str()
        )
    );
    assert_eq!(
        String::from_utf8_lossy(&got.stdout),
        "::warning::1 advisory finding(s), listed in the job summary\n"
    );
    assert_eq!(
        shim.argv(),
        format!("{ADVISORIES} --warn denied\n")
            .strip_prefix("cargo ")
            .unwrap()
            .replace(' ', "\n")
    );
}

/// On a change to the dependency inputs, an advisory fails the run with
/// cargo-deny's exit code and still reaches the summary.
#[cfg(unix)]
#[test]
fn a_dependency_change_fails_on_an_advisory() {
    let shim = Shim::new("blocked", "blocked.stderr", 1);
    let (got, summary) = shim.run("true");
    assert_eq!(got.status.code(), Some(1));
    assert_eq!(
        summary.as_deref(),
        Some(
            format!(
                "## Advisories\n\n\
                 This change touches the dependency inputs, so each finding fails it.\n\n{SMALLVEC}"
            )
            .as_str()
        )
    );
    assert_eq!(
        shim.argv(),
        format!("{ADVISORIES}\n")
            .strip_prefix("cargo ")
            .unwrap()
            .replace(' ', "\n")
    );
}

/// A cargo-deny that cannot run fails a report-only run, says why on stderr
/// and writes no summary. Exit 0 with no summary line fails too.
#[cfg(unix)]
#[test]
fn a_tool_error_fails_a_report_only_run() {
    let shim = Shim::new("config", "config-error.stderr", 1);
    let (got, summary) = shim.run("false");
    assert_eq!(got.status.code(), Some(1));
    assert_eq!(summary, None);
    assert_eq!(
        String::from_utf8_lossy(&got.stderr),
        "ERROR: failed to parse config from 'deny.toml': unexpected eof encountered\n"
    );

    let shim = Shim::new("silent", "config-error.stderr", 0);
    let (got, summary) = shim.run("false");
    assert_eq!(got.status.code(), Some(1));
    assert_eq!(summary, None);
    assert!(
        String::from_utf8_lossy(&got.stderr).ends_with("cargo-deny did not report its summary\n"),
        "{}",
        String::from_utf8_lossy(&got.stderr)
    );
}

/// A run that finds nothing passes silently and writes no summary.
#[cfg(unix)]
#[test]
fn a_clean_run_passes_and_writes_nothing() {
    let shim = Shim::new("clean", "clean.stderr", 0);
    let (got, summary) = shim.run("false");
    assert_eq!(got.status.code(), Some(0));
    assert_eq!(summary, None);
    assert_eq!(String::from_utf8_lossy(&got.stdout), "");
    assert_eq!(String::from_utf8_lossy(&got.stderr), "");
}
