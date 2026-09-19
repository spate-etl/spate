//! The command each member of `tidy` dispatches to, read off `--explain`, and
//! the members a whole run walks.

use std::path::Path;
use std::process::{Command, Output};

fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(args)
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap()
}

/// The exit status, stdout and stderr of one run, all three held at once.
#[track_caller]
fn held(args: &[&str], code: i32, out: &str, err: &str) {
    let got = xtask(args);
    let stdout = String::from_utf8_lossy(&got.stdout);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert_eq!(stdout, out, "stdout of {args:?}");
    assert_eq!(stderr, err, "stderr of {args:?}");
    assert_eq!(got.status.code(), Some(code), "{stdout}{stderr}");
}

/// Each member of the set `tidy` runs, in that order, and the line `--explain`
/// prints for it.
fn table() -> Vec<(&'static str, String)> {
    vec![
        ("zizmor", "zizmor --persona=regular .github/\n".to_owned()),
        (
            "shellcheck",
            format!("shellcheck {}\n", shell_scripts().join(" ")),
        ),
        ("self-test", "cargo test -p spate-xtask --locked\n".to_owned()),
        (
            "adr",
            "(reads docs/adr/*.md against docs/adr/README.mdx)\n".to_owned(),
        ),
        (
            "perf-report",
            "(renders the fixtures this check carries)\n".to_owned(),
        ),
        (
            "gungraun-benches",
            "(reads crates/*/benches/*_gungraun.rs against each crate's Cargo.toml)\n".to_owned(),
        ),
        (
            "transclusions",
            "./scripts/transclude.sh --check\n".to_owned(),
        ),
        (
            "supported-versions",
            "(reads ci/clickhouse/*/Dockerfile against docs/user-guide/04-connectors/sinks/clickhouse/README.mdx)\n".to_owned(),
        ),
        (
            "release-version",
            "./scripts/release-version.sh --check\n".to_owned(),
        ),
    ]
}

/// The arguments the shellcheck member carries, which are the shell scripts
/// present rather than a list stated anywhere.
fn shell_scripts() -> Vec<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".sh"))
        .map(|name| format!("scripts/{name}"))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "{} holds no shell script", dir.display());
    names
}

/// Naming one member runs the command that member is for, and nothing else.
#[test]
fn each_member_dispatches_to_the_command_it_names() {
    for (member, line) in table() {
        held(&["--explain", "tidy", member], 0, &line, "");
    }
}

/// A whole run is every member of the table, in the table's order, so a member
/// added to the set without an entry here is caught.
#[test]
fn a_whole_run_is_its_members_in_order() {
    let lines: String = table().into_iter().map(|(_, line)| line).collect();
    held(&["--explain", "tidy"], 0, &lines, "");
}
