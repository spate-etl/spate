//! The process-level contract of `cargo xtask mutants` short of running
//! cargo-mutants: what `--explain` prints, and the job summary a run that
//! stops early writes.

use std::process::{Command, Output};

/// A base no repository has, so a run stops before cargo-mutants starts.
const NO_BASE: &str = "spate-no-such-ref";

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

/// `--explain` prints the diff and the cargo-mutants command and runs
/// neither, so an unresolvable base is not reached.
#[test]
fn explain_prints_the_commands_and_runs_nothing() {
    let got = xtask(&["--explain", "mutants", "--since", NO_BASE], &[]);
    let stdout = String::from_utf8_lossy(&got.stdout);
    assert_eq!(got.status.code(), Some(0), "{stdout}");
    assert!(stdout.starts_with("git diff --no-ext-diff "), "{stdout}");
    assert!(
        stdout.contains(&format!("$(git merge-base {NO_BASE} HEAD)")),
        "{stdout}"
    );
    assert!(stdout.contains("cargo mutants --in-diff "), "{stdout}");
}

/// A run that stops before cargo-mutants starts writes its cause to the file
/// `$GITHUB_STEP_SUMMARY` names, and exits non-zero.
#[test]
fn an_early_stop_is_written_to_the_step_summary() {
    let path = std::env::temp_dir().join(format!("spate-mutants-summary-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let got = xtask(
        &["mutants", "--since", NO_BASE],
        &[("GITHUB_STEP_SUMMARY", path.to_str().unwrap())],
    );
    assert_eq!(
        got.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&got.stderr)
    );
    let text = std::fs::read_to_string(&path).expect("a summary");
    assert!(
        text.contains(&format!("did not finish: {NO_BASE} does not resolve")),
        "{text}"
    );
    std::fs::remove_file(&path).expect("remove summary");
}

/// With no `$GITHUB_STEP_SUMMARY`, the summary goes to stdout.
#[test]
fn a_local_run_prints_the_summary() {
    let got = xtask(&["mutants", "--since", NO_BASE], &[]);
    let stdout = String::from_utf8_lossy(&got.stdout);
    assert_eq!(got.status.code(), Some(1), "{stdout}");
    assert!(stdout.starts_with("## Mutants over the diff\n"), "{stdout}");
    assert!(
        stdout.contains(&format!("did not finish: {NO_BASE} does not resolve")),
        "{stdout}"
    );
}
