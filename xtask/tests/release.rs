//! The process-level contract of `cargo xtask release`: the checks a step makes
//! before it does any work.

use std::process::{Command, Output};

/// The variables the release steps read, cleared so nothing on the host
/// reaches the child.
const READS: [&str; 8] = [
    "GH_TOKEN",
    "DISPATCH_TOKEN",
    "GITHUB_REPOSITORY",
    "CARGO_REGISTRY_TOKEN",
    "VERSION",
    "EXPECTED_SHA",
    "BUNDLE_PATH",
    "PENDING",
];

fn xtask(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_spate-xtask"));
    command.args(args);
    for name in READS {
        command.env_remove(name);
    }
    command.envs(env.iter().copied());
    command.output().unwrap()
}

#[track_caller]
fn refuses(args: &[&str], env: &[(&str, &str)], message: &str) {
    let got = xtask(args, env);
    let stderr = String::from_utf8_lossy(&got.stderr);
    assert!(stderr.contains(message), "{stderr}");
    assert!(!got.status.success(), "{stderr}");
}

/// `assemble` names a missing token before checking tools or touching the
/// tree; an empty `PATH` would fail the tool check if it ran first.
#[test]
fn assemble_needs_its_token_first() {
    refuses(
        &["release", "assemble", "--version", "0.3.0"],
        &[("PATH", "")],
        "assemble needs GH_TOKEN",
    );
}

/// `finish` names a missing token before reading the registry.
#[test]
fn finish_needs_its_token_first() {
    refuses(
        &[
            "release",
            "finish",
            "--version",
            "0.3.0",
            "--expected-sha",
            "abc",
            "--artifacts",
            "artifacts",
        ],
        &[],
        "finish needs GH_TOKEN",
    );
}
