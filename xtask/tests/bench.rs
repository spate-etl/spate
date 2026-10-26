//! The argv `cargo xtask bench ab` hands the wall-clock driver.
//!
//! Run under `--explain`, so nothing builds and the plan is the output.

use std::process::{Command, Output};

/// A ref the wrapper passes through without resolving it.
const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

/// The driver invocation every `bench ab` line starts with.
const DRIVER: &str = "cargo run -p spate-bench --features driver --locked --bin bench -- ab";

fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .arg("--explain")
        .args(args)
        .output()
        .unwrap()
}

/// Every flag the wrapper takes reaches the driver, `--out` included, in the
/// order the driver documents.
#[test]
fn bench_ab_forwards_every_flag() {
    let out = xtask(&[
        "bench",
        "ab",
        "--ref",
        SHA,
        "--replicates",
        "20",
        "--format",
        "markdown",
        "--package",
        "spate-avro",
        "--package",
        "spate-json",
        "--filter",
        "x",
        "--out",
        "/tmp/x",
    ]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        format!(
            "{DRIVER} {SHA} --replicates 20 --format markdown --package spate-avro \
             --package spate-json --filter x --out /tmp/x\n"
        )
    );
    assert_eq!(String::from_utf8(out.stderr).unwrap(), "");
}

/// Without `--out` the driver picks its own directory, so the wrapper passes
/// no `--out` at all.
#[test]
fn bench_ab_without_out_passes_none() {
    let out = xtask(&["bench", "ab", "--ref", SHA]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        format!("{DRIVER} {SHA} --replicates 20 --format table\n")
    );
    assert_eq!(String::from_utf8(out.stderr).unwrap(), "");
}
