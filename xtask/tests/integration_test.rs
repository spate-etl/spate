//! The process-level contract of `cargo xtask integration-test`: the suites it
//! runs for the S3 lane `SPATE_S3_LANE` selects.

use std::process::Command;

/// The nextest line `--explain integration-test` prints with `lane` selected.
fn nextest_line(lane: &str) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["--explain", "integration-test"])
        .env("SPATE_S3_LANE", lane)
        .env_remove("GITHUB_ACTIONS")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .find(|line| line.starts_with("cargo nextest run"))
        .unwrap()
        .to_owned()
}

/// A selected lane without `notifications` leaves out the notification suite,
/// and one with it runs every suite.
#[test]
fn integration_test_reads_the_selected_lanes_capabilities() {
    let all = "cargo nextest run --profile docker --workspace --all-features --locked \
               --run-ignored ignored-only";
    assert_eq!(
        nextest_line("garage"),
        format!("{all} -E 'not (package(spate-kafka) & binary(feed_notifications))'")
    );
    assert_eq!(nextest_line("rgw"), all);
}
