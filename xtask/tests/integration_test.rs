//! The process-level contract of `cargo xtask integration-test`: the suites it
//! runs for the S3 lane `SPATE_S3_LANE` selects and the Kafka lane
//! `SPATE_KAFKA_LANE` selects.

use std::process::Command;

/// The nextest line `--explain integration-test` prints with the S3 lane
/// `s3` and the Kafka lane `kafka` selected.
fn nextest_line(s3: &str, kafka: &str) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args(["--explain", "integration-test"])
        .env("SPATE_S3_LANE", s3)
        .env("SPATE_KAFKA_LANE", kafka)
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

const ALL: &str = "cargo nextest run --profile docker --workspace --all-features --locked \
                   --run-ignored ignored-only";

/// A selected lane without `notifications` leaves out the notification suite,
/// and one with it runs every suite.
#[test]
fn integration_test_reads_the_selected_lanes_capabilities() {
    assert_eq!(
        nextest_line("garage", "apache"),
        format!("{ALL} -E 'not (package(spate-kafka) & binary(feed_notifications))'")
    );
    assert_eq!(nextest_line("rgw", "apache"), ALL);
}

/// The `redpanda` lane leaves out the suites only the primary broker runs.
#[test]
fn integration_test_on_redpanda_leaves_out_the_primary_broker_suites() {
    assert_eq!(
        nextest_line("rgw", "redpanda"),
        format!(
            "{ALL} -E 'not ((package(spate-kafka) & (binary(feed_notifications) \
             | binary(tls_system_ca))) | (package(spate) & binary(e2e_leader_move)))'"
        )
    );
}
