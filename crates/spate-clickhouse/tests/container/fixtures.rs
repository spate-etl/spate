//! The fixtures' wait for a started server.

use super::*;

/// A server that exits while it starts fails the wait with its exit code,
/// where a server still running at the deadline would fail it as not ready.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_server_that_exits_at_start_fails_the_wait_at_once() {
    let (name, tag) = lane_image();
    let container = started_only(
        ClickHouse::default()
            .with_name(name)
            .with_tag(tag)
            .with_env_var("CLICKHOUSE_PASSWORD", SERVER_PASSWORD)
            .with_copy_to(
                "/etc/clickhouse-server/config.d/broken.xml",
                b"<clickhouse><broken>\n".to_vec(),
            ),
    )
    .start()
    .await
    .expect("start clickhouse");
    let failure = answering(&container, SERVER_PASSWORD, "clickhouse")
        .await
        .expect_err("a server with a broken config never answers");
    assert!(
        failure.starts_with("clickhouse exited with code ")
            && failure.contains(" before it was ready: "),
        "{failure}"
    );
}

/// A server that has not answered by the deadline fails the wait as not
/// ready.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_server_that_never_answers_fails_at_its_deadline() {
    let (name, tag) = lane_image();
    let container = started_only(
        ClickHouse::default()
            .with_name(name)
            .with_tag(tag)
            .with_env_var("CLICKHOUSE_PASSWORD", SERVER_PASSWORD),
    )
    .start()
    .await
    .expect("start clickhouse");
    let failure = spate_test_support::ready_async(
        "clickhouse",
        Duration::ZERO,
        || async { Err("refused".to_owned()) },
        &container,
    )
    .await;
    assert_eq!(
        failure,
        Err("clickhouse was not ready within 0ns: refused".to_owned())
    );
}
