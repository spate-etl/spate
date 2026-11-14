//! The Kafka feed's report of offsets retention deleted below its group's
//! commit. The report is a log line from the feed's consumer thread, so the
//! test installs a process-wide subscriber and has a binary of its own.
//!
//! The test needs Docker and is `#[ignore]`d. Run it with:
//!
//! ```sh
//! cargo nextest run --profile docker -p spate-kafka --all-features --locked \
//!   --run-ignored ignored-only --test feed_retention
//! ```

mod support;

use spate_core::pipeline::ExitState;
use spate_test::{HANG, LogCapture, wait_until};
use spate_test_support::KAFKA_PORT;
use support::broker;
use support::delete_records_below;
use support::feed::{
    HeldUnit, STEP, WORK, committed, create_topics, group_stable, hold_for, produce_raw, recs,
    run_to_exit, stage, written,
};
use testcontainers::runners::SyncRunner;

/// Pins that a held unit lost to a rebalance after retention deleted its
/// record is reported: the next owner starts at the group's commit below the
/// log start, reads the unit published after it, and logs the range it could
/// not read.
#[test]
#[ignore = "requires Docker"]
fn a_unit_lost_to_a_rebalance_after_its_record_is_deleted_is_reported() {
    let capture = LogCapture::install_global(tracing::Level::WARN);

    let container = broker().start().expect("start kafka container");
    let port = container.get_host_port_ipv4(KAFKA_PORT).expect("port");
    let brokers = format!("127.0.0.1:{port}");
    create_topics(&brokers);
    let held = HeldUnit::start(&brokers, "deleted-lost", 5);
    let rows = recs("v", 5);
    let unit = stage(&held.data, "v.ndjson", &rows);
    produce_raw(&brokers, WORK, &[(Some(unit.id.as_bytes()), &unit.body)]);
    delete_records_below(&brokers, WORK, 0, 1);
    // A's exit rebalances the group, B's held receipt is gone, and B's next
    // assignment starts at the commit below the log start.
    assert_eq!(held.a.stop(), ExitState::Completed);
    group_stable(&brokers, &held.group, 1);
    held.b.script.release_held();
    wait_until(HANG, "B writes the later unit", || {
        let written = written(&held.b.script);
        rows.iter().all(|row| written.contains(row))
    });

    // One depth read hands the consumer thread the log start.
    hold_for(&held.b, &held.b_clock, STEP);
    let line = capture.wait_for_line(HANG, "deleted before this worker could read them");
    for field in ["topic=\"units\"", "partition=0", "from=0", "to=1"] {
        assert!(line.contains(field), "{field} in {line}");
    }
    assert_eq!(
        run_to_exit(&held.b, &held.b_clock).state,
        ExitState::Completed
    );
    assert_eq!(committed(&brokers, &held.group, WORK), Some(2));
}
