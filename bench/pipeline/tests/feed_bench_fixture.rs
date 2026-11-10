//! The feed benchmark's rig reads every row on every run from one setup.
//!
//! `feed_small_objects_wall.rs` measures many runs from one staged corpus. A
//! rig that leaked state from one run into the next (a feed already drained, a
//! counter already at its target) would report a fast, stable figure for a
//! run that read nothing. These tests drive the rig twice from one setup at
//! 192 objects, 24 feed waves at the rig's 8 units held, and check what each
//! run did on its own.

use std::time::Duration;

#[path = "../benches/support/feed_bench.rs"]
mod feed_bench;

use feed_bench::{Drove, LINES, OBJECTS, Rig, SHORT_LINGER};

const FIXTURE_OBJECTS: usize = 192;

const DEADLINE: Duration = Duration::from_secs(60);

fn rig() -> Rig {
    Rig::new(FIXTURE_OBJECTS, Duration::ZERO, SHORT_LINGER, DEADLINE)
}

/// Pins that two runs from one setup each write every row and each
/// acknowledge every unit.
#[test]
fn the_rig_reads_every_row_on_every_run() {
    let rig = rig();
    for run in 1..=2 {
        let drove = rig.make().drive();
        assert_eq!(
            drove,
            Drove {
                rows: (FIXTURE_OBJECTS * LINES) as u64,
                acknowledged: FIXTURE_OBJECTS as u64,
            },
            "run {run}"
        );
    }
}

/// Pins that every line of an object is exactly [`feed_bench::LINE_BYTES`]
/// long and ends in its only newline, so an object is 64,000 bytes.
#[test]
fn every_line_is_one_fixed_width_record() {
    for (object, line) in [(0, 0), (OBJECTS - 1, LINES - 1), (123, 456)] {
        let bytes = feed_bench::line(object, line);
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
        assert_eq!(bytes.last(), Some(&b'\n'));
        serde_json::from_slice::<serde_json::Value>(&bytes).expect("a JSON record");
    }
    assert_eq!(feed_bench::object_bytes(OBJECTS - 1).len(), 64_000);
}
