//! The pipeline benchmark's rig reads every line on every run from one setup.
//!
//! `pipeline_wall.rs` measures many runs from one staged corpus. A rig that
//! leaked state from one run into the next, or encoded nothing, would report
//! a fast, stable figure for a run that did no work. These tests drive the
//! rig twice from one setup at 8 objects of 512 lines, for each way the
//! objects are stored, and check what each run did on its own.

use std::time::Duration;

#[path = "../benches/support/pipeline_bench.rs"]
mod pipeline_bench;

use pipeline_bench::{Rig, Shape, Stored};

const DEADLINE: Duration = Duration::from_secs(60);

fn every_run_writes_every_line(stored: Stored) {
    let shape = Shape {
        objects: 8,
        lines: 512,
        stored,
        threads: 2,
    };
    let rig = Rig::new(shape, DEADLINE, |_| {});
    for run in 1..=2 {
        let drove = rig.make().drive();
        assert_eq!(
            drove.rows,
            shape.total_lines(),
            "{} run {run}",
            stored.name()
        );
        assert!(
            drove.bytes > 0,
            "{} run {run} encoded nothing",
            stored.name()
        );
    }
}

/// Pins that two runs over zstd objects each write every line as a row.
#[test]
fn the_rig_reads_every_zstd_line_on_every_run() {
    every_run_writes_every_line(Stored::Zstd);
}

/// Pins that two runs over uncompressed objects each write every line as a row.
#[test]
fn the_rig_reads_every_plain_line_on_every_run() {
    every_run_writes_every_line(Stored::Plain);
}
