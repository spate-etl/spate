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
    let rig = Rig::new(shape, DEADLINE, |_, _| {});
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

/// The corpus digest takes each object's stored bytes after its lines, so two
/// legs that staged different bytes from the same lines do not pair.
#[test]
fn the_digest_takes_each_object_as_stored() {
    let shape = Shape {
        objects: 2,
        lines: 64,
        stored: Stored::Gzip,
        threads: 1,
    };
    let mut seen = Vec::new();
    Rig::new(shape, DEADLINE, |label, bytes| {
        seen.push((label.to_owned(), bytes.to_vec()));
    });
    let labels: Vec<&str> = seen.iter().map(|(label, _)| label.as_str()).collect();
    assert_eq!(labels, ["object", "stored", "object", "stored"]);
    for pair in seen.chunks(2) {
        assert!(pair[1].1.starts_with(&[0x1f, 0x8b]), "a gzip member");
        assert_ne!(pair[0].1, pair[1].1);
    }
}

/// Pins that two runs over zstd objects each write every line as a row.
#[test]
fn the_rig_reads_every_zstd_line_on_every_run() {
    every_run_writes_every_line(Stored::Zstd);
}

/// Pins that two runs over gzip objects each write every line as a row.
#[test]
fn the_rig_reads_every_gzip_line_on_every_run() {
    every_run_writes_every_line(Stored::Gzip);
}

/// Pins that two runs over uncompressed objects each write every line as a row.
#[test]
fn the_rig_reads_every_plain_line_on_every_run() {
    every_run_writes_every_line(Stored::Plain);
}

/// Pins that the rig reports the corpus's uncompressed size, the bytes figure
/// a case divides by its run time.
#[test]
fn the_rig_counts_the_uncompressed_bytes() {
    let shape = Shape {
        objects: 2,
        lines: 64,
        stored: Stored::Zstd,
        threads: 1,
    };
    let mut lines = Vec::new();
    for line in 0..shape.total_lines() {
        spate_test_support::events::event_line(line, &mut lines);
    }
    assert_eq!(
        Rig::new(shape, DEADLINE, |_, _| {}).bytes(),
        lines.len() as u64
    );
}

/// Pins that the rig reports the lister's packing: eight small objects share
/// one unit.
#[test]
fn the_rig_reports_the_listers_units() {
    let shape = Shape {
        objects: 8,
        lines: 512,
        stored: Stored::Plain,
        threads: 2,
    };
    assert_eq!(Rig::new(shape, DEADLINE, |_, _| {}).units(), 1);
}

/// Pins that seventeen small zstd objects, a unit of sixteen and one of one,
/// are refused: a thread takes whole units, so the case could not say how
/// evenly its threads shared them.
#[test]
#[should_panic(expected = "unequal size: [16, 1]")]
fn objects_that_pack_unevenly_are_refused() {
    let shape = Shape {
        objects: 17,
        lines: 8,
        stored: Stored::Zstd,
        threads: 1,
    };
    Rig::new(shape, DEADLINE, |_, _| {}).units();
}
