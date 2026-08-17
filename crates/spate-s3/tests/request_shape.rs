//! Request-shape gates for a bounded backfill.
//!
//! These assert *counters*, never timings: how many LISTs a run issues, how
//! many GETs each object costs, which byte ranges those GETs ask for, and how
//! many reads the source keeps in flight. Every one of them is deterministic
//! on any machine, so a change that re-reads an object, splits a window
//! differently, or collapses read parallelism to one fetcher fails here
//! rather than showing up later as an object-storage bill.
//!
//! The metrics layer cannot stand in for the store: it counts bytes and
//! objects, not requests.

mod support;

use spate_coordination::StoreCoordinator;
use spate_coordination::store::memory::MemoryStore;
use spate_core::pipeline::ExitState;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use support::spy::{GetRecord, RangeKind, SpyOptions, StoreSpy, spying_local_store};
use support::{
    Launched, TEST_LEASE, captured_rows, launch_customized, line_framer, lines_bytes, recs,
    shared_store, sorted, test_options, test_tuning,
};

/// A bounded backfill over `data`, with the read-window knobs a test needs to
/// choose (`prefetch_bytes` is the size of one ranged GET).
///
/// `refresh_listing: false` is the default and is stated anyway: it is the
/// field these tests are about, since it makes the plan final, and a final
/// plan is never re-listed.
fn config_yaml(data: &std::path::Path, prefetch: &str, chunk: &str) -> String {
    format!(
        r#"
pipeline: {{ name: s3-request-shape, threads: 2 }}
admin: {{ listen: none }}
checkpoint: {{ interval: 100ms }}
metrics: {{ exporter: none }}
source:
  s3:
    url: "file://{data}/"
    split_target_bytes: 1MiB
    refresh_listing: false
    prefetch_bytes: {prefetch}
    chunk_bytes: {chunk}
sink: {{ capture: {{}} }}
"#,
        data = data.display(),
    )
}

/// Objects staged on disk, with the sizes the range assertions check against.
struct Staged {
    _dir: tempfile::TempDir,
    data: PathBuf,
    /// File name → byte length.
    sizes: BTreeMap<String, u64>,
    /// Every record written, for the coverage check.
    expected: Vec<String>,
}

impl Staged {
    fn names(&self) -> Vec<String> {
        self.sizes.keys().cloned().collect()
    }

    fn size(&self, name: &str) -> u64 {
        self.sizes[name]
    }
}

/// Write `objects` NDJSON objects of `records` lines each. At the 1 MiB split
/// target the planner's open-cost floor is 64 KiB per object, so these tiny
/// objects pack sixteen to a split, so the number of objects chooses the
/// number of splits and with it the read parallelism available.
fn stage(objects: usize, records: usize) -> Staged {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    let mut sizes = BTreeMap::new();
    let mut expected = Vec::new();
    for i in 0..objects {
        let name = format!("obj-{i:02}.ndjson");
        let lines = recs(&format!("o{i:02}"), records);
        let bytes = lines_bytes(&lines);
        sizes.insert(name.clone(), bytes.len() as u64);
        fs::write(data.join(&name), &bytes).unwrap();
        expected.extend(lines);
    }
    Staged {
        _dir: dir,
        data,
        sizes,
        expected,
    }
}

/// Launch one coordinated instance reading through the request spy.
fn launch_spied(
    yaml: &str,
    store: &MemoryStore,
    instance: &str,
    max_in_flight: u32,
    options: SpyOptions,
) -> (Launched, StoreSpy) {
    let (spy_store, spy) = spying_local_store(options);
    let store = store.clone();
    let mut tuning = test_tuning();
    tuning.instance_id = Some(instance.to_string());
    tuning.max_in_flight = max_in_flight;
    let launched = launch_customized(
        yaml,
        test_options(),
        |_| {},
        move |source, io| {
            let coordinator =
                StoreCoordinator::new(store, tuning, io, None).expect("coordinator builds");
            line_framer(source)
                .with_coordinator(Box::new(coordinator))
                .with_store(spy_store)
        },
    );
    (launched, spy)
}

/// Recorded GETs grouped by object, in the order each object's were issued.
fn by_object(gets: &[GetRecord]) -> BTreeMap<String, Vec<RangeKind>> {
    let mut out: BTreeMap<String, Vec<RangeKind>> = BTreeMap::new();
    for get in gets {
        out.entry(get.object().to_string())
            .or_default()
            .push(get.range);
    }
    out
}

/// Every window is bounded, and sorted by start they tile `[0, size)`
/// exactly. A repeated window, an overlapping one, and a gap all fail the
/// same contiguity walk: byte `n` is requested by exactly one GET.
fn assert_tiles_exactly(name: &str, ranges: &[RangeKind], size: u64) {
    let mut windows: Vec<(u64, u64)> = ranges
        .iter()
        .map(|range| match range {
            RangeKind::Bounded(start, end) => (*start, *end),
            other => panic!(
                "{name}: an ETag-pinned object is read as bounded windows, got {other:?} \
                 (the whole-object streaming fallback re-reads from byte zero on every retry)"
            ),
        })
        .collect();
    windows.sort_unstable();
    let mut at = 0;
    for (start, end) in &windows {
        assert!(end > start, "{name}: empty window {start}..{end}");
        assert_eq!(
            *start, at,
            "{name}: window {start}..{end} does not continue at byte {at} — an overlapping \
             or duplicated read (windows: {windows:?})"
        );
        at = *end;
    }
    assert_eq!(
        at, size,
        "{name}: windows cover {at} bytes of a {size}-byte object (windows: {windows:?})"
    );
}

#[test]
fn a_bounded_backfill_lists_once_and_reads_each_object_in_one_get() {
    // Sixteen objects fill exactly one split. Every object is far below the
    // 8 MiB read window, so one GET per object is the whole read plan.
    let staged = stage(16, 20);
    let yaml = config_yaml(&staged.data, "8MiB", "512KiB");
    let (l, spy) = launch_spied(
        &yaml,
        &shared_store(),
        "shape-one-get",
        4,
        SpyOptions::default(),
    );

    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(staged.expected.clone()),
        "the run delivers every record — request counts only mean something \
         against a complete backfill"
    );

    assert_eq!(
        spy.lists(),
        1,
        "one LIST for the whole run: the elected leader's plan. Workers read \
         descriptors, never listings"
    );

    let gets = by_object(&spy.gets());
    assert_eq!(
        gets.keys().cloned().collect::<Vec<_>>(),
        staged.names(),
        "every staged object is read, and nothing else is"
    );
    for (name, ranges) in &gets {
        assert_eq!(
            ranges.len(),
            1,
            "{name}: one GET per object ({ranges:?}) — a second GET is a request \
             the backfill pays for twice"
        );
        assert_tiles_exactly(name, ranges, staged.size(name));
    }
}

#[test]
fn windowed_reads_tile_each_object_without_overlap_or_repeat() {
    // A 4 KiB read window against ~7 KiB objects: every object spans several
    // ranged GETs, which is where duplicated or overlapping ranges could hide.
    let staged = stage(8, 400);
    let yaml = config_yaml(&staged.data, "4KiB", "1KiB");
    let (l, spy) = launch_spied(
        &yaml,
        &shared_store(),
        "shape-windows",
        4,
        SpyOptions::default(),
    );

    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(staged.expected.clone())
    );

    let gets = by_object(&spy.gets());
    assert_eq!(gets.keys().cloned().collect::<Vec<_>>(), staged.names());
    for (name, ranges) in &gets {
        // The premise of the test: without several windows per object the
        // tiling assertion below is the single-GET case again.
        assert!(
            ranges.len() > 1,
            "{name}: a {}-byte object must span several 4 KiB windows, got {ranges:?}",
            staged.size(name)
        );
        assert_tiles_exactly(name, ranges, staged.size(name));
    }
}

#[test]
fn concurrent_reads_reach_the_in_flight_budget() {
    // Read parallelism is one fetcher task per in-flight split and not a knob
    // of its own, so 64 objects (four splits) against `max_in_flight: 4` must
    // put four reads in flight together. The spy holds each GET until four are
    // in flight, so the observation is the source's parallelism and not a
    // scheduler artifact; its deadline latches the gate open, so a collapse to
    // one fetcher fails this assertion instead of hanging the run.
    let staged = stage(64, 20);
    let yaml = config_yaml(&staged.data, "8MiB", "512KiB");
    let (l, spy) = launch_spied(
        &yaml,
        &shared_store(),
        "shape-depth",
        4,
        SpyOptions {
            gate_depth: 4,
            ..SpyOptions::default()
        },
    );

    let report = l.run.wait_exit(Duration::from_secs(60)).unwrap().unwrap();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(staged.expected.clone())
    );

    assert_eq!(
        spy.peak_concurrent_gets(),
        4,
        "four in-flight splits must issue four concurrent get_opts calls"
    );
    assert_eq!(spy.lists(), 1, "still one LIST across four lanes");
    assert_eq!(
        spy.gets().len(),
        staged.names().len(),
        "one GET per object, whichever lane read it"
    );
}

#[test]
fn a_final_plan_is_never_re_listed() {
    // `refresh_listing: false` makes the plan final, and a final plan
    // disables replanning outright, so no tick is ever attempted. What this
    // test establishes is therefore that the run outlived the interval at
    // which an *open* plan would have re-listed, and still cost one LIST.
    // The injected per-GET latency is what guarantees that span: twelve
    // objects on one lane pay it twelve times over, so the elapsed lower
    // bound below holds however fast the machine is.
    let staged = stage(12, 20);
    let yaml = config_yaml(&staged.data, "8MiB", "512KiB");
    let hold = Duration::from_millis(300);
    let (l, spy) = launch_spied(
        &yaml,
        &shared_store(),
        "shape-final-plan",
        1,
        SpyOptions {
            hold,
            ..SpyOptions::default()
        },
    );

    let started = Instant::now();
    let report = l.run.wait_exit(Duration::from_secs(120)).unwrap().unwrap();
    let elapsed = started.elapsed();
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(staged.expected.clone())
    );

    // An open plan under `test_tuning` would re-list on the lease interval;
    // without spanning it, the LIST count below would pass vacuously.
    assert!(
        elapsed > TEST_LEASE * 2,
        "the run must outlive the interval an open plan re-lists at, or \
         lists == 1 proves nothing: {elapsed:?}"
    );
    assert_eq!(
        spy.lists(),
        1,
        "a final plan is never re-listed, however long the job runs"
    );
}
