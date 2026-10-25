//! Request-shape gates for a bounded backfill on the in-process feed.
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

use spate_core::feed::memory::MemoryFeed;
use spate_core::feed::{Confirmation, Delivery, Feed, FeedError, FeedWaker, Receipt, Unit};
use spate_core::pipeline::ExitState;
use spate_test::ScriptedFeed;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use support::spy::{GetRecord, RangeKind, SpyOptions, StoreSpy, spying_local_store};
use support::{
    Launched, PipelineYaml, captured_rows, fingerprint, launch_customized, launch_with_feed,
    line_framer, lines_bytes, recs, sorted, stop, test_options, unit_over,
};

/// A bounded backfill over `data` on the in-process feed holding
/// `units_held` units, with the read-window knobs a test needs to choose
/// (`prefetch_bytes` is the size of one ranged GET).
fn config_yaml(data: &std::path::Path, prefetch: &str, chunk: &str, units_held: u32) -> String {
    PipelineYaml::file("s3-request-shape", data)
        .source("prefetch_bytes", prefetch)
        .source("chunk_bytes", chunk)
        .section(&format!(
            "feed:\n  memory: {{}}\n  units_held: {units_held}\n"
        ))
        .build()
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

/// Write `objects` NDJSON objects of `records` lines each. Each is one unit, so
/// the number of objects chooses the read parallelism available.
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

/// Launch one instance reading through the request spy.
fn launch_spied(yaml: &str, options: SpyOptions) -> (Launched, StoreSpy) {
    let (spy_store, spy) = spying_local_store(options);
    let launched = launch_customized(
        yaml,
        test_options(),
        |_| {},
        move |source, _| line_framer(source).with_store(spy_store),
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
    // Every object is far below the 8 MiB read window, so one GET per object
    // is the whole read plan.
    let staged = stage(16, 20);
    let yaml = config_yaml(&staged.data, "8MiB", "512KiB", 4);
    let (l, spy) = launch_spied(&yaml, SpyOptions::default());

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
        "one LIST for the whole run: the solo lister's. Readers take units, \
         never listings"
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
    let yaml = config_yaml(&staged.data, "4KiB", "1KiB", 4);
    let (l, spy) = launch_spied(&yaml, SpyOptions::default());

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
    // Read parallelism is one fetcher task per held unit and not a knob of its
    // own, so 64 units against `units_held: 4` must put four reads in flight
    // together. The spy holds each GET until four are
    // in flight, so the observation is the source's parallelism and not a
    // scheduler artifact; its deadline latches the gate open, so a collapse to
    // one fetcher fails this assertion instead of hanging the run.
    let staged = stage(64, 20);
    let yaml = config_yaml(&staged.data, "8MiB", "512KiB", 4);
    let (l, spy) = launch_spied(
        &yaml,
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
        "four held units must issue four concurrent get_opts calls"
    );
    assert_eq!(spy.lists(), 1, "still one LIST across four lanes");
    assert_eq!(
        spy.gets().len(),
        staged.names().len(),
        "one GET per object, whichever lane read it"
    );
}

/// A `feed:` section's `units_held` sets the feed path's read parallelism:
/// `units_held: 12` over sixteen one-object units puts twelve reads in
/// flight, below the reader's default of 96, and the solo run on the
/// in-process feed lists the prefix once.
#[test]
fn the_feed_section_sets_read_parallelism() {
    let staged = stage(16, 20);
    let yaml = PipelineYaml::file("s3-request-shape-feed", &staged.data)
        .section("feed:\n  memory: {}\n  units_held: 12\n")
        .build();
    let (spy_store, spy) = spying_local_store(SpyOptions {
        gate_depth: 12,
        ..SpyOptions::default()
    });
    let feed = MemoryFeed::new();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed.clone()),
        move |source, _| line_framer(source).with_store(spy_store),
    );

    spate_test::wait_until(
        Duration::from_secs(60),
        "every unit is acknowledged",
        || feed.counts().acknowledged == 16,
    );
    stop(&l);
    assert_eq!(
        sorted(captured_rows(&l.script)),
        sorted(staged.expected.clone())
    );
    assert_eq!(
        spy.peak_concurrent_gets(),
        12,
        "twelve units held must issue twelve concurrent get_opts calls"
    );
    assert_eq!(spy.lists(), 1, "the solo run lists the prefix once");
}

/// A scripted feed that reports the backend `tag`, standing in for a
/// durable feed the default lane does not run.
struct Tagged {
    inner: ScriptedFeed,
    tag: &'static str,
}

impl Feed for Tagged {
    fn backend(&self) -> &str {
        self.tag
    }
    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        self.inner.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.acknowledge(receipt)
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.inner.fail(receipt, reason)
    }
    fn publish(&self, unit: Unit) -> Confirmation {
        self.inner.publish(unit)
    }

    fn read_depth(&self, of: spate_core::feed::DepthOf) -> Option<spate_core::feed::DepthReading> {
        self.inner.read_depth(of)
    }
    fn hands_back(&self) -> bool {
        self.inner.hands_back()
    }
    fn stop_receiving(&self) {
        self.inner.stop_receiving();
    }
    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.inner.hand_back(receipt)
    }
    fn close(&self, within: std::time::Duration) -> Option<Confirmation> {
        self.inner.close(within)
    }
}

/// A worker on a durable feed, continuous or bounded, reads the units
/// delivered to it and never lists.
#[test]
fn a_worker_on_a_durable_feed_issues_no_listing() {
    for (tag, bounded) in [("sqs", false), ("kafka", true)] {
        let staged = stage(1, 20);
        let jobs = staged._dir.path().join("jobs");
        let mut section = format!("feed:\n  {tag}: {{}}\n");
        if bounded {
            fs::create_dir_all(&jobs).unwrap();
            fs::write(
                jobs.join("fingerprint"),
                fingerprint(&staged.data, "Auto", "0a"),
            )
            .unwrap();
            section.push_str(&format!("  location: file://{}/\n", jobs.display()));
        }
        let yaml = PipelineYaml::file("s3-request-shape-worker", &staged.data)
            .section(&section)
            .build();
        let (spy_store, spy) = spying_local_store(SpyOptions::default());
        let (inner, script) = spate_test::scripted_feed();
        let receipt = script.deliver(unit_over(&staged.data, &staged.names()[0]));
        let l = launch_with_feed(
            &yaml,
            test_options(),
            |_| {},
            Arc::new(Tagged { inner, tag }),
            move |source, _| line_framer(source).with_store(spy_store),
        );
        spate_test::wait_until(Duration::from_secs(30), "the unit is acknowledged", || {
            script.acknowledged() == vec![receipt]
        });
        stop(&l);
        assert_eq!(
            sorted(captured_rows(&l.script)),
            sorted(staged.expected.clone()),
            "{tag}"
        );
        assert_eq!(spy.lists(), 0, "a worker on {tag} never lists");
    }
}
