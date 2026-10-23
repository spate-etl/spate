//! A bounded feed job's end: a worker on a job with a location ends once its
//! lister run's end marker holds over a settling window of empty feed reads,
//! and ends `Stalled` when the feed's dead-letter destination then holds
//! units. A solo run on the in-process feed the runtime built ends once its
//! lister has finished and the feed reads empty. Every timed rule runs on a
//! test clock the test advances, 15 s at a time, the end check's read step.

mod support;

use spate_core::clock::tokio::TestClock;
use spate_core::feed::memory::MemoryFeed;
use spate_core::feed::{
    Confirmation, Delivery, Depth, DepthOf, DepthReading, DepthReporter, Feed, FeedError,
    FeedErrorKind, FeedWaker, Receipt, Unit, depth_reading,
};
use spate_core::pipeline::{ExitReport, ExitState};
use spate_test::{FeedScript, ScriptedFeed, scripted_feed, wait_until};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::location::{LocationHooks, location_store};
use support::{
    Launched, PipelineYaml, captured_rows, fingerprint, launch_customized, launch_with_feed,
    line_framer, lines_bytes, recs, sorted, stop, test_options, unit_over,
};

const DEADLINE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_secs(15);
/// Steps in the default five-minute window.
const STEPS: usize = 20;
const LOCATION: &str = "s3://ops/jobs/backfill-1/";
const FINGERPRINT_KEY: &str = "jobs/backfill-1/fingerprint";
const END_KEY: &str = "jobs/backfill-1/end";
const RUN_A: &str = "run=0123456789abcdef0123456789abcdef\n";
const RUN_B: &str = "run=fedcba9876543210fedcba9876543210\n";

fn data_dir(dir: &tempfile::TempDir) -> PathBuf {
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    data
}

/// A bounded worker over `data`: its section names `location` and the
/// scripted feed, with `extra` feed keys, the location store and the clock
/// injected, and the job fingerprint already written.
fn launch_bounded(
    data: &Path,
    feed: Arc<dyn Feed>,
    hooks: &LocationHooks,
    store: Arc<dyn object_store::ObjectStore>,
    clock: &Arc<TestClock>,
    extra: &str,
) -> Launched {
    hooks.seed(FINGERPRINT_KEY, fingerprint(data, "Auto", "0a").as_bytes());
    launch_unlisted(data, feed, store, clock, extra)
}

/// [`launch_bounded`] before any lister run has written the fingerprint.
fn launch_unlisted(
    data: &Path,
    feed: Arc<dyn Feed>,
    store: Arc<dyn object_store::ObjectStore>,
    clock: &Arc<TestClock>,
    extra: &str,
) -> Launched {
    let yaml = PipelineYaml::file("s3-job-end", data)
        .section(&format!(
            "feed:\n  scripted: {{}}\n  location: {LOCATION}\n{extra}"
        ))
        .build();
    let clock = Arc::clone(clock);
    launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        feed,
        move |source, _io| {
            line_framer(source)
                .with_location_store(store)
                .with_clock(clock)
        },
    )
}

/// Depth reads of `of` the end check has taken: the worker's startup also
/// asks each depth once, to refuse a feed that cannot count.
fn check_reads(script: &FeedScript, of: DepthOf) -> usize {
    script.depth_reads(of).saturating_sub(1)
}

/// Wait until the end check has taken `n` depth reads of `of`.
fn wait_for_reads(script: &FeedScript, of: DepthOf, n: usize) {
    wait_until(DEADLINE, &format!("{n} reads of {of:?}"), || {
        check_reads(script, of) >= n
    });
}

/// Advance `steps` steps of 15 s, waiting after each for the next read of
/// the feed, counted from `reads` already answered. Returns the count.
fn step_reads(clock: &TestClock, script: &FeedScript, mut reads: usize, steps: usize) -> usize {
    for _ in 0..steps {
        clock.advance(STEP);
        reads += 1;
        wait_for_reads(script, DepthOf::Feed, reads);
    }
    reads
}

fn running(l: &Launched) -> bool {
    l.run.wait_exit(Duration::ZERO).is_none()
}

fn exit(l: &Launched) -> ExitReport {
    l.run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .expect("the run starts")
}

fn failure_reason(report: &ExitReport) -> String {
    match &report.state {
        ExitState::Failed(failure) => failure.reason.clone(),
        other => panic!("expected a failed run, got {other:?}"),
    }
}

fn count(n: u64) -> Result<Depth, FeedError> {
    let mut depth = Depth::new("the scripted feed", true);
    depth.in_flight = n;
    Ok(depth)
}

/// Pins the end of a bounded job on an empty prefix: the marker, written
/// before the worker started, opens a window at the worker's first read,
/// the feed is read every 15 s, one step short of five minutes the run is
/// still going, and at five minutes it ends `Completed` with exit code 0,
/// after one closing marker read and one dead-letter read.
#[test]
fn a_worker_waits_out_a_marker_older_than_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let opened = hooks.gets();
    let reads = step_reads(&clock, &script, 1, STEPS - 1);
    assert!(running(&l), "one step short of the window");
    assert_eq!(hooks.gets(), opened, "no marker read inside the window");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);

    clock.advance(STEP);
    let report = exit(&l);
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.exit_code(), 0);
    assert_eq!(check_reads(&script, DepthOf::Feed), reads + 1);
    assert_eq!(hooks.gets(), opened + 1, "one closing marker read");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 1);
}

/// Pins that reads further apart than the end check's 20 s bound do not
/// count as one window: a clock that jumps a whole window between two reads,
/// as a paused process sees, restarts the window, and the job ends one window
/// of 15-second reads later.
#[test]
fn a_gap_longer_than_the_interval_restarts_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let opened = hooks.gets();
    clock.advance(STEP * STEPS as u32);
    wait_until(DEADLINE, "the marker is read again", || {
        hooks.gets() > opened
    });
    wait_for_reads(&script, DepthOf::Feed, 2);
    assert!(running(&l), "a gap of a whole window restarted it");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    step_reads(&clock, &script, 2, STEPS);
    assert_eq!(exit(&l).state, ExitState::Completed);
}

/// Pins that a marker another lister run wrote during the window keeps the
/// job going: at the first window's end the closing read finds run B, and
/// the job ends one window after that read.
#[test]
fn a_marker_replaced_during_the_window_does_not_end_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, 5);
    hooks.seed(END_KEY, RUN_B.as_bytes());
    let reads = step_reads(&clock, &script, reads, STEPS - 5);
    // The closing read at 300 s finds run B and opens a window on it, with a
    // read of its own at once.
    wait_for_reads(&script, DepthOf::Feed, reads + 1);
    assert!(running(&l), "a window ending on run A's marker read run B");
    let reads = step_reads(&clock, &script, reads + 1, STEPS - 1);
    assert!(running(&l), "one step short of run B's window");
    clock.advance(STEP);
    let report = exit(&l);
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(check_reads(&script, DepthOf::Feed), reads + 1);
    assert_eq!(reads + 1, 42, "21 reads in each window");
}

/// Pins that a marker deleted during the window, as a new lister run does
/// before its first publish, keeps the job going: the closing read finds
/// none, and the worker reads the marker again until one appears.
#[test]
fn a_marker_deleted_during_the_window_does_not_end_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, 5);
    hooks.remove(END_KEY);
    step_reads(&clock, &script, 6, STEPS - 5);
    let closing = hooks.gets();
    for _ in 0..3 {
        clock.advance(STEP);
        let next = hooks.gets() + 1;
        wait_until(DEADLINE, "the marker is read again", || {
            hooks.gets() >= next
        });
    }
    assert!(hooks.gets() > closing);
    assert!(running(&l), "no marker, no end");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    stop(&l);
}

/// Pins that a read finding the feed not empty, as a peer holding a unit
/// gives, restarts the window from a new marker read.
#[test]
fn a_nonzero_read_inside_the_window_restarts_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, 5);
    script.answer_depth(DepthOf::Feed, count(1));
    let reads = step_reads(&clock, &script, reads, 1);
    let restart = hooks.gets() + 1;
    clock.advance(STEP);
    wait_until(DEADLINE, "the marker is read again", || {
        hooks.gets() >= restart
    });
    wait_for_reads(&script, DepthOf::Feed, reads + 1);
    let reads = step_reads(&clock, &script, reads + 1, STEPS - 1);
    assert!(running(&l), "the window counts from the restart");
    clock.advance(STEP);
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert_eq!(check_reads(&script, DepthOf::Feed), reads + 1);
}

/// Pins that a unit the worker takes during the window restarts it: once
/// the unit is read, the next step reads the marker again, where an
/// unbroken window would read the feed.
#[test]
fn a_unit_taken_during_the_window_restarts_it() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("a", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&rows)).unwrap();
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, 1);
    let receipt = script.deliver(unit_over(&data, "a.ndjson"));
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    let gets = hooks.gets();
    clock.advance(STEP);
    wait_until(DEADLINE, "the marker is read again", || hooks.gets() > gets);
    wait_for_reads(&script, DepthOf::Feed, reads + 1);
    assert!(running(&l));
    stop(&l);
    assert_eq!(sorted(captured_rows(&l.script)), sorted(rows));
}

/// Pins that a failed read of the feed's counts ends nothing and restarts
/// the window.
#[test]
fn a_failed_count_read_does_not_end_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, 5);
    script.answer_depth(
        DepthOf::Feed,
        Err(FeedError::new(FeedErrorKind::Retryable, "throttled")),
    );
    step_reads(&clock, &script, reads, STEPS - 5);
    assert!(running(&l), "a failed read restarted the window");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    stop(&l);
}

/// Pins that a closing marker read that fails ends nothing: the worker
/// reads the marker again one interval later and waits out a new window.
#[test]
fn a_failed_marker_read_at_the_window_end_does_not_end_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, STEPS - 1);
    hooks.fail_next_gets(1);
    let failed = hooks.gets() + 1;
    let reads = step_reads(&clock, &script, reads, 1);
    wait_until(DEADLINE, "the closing marker read", || {
        hooks.gets() >= failed
    });
    assert!(running(&l), "a failed closing read ends nothing");
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    clock.advance(STEP);
    wait_for_reads(&script, DepthOf::Feed, reads + 1);
    stop(&l);
}

/// Pins that a failed dead-letter read ends the job neither `Completed` nor
/// `Stalled`, and is read again one interval later.
#[test]
fn a_failed_dead_letter_read_does_not_end_the_job_completed() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    script.answer_depth(
        DepthOf::DeadLetters,
        Err(FeedError::new(FeedErrorKind::Retryable, "throttled")),
    );
    step_reads(&clock, &script, 1, STEPS);
    wait_for_reads(&script, DepthOf::DeadLetters, 1);
    assert!(running(&l), "a failed dead-letter read ends nothing");
    clock.advance(STEP);
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 2);
}

/// Pins the stalled end at its boundary: one unit in the dead-letter
/// destination ends the job with exit code 1, the reason naming the stall,
/// the destination and its count.
#[test]
fn exactly_one_dead_letter_ends_the_job_stalled() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let poison = script.deliver(Unit::new("poison", b"x".to_vec()));
    script.dead_letter(poison);
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, STEPS);
    let report = exit(&l);
    assert_eq!(report.exit_code(), 1);
    let reason = failure_reason(&report);
    assert!(
        reason.contains("bounded feed job stalled")
            && reason.contains("the scripted dead-letter destination")
            && reason.contains("1 visible")
            && reason.contains("copy them into a re-run's feed"),
        "{reason}"
    );
}

/// Pins that a bounded job reads every unit it is given, then ends
/// `Completed` once its dead-letter destination reads empty.
#[test]
fn an_empty_dead_letter_destination_ends_the_job_completed() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rows = recs("a", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&rows)).unwrap();
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "a.ndjson"));
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = check_reads(&script, DepthOf::Feed);
    step_reads(&clock, &script, reads, STEPS);
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert_eq!(sorted(captured_rows(&l.script)), sorted(rows));
}

/// Pins that a dead-lettered unit does not end a job whose feed still holds
/// work: while a peer holds a unit the window never closes, and no
/// dead-letter read is taken.
#[test]
fn a_dead_letter_does_not_stop_a_job_with_work_left() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let poison = script.deliver(Unit::new("poison", b"x".to_vec()));
    script.dead_letter(poison);
    for _ in 0..=STEPS * 2 {
        script.answer_depth(DepthOf::Feed, count(1));
    }
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    // Each step reads the marker, opens a window and finds the feed busy.
    let mut windows = 1;
    for _ in 0..STEPS + 2 {
        let gets = hooks.gets();
        clock.advance(STEP);
        windows += 1;
        wait_for_reads(&script, DepthOf::Feed, windows);
        assert_eq!(hooks.gets(), gets + 1, "one marker read per window");
    }
    assert!(running(&l));
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    stop(&l);
}

/// Pins that a job with no location runs on over an empty feed and a
/// dead-lettered unit, past two windows, and ends only at a shutdown. This
/// holds without the end check; it guards continuous jobs against it.
#[test]
fn a_continuous_job_does_not_end_on_an_empty_feed() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    let poison = script.deliver(Unit::new("poison", b"x".to_vec()));
    script.dead_letter(poison);
    let clock = TestClock::frozen();
    let yaml = PipelineYaml::file("s3-job-end-continuous", &data)
        .section("feed:\n  scripted: {}\n")
        .build();
    let worker_clock = Arc::clone(&clock);
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed),
        move |source, _io| line_framer(source).with_clock(worker_clock),
    );
    let fresh = script.deliver(unit_over(&data, "missing.ndjson"));
    wait_until(DEADLINE, "the worker takes a unit", || {
        script.delivered().iter().any(|d| d.receipt == fresh)
    });
    clock.advance(STEP * (2 * STEPS as u32 + 2));
    let late = script.deliver(Unit::new("late", b"x".to_vec()));
    wait_until(DEADLINE, "the worker still receives", || {
        script.delivered().iter().any(|d| d.receipt == late)
    });
    assert!(running(&l));
    assert_eq!(check_reads(&script, DepthOf::Feed), 0);
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
    stop(&l);
}

/// Pins that `settling_window` sets the fingerprint wait: with `1m` and no
/// fingerprint, the worker exits at 60 s naming the window.
#[test]
fn a_settling_window_key_sets_the_fingerprint_wait() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();
    let yaml = PipelineYaml::file("s3-job-end-window", &data)
        .section(&format!(
            "feed:\n  scripted: {{}}\n  location: {LOCATION}\n  settling_window: 1m\n"
        ))
        .build();
    let worker_clock = Arc::clone(&clock);
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed),
        move |source, _io| {
            line_framer(source)
                .with_location_store(store)
                .with_clock(worker_clock)
        },
    );
    wait_until(DEADLINE, "the first fingerprint read", || hooks.gets() >= 1);
    let mut reads = 1;
    clock.advance_stepped(Duration::from_secs(55), Duration::from_secs(5), || {
        reads += 1;
        wait_until(DEADLINE, "the next fingerprint read", || {
            hooks.gets() >= reads
        });
    });
    assert!(running(&l), "five seconds short of the window");
    clock.advance(Duration::from_secs(5));
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("60s"), "{reason}");
}

/// Pins that `settling_window` sets the end check's window: with `1m` the
/// job ends after four steps of 15 s, where the default window takes twenty.
#[test]
fn a_settling_window_key_sets_the_end_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(
        &data,
        Arc::new(feed),
        &hooks,
        store,
        &clock,
        "  settling_window: 1m\n",
    );

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, 3);
    assert!(running(&l), "one step short of the window");
    clock.advance(STEP);
    assert_eq!(exit(&l).state, ExitState::Completed);
}

/// Pins that `settling_window` on a durable feed with no location, where no
/// job ends, is refused at startup naming the key.
#[test]
fn a_settling_window_without_a_location_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (feed, script) = scripted_feed();
    let yaml = PipelineYaml::file("s3-job-end-window-alone", &data)
        .section("feed:\n  scripted: {}\n  settling_window: 1m\n")
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed),
        |source, _io| line_framer(source),
    );
    let err = l
        .run
        .wait_exit(DEADLINE)
        .expect("the run exits")
        .expect_err("startup refuses the section")
        .to_string();
    assert!(
        err.contains("`settling_window`") && err.contains("`location`"),
        "{err}"
    );
    assert!(script.delivered().is_empty());
}

/// Pins that a shutdown during the window ends the run `Completed` through
/// the shutdown, and that no read of the feed follows it.
#[test]
fn a_stop_during_the_window_ends_without_draining() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    let reads = step_reads(&clock, &script, 1, 3);
    stop(&l);
    clock.advance(STEP * STEPS as u32);
    assert_eq!(check_reads(&script, DepthOf::Feed), reads);
    assert_eq!(check_reads(&script, DepthOf::DeadLetters), 0);
}

/// Pins that a depth read the feed refuses for good, such as for a missing
/// permission, fails the run naming the error, where a retryable one is read
/// again.
#[test]
fn a_fatal_depth_read_fails_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    script.answer_depth(
        DepthOf::Feed,
        Err(FeedError::new(
            FeedErrorKind::Fatal,
            "access denied to the queue",
        )),
    );
    clock.advance(STEP);
    let report = exit(&l);
    assert_eq!(report.exit_code(), 1);
    let reason = failure_reason(&report);
    assert!(reason.contains("access denied to the queue"), "{reason}");
}

/// A feed that delegates to a scripted one. It counts only what `counts`
/// names, its receives fail retryably while `throttled` is set, counted in
/// `refused`, and with
/// `pending` its dead-letter reads wait there until the test settles them.
struct Wrapped {
    inner: ScriptedFeed,
    counts: &'static [DepthOf],
    throttled: Arc<AtomicBool>,
    refused: Arc<AtomicUsize>,
    pending: Option<Arc<Mutex<Vec<DepthReporter>>>>,
}

impl Wrapped {
    fn new(inner: ScriptedFeed, counts: &'static [DepthOf]) -> Wrapped {
        Wrapped {
            inner,
            counts,
            throttled: Arc::default(),
            refused: Arc::default(),
            pending: None,
        }
    }
}

impl Feed for Wrapped {
    fn backend(&self) -> &str {
        self.inner.backend()
    }
    fn set_waker(&self, waker: FeedWaker) {
        self.inner.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        if self.throttled.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(FeedError::new(FeedErrorKind::Retryable, "throttled"));
        }
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
    fn read_depth(&self, of: DepthOf) -> Option<DepthReading> {
        if !self.counts.contains(&of) {
            return None;
        }
        if of == DepthOf::DeadLetters
            && let Some(pending) = &self.pending
        {
            let (reporter, reading) = depth_reading();
            pending.lock().unwrap().push(reporter);
            return Some(reading);
        }
        self.inner.read_depth(of)
    }
}

const BOTH: &[DepthOf] = &[DepthOf::Feed, DepthOf::DeadLetters];

/// Pins that a bounded job on a feed that reports no depth fails at its
/// start, naming the backend, before it receives a unit.
#[test]
fn a_feed_that_reports_no_depth_fails_a_bounded_job_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    let (feed, script) = scripted_feed();
    script.deliver(Unit::new("u", b"x".to_vec()));
    let clock = TestClock::frozen();
    let l = launch_bounded(
        &data,
        Arc::new(Wrapped::new(feed, &[])),
        &hooks,
        store,
        &clock,
        "",
    );
    let report = exit(&l);
    assert_eq!(report.exit_code(), 1);
    let reason = failure_reason(&report);
    assert!(
        reason.contains("`scripted` feed reports no depth"),
        "{reason}"
    );
    assert!(script.delivered().is_empty(), "nothing was received");
}

/// Pins that a feed that cannot count fails a bounded job before the
/// fingerprint wait: no lister run has written one, the clock never moves,
/// and the run still exits naming the backend.
#[test]
fn a_feed_that_reports_no_depth_fails_before_the_fingerprint_wait() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, _hooks) = location_store();
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_unlisted(&data, Arc::new(Wrapped::new(feed, &[])), store, &clock, "");
    let reason = failure_reason(&exit(&l));
    assert!(
        reason.contains("`scripted` feed reports no depth"),
        "{reason}"
    );
}

/// Pins that a feed that counts its units but not its dead letters fails a
/// bounded job at its start, before any window.
#[test]
fn a_feed_that_cannot_count_its_dead_letters_fails_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let feed = Wrapped::new(feed, &[DepthOf::Feed]);
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");
    let reason = failure_reason(&exit(&l));
    assert!(
        reason.contains("`scripted` feed reports no depth"),
        "{reason}"
    );
    assert_eq!(check_reads(&script, DepthOf::Feed), 0, "no window opened");
}

/// Pins that a dead-letter read the feed refuses for good fails the run
/// naming the dead-letter destination.
#[test]
fn a_fatal_dead_letter_read_names_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (feed, script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    script.answer_depth(
        DepthOf::DeadLetters,
        Err(FeedError::new(FeedErrorKind::Fatal, "access denied")),
    );
    step_reads(&clock, &script, 1, STEPS);
    let reason = failure_reason(&exit(&l));
    assert!(
        reason.contains("dead-letter destination") && reason.contains("access denied"),
        "{reason}"
    );
}

/// Pins that a closing receive the feed answers `Retryable` restarts the
/// check: the window's last read lags a unit that has arrived, the receive
/// fails, and the worker reads the end marker again where it would have
/// ended `Completed` over the unit.
#[test]
fn a_failed_closing_receive_restarts_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (inner, script) = scripted_feed();
    let feed = Wrapped::new(inner, BOTH);
    let throttled = Arc::clone(&feed.throttled);
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, STEPS - 1);
    // An approximate count can lag a unit that has arrived.
    script.answer_depth(DepthOf::Feed, Ok(Depth::new("the scripted feed", true)));
    throttled.store(true, Ordering::SeqCst);
    let unit = script.deliver(Unit::new("late", b"x".to_vec()));
    let gets = hooks.gets();
    clock.advance(STEP);
    wait_until(DEADLINE, "the closing marker read", || hooks.gets() > gets);
    // A restarted window reads again one step after its last read.
    clock.advance(STEP);
    let mut exited = None;
    wait_until(DEADLINE, "the run ends or reads the marker again", || {
        exited = l.run.wait_exit(Duration::ZERO);
        exited.is_some() || hooks.gets() >= gets + 2
    });
    assert!(
        exited.is_none(),
        "the job ended over an unread unit: {exited:?}"
    );
    assert!(!script.delivered().iter().any(|d| d.receipt == unit));
    stop(&l);
}

/// Pins that a unit the reader's own receive takes and fails back in the
/// same poll restarts the check: it arrives while the dead-letter read is
/// outstanding, and the worker reads the end marker again where it would
/// have ended `Completed`.
#[test]
fn a_unit_failed_back_in_the_poll_that_took_it_restarts_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (inner, script) = scripted_feed();
    let pending = Arc::new(Mutex::new(Vec::new()));
    let feed = Wrapped {
        pending: Some(Arc::clone(&pending)),
        ..Wrapped::new(inner, BOTH)
    };
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, STEPS);
    // The startup probe's reading is the first.
    wait_until(DEADLINE, "the dead-letter read starts", || {
        pending.lock().unwrap().len() == 2
    });
    let receipt = script.deliver(Unit::new("unreadable", b"not a unit".to_vec()));
    wait_until(DEADLINE, "the unit is failed back", || {
        script.failed().iter().any(|(r, _)| *r == receipt)
    });
    let gets = hooks.gets();
    for reporter in pending.lock().unwrap().drain(..) {
        reporter.report(Ok(Depth::new(
            "the scripted dead-letter destination",
            false,
        )));
    }
    // A restarted window reads again one step after its last read.
    clock.advance(STEP);
    let mut exited = None;
    wait_until(DEADLINE, "the run ends or reads the marker again", || {
        exited = l.run.wait_exit(Duration::ZERO);
        exited.is_some() || hooks.gets() > gets
    });
    assert!(
        exited.is_none(),
        "the job ended over a failed unit: {exited:?}"
    );
    stop(&l);
}

/// Pins that a receive of the reader's own that fails retryably while the
/// dead-letter read is outstanding restarts the check: past the closing
/// receive no read of the feed follows that could find the unit it missed.
#[test]
fn a_failed_receive_during_the_dead_letter_read_restarts_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.seed(END_KEY, RUN_A.as_bytes());
    let (inner, script) = scripted_feed();
    let pending = Arc::new(Mutex::new(Vec::new()));
    let feed = Wrapped {
        pending: Some(Arc::clone(&pending)),
        ..Wrapped::new(inner, BOTH)
    };
    let throttled = Arc::clone(&feed.throttled);
    let refused = Arc::clone(&feed.refused);
    let clock = TestClock::frozen();
    let l = launch_bounded(&data, Arc::new(feed), &hooks, store, &clock, "");

    wait_for_reads(&script, DepthOf::Feed, 1);
    step_reads(&clock, &script, 1, STEPS);
    // The startup probe's reading is the first.
    wait_until(DEADLINE, "the dead-letter read starts", || {
        pending.lock().unwrap().len() == 2
    });
    throttled.store(true, Ordering::SeqCst);
    let unit = script.deliver(Unit::new("late", b"x".to_vec()));
    let seen = refused.load(Ordering::SeqCst);
    wait_until(
        DEADLINE,
        "a receive fails with the unit in the feed",
        || refused.load(Ordering::SeqCst) >= seen + 2,
    );
    for reporter in pending.lock().unwrap().drain(..) {
        reporter.report(Ok(Depth::new(
            "the scripted dead-letter destination",
            false,
        )));
    }
    let gets = hooks.gets();
    // A restarted window reads again one step after its last read.
    clock.advance(STEP);
    let mut exited = None;
    wait_until(DEADLINE, "the run ends or reads the marker again", || {
        exited = l.run.wait_exit(Duration::ZERO);
        exited.is_some() || hooks.gets() > gets
    });
    assert!(
        exited.is_none(),
        "the job ended over an unread unit: {exited:?}"
    );
    assert!(!script.delivered().iter().any(|d| d.receipt == unit));
    stop(&l);
}

/// Pins that `configure_feed` takes `settling_window` on an in-process feed
/// section built in code: its refusal of a window with no location is for
/// durable feeds, and the core refuses the key on `memory`.
#[test]
fn configure_feed_takes_a_settling_window_on_the_in_process_feed() {
    use spate_core::config::{ComponentConfig, FeedSection, PipelineConfig, YamlValue};
    use spate_core::feed::FeedOrigin;
    use spate_core::source::Source as _;
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let config =
        PipelineConfig::from_str(&PipelineYaml::file("s3-memory-window", &data).build()).unwrap();
    let mut section = FeedSection::new(ComponentConfig::new(
        "memory",
        YamlValue::Mapping(Default::default()),
    ));
    section.settling_window = Some(Duration::from_secs(60));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mut source =
        spate_s3::S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap();
    source
        .configure_feed(&section, Arc::new(MemoryFeed::new()), FeedOrigin::Section)
        .expect("the S3 source leaves the in-process feed's window to the core");
}

fn launch_solo(data: &Path, extra: &str, clock: &Arc<TestClock>) -> Launched {
    let yaml = PipelineYaml::file("s3-job-end-solo", data)
        .section(&format!("feed:\n  memory: {{}}\n{extra}"))
        .build();
    let clock = Arc::clone(clock);
    launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _io| line_framer(source).with_clock(clock),
    )
}

/// Pins that a solo run on the in-process feed the runtime built ends by
/// itself once its lister has published the prefix and every unit is read,
/// with no settling window: the clock never moves, and the run exits
/// `Completed` with exit code 0 and every row once.
#[test]
fn a_solo_bounded_job_ends_with_no_settling_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let a = recs("a", 5);
    let b = recs("b", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&a)).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&b)).unwrap();
    let clock = TestClock::frozen();
    let l = launch_solo(&data, "", &clock);
    let report = exit(&l);
    assert_eq!(report.state, ExitState::Completed);
    assert_eq!(report.exit_code(), 0);
    let both: Vec<String> = a.iter().chain(&b).cloned().collect();
    assert_eq!(sorted(captured_rows(&l.script)), sorted(both));
}

/// Pins that a solo run whose unit the in-process feed dead-letters ends
/// `Stalled`, naming the in-process dead-letter list and its count.
#[test]
fn a_solo_job_with_a_dead_lettered_unit_ends_stalled() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    // Not gzip, though the name says so: every read of it fails.
    fs::write(data.join("broken.ndjson.gz"), b"not gzip").unwrap();
    let clock = TestClock::frozen();
    let l = launch_solo(&data, "  attempt_cap: 1\n", &clock);
    let report = exit(&l);
    assert_eq!(report.exit_code(), 1);
    let reason = failure_reason(&report);
    assert!(
        reason.contains("bounded feed job stalled")
            && reason.contains("the in-process dead-letter list")
            && reason.contains("1 visible")
            && reason.contains("run the job again"),
        "{reason}"
    );
}

/// Pins that a solo run given a `MemoryFeed` in code keeps running after its
/// listing ends and the feed reads empty, so a unit code publishes later is
/// still read.
#[test]
fn a_solo_run_given_a_feed_in_code_keeps_running() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let a = recs("a", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&a)).unwrap();
    let feed = MemoryFeed::new();
    let yaml = PipelineYaml::file("s3-job-end-given", &data)
        .section("feed:\n  memory: {}\n")
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed.clone()),
        |source, _io| line_framer(source),
    );
    wait_until(DEADLINE, "the listed object is read", || {
        sorted(captured_rows(&l.script)) == sorted(a.clone()) && feed.counts().acknowledged == 1
    });
    let b = recs("b", 5);
    fs::write(data.join("b.ndjson"), lines_bytes(&b)).unwrap();
    drop(feed.publish(unit_over(&data, "b.ndjson")));
    let both: Vec<String> = a.iter().chain(&b).cloned().collect();
    wait_until(DEADLINE, "the unit published later is read", || {
        sorted(captured_rows(&l.script)) == sorted(both.clone())
    });
    assert!(running(&l));
    stop(&l);
}
