//! A bounded feed job's location: a worker compares the lister's job
//! fingerprint before it receives a unit, and the one-pass lister writes the
//! fingerprint, deletes an earlier end marker, publishes the listing and
//! writes the end marker. A solo run on the in-process feed runs the lister
//! itself. Every timed rule runs on a test clock the test advances.

mod support;

use spate_core::checkpoint::Checkpointer;
use spate_core::clock::TestClock as StopClock;
use spate_core::clock::tokio::{Clock, Sleep, TestClock};
use spate_core::config::{ConfigError, FeedSection, PipelineConfig};
use spate_core::feed::memory::MemoryFeed;
use spate_core::feed::{Confirmation, Delivery, Feed, FeedError, FeedWaker, Receipt, Unit};
use spate_core::pipeline::{ExitReport, ExitState};
use spate_core::source::{Source, SourceCtx, SourceEvent};
use spate_s3::{S3Source, SplitDescriptor, split_id_for, split_id_for_range};
use spate_test::{ScriptedFeed, WriteOutcome, scripted_feed, wait_until};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::location::{LocationHooks, OnCreate, location_store};
use support::spy::{SpyOptions, spying_local_store};
use support::{
    Launched, PipelineYaml, captured_rows, fingerprint, launch_customized, launch_with_feed,
    launch_with_feed_on_clock, line_framer, lines_bytes, recs, sorted, stage_two_ranges, stop,
    test_options, unit_over,
};

const DEADLINE: Duration = Duration::from_secs(30);
const WINDOW: Duration = Duration::from_secs(5 * 60);
const POLL: Duration = Duration::from_secs(5);
const LOCATION: &str = "s3://ops/jobs/backfill-1/";
const FINGERPRINT_KEY: &str = "jobs/backfill-1/fingerprint";
const CHECKPOINT_KEY: &str = "jobs/backfill-1/checkpoint";

fn data_dir(dir: &tempfile::TempDir) -> PathBuf {
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    data
}

/// A worker over `data` on a scripted feed whose section names `location`,
/// with the location store and clock injected.
fn launch_worker(
    data: &Path,
    feed: ScriptedFeed,
    location: Option<&str>,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    clock: Option<Arc<TestClock>>,
) -> Launched {
    launch_worker_on(data, feed, location, store, clock, None)
}

/// [`launch_worker`] with the runtime's stop timed on `stop_clock` when
/// given.
fn launch_worker_on(
    data: &Path,
    feed: ScriptedFeed,
    location: Option<&str>,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    clock: Option<Arc<TestClock>>,
    stop_clock: Option<Arc<StopClock>>,
) -> Launched {
    let mut section = "feed:\n  scripted: {}\n".to_owned();
    if let Some(location) = location {
        section.push_str(&format!("  location: {location}\n"));
    }
    let yaml = PipelineYaml::file("s3-job-location", data)
        .section(&section)
        .build();
    let make_source = move |source, _io| {
        let mut source = line_framer(source);
        if let Some(store) = store {
            source = source.with_location_store(store);
        }
        if let Some(clock) = clock {
            source = source.with_clock(clock);
        }
        source
    };
    let feed = Arc::new(feed);
    match stop_clock {
        Some(stop_clock) => {
            launch_with_feed_on_clock(&yaml, test_options(), |_| {}, feed, stop_clock, make_source)
        }
        None => launch_with_feed(&yaml, test_options(), |_| {}, feed, make_source),
    }
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

/// Wait until the location store has answered `n` reads in all.
fn wait_for_gets(hooks: &LocationHooks, n: usize) {
    wait_until(DEADLINE, &format!("{n} location reads"), || {
        hooks.gets() >= n
    });
}

/// Pins the end-to-end refusal through a pipeline file: a worker whose
/// fingerprint differs from the lister's in compression fails startup with
/// exit code 1, naming the location and both fingerprints, before it
/// receives a unit. The location is a `file://` prefix the source builds a
/// store for itself.
#[test]
fn a_worker_with_another_fingerprint_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let jobs = dir.path().join("jobs").join("a");
    fs::create_dir_all(&jobs).unwrap();
    let theirs = fingerprint(&data, "Zstd", "0a");
    fs::write(jobs.join("fingerprint"), &theirs).unwrap();
    let location = format!("file://{}/", jobs.display());
    let (feed, script) = scripted_feed();
    script.deliver(unit_over(&data, "a.ndjson"));

    let l = launch_worker(&data, feed, Some(&location), None, None);
    let report = exit(&l);
    assert_eq!(report.exit_code(), 1);
    let reason = failure_reason(&report);
    assert!(reason.contains(&location), "{reason}");
    assert!(reason.contains(&theirs), "{reason}");
    assert!(
        reason.contains(&fingerprint(&data, "Auto", "0a")),
        "{reason}"
    );
    assert!(script.delivered().is_empty(), "nothing was received");
}

/// Pins the window's boundary: one poll short of the window the worker is
/// still reading, and at the window it fails naming the location and the
/// window, having received nothing.
#[test]
fn a_worker_exits_when_no_fingerprint_appears_within_a_settling_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    let (feed, script) = scripted_feed();
    script.deliver(unit_over(&data, "a.ndjson"));
    let clock = TestClock::frozen();

    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    let mut reads = 1;
    clock.advance_stepped(WINDOW - POLL, POLL, || {
        reads += 1;
        wait_for_gets(&hooks, reads);
    });
    assert!(l.run.wait_exit(Duration::ZERO).is_none(), "still waiting");
    clock.advance(POLL);
    let report = exit(&l);
    let reason = failure_reason(&report);
    assert!(
        reason.contains(LOCATION) && reason.contains("300s"),
        "{reason}"
    );
    assert_eq!(hooks.gets(), reads, "no read at the window itself");
    assert!(script.delivered().is_empty(), "nothing was received");
}

/// Pins that a fingerprint the lister writes inside the window lets the
/// worker start and read its units.
#[test]
fn a_fingerprint_written_within_the_window_lets_the_worker_start() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "a.ndjson"));
    let clock = TestClock::frozen();

    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    clock.advance(POLL);
    wait_for_gets(&hooks, 2);
    hooks.seed(FINGERPRINT_KEY, fingerprint(&data, "Auto", "0a").as_bytes());
    clock.advance(POLL);
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    stop(&l);
}

/// Pins that a fingerprint read the store never answers is abandoned at the
/// window.
#[test]
fn a_fingerprint_read_that_never_answers_ends_at_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.hang_gets();
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();

    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    clock.advance(WINDOW);
    let reason = failure_reason(&exit(&l));
    assert!(
        reason.contains("answered no read") && reason.contains("300s"),
        "{reason}"
    );
}

/// Pins that a retryable read error is retried at the next poll.
#[test]
fn a_transient_location_error_is_retried_within_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    hooks.seed(FINGERPRINT_KEY, fingerprint(&data, "Auto", "0a").as_bytes());
    hooks.fail_next_gets(1);
    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "a.ndjson"));
    let clock = TestClock::frozen();

    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    clock.advance(POLL);
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    stop(&l);
}

/// Pins that a stop during the wait ends the run at the next poll, before
/// the window and with nothing received, on a feed that hands back as on
/// one that does not: holding nothing, the stop reads nothing on.
#[test]
fn a_stop_during_the_fingerprint_wait_ends_the_run() {
    for hands_back in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let data = data_dir(&dir);
        let (store, hooks) = location_store();
        let (feed, script) = scripted_feed();
        if hands_back {
            script.hand_back_on_stop();
        }
        script.deliver(unit_over(&data, "a.ndjson"));
        let clock = TestClock::frozen();

        let l = launch_worker_on(
            &data,
            feed,
            Some(LOCATION),
            Some(store),
            Some(Arc::clone(&clock)),
            Some(StopClock::frozen()),
        );
        wait_for_gets(&hooks, 1);
        l.shutdown.trigger();
        clock.advance(POLL);
        let report = exit(&l);
        assert_eq!(report.state, ExitState::Completed);
        assert!(script.delivered().is_empty(), "nothing was received");
        assert_eq!(script.stopped_receiving(), usize::from(hands_back));
        assert!(script.handed_back().is_empty());
    }
}

/// Pins that a worker's own `compression` enters the fingerprint it
/// compares: one set to `none` refuses a job fingerprinted with `auto`.
#[test]
fn a_worker_with_its_own_compression_refuses_the_listers_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let jobs = dir.path().join("jobs").join("a");
    fs::create_dir_all(&jobs).unwrap();
    fs::write(jobs.join("fingerprint"), fingerprint(&data, "Auto", "0a")).unwrap();
    let location = format!("file://{}/", jobs.display());
    let (feed, script) = scripted_feed();
    script.deliver(unit_over(&data, "a.ndjson"));
    let yaml = PipelineYaml::file("s3-job-location-compression", &data)
        .source("compression", "none")
        .section(&format!(
            "feed:\n  scripted: {{}}\n  location: {location}\n"
        ))
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed),
        |s, _| line_framer(s),
    );
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("compression=None"), "{reason}");
    assert!(script.delivered().is_empty(), "nothing was received");
}

/// Pins that `configure_feed` refuses a location under the source's prefix
/// as a `feed` component error naming the key.
#[test]
fn configure_feed_refuses_a_location_under_the_source_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let rt = runtime();
    let mut source = lister_source(&data, &rt, "");
    let (feed, _script) = scripted_feed();
    let err = source
        .configure_feed(
            &section(
                "scripted",
                Some(&format!("file://{}/jobs/a/", data.display())),
            ),
            Arc::new(feed),
            spate_core::feed::FeedOrigin::Section,
        )
        .expect_err("a location under the source prefix is refused");
    assert!(
        matches!(&err, ConfigError::Component { context, .. } if context == "feed"),
        "{err:?}"
    );
    assert!(err.to_string().contains("outside the prefix"), "{err}");
}

/// Pins that the fingerprint reads keep a fixed schedule from the wait's
/// start: after reads at 0 s and a late 7 s, the third falls at 10 s.
#[test]
fn a_late_wakeup_does_not_shift_the_next_fingerprint_read() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    clock.advance(Duration::from_secs(7));
    wait_for_gets(&hooks, 2);
    clock.advance(Duration::from_secs(3));
    wait_for_gets(&hooks, 3);
    l.shutdown.trigger();
    clock.advance(POLL);
    assert_eq!(exit(&l).state, ExitState::Completed);
}

/// Pins that a shutdown ends the wait while a read is in flight that the
/// store never answers, at the next poll step and well before the window.
#[test]
fn a_stop_during_a_read_that_never_answers_ends_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.hang_gets();
    let (feed, script) = scripted_feed();
    script.deliver(unit_over(&data, "a.ndjson"));
    let clock = TestClock::frozen();
    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    l.shutdown.trigger();
    clock.advance(POLL);
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert!(script.delivered().is_empty(), "nothing was received");
}

/// A test clock that counts wakeups and notes any asked for at an instant
/// already reached.
#[derive(Debug)]
struct PastWakeups {
    inner: Arc<TestClock>,
    asked: AtomicBool,
    wakeups: AtomicUsize,
}

impl Clock for PastWakeups {
    fn now(&self) -> tokio::time::Instant {
        self.inner.now()
    }

    fn sleep_until(&self, deadline: tokio::time::Instant) -> Sleep {
        if deadline <= self.inner.now() {
            self.asked.store(true, Ordering::SeqCst);
        }
        self.wakeups.fetch_add(1, Ordering::SeqCst);
        self.inner.sleep_until(deadline)
    }
}

/// Pins that a read in flight is checked at each next poll step, never at
/// an instant already reached, which would spin while the store does not
/// answer: the wait re-arms past the first step, then ends on the stop.
#[test]
fn a_read_in_flight_is_checked_at_the_next_step_only() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.hang_gets();
    let (feed, _script) = scripted_feed();
    let test_clock = TestClock::frozen();
    let clock = Arc::new(PastWakeups {
        inner: Arc::clone(&test_clock),
        asked: AtomicBool::new(false),
        wakeups: AtomicUsize::new(0),
    });
    let yaml = PipelineYaml::file("s3-job-location-steps", &data)
        .section(&format!(
            "feed:\n  scripted: {{}}\n  location: {LOCATION}\n"
        ))
        .build();
    let l = {
        let clock = Arc::clone(&clock);
        launch_with_feed(
            &yaml,
            test_options(),
            |_| {},
            Arc::new(feed),
            move |s, _| {
                line_framer(s)
                    .with_location_store(store)
                    .with_clock(clock as Arc<dyn Clock>)
            },
        )
    };
    wait_for_gets(&hooks, 1);
    test_clock.advance(POLL);
    wait_until(DEADLINE, "the wait re-arms past the first step", || {
        clock.wakeups.load(Ordering::SeqCst) >= 2
    });
    l.shutdown.trigger();
    test_clock.advance(POLL);
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert!(
        !clock.asked.load(Ordering::SeqCst),
        "the wait asked for a wakeup already reached"
    );
}

/// Pins that a wait whose every read fails retryably ends at the window
/// with the last read's error, logged at WARN as each read fails.
#[test]
fn a_location_failing_every_read_reports_the_last_error_at_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let yaml = PipelineYaml::file("s3-job-location-failing", &data).build();
    let config = PipelineConfig::from_str(&yaml).unwrap();
    let rt = runtime();
    let (store, hooks) = location_store();
    hooks.fail_next_gets(usize::MAX);
    let clock = TestClock::frozen();
    let mut source =
        line_framer(S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap())
            .with_location_store(store)
            .with_clock(Arc::clone(&clock) as Arc<dyn Clock>);
    let (feed, _script) = scripted_feed();
    source
        .configure_feed(
            &section("scripted", Some(LOCATION)),
            Arc::new(feed),
            spate_core::feed::FeedOrigin::Section,
        )
        .unwrap();
    let stepper = {
        let hooks = hooks.clone();
        let clock = Arc::clone(&clock);
        std::thread::spawn(move || {
            let mut reads = 1;
            wait_for_gets(&hooks, reads);
            clock.advance_stepped(WINDOW, POLL, || {
                reads += 1;
                if reads <= 60 {
                    wait_for_gets(&hooks, reads);
                }
            });
        })
    };
    let mut err = None;
    let lines = spate_test::capture_logs(tracing::Level::WARN, || {
        err = source
            .open(SourceCtx::new(Checkpointer::new().handle()))
            .err();
    });
    stepper.join().unwrap();
    let err = err.expect("the wait fails at the window").to_string();
    assert!(
        err.contains("no read succeeded") && err.contains("connection reset"),
        "{err}"
    );
    let warned = lines
        .iter()
        .filter(|l| l.contains("reading the job fingerprint failed"))
        .count();
    assert!(warned >= 2, "one WARN per failed read: {lines:#?}");
}

/// Pins that a read in flight at the window, after an earlier read failed,
/// ends the wait naming that failure.
#[test]
fn an_unanswered_read_after_a_failed_one_names_the_failure() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.fail_next_gets(1);
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    hooks.hang_gets();
    clock.advance(POLL);
    wait_for_gets(&hooks, 2);
    clock.advance(WINDOW);
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("connection reset"), "{reason}");
}

/// Pins that a read finding nothing clears an earlier read's failure, so a
/// wait whose last reads found no fingerprint reports "none appeared".
#[test]
fn a_read_finding_nothing_clears_an_earlier_failure() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.fail_next_gets(1);
    let (feed, _script) = scripted_feed();
    let clock = TestClock::frozen();
    let l = launch_worker(
        &data,
        feed,
        Some(LOCATION),
        Some(store),
        Some(Arc::clone(&clock)),
    );
    wait_for_gets(&hooks, 1);
    let mut reads = 1;
    clock.advance_stepped(WINDOW, POLL, || {
        reads += 1;
        if reads <= 60 {
            wait_for_gets(&hooks, reads);
        }
    });
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("none appeared"), "{reason}");
    assert!(!reason.contains("connection reset"), "{reason}");
}

/// Pins the INFO line naming the location and the window when the wait
/// starts, and that the source becomes ready once the fingerprint matches.
#[test]
fn the_fingerprint_wait_logs_the_location_and_window() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let yaml = PipelineYaml::file("s3-job-location-log", &data).build();
    let config = PipelineConfig::from_str(&yaml).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (store, hooks) = location_store();
    let clock = TestClock::frozen();
    let mut source =
        line_framer(S3Source::from_component_config(&config.source, rt.handle().clone()).unwrap())
            .with_location_store(store)
            .with_clock(Arc::clone(&clock) as Arc<dyn Clock>);
    let mut section = FeedSection::new(spate_test::component_config("scripted", "{}"));
    section.location = Some(LOCATION.to_owned());
    let (feed, _script) = scripted_feed();
    source
        .configure_feed(
            &section,
            Arc::new(feed),
            spate_core::feed::FeedOrigin::Section,
        )
        .unwrap();

    let writer = {
        let hooks = hooks.clone();
        let clock = Arc::clone(&clock);
        let fp = fingerprint(&data, "Auto", "0a");
        std::thread::spawn(move || {
            wait_for_gets(&hooks, 1);
            hooks.seed(FINGERPRINT_KEY, fp.as_bytes());
            clock.advance(POLL);
        })
    };
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        source
            .open(SourceCtx::new(Checkpointer::new().handle()))
            .unwrap();
    });
    writer.join().unwrap();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("waiting for the job fingerprint")
                && l.contains(LOCATION)
                && l.contains("300s")),
        "{lines:#?}"
    );
    assert!(matches!(
        source.poll_events(Duration::from_millis(10)).unwrap(),
        SourceEvent::LanesAssigned(_)
    ));
}

/// Pins that a job whose `feed:` section names no location reads nothing
/// outside its data prefix and starts at once. This holds without the job
/// location; it guards the continuous path against the change.
#[test]
fn a_continuous_job_reads_no_marker_or_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, spy) = spying_local_store(SpyOptions::default());
    let (feed, script) = scripted_feed();
    let receipt = script.deliver(unit_over(&data, "a.ndjson"));
    let yaml = PipelineYaml::file("s3-continuous", &data)
        .section("feed:\n  scripted: {}\n")
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed),
        move |source, _io| line_framer(source).with_store(store),
    );
    wait_until(DEADLINE, "the unit is acknowledged", || {
        script.acknowledged() == vec![receipt]
    });
    stop(&l);
    let prefix = data.display().to_string();
    let prefix = prefix.trim_start_matches('/');
    for get in spy.gets() {
        assert!(
            get.key.starts_with(prefix),
            "read outside the data prefix: {}",
            get.key
        );
    }
    assert_eq!(spy.lists(), 0, "a worker never lists");
}

/// A source over `data` for a lister run, with `extra` source keys.
fn lister_source(data: &Path, rt: &tokio::runtime::Runtime, extra: &str) -> S3Source {
    let body = format!(
        "url: \"file://{}/\"\nsplit_target_bytes: 1MiB\n{extra}",
        data.display()
    );
    let section = spate_test::component_config("s3", &body);
    line_framer(S3Source::from_component_config(&section, rt.handle().clone()).unwrap())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn section(backend: &str, location: Option<&str>) -> FeedSection {
    let mut section = FeedSection::new(spate_test::component_config(backend, "{}"));
    section.location = location.map(str::to_owned);
    section
}

/// A file location under `dir`, outside the data prefix.
fn file_location(dir: &tempfile::TempDir) -> (String, PathBuf) {
    let path = dir.path().join("jobs").join("a");
    (format!("file://{}/", path.display()), path)
}

/// A feed that notes, at its first publish, whether the location holds an
/// end marker and a fingerprint, then publishes to a scripted feed.
struct Observing {
    inner: ScriptedFeed,
    location: PathBuf,
    first: Mutex<Option<(bool, bool)>>,
}

impl Feed for Observing {
    fn backend(&self) -> &str {
        self.inner.backend()
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
        self.first.lock().unwrap().get_or_insert_with(|| {
            (
                self.location.join("end").exists(),
                self.location.join("fingerprint").exists(),
            )
        });
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

/// Pins that a lister run deletes the end marker an earlier run left, and
/// writes its fingerprint, before its first publish, and logs the delete.
#[test]
fn a_lister_run_deletes_an_earlier_marker_before_its_first_publish() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (location, path) = file_location(&dir);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("end"), b"run=earlier\n").unwrap();
    let rt = runtime();
    let (inner, script) = scripted_feed();
    let feed = Arc::new(Observing {
        inner,
        location: path.clone(),
        first: Mutex::new(None),
    });

    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        lister_source(&data, &rt, "")
            .list_once(
                &section("scripted", Some(&location)),
                Arc::clone(&feed) as Arc<dyn Feed>,
            )
            .unwrap();
    });
    assert_eq!(script.published().len(), 1);
    assert_eq!(
        *feed.first.lock().unwrap(),
        Some((false, true)),
        "at the first publish the marker is gone and the fingerprint is there"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("deleted the end marker") && l.contains(&location)),
        "{lines:#?}"
    );
}

/// Pins that a first lister run on a location that does not exist yet
/// creates its fingerprint and publishes, and over a `file://` source keeps
/// no checkpoint.
#[test]
fn a_first_lister_run_on_an_empty_file_location_starts() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (location, path) = file_location(&dir);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();
    assert_eq!(script.published().len(), 1);
    assert_eq!(
        fs::read_to_string(path.join("fingerprint")).unwrap(),
        fingerprint(&data, "Auto", "0a")
    );
    assert!(!path.join("checkpoint").exists());
}

/// Pins that a lister over a store given to `with_store` under a `file://`
/// URL keeps no checkpoint, since that store's offset listing walks
/// unsorted.
#[test]
fn a_lister_over_an_injected_store_under_a_file_url_keeps_no_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (data_store, _) = spying_local_store(SpyOptions::default());
    let (location, path) = file_location(&dir);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .with_store(data_store)
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();
    assert_eq!(script.published().len(), 1);
    assert!(path.join("fingerprint").exists());
    assert!(!path.join("checkpoint").exists());
}

/// Pins that a lister finding another fingerprint, differing in the
/// delimiter, refuses before it publishes and leaves the location as it was.
#[test]
fn a_lister_with_another_fingerprint_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (location, path) = file_location(&dir);
    fs::create_dir_all(&path).unwrap();
    let theirs = fingerprint(&data, "Auto", "09");
    fs::write(path.join("fingerprint"), &theirs).unwrap();
    fs::write(path.join("end"), b"run=earlier\n").unwrap();
    fs::write(path.join("checkpoint"), b"earlier/key").unwrap();
    let rt = runtime();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap_err()
        .to_string();
    assert!(err.contains(&theirs), "{err}");
    assert!(err.contains(&fingerprint(&data, "Auto", "0a")), "{err}");
    assert!(script.published().is_empty());
    assert_eq!(
        fs::read_to_string(path.join("fingerprint")).unwrap(),
        theirs
    );
    assert_eq!(fs::read(path.join("end")).unwrap(), b"run=earlier\n");
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), b"earlier/key");
}

/// Pins that a re-run whose compression changed refuses the earlier run's
/// location.
#[test]
fn a_rerun_with_a_changed_configuration_refuses_the_earlier_location() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (location, _) = file_location(&dir);
    let rt = runtime();
    let (feed, _) = scripted_feed();
    lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "compression: none\n")
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap_err()
        .to_string();
    assert!(err.contains("compression=None"), "{err}");
    assert!(script.published().is_empty());
}

/// Pins that a lister finding its own fingerprint publishes and leaves it
/// as it was.
#[test]
fn a_lister_finding_its_own_fingerprint_proceeds() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    let mine = fingerprint(&data, "Auto", "0a");
    hooks.seed(FINGERPRINT_KEY, mine.as_bytes());
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed))
        .unwrap();
    assert_eq!(script.published().len(), 1);
    assert_eq!(hooks.read(FINGERPRINT_KEY).unwrap(), mine.as_bytes());
}

/// Pins that the lister writes its fingerprint with a conditional create,
/// and on `AlreadyExists` compares the stored one and proceeds.
#[test]
fn a_lister_whose_fingerprint_write_reports_already_exists_compares_the_stored_one() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    hooks.on_next_create(OnCreate::StoreThenAlreadyExists);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed))
        .unwrap();
    assert_eq!(hooks.creates(), 1, "one conditional create");
    assert_eq!(script.published().len(), 1);
}

/// Pins that a fingerprint reported present but read as absent is created
/// again.
#[test]
fn a_lister_recreates_a_fingerprint_missing_after_already_exists() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    hooks.on_next_create(OnCreate::AlreadyExistsOnce);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed))
        .unwrap();
    assert_eq!(hooks.creates(), 2);
    assert_eq!(
        hooks.read(FINGERPRINT_KEY).unwrap(),
        fingerprint(&data, "Auto", "0a").as_bytes()
    );
    assert_eq!(script.published().len(), 1);
}

/// Pins that a store client with conditional put turned off fails the
/// lister, naming the setting.
#[test]
fn a_lister_whose_location_refuses_conditional_put_names_the_setting() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let (store, hooks) = location_store();
    hooks.on_next_create(OnCreate::NotImplemented);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed))
        .unwrap_err()
        .to_string();
    assert!(err.contains("aws_conditional_put"), "{err}");
    assert!(script.published().is_empty());
}

/// Pins that a lister on a durable feed with no location refuses before it
/// sends any request.
#[test]
fn a_lister_on_a_durable_feed_without_a_location_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (data_store, spy) = spying_local_store(SpyOptions::default());
    let (store, hooks) = location_store();
    let rt = runtime();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .with_store(data_store)
        .with_location_store(store)
        .list_once(&section("scripted", None), Arc::new(feed))
        .unwrap_err()
        .to_string();
    assert!(err.contains("`feed.location`"), "{err}");
    assert!(script.published().is_empty());
    assert_eq!(spy.lists(), 0);
    assert!(spy.gets().is_empty());
    assert_eq!(hooks.requests(), 0);
}

/// A lister source over `s3://data-bucket/data/`, reading `data` and keeping
/// its job location in `location`.
fn resuming_source(
    rt: &tokio::runtime::Runtime,
    data: &Arc<dyn object_store::ObjectStore>,
    location: &Arc<dyn object_store::ObjectStore>,
) -> S3Source {
    let section = spate_test::component_config(
        "s3",
        "url: \"s3://data-bucket/data/\"\nsplit_target_bytes: 1MiB\n",
    );
    line_framer(S3Source::from_component_config(&section, rt.handle().clone()).unwrap())
        .with_store(Arc::clone(data))
        .with_location_store(Arc::clone(location))
}

/// The key of each published unit, in publish order.
fn published_keys(script: &spate_test::FeedScript) -> Vec<String> {
    script
        .published()
        .iter()
        .map(|u| {
            SplitDescriptor::decode(&u.body).unwrap().objects[0]
                .key
                .clone()
        })
        .collect()
}

/// Pins resume end to end through `list_once`: a second run lists only the
/// keys after the checkpoint the first left, and a run after the operator
/// deletes the checkpoint lists the whole prefix.
#[test]
fn a_lister_resumes_after_its_checkpoint() {
    use object_store::{ObjectStoreExt as _, PutPayload};
    let rt = runtime();
    let data: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let put = |key: &str| {
        rt.block_on(data.put(
            &object_store::path::Path::from(key),
            PutPayload::from_static(b"row\n"),
        ))
        .unwrap();
    };
    put("data/a");
    put("data/b");
    let (location, hooks) = location_store();
    let section = section("scripted", Some(LOCATION));

    let (feed, script) = scripted_feed();
    resuming_source(&rt, &data, &location)
        .list_once(&section, Arc::new(feed))
        .unwrap();
    assert_eq!(published_keys(&script), ["data/a", "data/b"]);
    assert_eq!(hooks.read(CHECKPOINT_KEY).unwrap(), b"data/b");

    put("data/c");
    put("data/d");
    let (feed, script) = scripted_feed();
    resuming_source(&rt, &data, &location)
        .list_once(&section, Arc::new(feed))
        .unwrap();
    assert_eq!(published_keys(&script), ["data/c", "data/d"]);
    assert_eq!(hooks.read(CHECKPOINT_KEY).unwrap(), b"data/d");

    rt.block_on(location.delete(&object_store::path::Path::from(CHECKPOINT_KEY)))
        .unwrap();
    let (feed, script) = scripted_feed();
    resuming_source(&rt, &data, &location)
        .list_once(&section, Arc::new(feed))
        .unwrap();
    assert_eq!(
        published_keys(&script),
        ["data/a", "data/b", "data/c", "data/d"]
    );
}

/// Pins resume at a `file://` location, whose store answers a delete of an
/// absent object `NotFound`: a first run over an injected data store lists
/// everything and leaves the checkpoint, and the next lists after it.
#[test]
fn a_lister_resumes_at_a_file_location() {
    use object_store::{ObjectStoreExt as _, PutPayload};
    let dir = tempfile::tempdir().unwrap();
    let (location, path) = file_location(&dir);
    let rt = runtime();
    let data: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let put = |key: &str| {
        rt.block_on(data.put(
            &object_store::path::Path::from(key),
            PutPayload::from_static(b"row\n"),
        ))
        .unwrap();
    };
    put("data/a");
    let source = || {
        let section = spate_test::component_config(
            "s3",
            "url: \"s3://data-bucket/data/\"\nsplit_target_bytes: 1MiB\n",
        );
        line_framer(S3Source::from_component_config(&section, rt.handle().clone()).unwrap())
            .with_store(Arc::clone(&data))
    };
    let (feed, script) = scripted_feed();
    source()
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();
    assert_eq!(published_keys(&script), ["data/a"]);
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), b"data/a");

    put("data/b");
    let (feed, script) = scripted_feed();
    source()
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();
    assert_eq!(published_keys(&script), ["data/b"]);
}

/// Pins that a lister on the in-process feed refuses a job location, which
/// that feed has no use for, before it sends any request.
#[test]
fn a_lister_on_the_memory_feed_with_a_location_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, hooks) = location_store();
    let rt = runtime();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("memory", Some(LOCATION)), Arc::new(feed))
        .unwrap_err()
        .to_string();
    assert!(err.contains("`feed.location`"), "{err}");
    assert!(script.published().is_empty());
    assert_eq!(hooks.requests(), 0);
}

/// Pins the published units: one whole unit per small object and ranges
/// tiling a large one, with the ids the public derivations mint, and
/// nothing from the location.
#[test]
fn a_lister_run_publishes_one_unit_per_listed_part() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&recs("b", 7))).unwrap();
    let big: Vec<String> = (0..40_000)
        .map(|i| format!("{{\"row\":{i:08},\"pad\":\"{}\"}}", "x".repeat(48)))
        .collect();
    fs::write(data.join("big.ndjson"), lines_bytes(&big)).unwrap();
    let big_size = fs::metadata(data.join("big.ndjson")).unwrap().len();
    assert!(
        big_size > 2 << 20,
        "the big object spans several 1 MiB ranges"
    );
    let (location, _) = file_location(&dir);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), Arc::new(feed))
        .unwrap();

    let mut by_key: BTreeMap<String, Vec<(String, SplitDescriptor)>> = BTreeMap::new();
    for unit in script.published() {
        let descriptor = SplitDescriptor::decode(&unit.body).unwrap();
        assert_eq!(descriptor.objects.len(), 1);
        let key = descriptor.objects[0].key.clone();
        by_key.entry(key).or_default().push((unit.id, descriptor));
    }
    let names: Vec<&str> = by_key
        .keys()
        .map(|k| k.rsplit('/').next().unwrap())
        .collect();
    assert_eq!(names, ["a.ndjson", "b.ndjson", "big.ndjson"]);
    for (key, units) in &by_key {
        let object = &units[0].1.objects[0];
        let etag = object.etag.as_deref();
        if key.ends_with("big.ndjson") {
            let mut next = 0;
            for (id, descriptor) in units {
                let range = descriptor.range.expect("the big object is read in ranges");
                assert_eq!(range.start, next);
                assert_eq!(
                    id,
                    split_id_for_range(key, etag.unwrap(), range)
                        .unwrap()
                        .as_str()
                );
                next = range.end;
            }
            assert_eq!(next, big_size, "the ranges tile the object");
            assert!(units.len() >= 2);
        } else {
            assert_eq!(units.len(), 1, "{key} is one whole unit");
            let (id, descriptor) = &units[0];
            assert_eq!(descriptor.range, None);
            assert_eq!(id, split_id_for([(key.as_str(), etag)]).unwrap().as_str());
        }
    }
}

/// Pins the store behaviour the lister's conditional create rests on: a
/// second create of one key reports `AlreadyExists`.
#[test]
#[ignore = "requires Docker"]
fn a_second_conditional_create_at_the_location_reports_already_exists() {
    use object_store::{ObjectStoreExt as _, PutMode, PutPayload};
    let gw = support::store::Gateway::start("job-location");
    let key = object_store::path::Path::from("jobs/a/fingerprint");
    gw.rt.block_on(async {
        gw.client
            .put_opts(
                &key,
                PutPayload::from_static(b"first"),
                PutMode::Create.into(),
            )
            .await
            .expect("the first create");
        let second = gw
            .client
            .put_opts(
                &key,
                PutPayload::from_static(b"second"),
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(second, object_store::Error::AlreadyExists { .. }),
            "{second:?}"
        );
        let stored = gw.client.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(&stored[..], b"first");
    });
}

/// Pins resume against a real S3 API: after each chosen key the store
/// returned, a lister run publishes exactly the keys the store lists after
/// it, across a page boundary that falls on a zero-byte folder marker and
/// for keys that are directory prefixes, carry characters URL encoding
/// changes, or follow a key since deleted.
#[test]
#[ignore = "requires Docker"]
fn resume_after_each_returned_key_publishes_the_rest() {
    use futures_util::StreamExt as _;
    use object_store::list::{PaginatedListOptions, PaginatedListStore as _};
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStoreExt as _, PutPayload};
    let gw = support::store::Gateway::start("lister-resume");
    let named = [
        "data/a-c",
        "data/a/b/y",
        "data/a/c",
        "data/amp&eq=.ndjson",
        "data/plus+.ndjson",
        "data/q/r",
        "data/q/r/s",
        "data/sp ace.ndjson",
        "data/x/1.ndjson",
        "data/x/2.ndjson",
        "data/\u{e9}.ndjson",
    ];
    // Eight named keys sort before the folder marker, so 991 fillers make
    // it the 1,000th key and the last of the first page.
    let fillers: Vec<String> = (0..991).map(|i| format!("data/f{i:04}")).collect();
    gw.rt.block_on(async {
        for key in named
            .iter()
            .copied()
            .chain(fillers.iter().map(String::as_str))
        {
            gw.client
                .put(
                    &StorePath::parse(key).unwrap(),
                    PutPayload::from_static(b"row\n"),
                )
                .await
                .unwrap_or_else(|e| panic!("the store refused {key}: {e}"));
        }
    });
    gw.put_raw("data/x/", "");

    let first = gw
        .rt
        .block_on(
            gw.s3()
                .list_paginated(Some("data/"), PaginatedListOptions::default()),
        )
        .unwrap();
    let last = first.result.objects.last().unwrap();
    assert_eq!(
        first.result.objects.len(),
        1000,
        "the store pages hold 1,000 keys"
    );
    assert_eq!((last.location.as_ref(), last.size), ("data/x", 0));
    assert!(first.page_token.is_some());

    // The store's own order, with the empty marker dropped as the lister
    // drops it.
    let listed: Vec<String> = gw.rt.block_on(async {
        gw.client
            .list(Some(&StorePath::from("data")))
            .map(|meta| meta.unwrap())
            .filter(|meta| futures_util::future::ready(meta.size > 0))
            .map(|meta| meta.location.to_string())
            .collect()
            .await
    });
    for key in named
        .iter()
        .copied()
        .chain(fillers.iter().map(String::as_str))
    {
        assert!(listed.iter().any(|k| k == key), "the store lists {key}");
    }
    assert_eq!(listed.len(), named.len() + fillers.len());

    let section = section("scripted", Some(&format!("s3://{}/jobs/a/", gw.bucket)));
    let rt = runtime();
    let source = || {
        let body = format!(
            "url: \"s3://{}/data/\"\nsplit_target_bytes: 1MiB\nstore:\n{}\n",
            gw.bucket,
            gw.store_options_yaml()
        );
        let config = spate_test::component_config("s3", &body);
        line_framer(S3Source::from_component_config(&config, rt.handle().clone()).unwrap())
    };
    let (feed, script) = scripted_feed();
    source().list_once(&section, Arc::new(feed)).unwrap();
    assert_eq!(published_keys(&script), listed);

    let checkpoint = StorePath::from("jobs/a/checkpoint");
    let page_two_first = listed.iter().position(|k| k == "data/x/1.ndjson").unwrap();
    let mut after: Vec<&str> = named.to_vec();
    after.push(fillers.last().unwrap());
    after.push(&listed[page_two_first - 1]);
    for key in after {
        gw.rt
            .block_on(gw.client.put(&checkpoint, PutPayload::from(key.to_owned())))
            .unwrap();
        let (feed, script) = scripted_feed();
        source().list_once(&section, Arc::new(feed)).unwrap();
        let at = listed.iter().position(|k| k == key).unwrap();
        assert_eq!(published_keys(&script), listed[at + 1..], "after {key}");
    }

    // A checkpoint naming a key since deleted resumes after its place.
    gw.rt.block_on(async {
        gw.client
            .put(&checkpoint, PutPayload::from_static(b"data/q/r"))
            .await
            .unwrap();
        gw.client
            .delete(&StorePath::from("data/q/r"))
            .await
            .unwrap();
    });
    let (feed, script) = scripted_feed();
    source().list_once(&section, Arc::new(feed)).unwrap();
    let at = listed.iter().position(|k| k == "data/q/r").unwrap();
    assert_eq!(published_keys(&script), listed[at + 1..]);
}

/// The key a published unit names.
fn unit_key(unit: &Unit) -> String {
    SplitDescriptor::decode(&unit.body).unwrap().objects[0]
        .key
        .clone()
}

/// The end marker's body at a file location, if there is one.
fn end_marker(location: &Path) -> Option<String> {
    fs::read_to_string(location.join("end")).ok()
}

/// Whether `body` is an end marker: `run=`, 32 lowercase hex digits and a
/// newline.
fn is_marker(body: &str) -> bool {
    body.strip_prefix("run=")
        .and_then(|rest| rest.strip_suffix('\n'))
        .is_some_and(|id| {
            id.len() == 32
                && id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

fn client_error(err: &spate_core::error::SourceError) -> (spate_core::error::ErrorClass, &str) {
    match err {
        spate_core::error::SourceError::Client { class, reason } => (*class, reason),
        other => panic!("expected a client error, got {other:?}"),
    }
}

/// Pins a lister run end to end through `list_once`: it publishes one unit
/// per listed object, receives nothing, writes an end marker of at most the
/// location's object cap, and logs the marker with the location and the
/// unit count.
#[test]
fn a_lister_run_publishes_every_unit_and_an_end_marker() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&recs("b", 5))).unwrap();
    let (location, path) = file_location(&dir);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    script.deliver(unit_over(&data, "a.ndjson"));
    let lines = spate_test::capture_logs(tracing::Level::INFO, || {
        lister_source(&data, &rt, "")
            .list_once(&section("scripted", Some(&location)), Arc::new(feed))
            .unwrap();
    });
    let mut keys: Vec<String> = script.published().iter().map(unit_key).collect();
    keys.sort();
    assert_eq!(keys.len(), 2);
    assert!(
        keys[0].ends_with("a.ndjson") && keys[1].ends_with("b.ndjson"),
        "{keys:?}"
    );
    assert!(
        script.delivered().is_empty(),
        "a lister run receives nothing"
    );
    let end = end_marker(&path).expect("an end marker");
    assert!(is_marker(&end), "{end:?}");
    assert!(end.len() <= 4096);
    assert!(
        lines.iter().any(|l| l.contains("INFO")
            && l.contains("end marker")
            && l.contains(&location)
            && l.contains("units=2")),
        "{lines:#?}"
    );
}

/// Pins that a lister run whose unit the feed refuses fails as fatal,
/// naming the refused object, and leaves no end marker, though an earlier
/// run at the location left one.
#[test]
fn a_refused_unit_withholds_the_end_marker() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&recs("b", 5))).unwrap();
    let (location, path) = file_location(&dir);
    let rt = runtime();
    let (feed, script) = scripted_feed();
    let feed: Arc<dyn Feed> = Arc::new(feed);
    lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), Arc::clone(&feed))
        .unwrap();
    assert!(end_marker(&path).is_some(), "the first run leaves a marker");

    script.refuse_next_publish(spate_core::feed::FeedErrorKind::Refused);
    let err = lister_source(&data, &rt, "")
        .list_once(&section("scripted", Some(&location)), feed)
        .unwrap_err();
    let (class, reason) = client_error(&err);
    assert_eq!(class, spate_core::error::ErrorClass::Fatal, "{err}");
    let refused = unit_key(&script.published()[2]);
    assert!(reason.contains(&refused), "{reason}");
    assert_eq!(end_marker(&path), None);
}

/// Pins that a lister run refuses the in-process feed before any store
/// request.
#[test]
fn a_lister_run_refuses_the_in_process_feed() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let (store, hooks) = location_store();
    let feed = MemoryFeed::new();
    let err = lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("memory", None), Arc::new(feed.clone()))
        .unwrap_err();
    let (class, reason) = client_error(&err);
    assert_eq!(class, spate_core::error::ErrorClass::Fatal, "{err}");
    assert!(reason.contains("durable feed"), "{reason}");
    assert_eq!(feed.counts().visible, 0, "nothing is published");
    assert_eq!(hooks.requests(), 0);
}

/// Pins that a lister run refuses a feed other than the one the section
/// names, naming both, before any store request.
#[test]
fn a_lister_run_refuses_a_feed_the_section_does_not_name() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let (store, hooks) = location_store();
    let feed = MemoryFeed::new();
    let err = lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed.clone()))
        .unwrap_err();
    let (class, reason) = client_error(&err);
    assert_eq!(class, spate_core::error::ErrorClass::Fatal, "{err}");
    assert!(
        reason.contains("`scripted`") && reason.contains("`memory`"),
        "{reason}"
    );
    assert_eq!(feed.counts().visible, 0, "nothing is published");
    assert_eq!(hooks.requests(), 0);
}

/// Pins that a lister run called from a thread in a Tokio runtime context
/// refuses: from async code, where blocking on the I/O handle would panic,
/// from a `spawn_blocking` closure and under a `Handle::enter` guard.
#[test]
fn a_lister_run_called_from_async_code_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (location, _) = file_location(&dir);
    let rt = runtime();
    let caller = runtime();
    let refused = |outcome: Result<(), spate_core::error::SourceError>,
                   script: &spate_test::FeedScript| {
        let err = outcome.unwrap_err();
        let (class, reason) = client_error(&err);
        assert_eq!(class, spate_core::error::ErrorClass::Fatal, "{err}");
        assert!(reason.contains("Tokio runtime context"), "{reason}");
        assert!(script.published().is_empty());
    };

    let source = lister_source(&data, &rt, "");
    let (feed, script) = scripted_feed();
    let section_here = section("scripted", Some(&location));
    let outcome = caller.block_on(async { source.list_once(&section_here, Arc::new(feed)) });
    refused(outcome, &script);

    let source = lister_source(&data, &rt, "");
    let (feed, script) = scripted_feed();
    let section_here = section("scripted", Some(&location));
    let outcome = caller.block_on(async move {
        tokio::task::spawn_blocking(move || source.list_once(&section_here, Arc::new(feed)))
            .await
            .unwrap()
    });
    refused(outcome, &script);

    let source = lister_source(&data, &rt, "");
    let (feed, script) = scripted_feed();
    let _guard = caller.handle().enter();
    let outcome = source.list_once(&section("scripted", Some(&location)), Arc::new(feed));
    refused(outcome, &script);
}

/// A solo pipeline on the in-process feed the runtime builds from the
/// file, over `data` behind `store`.
fn launch_solo(data: &Path, store: Arc<dyn object_store::ObjectStore>) -> Launched {
    let yaml = PipelineYaml::file("s3-solo-lister", data)
        .section("feed:\n  memory: {}\n")
        .build();
    launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _io| line_framer(source).with_store(store),
    )
}

/// Pins that a solo run on the in-process feed lists its prefix once and
/// reads it, and that a restart lists and reads the whole prefix again.
#[test]
fn a_restarted_solo_run_lists_from_the_beginning() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let a = recs("a", 5);
    let b = recs("b", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&a)).unwrap();

    let (store, spy) = spying_local_store(SpyOptions::default());
    let l = launch_solo(&data, store);
    wait_until(DEADLINE, "the first run reads a.ndjson", || {
        sorted(captured_rows(&l.script)) == sorted(a.clone())
    });
    stop(&l);
    assert_eq!(spy.lists(), 1);

    fs::write(data.join("b.ndjson"), lines_bytes(&b)).unwrap();
    let (store, spy) = spying_local_store(SpyOptions::default());
    let l = launch_solo(&data, store);
    let both: Vec<String> = a.iter().chain(&b).cloned().collect();
    wait_until(DEADLINE, "the restart reads both objects", || {
        sorted(captured_rows(&l.script)) == sorted(both.clone())
    });
    stop(&l);
    assert_eq!(spy.lists(), 1);
}

/// Pins that a solo run on the in-process feed warns once that it lists the
/// prefix from the beginning after a restart, in place of the warning that
/// it publishes nothing, and that a run on another feed does not.
#[test]
fn a_solo_run_warns_that_it_lists_from_the_beginning() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let solo = "lists and reads the prefix from the beginning";
    for (backend, feed) in [
        ("memory", Arc::new(MemoryFeed::new()) as Arc<dyn Feed>),
        ("scripted", Arc::new(scripted_feed().0) as Arc<dyn Feed>),
    ] {
        let mut source = lister_source(&data, &rt, "");
        source
            .configure_feed(
                &section(backend, None),
                feed,
                spate_core::feed::FeedOrigin::Section,
            )
            .unwrap();
        let lines = spate_test::capture_logs(tracing::Level::WARN, || {
            source
                .open(SourceCtx::new(Checkpointer::new().handle()))
                .unwrap();
        });
        let warned = lines.iter().filter(|l| l.contains(solo)).count();
        let silent = lines
            .iter()
            .any(|l| l.contains("reads the units and storage notifications published to it"));
        if backend == "memory" {
            assert_eq!(warned, 1, "{lines:#?}");
            assert!(!silent, "{lines:#?}");
        } else {
            assert_eq!(warned, 0, "{lines:#?}");
            assert!(silent, "{lines:#?}");
        }
        assert!(!lines.iter().any(|l| l.contains("EPHEMERAL")), "{lines:#?}");
    }
}

/// Pins that a solo run reads a listed unit while its listing is still
/// running, and reads every row once after the listing ends.
#[test]
fn a_solo_run_reads_while_it_lists() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    let a = recs("a", 5);
    let b = recs("b", 5);
    fs::write(data.join("a.ndjson"), lines_bytes(&a)).unwrap();
    fs::write(data.join("b.ndjson"), lines_bytes(&b)).unwrap();
    let (store, spy) = spying_local_store(SpyOptions {
        list_gate: Some(1),
        ..SpyOptions::default()
    });
    let feed = MemoryFeed::new();
    let yaml = PipelineYaml::file("s3-solo-lister", &data)
        .section("feed:\n  memory: {}\n")
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(feed.clone()),
        move |source, _io| line_framer(source).with_store(store),
    );
    wait_until(
        DEADLINE,
        "the first unit is read while the listing is held",
        || feed.counts().acknowledged == 1 && spy.held_lists() == 1,
    );
    spy.release_lists();
    wait_until(DEADLINE, "both units are read", || {
        feed.counts().acknowledged == 2
    });
    stop(&l);
    let both: Vec<String> = a.iter().chain(&b).cloned().collect();
    assert_eq!(sorted(captured_rows(&l.script)), sorted(both));
}

/// A scripted feed that names itself `memory`, so a section naming the
/// in-process feed runs its solo lister over the script.
struct AsMemory(ScriptedFeed);

impl Feed for AsMemory {
    fn backend(&self) -> &str {
        "memory"
    }
    fn set_waker(&self, waker: FeedWaker) {
        self.0.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        self.0.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.0.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.0.acknowledge(receipt)
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.0.fail(receipt, reason)
    }
    fn publish(&self, unit: Unit) -> Confirmation {
        self.0.publish(unit)
    }
    fn read_depth(&self, of: spate_core::feed::DepthOf) -> Option<spate_core::feed::DepthReading> {
        self.0.read_depth(of)
    }
    fn hands_back(&self) -> bool {
        self.0.hands_back()
    }
    fn stop_receiving(&self) {
        self.0.stop_receiving();
    }
    fn hand_back(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.0.hand_back(receipt)
    }
    fn close(&self, within: Duration) -> Option<Confirmation> {
        self.0.close(within)
    }
}

/// Pins that a solo run stopped once it has received its first listed unit
/// acknowledges that unit and receives no other unit its lister published.
#[test]
fn a_stopping_solo_run_receives_nothing_more_from_its_lister() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    stage_two_ranges(&data, "a.ndjson");
    fs::write(data.join("b.ndjson"), lines_bytes(&recs("b", 2))).unwrap();
    fs::write(data.join("c.ndjson"), lines_bytes(&recs("c", 2))).unwrap();
    let (feed, script) = scripted_feed();
    script.hand_back_on_stop();
    let yaml = PipelineYaml::file("s3-solo-stop", &data)
        .section("feed:\n  memory: {}\n  units_held: 1\n")
        .build();
    let l = launch_with_feed_on_clock(
        &yaml,
        test_options(),
        |sink| sink.enqueue_global(WriteOutcome::ok().held()),
        Arc::new(AsMemory(feed)),
        StopClock::frozen(),
        |source, _io| line_framer(source),
    );
    // The lister cuts `a.ndjson` into two range units.
    wait_until(DEADLINE, "the lister publishes the prefix", || {
        script.published().len() == 4
    });
    wait_until(DEADLINE, "the first write is held", || {
        l.script.held_writes() == 1
    });
    l.shutdown.trigger();
    wait_until(DEADLINE, "the stop begins", || {
        script.stopped_receiving() == 1
    });
    l.script.release_held();
    assert_eq!(exit(&l).state, ExitState::Completed);
    assert_eq!(script.delivered().len(), 1, "received after the stop began");
    assert!(script.handed_back().is_empty());
    assert_eq!(script.acknowledged().len(), 1);
}

/// Pins that a solo run stops at a shutdown while its listing never ends.
#[test]
fn a_solo_run_stops_while_its_lister_lists() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, spy) = spying_local_store(SpyOptions {
        list_gate: Some(0),
        ..SpyOptions::default()
    });
    let l = launch_solo(&data, store);
    wait_until(DEADLINE, "the listing is held", || spy.held_lists() == 1);
    stop(&l);
}

/// Pins that dropping a solo source ends its lister, though the I/O runtime
/// lives on.
#[test]
fn a_dropped_solo_source_stops_its_lister() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let (store, spy) = spying_local_store(SpyOptions {
        list_gate: Some(0),
        ..SpyOptions::default()
    });
    let mut source = lister_source(&data, &rt, "").with_store(store);
    source
        .configure_feed(
            &section("memory", None),
            Arc::new(MemoryFeed::new()),
            spate_core::feed::FeedOrigin::Section,
        )
        .unwrap();
    source
        .open(SourceCtx::new(Checkpointer::new().handle()))
        .unwrap();
    wait_until(DEADLINE, "the listing is held", || spy.held_lists() == 1);
    drop(source);
    wait_until(DEADLINE, "the listing is dropped", || spy.held_lists() == 0);
}

/// Pins that a solo run whose listing fails ends as failed, naming the
/// listing, though the store error is retryable.
#[test]
fn a_failed_solo_listing_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, _spy) = spying_local_store(SpyOptions {
        list_fails: true,
        ..SpyOptions::default()
    });
    let l = launch_solo(&data, store);
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("listing"), "{reason}");
}

/// The in-process feed, except that a publish panics.
struct PanickingPublish(MemoryFeed);

impl Feed for PanickingPublish {
    fn backend(&self) -> &str {
        self.0.backend()
    }
    fn set_waker(&self, waker: FeedWaker) {
        self.0.set_waker(waker);
    }
    fn receive(&self, max: usize) -> Result<Vec<Delivery>, FeedError> {
        self.0.receive(max)
    }
    fn renew(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.0.renew(receipt)
    }
    fn acknowledge(&self, receipt: Receipt) -> Result<(), FeedError> {
        self.0.acknowledge(receipt)
    }
    fn fail(&self, receipt: Receipt, reason: &str) -> Result<(), FeedError> {
        self.0.fail(receipt, reason)
    }
    fn publish(&self, _unit: Unit) -> Confirmation {
        panic!("the feed panics on publish");
    }

    fn read_depth(&self, of: spate_core::feed::DepthOf) -> Option<spate_core::feed::DepthReading> {
        self.0.read_depth(of)
    }
}

/// Pins that a solo run whose lister task panics ends as failed, naming
/// the lister, and does not go on reading an empty feed.
#[test]
fn a_panicked_solo_lister_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let (store, _spy) = spying_local_store(SpyOptions::default());
    let yaml = PipelineYaml::file("s3-solo-lister", &data)
        .section("feed:\n  memory: {}\n")
        .build();
    let l = launch_with_feed(
        &yaml,
        test_options(),
        |_| {},
        Arc::new(PanickingPublish(MemoryFeed::new())),
        move |source, _io| line_framer(source).with_store(store),
    );
    let reason = failure_reason(&exit(&l));
    assert!(reason.contains("lister ended"), "{reason}");
}

/// Pins that a listing failed by a retryable store error fails a lister
/// run as retryable, the class of its store error.
#[test]
fn a_retryable_listing_error_fails_the_lister_run_as_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let (data_store, _spy) = spying_local_store(SpyOptions {
        list_fails: true,
        ..SpyOptions::default()
    });
    let (store, _hooks) = location_store();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .with_store(data_store)
        .with_location_store(store)
        .list_once(&section("scripted", Some(LOCATION)), Arc::new(feed))
        .unwrap_err();
    let (class, reason) = client_error(&err);
    assert_eq!(class, spate_core::error::ErrorClass::Retryable, "{err}");
    assert!(reason.contains("listing"), "{reason}");
    assert!(script.published().is_empty());
}

/// Pins that a lister run refuses a `location` that does not parse as
/// fatal, before any store request.
#[test]
fn a_lister_run_refuses_a_location_that_does_not_parse() {
    let dir = tempfile::tempdir().unwrap();
    let data = data_dir(&dir);
    fs::write(data.join("a.ndjson"), lines_bytes(&recs("a", 5))).unwrap();
    let rt = runtime();
    let (store, hooks) = location_store();
    let (feed, script) = scripted_feed();
    let err = lister_source(&data, &rt, "")
        .with_location_store(store)
        .list_once(&section("scripted", Some("no scheme here")), Arc::new(feed))
        .unwrap_err();
    let (class, reason) = client_error(&err);
    assert_eq!(class, spate_core::error::ErrorClass::Fatal, "{err}");
    assert!(reason.contains("is not a URL"), "{reason}");
    assert!(script.published().is_empty());
    assert_eq!(hooks.requests(), 0);
}
