//! What the log says while a peer joins.
//!
//! A joining instance probes the store before it trusts fencing to it, and
//! that probe writes and deletes a key in the durable keyspace every peer is
//! already watching. The routine case and the alarming case are both a
//! durable delete, so this asserts on the output an operator running at
//! `info` actually sees: a fleet growing says so, and nothing says a record
//! went missing under the coordinator.
//!
//! One test in its own binary, because the capture below is a process-wide
//! global subscriber and `cargo test` shares one process across a binary.

mod support;

use spate_coordination::SplitCoordinator;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use support::{Held, PhasedPlanner, drive, runtime, store, worker};

/// Everything the global subscriber has formatted, shared with the
/// subscriber it is installed into.
#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn new() -> Capture {
        Capture(Arc::new(Mutex::new(Vec::new())))
    }

    fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().expect("capture"))
            .lines()
            .map(str::to_string)
            .collect()
    }
}

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;

    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

/// Wait for `what` to appear in the capture, so the assertions below run
/// against a fleet that has finished reacting to the join rather than one
/// still mid-flight.
fn wait_for_line(capture: &Capture, needle: &str) {
    let deadline = Instant::now() + support::DEADLINE;
    while Instant::now() < deadline {
        if capture.lines().iter().any(|l| l.contains(needle)) {
            return;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    panic!(
        "no line containing {needle:?} was logged\n--- captured ---\n{}",
        capture.lines().join("\n")
    );
}

#[test]
fn a_peer_joining_is_announced_and_nothing_reads_as_a_fault() {
    let capture = Capture::new();
    // `info` is the level a deployment runs at and the level the coordinated
    // examples set, so it is the level this asserts about: the startup probe
    // is DEBUG and must not reach here at all.
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .init();

    let rt = runtime();
    let store = store();
    let ids = ["s0", "s1", "s2", "s3"];
    let planner = || Box::new(PhasedPlanner::one_final("joinlog:v1", &ids));

    let mut a = worker(&store, rt.handle(), Some("worker-a"));
    a.start(planner()).unwrap();
    let mut held_a = Held::default();
    drive(&mut a, &mut held_a, "worker-a takes the whole plan", |h| {
        h.splits.len() == ids.len()
    });
    support::commit_held(&mut a, &held_a);

    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 2 && held_b.splits.len() == 2) {
        assert!(
            Instant::now() < deadline,
            "timed out balancing: a={:?} b={:?}",
            held_a.splits.keys().collect::<Vec<_>>(),
            held_b.splits.keys().collect::<Vec<_>>()
        );
        held_a.fold(a.poll().unwrap());
        held_b.fold(b.poll().unwrap());
        support::commit_held(&mut a, &held_a);
        support::consent_to_revocations(&mut a, &mut held_a);
        std::thread::sleep(support::POLL_INTERVAL);
    }

    // The incumbent says the fleet grew, and says once that it moved work.
    wait_for_line(&capture, "peer joined");
    wait_for_line(&capture, "assignment published");
    // The newcomer says what it found, rather than announcing every member
    // of it as an arrival.
    wait_for_line(&capture, "joined a fleet already running");

    let lines = capture.lines();
    let joins: Vec<&String> = lines.iter().filter(|l| l.contains("peer joined")).collect();
    assert_eq!(
        joins.len(),
        1,
        "worker-b joined once, so it is announced once\n--- captured ---\n{}",
        lines.join("\n")
    );
    assert!(
        joins[0].contains("worker-b"),
        "the join names the instance that joined: {}",
        joins[0]
    );

    // A publish that moves nothing is not a rebalance. A split completing
    // rewrites its owner's assignment too, so counting those would put a
    // line on every completion in the job and bury the ones that matter.
    for line in lines.iter().filter(|l| l.contains("assignment published")) {
        assert!(
            !line.contains("moved=0"),
            "an assignment that moved nothing was announced as a rebalance: {line}"
        );
    }

    // The point of the exercise: the joining instance's own startup probe is
    // a durable delete every peer observes, and it must not be reported as a
    // record vanishing from under the coordinator.
    let alarming: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("deleted externally") || l.contains("_probe"))
        .collect();
    assert!(
        alarming.is_empty(),
        "a routine join logged {} line(s) that read as a fault:\n{}",
        alarming.len(),
        alarming
            .iter()
            .map(|l| l.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}
