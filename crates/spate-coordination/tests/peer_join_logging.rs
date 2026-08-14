//! What the log says while a peer joins.
//!
//! A joining instance probes the store before it trusts fencing to it, and
//! that probe writes and deletes a key in the durable keyspace every peer is
//! already watching. The routine case and the alarming case are both a
//! durable delete, so this asserts on the output an operator running at
//! `info` sees: a fleet growing says so, and nothing says a record went
//! missing under the coordinator.
//!
//! One test in its own binary, because the capture below is a process-wide
//! global subscriber and `cargo test` shares one process across a binary.

mod support;

use spate_coordination::{SplitCoordinator, SplitProgress};
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

/// How many rebalances the leader has announced. Separate from
/// [`announced_moves`] so a wait can poll it without parsing: a line read
/// mid-write would panic there, and inside a poll loop that reads as a
/// timeout in the wrong place.
fn announcements(capture: &Capture) -> usize {
    capture
        .lines()
        .iter()
        .filter(|l| l.contains("assignment published"))
        .count()
}

/// The `moved` count of every rebalance the leader has announced so far, in
/// order. Reading the field rather than testing for the absence of
/// `moved=0`: a formatter that stopped rendering fields this way produces
/// that absence too, and such an assertion would pass having checked
/// nothing.
fn announced_moves(capture: &Capture) -> Vec<u64> {
    capture
        .lines()
        .iter()
        .filter(|l| l.contains("assignment published"))
        .map(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("moved="))
                .unwrap_or_else(|| panic!("no `moved` field to read on: {l}"))
                .parse()
                .expect("moved is a count")
        })
        .collect()
}

/// Wait for a line containing `needle`.
fn wait_for_line(capture: &Capture, needle: &str) {
    wait_until(&format!("a line containing {needle:?}"), capture, |c| {
        c.lines().iter().any(|l| l.contains(needle))
    });
}

/// Wait for `what` to become true of the capture, so the assertions below
/// run against a fleet that has finished reacting to the join rather than
/// one still mid-flight.
fn wait_until(what: &str, capture: &Capture, mut check: impl FnMut(&Capture) -> bool) {
    let deadline = Instant::now() + support::DEADLINE;
    while Instant::now() < deadline {
        if check(capture) {
            return;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    panic!(
        "timed out waiting for {what}\n--- captured ---\n{}",
        capture.lines().join("\n")
    );
}

#[test]
fn a_peer_joining_is_announced_and_nothing_reads_as_a_fault() {
    let capture = Capture::new();
    // `info` is the level a deployment runs at and the level the coordinated
    // examples set, so it is the level this asserts about.
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

    // A split completing rewrites its owner's assignment, so the leader
    // republishes on every completion in the job. None of those is a
    // rebalance, and announcing them would put a line per split on a large
    // backfill. Held here while the fleet is one worker, which is the only
    // shape where a completion *cannot* move anything: with two members a
    // shrinking pool rebalances, and the silence would prove nothing.
    let announced_alone = announcements(&capture);
    for id in ["s0", "s1"] {
        let done = SplitProgress::completed(support::DRAINED_WATERMARK, vec![]);
        a.commit(&support::split_id(id), &done).expect("commit");
        // A completion is not a loss, so nothing retracts it from the
        // worker's own view; the suite drops it by hand, as its siblings do.
        held_a.splits.remove(id);
    }
    // Long enough for several reconcile ticks, so the leader has observed
    // both completions and republished its own shrunken assignment. Silence
    // over a window in which nothing was published would prove nothing.
    let settle_until = Instant::now() + support::LEASE;
    while Instant::now() < settle_until {
        held_a.fold(a.poll().unwrap());
        std::thread::sleep(support::POLL_INTERVAL);
    }
    assert_eq!(held_a.splits.len(), 2, "two splits are left to hold");
    assert_eq!(
        announcements(&capture),
        announced_alone,
        "a completion was announced as a rebalance\n--- captured ---\n{}",
        capture.lines().join("\n")
    );

    let mut b = worker(&store, rt.handle(), Some("worker-b"));
    b.start(planner()).unwrap();
    let mut held_b = Held::default();

    let deadline = Instant::now() + support::DEADLINE;
    while !(held_a.splits.len() == 1 && held_b.splits.len() == 1) {
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

    // The incumbent says the fleet grew, and announces the rebalance that
    // followed. Waiting on the *count* rather than on the phrase: the solo
    // handout above already emitted one `assignment published`, so waiting
    // for the phrase would be satisfied before the join happened.
    wait_for_line(&capture, "peer joined");
    wait_until(
        "the join is announced as a rebalance of its own",
        &capture,
        |c| announcements(c) > announced_alone,
    );
    // The newcomer says what it found, rather than announcing every member
    // of it as an arrival.
    wait_for_line(&capture, "joined a fleet already running");

    let lines = capture.lines();
    // Neither an exact count nor a claim about every line. This runs on the
    // real clock, so a scheduler stall long enough to lapse a presence key
    // produces an extra leave/join pair, for either worker, since each
    // renews its own. An assertion over all the lines would fail on that
    // while reporting something untrue. The property is that worker-b's
    // arrival is announced.
    let joins: Vec<&String> = lines.iter().filter(|l| l.contains("peer joined")).collect();
    assert!(
        joins.iter().any(|l| l.contains("worker-b")),
        "no join named worker-b\n--- captured ---\n{}",
        lines.join("\n")
    );

    // The join moved work, and the line that announced it says so.
    let moves = announced_moves(&capture);
    assert!(
        moves[announced_alone..].iter().any(|m| *m > 0),
        "worker-b took a split off worker-a, so the publish that followed \
         the join moved it\n--- captured ---\n{}",
        lines.join("\n")
    );

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

    // The other half of a fleet changing size, and the harder one to count.
    // A graceful departure clears the owner of every split it held before
    // dropping its presence key, so nothing is reserved and the survivor is
    // handed the whole share at once, the largest rebalance there is. The
    // departing instance is no longer a member, so its assignment record is
    // the only remaining evidence of where those splits were assigned; a
    // count that reads live members only reports the biggest move in the
    // fleet's life as work handed out for the first time.
    //
    // Drop the coordinator, as the other suites do. `parting` is read by
    // the control step and not by the heartbeat, so a departed worker still
    // in scope re-creates its own presence key at its next beat, rejoins
    // the fleet it left, and is assigned work its step will never claim.
    let before_departure = announcements(&capture);
    let leaving: Vec<_> = held_b
        .splits
        .keys()
        .map(|id| support::split_id(id))
        .collect();
    b.release(&leaving).expect("worker-b departs");
    drop(b);
    drive(
        &mut a,
        &mut held_a,
        "worker-a inherits the departed share",
        |h| h.splits.len() == 2,
    );
    wait_for_line(&capture, "peer left");
    let moves = announced_moves(&capture);
    assert!(
        moves[before_departure..].iter().any(|m| *m > 0),
        "worker-a inherited worker-b's split, so the publish that followed \
         the departure moved it\n--- captured ---\n{}",
        capture.lines().join("\n")
    );
}
