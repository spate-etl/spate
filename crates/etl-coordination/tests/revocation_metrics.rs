//! The revocation observability seam, end to end.
//!
//! Registering a metric family proves nothing about whether anything ever
//! records into it — a family in this repo once read 0 forever because only
//! its registration was asserted, and the assignment-latency histogram
//! here read ~0 for the same reason until a benchmark caught it. These
//! tests therefore drive a REAL revocation between two coordinators built
//! with metrics and assert the series moved.
//!
//! This lives in its own file deliberately: the exporter installs a
//! process-global recorder, and each `tests/*.rs` is its own test binary,
//! so nothing else in the suite can race the install.

mod support;

use etl_coordination::StoreCoordinator;
use etl_core::coordination::{CoordinationEvent, SplitCoordinator, SplitProgress};
use etl_core::metrics::{ComponentLabels, CoordinationMetrics, Exporter, MetricsSettings, install};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use support::{PhasedPlanner, runtime, split_id, store};

/// Total observations in a histogram, summed over every component's
/// series. `None` = the family is absent entirely. Summing matters:
/// handles are pre-registered, so both workers publish both families and
/// two of the series are legitimately 0; taking the first match would read
/// whichever component the exporter rendered first and call a wired seam
/// dead.
fn histogram_count(text: &str, name: &str) -> Option<f64> {
    let needle = format!("{name}_count");
    let mut seen = false;
    let mut total = 0.0;
    for line in text.lines().filter(|l| l.starts_with(&needle)) {
        if let Some(value) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            seen = true;
            total += value;
        }
    }
    seen.then_some(total)
}

/// How many observations across all component series landed strictly above
/// `threshold` seconds — `total_count - cumulative_count(le = threshold)`.
///
/// This is the statistic with teeth for the assignment-latency regression:
/// the mean is diluted toward 0 by the many splits a worker claims the
/// instant they are assigned, but a single genuine wait still shows up as
/// one observation above the floor. The bug put every observation in the
/// lowest buckets, so this returns 0 against it.
#[track_caller]
fn observations_above(text: &str, name: &str, threshold: f64) -> f64 {
    let count = format!("{name}_count");
    let total: f64 = text
        .lines()
        .filter(|l| l.starts_with(&count))
        .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
        .sum();
    // Cumulative count in the smallest bucket whose `le` is >= threshold.
    let bucket = format!("{name}_bucket");
    let le_needle = format!("le=\"{threshold}\"");
    let matched: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with(&bucket) && l.contains(&le_needle))
        .collect();
    // Without this the helper degrades to vacuous exactly when it stops
    // working: no matching bucket line means `below == 0`, so it returns
    // the full count and every assertion built on it passes against any
    // observation at all. It matches today only because the threshold
    // happens to format as a real bucket boundary — a boundary set the
    // exporter is free to change.
    assert!(
        !matched.is_empty(),
        "no `{bucket}` line at `{le_needle}` — the threshold is not a bucket boundary, \
         so this assertion would silently stop testing anything"
    );
    let below: f64 = matched
        .iter()
        .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
        .sum();
    total - below
}

/// Sum a counter across every component/label series.
fn counter_sum(text: &str, name: &str, label: &str) -> Option<f64> {
    let mut seen = false;
    let mut total = 0.0;
    for line in text
        .lines()
        .filter(|l| l.starts_with(name) && l.contains(label))
    {
        if let Some(value) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            seen = true;
            total += value;
        }
    }
    seen.then_some(total)
}

/// A real cooperative revocation must move every seam it touches: the
/// `revocations_total{outcome=requested}` denominator and its
/// `outcome=drained` terminal, the `splits_draining` gauge while the drain
/// is in flight, the releasing worker's `drain_duration_seconds`, and the
/// gaining worker's `assignment_latency_seconds`.
///
/// The last is the one that regressed: a foreign owner on an
/// awaited split (the normal mid-drain state) used to clear the latency
/// timer, so every assignment latency read ~0.
#[test]
fn a_real_revocation_moves_every_metric_seam() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        ..MetricsSettings::default()
    })
    .expect("install the exporter");

    let rt = runtime();
    let store = store();
    let ids = ["m0", "m1", "m2", "m3"];
    let planner = || Box::new(PhasedPlanner::one_final("revoke-metrics:v1", &ids));

    // A takes the whole plan and commits, so its splits carry a resume
    // point before B ever joins.
    let a_labels = ComponentLabels::new("coord-metrics", "worker-a", "s3");
    let mut a = StoreCoordinator::new(
        store.clone(),
        support::config(Some("worker-a")),
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&a_labels)),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = support::Held::default();
    support::drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // B joins; the leader will revoke a split from A toward it. B's drain
    // deadline is generous so the move can only complete cooperatively.
    let b_labels = ComponentLabels::new("coord-metrics", "worker-b", "s3");
    let mut b_config = support::config(Some("worker-b"));
    b_config.drain_deadline = support::LEASE * 4;
    let mut b = StoreCoordinator::new(
        store.clone(),
        b_config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&b_labels)),
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = support::Held::default();

    // The latency floor. B is deliberately kept waiting at least this long
    // between being assigned a split and being allowed to claim it, so a
    // correctly-timed observation must land at or above it — a mere "an
    // observation was recorded" check would pass even against the bug,
    // which recorded ~0.
    const AWAIT_FLOOR: Duration = Duration::from_millis(200);
    const TAIL: i64 = 42;

    let mut asked: Option<String> = None;
    let mut asked_at: Option<Instant> = None;
    let mut revoked: Option<String> = None;
    let mut saw_draining = false;
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "the revocation never completed");
        for event in a.poll().unwrap() {
            if let CoordinationEvent::RevokeRequested { split } = &event
                && asked.is_none()
            {
                asked = Some(split.as_str().to_string());
                asked_at = Some(Instant::now());
            }
            held_a.fold(vec![event]);
        }
        if let Some(id) = &asked
            && revoked.is_none()
        {
            // The leader can revoke more than one split at once, so the
            // gauge is however many are draining — assert it is positive.
            if handle.render().lines().any(|l| {
                l.starts_with("etl_coordination_splits_draining")
                    && l.rsplit(' ')
                        .next()
                        .and_then(|v| v.parse::<f64>().ok())
                        .is_some_and(|v| v >= 1.0)
            }) {
                saw_draining = true;
            }
            // Keep A committing its still-held splits while B waits, exactly
            // as a running pipeline checkpoints — this rewrites the durable
            // record under B's awaited split, which is the event the buggy
            // timer-clear latched onto. Hold the release until both the
            // gauge has been seen AND the floor has elapsed, so B's wait is
            // real and long enough to distinguish from ~0.
            let waited = asked_at.is_some_and(|t| t.elapsed() >= AWAIT_FLOOR);
            if saw_draining && waited {
                a.commit(&split_id(id), &SplitProgress::new(TAIL, b"tail".to_vec()))
                    .unwrap();
                a.release_drained(&[split_id(id)]).unwrap();
                revoked = Some(id.clone());
            } else {
                support::commit_held(&mut a, &held_a);
            }
        }
        held_b.fold(b.poll().unwrap());
        if let Some(revoked) = &revoked
            && held_b.splits.contains_key(revoked)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let revoked = revoked.expect("A was asked to give a split up");

    // The move was replay-free — otherwise the latencies would describe
    // something other than a cooperative revocation.
    assert_eq!(
        held_b.splits[&revoked].1.as_ref().map(|p| p.watermark),
        Some(TAIL),
        "the revoked split must resume from A's final commit"
    );

    // Let B's assignment-latency observation land: it is recorded on the
    // claim that transferred ownership, which the loop above waited for,
    // but the exporter render is a separate read.
    std::thread::sleep(Duration::from_millis(50));
    let text = handle.render();

    assert!(
        counter_sum(
            &text,
            "etl_coordination_revocations_total",
            r#"outcome="requested""#
        )
        .is_some_and(|c| c > 0.0),
        "revocations_total{{outcome=requested}} never moved:\n{text}"
    );
    assert!(
        counter_sum(
            &text,
            "etl_coordination_revocations_total",
            r#"outcome="drained""#
        )
        .is_some_and(|c| c > 0.0),
        "revocations_total{{outcome=drained}} never moved — the clean release \
         is not counted:\n{text}"
    );
    assert!(
        histogram_count(&text, "etl_coordination_drain_duration_seconds").is_some_and(|c| c > 0.0),
        "the releasing worker never observed a drain duration:\n{text}"
    );
    let name = "etl_coordination_assignment_latency_seconds";
    assert!(
        histogram_count(&text, name).is_some_and(|c| c > 0.0),
        "the gaining worker never observed an assignment latency — the \
         awaited-split timer is not reaching the claim:\n{text}"
    );
    // Value, not just count: B was held off its claim for at least
    // AWAIT_FLOOR, so at least one observation must land above half of it.
    // The regression recorded every latency as ~0 (the timer was cleared
    // mid-drain), so this count drops to 0 against it even though the many
    // fast claims keep `_count` positive.
    let floor = AWAIT_FLOOR.as_secs_f64() / 2.0;
    assert!(
        observations_above(&text, name, floor) >= 1.0,
        "no assignment latency exceeded {floor:.3}s despite B waiting \
         {:.3}s — the timer is being cleared mid-drain and re-read as ~0:\n{text}",
        AWAIT_FLOOR.as_secs_f64()
    );
    assert!(
        saw_draining,
        "etl_coordination_splits_draining never read 1 while a drain was in flight"
    );
}
