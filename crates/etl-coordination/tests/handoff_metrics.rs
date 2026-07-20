//! The cooperative-handoff observability seam, end to end.
//!
//! Registering a metric family proves nothing about whether anything ever
//! records into it — a family in this repo once read 0 forever because only
//! its registration was asserted. These tests therefore drive a REAL handoff
//! between two coordinators built with metrics and assert the series moved.
//!
//! This lives in its own file deliberately: the exporter installs a
//! process-global recorder, and each `tests/*.rs` is its own test binary, so
//! nothing else in the suite can race the install.

mod support;

use etl_coordination::StoreCoordinator;
use etl_core::coordination::{CoordinationEvent, SplitCoordinator, SplitProgress};
use etl_core::metrics::{ComponentLabels, CoordinationMetrics, Exporter, MetricsSettings, install};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Instant;
use support::{PhasedPlanner, runtime, split_id, store};

/// Total observations recorded into a labeled histogram phase, summed over
/// every component's series. `None` = the family is absent entirely.
///
/// Summing matters: handles are pre-registered, so BOTH workers publish
/// BOTH phases and two of the four series are legitimately 0. Taking the
/// first match would read whichever component the exporter happened to
/// render first and call a wired seam dead.
fn histogram_count(text: &str, name: &str, phase: &str) -> Option<f64> {
    let needle = format!("{name}_count");
    let label = format!("phase=\"{phase}\"");
    let mut seen = false;
    let mut total = 0.0;
    for line in text
        .lines()
        .filter(|l| l.starts_with(&needle) && l.contains(&label))
    {
        if let Some(value) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            seen = true;
            total += value;
        }
    }
    seen.then_some(total)
}

/// A real consented handoff must move BOTH phases of
/// `etl_coordination_handoff_duration_seconds` — `request` on the asking
/// side, `drain` on the giving side — and must expose the in-flight gauge.
#[test]
fn a_real_handoff_records_both_latency_phases() {
    let handle = install(&MetricsSettings {
        exporter: Exporter::Prometheus,
        listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        ..MetricsSettings::default()
    })
    .expect("install the exporter");

    let rt = runtime();
    let store = store();
    let ids = ["m0", "m1", "m2", "m3"];
    let planner = || Box::new(PhasedPlanner::one_final("handoff-metrics:v1", &ids));

    let victim_labels = ComponentLabels::new("coord-metrics", "worker-a", "s3");
    let mut a_config = support::config(Some("worker-a"));
    // One at a time: the loop below hand-plays A's embedder.
    a_config.handoff_max_grants = 1;
    let mut a = StoreCoordinator::new(
        store.clone(),
        a_config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&victim_labels)),
    )
    .expect("coordinator");
    a.start(planner()).unwrap();
    let mut held_a = support::Held::default();
    support::drive(&mut a, &mut held_a, "A claiming everything", |h| {
        h.splits.len() == 4
    });
    support::commit_held(&mut a, &held_a);

    // Patient, so the move below can only be a consented handoff.
    let requester_labels = ComponentLabels::new("coord-metrics", "worker-b", "s3");
    let mut b_config = support::config(Some("worker-b"));
    b_config.handoff_rounds = 1_000;
    let mut b = StoreCoordinator::new(
        store.clone(),
        b_config,
        rt.handle().clone(),
        Some(CoordinationMetrics::new(&requester_labels)),
    )
    .expect("coordinator");
    b.start(planner()).unwrap();
    let mut held_b = support::Held::default();

    const TAIL: i64 = 42;
    let mut handed: Option<String> = None;
    let mut saw_in_flight = false;
    let deadline = Instant::now() + support::DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "the handoff never completed");
        for event in a.poll().unwrap() {
            if let CoordinationEvent::HandoffRequested { split } = &event
                && handed.is_none()
            {
                let id = split.as_str().to_string();
                // Sample the gauge while the drain is genuinely in flight:
                // the grant is annotated and not yet released.
                if handle.render().lines().any(|l| {
                    l.starts_with("etl_coordination_handoffs_in_flight") && l.ends_with(" 1")
                }) {
                    saw_in_flight = true;
                }
                a.commit(&split_id(&id), &SplitProgress::new(TAIL, b"tail".to_vec()))
                    .unwrap();
                a.release_handoff(&[split_id(&id)]).unwrap();
                handed = Some(id);
            }
            held_a.fold(vec![event]);
        }
        held_b.fold(b.poll().unwrap());
        if let Some(handed) = &handed
            && held_b.splits.contains_key(handed)
        {
            break;
        }
        std::thread::sleep(support::POLL_INTERVAL);
    }
    let handed = handed.expect("A was asked to hand a split over");

    // The move itself was replay-free — otherwise the latencies would be
    // describing something other than a cooperative handoff.
    assert_eq!(
        held_b.splits[&handed].1.as_ref().map(|p| p.watermark),
        Some(TAIL),
        "the handed split must resume from the victim's final commit"
    );

    let name = "etl_coordination_handoff_duration_seconds";
    let text = handle.render();
    assert!(
        histogram_count(&text, name, "drain").is_some_and(|c| c > 0.0),
        "the victim never observed a `drain` duration — the seam from \
         `release_splits` to the histogram is not wired:\n{text}"
    );
    assert!(
        histogram_count(&text, name, "request").is_some_and(|c| c > 0.0),
        "the requester never observed a `request` duration — the shortfall \
         clock never reached an `AcquireReason::Handoff` claim:\n{text}"
    );
    assert!(
        saw_in_flight,
        "`etl_coordination_handoffs_in_flight` never read 1 while a grant \
         was mid-drain"
    );
}
