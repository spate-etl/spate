use super::*;

const KEY: &str = "split.a";
const SLEEPER: u32 = 200;

/// The inputs to [`classify`], owned, defaulting to a clean ordinary run.
struct Run {
    setup_failure: Option<&'static str>,
    scenario: Scenario,
    worker_exits: Vec<WorkerExit>,
    timed_out: bool,
    violations: Vec<Violation>,
    expectations: Vec<String>,
    lost_replies: LostReplies,
    health: Vec<HealthPoll>,
}

impl Run {
    fn ordinary() -> Run {
        Run {
            setup_failure: None,
            scenario: Scenario::Ordinary,
            worker_exits: vec![exit(0)],
            timed_out: false,
            violations: Vec::new(),
            expectations: Vec::new(),
            lost_replies: LostReplies::default(),
            health: Vec::new(),
        }
    }

    fn stopped(broken_fence: bool) -> Run {
        Run {
            scenario: Scenario::StoppedWriter {
                broken_fence,
                stop: Some(StopSeen {
                    key: KEY.to_owned(),
                    pid: SLEEPER,
                    resend_rev: Some(9),
                }),
            },
            ..Run::ordinary()
        }
    }

    fn violation(mut self, v: Violation) -> Run {
        self.violations.push(v);
        self
    }

    fn container_down(mut self) -> Run {
        self.health = vec![poll(1, false), poll(2, false)];
        self
    }

    fn kind(&self) -> Kind {
        classify(&Evidence {
            setup_failure: self.setup_failure,
            scenario: &self.scenario,
            worker_exits: &self.worker_exits,
            timed_out: self.timed_out,
            violations: &self.violations,
            expectations: &self.expectations,
            lost_replies: &self.lost_replies,
            health: &self.health,
        })
        .0
    }
}

fn exit(code: i32) -> WorkerExit {
    WorkerExit {
        instance: "w0".to_owned(),
        pid: 100,
        code: Some(code),
        signal: None,
        scheduled: false,
    }
}

fn poll(t_ms: u64, ok: bool) -> HealthPoll {
    HealthPoll {
        t_ms,
        container: "nats".to_owned(),
        ok,
        error: (!ok).then(|| "connection refused".to_owned()),
    }
}

fn found(check: Check) -> Violation {
    Violation {
        check,
        key: Some("split.b".to_owned()),
        rev: Some(4),
        instance: Some("w1".to_owned()),
        pid: Some(101),
        detail: String::new(),
    }
}

/// The `EpochRegressed` a broken fence must produce: on the stopped key, at
/// the re-send's revision, by the stopped process.
fn regressed() -> Violation {
    Violation {
        key: Some(KEY.to_owned()),
        rev: Some(9),
        pid: Some(SLEEPER),
        ..found(Check::EpochRegressed)
    }
}

/// A broken fence that the oracle did not catch on the stopped key is an
/// expectation failure, whatever else the aftermath produced.
#[test]
fn broken_fence_without_epoch_regressed_is_expectation_even_with_other_violations() {
    let aftermath = Run::stopped(true)
        .violation(found(Check::TwoOwners))
        .violation(found(Check::UnexplainedDuplicate));
    assert_eq!(aftermath.kind(), Kind::Expectation);
    let other_pid = Run::stopped(true).violation(Violation {
        pid: Some(SLEEPER + 1),
        ..regressed()
    });
    assert_eq!(other_pid.kind(), Kind::Expectation);
    let other_rev = Run::stopped(true).violation(Violation {
        rev: Some(8),
        ..regressed()
    });
    assert_eq!(other_rev.kind(), Kind::Expectation);
}

/// A broken fence whose re-send the oracle caught passes, even with other
/// violations from the aftermath.
#[test]
fn broken_fence_with_epoch_regressed_passes_despite_other_violations() {
    let run = Run::stopped(true)
        .violation(regressed())
        .violation(found(Check::TwoOwners))
        .violation(found(Check::Unfinished));
    assert_eq!(run.kind(), Kind::Pass);
}

/// A violation in a control with the correct fence is real.
#[test]
fn stopped_writer_control_violation_stays_violation() {
    assert_eq!(
        Run::stopped(false).violation(regressed()).kind(),
        Kind::Violation
    );
    assert_eq!(Run::stopped(false).kind(), Kind::Pass);
}

/// A stopped split the peer never claimed is a property-4 violation, in the
/// control and in the broken-fence scenario alike.
#[test]
fn peer_never_claimed_is_a_violation() {
    let missed = || found(Check::StoppedSplitNotReassigned);
    assert_eq!(
        Run::stopped(false).violation(missed()).kind(),
        Kind::Violation
    );
    assert_eq!(
        Run::stopped(true).violation(missed()).kind(),
        Kind::Violation
    );
}

/// A killed leader that no other instance replaced is a property 4
/// violation, and fails an ordinary run as one.
#[test]
fn leader_not_replaced_is_a_property_4_violation() {
    assert_eq!(Check::LeaderNotReplaced.property(), 4);
    assert_eq!(
        Run::ordinary()
            .violation(found(Check::LeaderNotReplaced))
            .kind(),
        Kind::Violation
    );
}

/// A stopped-writer scenario whose stop never fired did not test anything.
#[test]
fn stop_line_never_seen_is_expectation() {
    for broken_fence in [false, true] {
        let run = Run {
            scenario: Scenario::StoppedWriter {
                broken_fence,
                stop: None,
            },
            ..Run::ordinary()
        };
        assert_eq!(run.kind(), Kind::Expectation);
    }
}

/// A drawn lost reply that left no `err_after_land` line in any journal is an
/// expectation failure.
#[test]
fn err_after_land_drawn_with_no_line_is_expectation() {
    let run = Run {
        lost_replies: LostReplies {
            drawn: true,
            lines: 0,
            unexplained: Vec::new(),
        },
        ..Run::ordinary()
    };
    assert_eq!(run.kind(), Kind::Expectation);
}

/// A run that timed out with a split left unfinished is a violation.
#[test]
fn running_timeout_with_an_unfinished_split_is_violation() {
    let run = Run {
        timed_out: true,
        ..Run::ordinary()
    }
    .violation(found(Check::Unfinished));
    assert_eq!(run.kind(), Kind::Violation);
}

/// A run that timed out with every split complete is a worker failure.
#[test]
fn running_timeout_with_every_split_complete_is_worker() {
    let run = Run {
        timed_out: true,
        ..Run::ordinary()
    };
    assert_eq!(run.kind(), Kind::Worker);
}

/// A worker that failed while a container was down is infrastructure, and a
/// setup failure is too.
#[test]
fn worker_exit_while_a_container_was_down_is_harness() {
    let run = Run {
        worker_exits: vec![exit(2)],
        ..Run::ordinary()
    }
    .container_down();
    assert_eq!(run.kind(), Kind::Harness);
    let setup = Run {
        setup_failure: Some("image pull failed"),
        ..Run::ordinary()
    };
    assert_eq!(setup.kind(), Kind::Harness);
}

/// A worker that failed while every container stayed healthy is a worker
/// failure, and one the harness killed is not.
#[test]
fn worker_exit_with_healthy_containers_is_worker() {
    let mut run = Run {
        worker_exits: vec![exit(2)],
        ..Run::ordinary()
    };
    run.health = vec![poll(1, true), poll(2, true)];
    assert_eq!(run.kind(), Kind::Worker);
    let signalled = Run {
        worker_exits: vec![WorkerExit {
            code: None,
            signal: Some(6),
            ..exit(0)
        }],
        ..Run::ordinary()
    };
    assert_eq!(signalled.kind(), Kind::Worker);
    let scheduled = Run {
        worker_exits: vec![WorkerExit {
            code: None,
            signal: Some(9),
            scheduled: true,
            ..exit(0)
        }],
        ..Run::ordinary()
    };
    assert_eq!(scheduled.kind(), Kind::Pass);
}

/// A worker that exited 3, unable to write its journal, is a harness failure,
/// even beside a violation the incomplete journal could explain.
#[test]
fn journal_write_failure_is_harness() {
    let run = Run {
        worker_exits: vec![exit(0), exit(3)],
        ..Run::ordinary()
    };
    assert_eq!(run.kind(), Kind::Harness);
    for check in [Check::RecordMissing, Check::AheadOfRows, Check::TwoOwners] {
        let with = Run {
            worker_exits: vec![exit(0), exit(3)],
            ..Run::ordinary()
        };
        assert_eq!(
            with.violation(found(check)).kind(),
            Kind::Harness,
            "{check:?}"
        );
    }
    let caught = Run {
        worker_exits: vec![exit(0), exit(3)],
        ..Run::stopped(true)
    }
    .violation(regressed());
    assert_eq!(caught.kind(), Kind::Harness);
}

/// A property-3 or property-5 violation stays a violation through a container
/// outage outside a broken-fence scenario; every other failure during an
/// outage is infrastructure.
#[test]
fn p3_or_p5_violation_with_a_container_down_is_violation() {
    for check in [Check::AheadOfRows, Check::TwoOwners] {
        let run = Run::ordinary().violation(found(check)).container_down();
        assert_eq!(run.kind(), Kind::Violation, "{check:?}");
    }
    let p2 = Run::ordinary()
        .violation(found(Check::UnexplainedDuplicate))
        .container_down();
    assert_eq!(p2.kind(), Kind::Harness);
    let broken = Run::stopped(true)
        .violation(found(Check::TwoOwners))
        .container_down();
    assert_eq!(broken.kind(), Kind::Harness);
    let caught = Run::stopped(true).violation(regressed()).container_down();
    assert_eq!(caught.kind(), Kind::Pass, "a pass is never a failure");
}

/// One failed poll is not an outage; two consecutive failed polls of one
/// container are.
#[test]
fn one_failed_health_poll_is_not_a_container_failure() {
    let other = |t_ms| HealthPoll {
        container: "seaweedfs".to_owned(),
        ..poll(t_ms, false)
    };
    assert_eq!(container_outage(&[poll(1, false), poll(2, true)]), None);
    assert_eq!(
        container_outage(&[poll(1, false), poll(2, true), poll(3, false)]),
        None
    );
    assert_eq!(container_outage(&[poll(1, false), other(2)]), None);
    assert_eq!(
        container_outage(&[poll(1, false), other(2), poll(3, false)]),
        Some(("nats", 3))
    );
    let mut run = Run {
        worker_exits: vec![exit(2)],
        ..Run::ordinary()
    };
    run.health = vec![poll(1, false), poll(2, true)];
    assert_eq!(run.kind(), Kind::Worker);
}

/// A lost reply with no accepted recovery after it is an expectation failure,
/// and a delivery violation in the same run outranks it.
#[test]
fn unexercised_err_after_land_is_expectation_and_yields_to_a_violation() {
    let unexercised = || Run {
        lost_replies: LostReplies {
            drawn: true,
            lines: 1,
            unexplained: vec!["split.a rev 5: no recovery followed".to_owned()],
        },
        ..Run::ordinary()
    };
    assert_eq!(unexercised().kind(), Kind::Expectation);
    assert_eq!(
        unexercised().violation(found(Check::AheadOfRows)).kind(),
        Kind::Violation
    );
    let exercised = Run {
        lost_replies: LostReplies {
            drawn: true,
            lines: 1,
            unexplained: Vec::new(),
        },
        ..Run::ordinary()
    };
    assert_eq!(exercised.kind(), Kind::Pass);
}

/// A property-1 or property-4 violation during a container outage is
/// infrastructure.
#[test]
fn p1_or_p4_violation_with_a_container_down_is_harness() {
    for check in [Check::RecordMissing, Check::Unfinished] {
        let run = Run::ordinary().violation(found(check)).container_down();
        assert_eq!(run.kind(), Kind::Harness, "{check:?}");
    }
}

/// An `EpochRegressed` by the stopped process at the re-send's revision on
/// another key does not pass a broken fence.
#[test]
fn broken_fence_epoch_regressed_on_another_key_is_expectation() {
    let run = Run::stopped(true).violation(Violation {
        key: Some("split.b".to_owned()),
        ..regressed()
    });
    assert_eq!(run.kind(), Kind::Expectation);
}

/// A broken fence the oracle caught still fails when a scenario assertion
/// failed: rule 3 comes before rule 4.
#[test]
fn caught_broken_fence_with_a_failed_assertion_is_expectation() {
    let mut run = Run::stopped(true).violation(regressed());
    run.expectations = vec!["two processes never held claims at overlapping times".to_owned()];
    assert_eq!(run.kind(), Kind::Expectation);
    let lost = Run {
        lost_replies: LostReplies {
            drawn: true,
            lines: 0,
            unexplained: Vec::new(),
        },
        ..Run::stopped(true).violation(regressed())
    };
    assert_eq!(lost.kind(), Kind::Expectation);
}

/// `outcome.json` carries each violation's property number beside its check,
/// and reads back without it.
#[test]
fn outcome_json_numbers_each_violation_with_its_property() {
    let outcome = Outcome {
        scenario: "s".to_owned(),
        store: "nats".to_owned(),
        instances: 1,
        seed: 1,
        replay: String::new(),
        stage: Stage::Oracle,
        kind: Kind::Violation,
        message: String::new(),
        violations: vec![found(Check::RecordMissing), found(Check::TwoOwners)],
        expectations: Vec::new(),
        faults_fired: Vec::new(),
    };
    let json: serde_json::Value = serde_json::to_value(&outcome).unwrap();
    let numbered: Vec<(&str, u64)> = json["violations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["check"].as_str().unwrap(),
                v["property"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(numbered, [("RecordMissing", 1), ("TwoOwners", 5)]);
    let back: Outcome = serde_json::from_value(json).unwrap();
    assert_eq!(back, outcome);
}
