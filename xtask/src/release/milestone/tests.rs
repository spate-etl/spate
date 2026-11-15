use std::cell::RefCell;
use std::collections::BTreeMap;

use super::*;
use crate::release::fakes::*;
use crate::release::io::{DryForge, Forge, Issue, Milestone, MilestoneEvent, Target, Tracked};

/// When the previous release and this one landed.
const AFTER: &str = "2026-10-11T22:40:05Z";
const UNTIL: &str = "2026-11-01T17:12:38Z";
const INSIDE: &str = "2026-10-20T12:00:00Z";

fn issue(number: u64) -> Issue {
    Issue {
        number,
        title: format!("issue {number}"),
        url: format!("https://x/{number}"),
    }
}

fn closed(number: u64, reason: &str, at: &str, milestone: Option<u64>) -> Tracked {
    Tracked {
        issue: issue(number),
        state_reason: Some(reason.to_owned()),
        closed_at: Some(at.to_owned()),
        milestone,
        labels: Vec::new(),
    }
}

fn open(number: u64, milestone: u64, labels: &[&str]) -> Tracked {
    Tracked {
        issue: issue(number),
        state_reason: None,
        closed_at: None,
        milestone: Some(milestone),
        labels: labels.iter().map(|l| (*l).to_owned()).collect(),
    }
}

fn milestone(number: u64, title: &str, open: bool) -> Milestone {
    Milestone {
        number,
        title: title.to_owned(),
        open,
    }
}

fn added(title: &str, at: &str) -> MilestoneEvent {
    MilestoneEvent {
        added: true,
        title: title.to_owned(),
        at: at.to_owned(),
    }
}

fn removed(title: &str, at: &str) -> MilestoneEvent {
    MilestoneEvent {
        added: false,
        title: title.to_owned(),
        at: at.to_owned(),
    }
}

fn target(m: &Milestone) -> Target {
    Target::from(m)
}

/// The milestones the 0.4.0 cases run against.
fn milestones() -> Vec<Milestone> {
    vec![
        milestone(3, "v0.3.0", false),
        milestone(4, "v0.4.0", true),
        milestone(12, "v0.5.0", true),
        milestone(5, "v0.6.0", true),
    ]
}

fn reads(
    milestones: Vec<Milestone>,
    closed: Vec<Tracked>,
    open: Vec<(Tracked, Vec<MilestoneEvent>)>,
) -> Reads {
    Reads {
        milestones,
        closed,
        open,
        after: Some(seconds(AFTER).unwrap()),
        until: seconds(UNTIL).unwrap(),
    }
}

/// Only an issue closed as completed inside `(after, until]`, and not already
/// on the release's milestone, is stamped, whatever milestone it carries.
#[test]
fn a_stamp_takes_each_issue_completed_inside_the_window() {
    let closed = vec![
        closed(1, "completed", UNTIL, None),
        closed(2, "completed", INSIDE, None),
        closed(3, "completed", INSIDE, Some(5)),
        closed(4, "not_planned", INSIDE, None),
        closed(5, "completed", AFTER, None),
        closed(6, "completed", "2026-10-01T00:00:00Z", None),
        closed(7, "completed", "2026-11-01T17:12:39Z", None),
        closed(8, "completed", INSIDE, Some(4)),
    ];
    let changes = plan(v("0.4.0"), &reads(milestones(), closed.clone(), vec![])).unwrap();
    assert_eq!(
        changes,
        [
            Change::Stamp(issue(1)),
            Change::Stamp(issue(2)),
            Change::Stamp(issue(3)),
            Change::Close,
        ]
    );

    // The first release has no lower bound.
    let first = Reads {
        after: None,
        ..reads(milestones(), closed, vec![])
    };
    let stamped: Vec<u64> = plan(v("0.4.0"), &first)
        .unwrap()
        .into_iter()
        .filter_map(|c| match c {
            Change::Stamp(i) => Some(i.number),
            _ => None,
        })
        .collect();
    assert_eq!(stamped, [1, 2, 3, 5, 6]);
}

/// An unfinished issue moves to the open version milestone with the lowest
/// version above the release, never to a closed one, the release's own, or one
/// whose title is not a version.
#[test]
fn an_unfinished_issue_moves_to_the_next_open_version_milestone() {
    let mut all = milestones();
    all.push(milestone(20, "v0.4.1", false));
    all.push(milestone(21, "Backlog", true));
    all.push(milestone(22, "v0.4.0-next", true));
    let changes = plan(
        v("0.4.0"),
        &reads(all.clone(), vec![], vec![(open(9, 4, &[]), vec![])]),
    )
    .unwrap();
    assert_eq!(
        changes,
        [
            Change::Move {
                issue: issue(9),
                to: target(&all[2]),
                held: None,
            },
            Change::Close,
        ]
    );
}

/// What happens at 0.6.0 to one open issue on v0.6.0 with `events` and
/// `labels`.
fn fate(events: Vec<MilestoneEvent>, labels: &[&str]) -> Change {
    let all = vec![
        milestone(5, "v0.6.0", true),
        milestone(6, "v0.7.0", true),
    ];
    let changes = plan(
        v("0.6.0"),
        &reads(all, vec![], vec![(open(9, 5, labels), events)]),
    )
    .unwrap();
    assert_eq!(changes.last(), Some(&Change::Close));
    assert_eq!(changes.len(), 2, "{changes:?}");
    changes[0].clone()
}

fn moved(held: Option<&'static str>) -> Change {
    Change::Move {
        issue: issue(9),
        to: Target {
            title: "v0.7.0".to_owned(),
            number: Some(6),
        },
        held,
    }
}

fn demoted(label: bool) -> Change {
    Change::Demote {
        issue: issue(9),
        label,
    }
}

/// A move off an earlier release's milestone, the two events in one second.
fn slipped_once() -> Vec<MilestoneEvent> {
    vec![
        added("v0.4.0", "2026-09-26T04:47:43Z"),
        removed("v0.4.0", "2026-10-15T20:10:30Z"),
        added("v0.6.0", "2026-10-15T20:10:30Z"),
    ]
}

/// An issue moved off an earlier release returns to the backlog with
/// `priority: medium`, and one with a priority keeps it and gets none.
#[test]
fn an_issue_that_slipped_before_returns_to_the_backlog() {
    assert_eq!(fate(slipped_once(), &[]), demoted(true));
    assert_eq!(
        fate(slipped_once(), &["area: docs", "priority: high"]),
        demoted(false)
    );
    assert_eq!(fate(vec![], &[]), moved(None));
    assert_eq!(fate(vec![added("v0.6.0", INSIDE)], &[]), moved(None));
}

/// Only a move off a milestone below the release is a slip: neither a move
/// off the release's own milestone and back, nor a pull forward from a later
/// one, nor a move off a milestone whose title is not a version.
#[test]
fn only_a_move_off_an_earlier_release_is_a_slip() {
    let away_and_back = vec![
        added("v0.6.0", "2026-09-01T00:00:00Z"),
        removed("v0.6.0", "2026-09-02T00:00:00Z"),
        added("v0.7.0", "2026-09-02T00:00:00Z"),
        removed("v0.7.0", "2026-09-03T00:00:00Z"),
        added("v0.6.0", "2026-09-03T00:00:01Z"),
    ];
    assert_eq!(fate(away_and_back, &[]), moved(None));
    let pulled_forward = vec![
        added("v0.7.0", "2026-09-01T00:00:00Z"),
        removed("v0.7.0", "2026-09-02T00:00:00Z"),
        added("v0.6.0", "2026-09-02T00:00:00Z"),
    ];
    assert_eq!(fate(pulled_forward, &[]), moved(None));
    let off_a_name = vec![
        added("Later", "2026-09-01T00:00:00Z"),
        removed("Later", "2026-09-02T00:00:00Z"),
        added("v0.6.0", "2026-09-02T00:00:00Z"),
    ];
    assert_eq!(fate(off_a_name, &[]), moved(None));
}

/// The events of one move may fall a second or a minute apart; further apart,
/// the removal was a return to the backlog.
#[test]
fn a_move_is_a_removal_followed_within_a_minute() {
    let apart = |at: &str| {
        vec![
            added("v0.4.0", "2026-09-26T04:47:43Z"),
            removed("v0.4.0", "2026-10-15T20:10:30Z"),
            added("v0.6.0", at),
        ]
    };
    assert_eq!(fate(apart("2026-10-15T20:10:31Z"), &[]), demoted(true));
    assert_eq!(fate(apart("2026-10-15T20:11:30Z"), &[]), demoted(true));
    assert_eq!(fate(apart("2026-10-15T20:11:31Z"), &[]), moved(None));
}

/// A return to the backlog after a slip starts the count again, so an issue
/// committed afresh gets one move.
#[test]
fn a_return_to_the_backlog_resets_the_slip() {
    let mut events = slipped_once();
    events.push(removed("v0.6.0", "2026-10-20T00:00:00Z"));
    events.push(added("v0.6.0", "2026-10-25T00:00:00Z"));
    assert_eq!(fate(events, &[]), moved(None));

    // A slip after the return counts again.
    let mut events = slipped_once();
    events.push(removed("v0.6.0", "2026-10-20T00:00:00Z"));
    events.push(added("v0.5.0", "2026-10-25T00:00:00Z"));
    events.push(removed("v0.5.0", "2026-11-01T00:00:00Z"));
    events.push(added("v0.6.0", "2026-11-01T00:00:00Z"));
    assert_eq!(fate(events, &[]), demoted(true));
}

/// An issue labeled `security` or `release blocker` keeps a milestone, and its
/// change says why only when it slipped before.
#[test]
fn a_security_issue_or_blocker_moves_instead_of_returning() {
    assert_eq!(
        fate(slipped_once(), &[SECURITY_LABEL]),
        moved(Some(SECURITY_LABEL))
    );
    assert_eq!(
        fate(slipped_once(), &[RELEASE_BLOCKER]),
        moved(Some(RELEASE_BLOCKER))
    );
    assert_eq!(fate(vec![], &[SECURITY_LABEL]), moved(None));
}

/// An unfinished issue with nowhere to go refuses the whole plan; with no
/// unfinished issue, a missing next milestone is no matter.
#[test]
fn an_issue_with_no_next_milestone_refuses_the_plan() {
    let only = vec![milestone(4, "v0.4.0", true)];
    let err = plan(
        v("0.4.0"),
        &reads(
            only.clone(),
            vec![closed(1, "completed", INSIDE, None)],
            vec![(open(9, 4, &[]), vec![])],
        ),
    )
    .unwrap_err();
    assert!(err.message.contains("no later open milestone"), "{}", err.message);
    assert_eq!(
        plan(v("0.4.0"), &reads(only, vec![], vec![])).unwrap(),
        [Change::Close]
    );
}

/// A missing milestone is created for what the release shipped, a patch's or a
/// minor's, and a closed one is reopened for it; with nothing to stamp, neither
/// is touched.
#[test]
fn the_release_milestone_is_created_or_reopened_for_its_stamps() {
    let stamp = || vec![closed(1, "completed", INSIDE, None)];
    for version in ["0.4.1", "0.13.0"] {
        assert_eq!(
            plan(v(version), &reads(milestones(), stamp(), vec![])).unwrap(),
            [Change::Create, Change::Stamp(issue(1)), Change::Close],
            "{version}"
        );
        assert!(
            plan(v(version), &reads(milestones(), vec![], vec![]))
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        plan(v("0.3.0"), &reads(milestones(), stamp(), vec![])).unwrap(),
        [Change::Reopen, Change::Stamp(issue(1)), Change::Close]
    );
    assert!(
        plan(v("0.3.0"), &reads(milestones(), vec![], vec![]))
            .unwrap()
            .is_empty()
    );
}

fn origin() -> Vec<(String, String)> {
    ["v0.2.0", "v0.4.0", "v0.1.0", "v0.3.0", "v0.10.0", "nightly"]
        .iter()
        .map(|t| ((*t).to_owned(), format!("sha-{t}")))
        .collect()
}

/// The window runs from the newest release tag below the version, in version
/// order; the first release has none, and an untagged version refuses.
#[test]
fn the_window_opens_at_the_newest_tag_below_the_version() {
    assert_eq!(
        tags(&origin(), v("0.4.0")).unwrap(),
        ("sha-v0.4.0".to_owned(), Some("sha-v0.3.0".to_owned()))
    );
    assert_eq!(
        tags(&origin(), v("0.10.0")).unwrap(),
        ("sha-v0.10.0".to_owned(), Some("sha-v0.4.0".to_owned()))
    );
    assert_eq!(
        tags(&origin(), v("0.1.0")).unwrap(),
        ("sha-v0.1.0".to_owned(), None)
    );
    let err = tags(&origin(), v("0.5.0")).unwrap_err();
    assert!(err.message.contains("no v0.5.0 tag"), "{}", err.message);
}

/// GitHub's timestamps, read as seconds since the epoch, across a leap day.
#[test]
fn a_timestamp_reads_as_seconds_since_the_epoch() {
    for (at, want) in [
        ("1970-01-01T00:00:00Z", 0),
        ("1969-12-31T23:59:59Z", -1),
        ("2000-02-29T12:00:00Z", 951_825_600),
        ("2000-03-01T00:00:00Z", 951_868_800),
        ("2026-11-01T17:12:38Z", 1_793_553_158),
    ] {
        assert_eq!(seconds(at).unwrap(), want, "{at}");
    }
    for bad in [
        "",
        "2026-11-01 17:12:38Z",
        "2026-11-01T17:12:38",
        "2026-11-01T17:12:38+00:00",
        "2026-1x-01T17:12:38Z",
        "2026/11/01T17:12:38Z",
        "2026-11/01T17:12:38Z",
        "2026-11-01T17-12:38Z",
        "2026-11-01T17:12-38Z",
    ] {
        assert!(seconds(bad).is_err(), "{bad}");
    }
}

/// Each printed change escapes the issue's title and url, which an outside
/// author writes.
#[test]
fn a_printed_change_escapes_the_issue_text() {
    let hostile = Issue {
        number: 1,
        title: "x\n::warning::boom ##[error]".to_owned(),
        url: "https://x/1\r".to_owned(),
    };
    let line = Change::Stamp(hostile.clone()).describe("v0.4.0");
    assert_eq!(
        line,
        format!(
            "stamp #1 on v0.4.0: {} ({})",
            printable(&hostile.title),
            printable(&hostile.url)
        )
    );
    assert!(!line.contains('\n') && !line.contains('\r') && !line.contains("##["));
}

/// Each change prints as one line naming what it does.
#[test]
fn each_change_prints_what_it_does() {
    let to = Target {
        title: "v0.5.0".to_owned(),
        number: Some(12),
    };
    let lines: Vec<String> = [
        Change::Create,
        Change::Reopen,
        Change::Stamp(issue(1)),
        Change::Move {
            issue: issue(2),
            to: to.clone(),
            held: None,
        },
        Change::Move {
            issue: issue(3),
            to,
            held: Some(SECURITY_LABEL),
        },
        Change::Demote {
            issue: issue(4),
            label: true,
        },
        Change::Demote {
            issue: issue(5),
            label: false,
        },
        Change::Close,
    ]
    .iter()
    .map(|c| c.describe("v0.4.0"))
    .collect();
    assert_eq!(
        lines,
        [
            "create v0.4.0",
            "reopen v0.4.0",
            "stamp #1 on v0.4.0: issue 1 (https://x/1)",
            "move #2 to v0.5.0: issue 2 (https://x/2)",
            "move #3 to v0.5.0, which slipped before and keeps a milestone as `security`: issue 3 (https://x/3)",
            "return #4 to the backlog, labeled `priority: medium`: issue 4 (https://x/4)",
            "return #5 to the backlog: issue 5 (https://x/5)",
            "close v0.4.0",
        ]
    );
}

/// A forge holding a backfill of 0.4.0: v0.4.0 closed with one issue left on
/// it, one completed issue it shipped without a milestone, one that slipped
/// before, and the tags and landing times of 0.3.0 and 0.4.0. The clone's own
/// last tag is ignored.
fn backfill(log: &Log) -> (FakeGit<'_>, FakeForge<'_>) {
    let mut git = FakeGit::new(log);
    git.last_tag = "v0.9.9".to_owned();
    git.remote.borrow_mut().extend([
        ("v0.3.0".to_owned(), "a3".to_owned()),
        ("v0.4.0".to_owned(), "a4".to_owned()),
    ]);
    let forge = FakeForge {
        milestones: RefCell::new(vec![
            milestone(4, "v0.4.0", false),
            milestone(12, "v0.5.0", true),
        ]),
        issues: RefCell::new(vec![
            (closed(1, "completed", INSIDE, None), false),
            (closed(2, "not_planned", INSIDE, None), false),
            (open(3, 4, &[]), true),
            (open(4, 4, &[]), true),
        ]),
        events: BTreeMap::from([(4, slipped_once_at("v0.4.0"))]),
        landed: BTreeMap::from([
            ("a3".to_owned(), AFTER.to_owned()),
            ("a4".to_owned(), UNTIL.to_owned()),
        ]),
        ..FakeForge::new(log)
    };
    (git, forge)
}

/// A move off v0.3.0 onto `onto`.
fn slipped_once_at(onto: &str) -> Vec<MilestoneEvent> {
    vec![
        added("v0.3.0", "2026-09-26T04:47:43Z"),
        removed("v0.3.0", "2026-10-11T22:50:00Z"),
        added(onto, "2026-10-11T22:50:00Z"),
    ]
}

/// The collaborators a [`Host`] holds that the milestone step never calls.
fn idle(log: &Log) -> (FakeRegistry, FakeWorkspace<'_>, Pauses) {
    (
        FakeRegistry::default(),
        FakeWorkspace::new(log, &[]),
        Pauses::default(),
    )
}

/// With `--apply`, the milestone is reopened, stamped, emptied and closed in
/// that order, the label before the milestone is cleared, and a second run
/// finds nothing to change.
#[test]
fn an_applied_run_writes_in_order_and_leaves_nothing_to_do() {
    let log = Log::default();
    let (git, forge) = backfill(&log);
    let (registry, workspace, pauses) = idle(&log);
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    run(&host, v("0.4.0"), true).unwrap();
    assert_eq!(
        log.entries(),
        [
            "reopen v0.4.0",
            "milestone #1 v0.4.0",
            "milestone #3 v0.5.0",
            "label #4 priority: medium",
            "milestone #4 none",
            "close v0.4.0",
        ]
    );
    assert_eq!(
        *forge.since_queried.borrow(),
        [Some(AFTER.to_owned()), Some(AFTER.to_owned())],
        "read once to plan and once to read back"
    );

    run(&host, v("0.4.0"), true).unwrap();
    assert_eq!(log.entries().len(), 6, "{:?}", log.entries());
}

/// A milestone write GitHub drops without an error fails the run, naming the
/// change still planned.
#[test]
fn a_write_that_did_not_take_fails_the_run() {
    let log = Log::default();
    let (git, mut forge) = backfill(&log);
    forge.drops_milestone_writes = true;
    let (registry, workspace, pauses) = idle(&log);
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    let err = run(&host, v("0.4.0"), true).unwrap_err();
    assert!(err.message.contains("did not take"), "{}", err.message);
    assert!(err.message.contains("stamp #1 on v0.4.0"), "{}", err.message);
}

/// Without `--apply` nothing is written, a milestone a real run would create
/// included, whose number the dry run never has.
#[test]
fn a_dry_run_writes_nothing() {
    let log = Log::default();
    let (git, forge) = backfill(&log);
    let (registry, workspace, pauses) = idle(&log);
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    run(&host, v("0.4.0"), false).unwrap();
    assert!(log.entries().is_empty(), "{:?}", log.entries());

    forge.milestones.borrow_mut().remove(0);
    run(&host, v("0.4.0"), false).unwrap();
    assert!(log.entries().is_empty(), "{:?}", log.entries());
    assert_eq!(forge.milestones.borrow().len(), 1);
}

/// A failed write stops the run, so the milestone is not closed over the
/// changes still to make.
#[test]
fn a_failed_write_stops_before_the_close() {
    for failing in [1, 3] {
        let log = Log::default();
        let (git, mut forge) = backfill(&log);
        forge.milestone_write_fails = Some(failing);
        let (registry, workspace, pauses) = idle(&log);
        let host = host!(&git, &forge, &registry, &workspace, pauses);
        assert!(run(&host, v("0.4.0"), true).is_err(), "#{failing}");
        assert!(!log.has("close"), "#{failing}: {:?}", log.entries());
    }
}

/// A run refuses before any write when origin has no tag for the version.
#[test]
fn a_version_origin_has_not_tagged_refuses() {
    let log = Log::default();
    let (git, forge) = backfill(&log);
    let (registry, workspace, pauses) = idle(&log);
    let host = host!(&git, &forge, &registry, &workspace, pauses);
    assert!(run(&host, v("0.5.0"), true).is_err());
    assert!(log.entries().is_empty());
}

/// The dry forge passes a failed read on; it never reads as an empty listing.
#[test]
fn a_dry_forge_passes_a_failed_milestone_read_on() {
    let log = Log::default();
    let forge = FakeForge {
        milestone_reads_fail: true,
        ..FakeForge::new(&log)
    };
    assert!(DryForge(&forge, true).milestones().is_err());
    assert!(DryForge(&forge, false).milestones().is_err());
    assert!(DryForge(&forge, false).landed_at("missing").is_err());
}
