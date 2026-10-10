use spate_s3::fuzz_seams::encode_position;
use spate_s3::{DescriptorObject, SplitRange};

use super::*;
use crate::journal::{LeaderAtKill, SCHEMA, Source, WriteOp};

const A: &str = "data/o000.ndjson";
const B: &str = "data/o001.ndjson";
const C: &str = "data/o002.ndjson";

fn id(object: u32, record: u32) -> String {
    format!("o{object:03}-r{record:06}")
}

/// Object `n` under `key` with `records` records of 10 bytes each.
fn object(n: u32, key: &str, records: u32) -> GeneratedObject {
    GeneratedObject {
        key: key.to_owned(),
        records: (0..records)
            .map(|r| GeneratedRecord {
                id: id(n, r),
                offset: u64::from(r) * 10,
            })
            .collect(),
    }
}

fn member(key: &str, size: u64) -> DescriptorObject {
    DescriptorObject {
        key: key.to_owned(),
        size,
        etag: Some(format!("etag-{key}")),
        last_modified_ms: 0,
    }
}

fn value(epoch: u64, owner: Option<&str>, watermark: Option<i64>, completed: bool) -> Progress {
    Progress {
        schema: SCHEMA,
        epoch,
        owner: owner.map(str::to_owned),
        watermark,
        completed,
        status: if completed {
            Status::Completed
        } else {
            Status::Runnable
        },
        attempts: 0,
    }
}

/// The watermark one past the record at `(ordinal, record)`.
fn after(ordinal: u32, record: u64) -> Option<i64> {
    encode_position(ordinal, record + 1)
}

/// A journal under construction. Each line is stamped one millisecond after
/// the last unless [`Proc::at`] moves the clock.
struct Proc {
    journal: ProcessJournal,
    calls: u64,
    clock: u64,
}

impl Proc {
    fn new(instance: &str, pid: u32) -> Proc {
        Proc {
            journal: ProcessJournal {
                instance: instance.to_owned(),
                pid,
                lines: Vec::new(),
            },
            calls: 0,
            clock: 0,
        }
    }

    /// Stamps the next line at `t_ms`.
    fn at(&mut self, t_ms: u64) {
        self.clock = t_ms;
    }

    fn push(&mut self, event: Event) {
        let t_ms = self.clock;
        self.clock += 1;
        self.journal.lines.push(Line { t_ms, event });
    }

    fn rows(&mut self, ids: &[String]) {
        self.push(Event::Rows { ids: ids.to_vec() });
    }

    fn seen(&mut self, key: &str, rev: u64, value: &Progress) {
        self.push(Event::Seen {
            key: key.to_owned(),
            rev,
            value: value.clone(),
            from: Source::Get,
        });
    }

    fn send(&mut self, key: &str, expected: u64, value: &Progress) -> u64 {
        self.calls += 1;
        self.push(Event::Send {
            call: self.calls,
            op: WriteOp::Update,
            key: key.to_owned(),
            expected: Some(expected),
            value: value.clone(),
        });
        self.calls
    }

    fn done(&mut self, call: u64, key: &str, reply: Reply) {
        self.push(Event::Done {
            call,
            key: key.to_owned(),
            reply,
        });
    }

    /// A write at `expected` that lands at `expected + 1`.
    fn write(&mut self, key: &str, expected: u64, value: &Progress) {
        self.landed(key, expected, expected + 1, value);
    }

    /// A write at `expected` that lands at `rev`.
    fn landed(&mut self, key: &str, expected: u64, rev: u64, value: &Progress) {
        let call = self.send(key, expected, value);
        self.done(call, key, Reply::Won(rev));
    }

    fn me(&self) -> Option<&str> {
        Some(self.journal.instance.as_str())
    }

    /// Claims `key` at revision 1, writes the first `half` of `ids` and
    /// commits to `mid`, then writes the rest and completes at `end`. Returns
    /// the completed value, landed at revision 4.
    fn deliver(
        &mut self,
        key: &str,
        ids: &[String],
        half: usize,
        mid: Option<i64>,
        end: Option<i64>,
    ) -> Progress {
        self.seen(key, 1, &value(0, None, None, false));
        self.write(key, 1, &value(1, self.me(), None, false));
        self.rows(&ids[..half]);
        self.write(key, 2, &value(1, self.me(), mid, false));
        self.rows(&ids[half..]);
        let completed = value(1, self.me(), end, true);
        self.write(key, 3, &completed);
        completed
    }
}

/// A run over three splits: `s0` reads objects A and C whole, and `s1` and
/// `s2` read the two halves of object B by byte range. Every split's progress
/// moves through revisions 1 to 4, so the keys share revision numbers.
struct Fixture {
    store: StoreKind,
    generated: Vec<GeneratedObject>,
    specs: Vec<(String, SplitDescriptor)>,
    processes: Vec<ProcessJournal>,
    finals: Vec<(String, u64, Progress)>,
    verdict: bool,
    faults: Vec<Line>,
}

impl Fixture {
    /// One worker, `w0`, delivers every split once.
    fn clean() -> Fixture {
        let mut fixture = Fixture::empty();
        let mut w0 = Proc::new("w0", 100);
        let s0 = w0.deliver(
            "split.s0",
            &[id(0, 0), id(0, 1), id(2, 0), id(2, 1)],
            2,
            after(0, 1),
            after(1, 1),
        );
        let s1 = w0.deliver(
            "split.s1",
            &[id(1, 0), id(1, 1)],
            1,
            after(0, 0),
            after(0, 1),
        );
        let s2 = w0.deliver(
            "split.s2",
            &[id(1, 2), id(1, 3)],
            1,
            after(0, 0),
            after(0, 1),
        );
        fixture.processes.push(w0.journal);
        fixture.finals = vec![
            ("split.s0".to_owned(), 4, s0),
            ("split.s1".to_owned(), 4, s1),
            ("split.s2".to_owned(), 4, s2),
        ];
        fixture
    }

    /// The data set and descriptors of [`Fixture::clean`], with no journals
    /// and no progress.
    fn empty() -> Fixture {
        Fixture {
            store: StoreKind::DynamoDb,
            generated: vec![object(0, A, 2), object(1, B, 4), object(2, C, 2)],
            specs: vec![
                (
                    "s0".to_owned(),
                    SplitDescriptor::new(vec![member(A, 20), member(C, 20)]),
                ),
                (
                    "s1".to_owned(),
                    SplitDescriptor::with_range(member(B, 40), SplitRange::new(0, 20, b'\n')),
                ),
                (
                    "s2".to_owned(),
                    SplitDescriptor::with_range(member(B, 40), SplitRange::new(20, 40, b'\n')),
                ),
            ],
            processes: Vec::new(),
            finals: Vec::new(),
            verdict: true,
            faults: Vec::new(),
        }
    }

    /// The tuning the fault runs use on the fixture's store.
    fn timing(&self) -> Timing {
        match self.store {
            StoreKind::Nats => Timing {
                lease_ms: 2_000,
                drain_deadline_ms: 5_000,
                op_timeout_ms: 500,
                poll_interval_ms: 0,
            },
            StoreKind::DynamoDb => Timing {
                lease_ms: 3_000,
                drain_deadline_ms: 5_000,
                op_timeout_ms: 1_000,
                poll_interval_ms: 500,
            },
        }
    }

    /// Property 2's bound after a fault window, written out from
    /// [`Fixture::timing`]: two leases, the drain deadline, two store
    /// timeouts, two poll intervals and two seconds.
    fn slack(&self) -> u64 {
        match self.store {
            StoreKind::Nats => 2 * 2_000 + 5_000 + 2 * 500 + 2_000,
            StoreKind::DynamoDb => 2 * 3_000 + 5_000 + 2 * 1_000 + 2 * 500 + 2_000,
        }
    }

    fn fault(&mut self, t_ms: u64, event: Event) {
        self.faults.push(Line { t_ms, event });
    }

    fn sweep(&self) -> Vec<SweptEntry> {
        let mut sweep = Vec::new();
        for (id, descriptor) in &self.specs {
            let record = serde_json::json!({
                "schema": SCHEMA,
                "id": id,
                "fp": 1,
                "generation": 1,
                "weight": 1,
                "descriptor": STANDARD.encode(descriptor.encode().unwrap()),
            });
            sweep.push(SweptEntry {
                key: format!("spec.{id}"),
                rev: 1,
                value: serde_json::to_vec(&record).unwrap(),
            });
        }
        for (key, rev, value) in &self.finals {
            sweep.push(SweptEntry {
                key: key.clone(),
                rev: *rev,
                value: serde_json::to_vec(value).unwrap(),
            });
        }
        if self.verdict {
            sweep.push(SweptEntry {
                key: VERDICT_KEY.to_owned(),
                rev: 1,
                value: b"{}".to_vec(),
            });
        }
        sweep
    }

    fn check(&self) -> Vec<Violation> {
        let sweep = self.sweep();
        check(&Inputs {
            store: self.store,
            generated: &self.generated,
            processes: &self.processes,
            sweep: &sweep,
            faults: &self.faults,
            timing: self.timing(),
        })
        .unwrap()
    }

    /// The checks of the violations of `property`, in order.
    fn checks(&self, property: u8) -> Vec<Check> {
        self.check()
            .iter()
            .map(|v| v.check)
            .filter(|c| c.property() == property)
            .collect()
    }

    /// The index of the first line of process `p` matching `pred`.
    fn find(&self, p: usize, pred: impl Fn(&Event) -> bool) -> usize {
        self.processes[p]
            .lines
            .iter()
            .position(|l| pred(&l.event))
            .unwrap()
    }

    /// Moves line `from` of process `p` to just after line `to`.
    fn move_after(&mut self, p: usize, from: usize, to: usize) {
        let line = self.processes[p].lines.remove(from);
        let to = if from < to { to } else { to + 1 };
        self.processes[p].lines.insert(to, line);
    }
}

fn is_rows_with(id: &str) -> impl Fn(&Event) -> bool + '_ {
    move |e| matches!(e, Event::Rows { ids } if ids.iter().any(|i| i == id))
}

fn is_send_at(key: &str, expected: u64) -> impl Fn(&Event) -> bool + '_ {
    move |e| matches!(e, Event::Send { key: k, expected: Some(x), .. } if k == key && *x == expected)
}

/// Writes that land at one revision number on several keys are judged against
/// their own key's earlier revisions.
#[test]
fn p3_reads_w0_per_key_not_per_revision() {
    assert_eq!(Fixture::clean().check(), Vec::new());
}

/// A generated id no sink wrote is flagged though another id was written
/// twice, so the row counts match.
#[test]
fn p1_flags_a_record_no_sink_wrote() {
    let mut fixture = Fixture::clean();
    let mut w1 = Proc::new("w1", 101);
    w1.rows(&[id(1, 0)]);
    fixture.processes.push(w1.journal);
    let last = fixture.find(0, is_rows_with(&id(1, 3)));
    let Event::Rows { ids } = &mut fixture.processes[0].lines[last].event else {
        unreachable!()
    };
    ids.retain(|i| *i != id(1, 3));
    let violations = fixture.check();
    let missing: Vec<&Violation> = violations
        .iter()
        .filter(|v| v.check.property() == 1)
        .collect();
    assert_eq!(missing.len(), 1, "{violations:?}");
    assert_eq!(missing[0].check, Check::RecordMissing);
    assert!(
        missing[0].detail.contains(&id(1, 3)),
        "{}",
        missing[0].detail
    );
}

/// An id a sink wrote that the harness never generated is flagged.
#[test]
fn p1_flags_an_id_the_harness_never_generated() {
    let mut fixture = Fixture::clean();
    let mut w1 = Proc::new("w1", 101);
    w1.rows(&[id(9, 0)]);
    fixture.processes.push(w1.journal);
    assert_eq!(
        fixture.check().iter().map(|v| v.check).collect::<Vec<_>>(),
        vec![Check::RecordUnknown]
    );
}

/// A commit that lands before its process wrote the rows below its watermark
/// is flagged against that process.
#[test]
fn p3_flags_a_commit_ahead_of_its_own_rows() {
    let mut fixture = Fixture::clean();
    let rows = fixture.find(0, is_rows_with(&id(0, 0)));
    let commit = fixture.find(0, is_send_at("split.s0", 2));
    fixture.move_after(0, rows, commit + 1);
    let violations = fixture.check();
    assert_eq!(violations.len(), 1, "{violations:?}");
    let v = &violations[0];
    assert_eq!(v.check, Check::AheadOfRows);
    assert_eq!((v.key.as_deref(), v.rev), (Some("split.s0"), Some(3)));
    assert_eq!((v.instance.as_deref(), v.pid), (Some("w0"), Some(100)));
}

/// Rows written by another process do not cover a commit.
#[test]
fn p3_ignores_rows_another_process_wrote() {
    let mut fixture = Fixture::clean();
    let rows = fixture.find(0, is_rows_with(&id(0, 0)));
    let line = fixture.processes[0].lines.remove(rows);
    fixture.processes.push(ProcessJournal {
        instance: "w1".to_owned(),
        pid: 101,
        lines: vec![line],
    });
    let violations = fixture.check();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].check, Check::AheadOfRows);
    assert_eq!(violations[0].instance.as_deref(), Some("w0"));
}

/// Rows written after a commit's `send` and before its `done` do not cover it.
#[test]
fn p3_uses_the_send_line_not_the_done_line() {
    let mut fixture = Fixture::clean();
    let rows = fixture.find(0, is_rows_with(&id(0, 0)));
    let commit = fixture.find(0, is_send_at("split.s0", 2));
    fixture.move_after(0, rows, commit);
    let send = fixture.find(0, is_send_at("split.s0", 2));
    assert!(matches!(
        fixture.processes[0].lines[send + 1].event,
        Event::Rows { .. }
    ));
    assert!(matches!(
        fixture.processes[0].lines[send + 2].event,
        Event::Done { .. }
    ));
    let checks: Vec<Check> = fixture.check().iter().map(|v| v.check).collect();
    assert_eq!(checks, vec![Check::AheadOfRows]);
}

/// A commit whose call was cancelled is checked when its value is seen at a
/// revision.
#[test]
fn p3_checks_a_landed_commit_with_no_done_line() {
    let mut fixture = Fixture::empty();
    let mut w0 = Proc::new("w0", 100);
    let key = "split.s1";
    w0.seen(key, 1, &value(0, None, None, false));
    w0.write(key, 1, &value(1, w0.me(), None, false));
    let committed = value(1, w0.me(), after(0, 0), false);
    let call = w0.send(key, 2, &committed);
    w0.done(call, key, Reply::Cancelled);
    w0.rows(&[id(1, 0), id(1, 1)]);
    w0.seen(key, 3, &committed);
    let completed = value(1, w0.me(), after(0, 1), true);
    w0.write(key, 3, &completed);
    let s0 = w0.deliver(
        "split.s0",
        &[id(0, 0), id(0, 1), id(2, 0), id(2, 1)],
        2,
        after(0, 1),
        after(1, 1),
    );
    let s2 = w0.deliver(
        "split.s2",
        &[id(1, 2), id(1, 3)],
        1,
        after(0, 0),
        after(0, 1),
    );
    fixture.processes.push(w0.journal);
    fixture.finals = vec![
        ("split.s0".to_owned(), 4, s0),
        (key.to_owned(), 4, completed),
        ("split.s2".to_owned(), 4, s2),
    ];
    let violations = fixture.check();
    assert_eq!(violations.len(), 1, "{violations:?}");
    let v = &violations[0];
    assert_eq!(v.check, Check::AheadOfRows);
    assert_eq!((v.key.as_deref(), v.rev), (Some(key), Some(3)));
    assert_eq!(v.instance.as_deref(), Some("w0"));
}

/// An owner that claimed a split mid-way needs only the rows from the
/// watermark it took over to complete it.
#[test]
fn p3_accepts_a_completion_by_an_owner_that_resumed_mid_split() {
    let mut fixture = Fixture::clean();
    let key = "split.s0";
    let mut w0 = Proc::new("w0", 104);
    w0.seen(key, 1, &value(0, None, None, false));
    w0.write(key, 1, &value(1, w0.me(), None, false));
    w0.rows(&[id(0, 0), id(0, 1)]);
    let committed = value(1, w0.me(), after(0, 1), false);
    w0.write(key, 2, &committed);
    let mut w0b = Proc::new("w0", 102);
    w0b.seen(key, 3, &committed);
    let mut claimed = value(2, w0b.me(), after(0, 1), false);
    claimed.attempts = 1;
    w0b.write(key, 3, &claimed);
    w0b.rows(&[id(2, 0), id(2, 1)]);
    let mut completed = value(2, w0b.me(), after(1, 1), true);
    completed.attempts = 1;
    w0b.write(key, 4, &completed);
    let first = &mut fixture.processes[0];
    let end = first
        .lines
        .iter()
        .position(|l| matches!(&l.event, Event::Seen { key: k, .. } if k == "split.s1"))
        .unwrap();
    first.lines.drain(..end);
    fixture.processes.extend([w0.journal, w0b.journal]);
    fixture.finals[0] = (key.to_owned(), 5, completed);
    assert_eq!(fixture.check(), Vec::new());
}

/// A completion whose watermark leaves a record of the split above it is
/// flagged.
#[test]
fn p3_flags_a_completion_below_the_last_record() {
    let mut fixture = Fixture::empty();
    let mut w0 = Proc::new("w0", 100);
    let s0 = w0.deliver(
        "split.s0",
        &[id(0, 0), id(0, 1), id(2, 0), id(2, 1)],
        2,
        after(0, 1),
        after(1, 0),
    );
    let s1 = w0.deliver(
        "split.s1",
        &[id(1, 0), id(1, 1)],
        1,
        after(0, 0),
        after(0, 1),
    );
    let s2 = w0.deliver(
        "split.s2",
        &[id(1, 2), id(1, 3)],
        1,
        after(0, 0),
        after(0, 1),
    );
    fixture.processes.push(w0.journal);
    fixture.finals = vec![
        ("split.s0".to_owned(), 4, s0),
        ("split.s1".to_owned(), 4, s1),
        ("split.s2".to_owned(), 4, s2),
    ];
    let violations = fixture.check();
    assert_eq!(violations.len(), 1, "{violations:?}");
    let v = &violations[0];
    assert_eq!(v.check, Check::AheadOfRows);
    assert_eq!((v.key.as_deref(), v.rev), (Some("split.s0"), Some(4)));
    assert!(v.detail.contains(&id(2, 1)), "{}", v.detail);
}

/// A split whose landed values set `completed` twice is flagged.
#[test]
fn p4_flags_a_second_completion() {
    let mut fixture = Fixture::clean();
    let key = "split.s1";
    let mut w0 = Proc::new("w0", 103);
    let reopened = value(1, w0.me(), after(0, 1), false);
    w0.seen(key, 4, &fixture.finals[1].2);
    w0.write(key, 4, &reopened);
    let again = value(1, w0.me(), after(0, 1), true);
    w0.write(key, 5, &again);
    fixture.processes.push(w0.journal);
    fixture.finals[1] = (key.to_owned(), 6, again);
    assert_eq!(fixture.checks(4), vec![Check::CompletedTwice]);
}

/// A split that ends quarantined is flagged as quarantined.
#[test]
fn p4_flags_a_quarantined_split() {
    let mut fixture = Fixture::clean();
    let mut quarantined = value(2, None, after(0, 1), false);
    quarantined.status = Status::Quarantined;
    let mut w1 = Proc::new("w1", 101);
    w1.seen("split.s1", 4, &fixture.finals[1].2);
    w1.write("split.s1", 4, &quarantined);
    fixture.processes.push(w1.journal);
    fixture.finals[1] = ("split.s1".to_owned(), 5, quarantined);
    assert_eq!(fixture.checks(4), vec![Check::Quarantined]);
}

/// A generated record no descriptor holds is flagged.
#[test]
fn p4_flags_a_generated_record_in_no_split() {
    let mut fixture = Fixture::clean();
    fixture.generated.push(object(3, "data/o003.ndjson", 1));
    assert_eq!(fixture.checks(4), vec![Check::RecordInNoSplit]);
}

/// A generated record two descriptors hold is flagged.
#[test]
fn p4_flags_a_record_in_two_splits() {
    let mut fixture = Fixture::clean();
    fixture.specs[2].1 = SplitDescriptor::with_range(member(B, 40), SplitRange::new(10, 40, b'\n'));
    assert_eq!(fixture.checks(4), vec![Check::RecordInTwoSplits]);
}

/// A split whose progress ends runnable is flagged as unfinished.
#[test]
fn p4_flags_a_split_left_unfinished() {
    let mut fixture = Fixture::clean();
    let lines = &mut fixture.processes[0].lines;
    let complete = lines
        .iter()
        .position(|l| is_send_at("split.s2", 3)(&l.event))
        .unwrap();
    lines.truncate(complete);
    fixture.finals[2] = (
        "split.s2".to_owned(),
        3,
        value(1, Some("w0"), after(0, 0), false),
    );
    assert_eq!(fixture.checks(4), vec![Check::Unfinished]);
}

/// A missing verdict is flagged on DynamoDB and not on NATS.
#[test]
fn p4_requires_the_verdict_on_dynamodb_only() {
    let mut fixture = Fixture::clean();
    fixture.verdict = false;
    assert_eq!(fixture.checks(4), vec![Check::VerdictMissing]);
    fixture.store = StoreKind::Nats;
    assert_eq!(fixture.check(), Vec::new());
}

/// The swept value at a revision is judged over a journalled `won` at the same revision.
#[test]
fn p4_judges_the_swept_value_over_a_journalled_won() {
    let mut fixture = Fixture::clean();
    fixture.finals[0].2 = value(2, Some("w1"), after(0, 1), false);
    let violations = fixture.check();
    assert!(
        violations.iter().any(|v| v.check == Check::Unfinished),
        "{violations:?}"
    );
}

/// A negative landed watermark is an `OracleError`.
#[test]
fn oracle_rejects_a_negative_watermark() {
    let mut fixture = Fixture::clean();
    fixture.finals[1] = (
        "split.s1".to_owned(),
        5,
        value(1, Some("w0"), Some(-1), true),
    );
    let sweep = fixture.sweep();
    let result = check(&Inputs {
        store: fixture.store,
        generated: &fixture.generated,
        processes: &fixture.processes,
        sweep: &sweep,
        faults: &fixture.faults,
        timing: fixture.timing(),
    });
    assert!(result.is_err(), "{result:?}");
}

/// A value that lands at a revision no journal sent it against is flagged with no sender.
#[test]
fn p3_flags_a_value_no_journal_sent_against_its_predecessor() {
    let mut fixture = Fixture::clean();
    fixture.finals[1] = (
        "split.s1".to_owned(),
        5,
        value(1, Some("w0"), after(0, 0), false),
    );
    let violations: Vec<Violation> = fixture
        .check()
        .into_iter()
        .filter(|v| v.check.property() == 3)
        .collect();
    assert_eq!(violations.len(), 1, "{violations:?}");
    let v = &violations[0];
    assert_eq!(v.check, Check::AheadOfRows);
    assert_eq!(
        (v.key.as_deref(), v.rev, v.instance.as_deref()),
        (Some("split.s1"), Some(5), None)
    );
}

/// A split with a descriptor and no landed progress is flagged unfinished.
#[test]
fn p4_flags_a_split_with_no_progress() {
    assert_eq!(Fixture::empty().checks(4), vec![Check::Unfinished; 3]);
}

/// An owner-clear written over a completed value is not a second completion.
#[test]
fn p4_counts_a_completion_once_when_its_owner_is_cleared() {
    let mut fixture = Fixture::clean();
    let mut released = fixture.finals[1].2.clone();
    released.owner = None;
    let mut w0 = Proc::new("w0", 100);
    w0.journal.lines = std::mem::take(&mut fixture.processes[0].lines);
    w0.calls = 100;
    w0.write("split.s1", 4, &released);
    fixture.processes[0] = w0.journal;
    fixture.finals[1] = ("split.s1".to_owned(), 5, released);
    assert_eq!(fixture.check(), Vec::new());
}

/// A split the store holds runnable is unfinished when a journal holds a
/// completion `won` at a revision above the swept one.
#[test]
fn p4_reads_the_final_status_from_the_sweep() {
    let mut fixture = Fixture::clean();
    fixture.finals[1] = (
        "split.s1".to_owned(),
        3,
        value(1, Some("w0"), after(0, 0), false),
    );
    assert_eq!(fixture.checks(4), vec![Check::Unfinished]);
}

/// A commit's rows are checked from its own key's previous watermark when
/// another key holds a higher watermark at the same revision.
#[test]
fn p3_checks_from_its_own_keys_watermark() {
    let mut fixture = Fixture::clean();
    let rows = fixture.find(0, is_rows_with(&id(1, 1)));
    let commit = fixture.find(0, is_send_at("split.s1", 3));
    fixture.move_after(0, rows, commit + 1);
    let violations = fixture.check();
    assert_eq!(violations.len(), 1, "{violations:?}");
    let v = &violations[0];
    assert_eq!(v.check, Check::AheadOfRows);
    assert_eq!((v.key.as_deref(), v.rev), (Some("split.s1"), Some(4)));
    assert!(v.detail.contains(&id(1, 1)), "{}", v.detail);
}

/// A key whose revisions skip numbers, as a NATS stream sequence does, is
/// attributed against its previous landed revision.
#[test]
fn p3_attributes_across_revision_gaps() {
    let mut fixture = Fixture::empty();
    fixture.store = StoreKind::Nats;
    let mut w0 = Proc::new("w0", 100);
    let s0 = w0.deliver(
        "split.s0",
        &[id(0, 0), id(0, 1), id(2, 0), id(2, 1)],
        2,
        after(0, 1),
        after(1, 1),
    );
    let s2 = w0.deliver(
        "split.s2",
        &[id(1, 2), id(1, 3)],
        1,
        after(0, 0),
        after(0, 1),
    );
    w0.seen("split.s1", 10, &value(0, None, None, false));
    let c = w0.send("split.s1", 10, &value(1, Some("w0"), None, false));
    w0.done(c, "split.s1", Reply::Won(13));
    w0.rows(&[id(1, 0)]);
    let c = w0.send("split.s1", 13, &value(1, Some("w0"), after(0, 0), false));
    w0.done(c, "split.s1", Reply::Won(17));
    w0.rows(&[id(1, 1)]);
    let s1 = value(1, Some("w0"), after(0, 1), true);
    let c = w0.send("split.s1", 17, &s1);
    w0.done(c, "split.s1", Reply::Won(20));
    fixture.processes.push(w0.journal);
    fixture.finals = vec![
        ("split.s0".to_owned(), 4, s0),
        ("split.s1".to_owned(), 20, s1),
        ("split.s2".to_owned(), 4, s2),
    ];
    assert_eq!(fixture.check(), Vec::new());
}

/// A peer's claim that loses at the revision a commit landed against is not
/// attributed the commit's value.
#[test]
fn p3_does_not_attribute_a_commit_to_a_losing_claim() {
    let mut fixture = Fixture::clean();
    let mut w1 = Proc::new("w1", 101);
    w1.seen("split.s1", 3, &value(1, Some("w0"), after(0, 0), false));
    let call = w1.send("split.s1", 3, &value(2, w1.me(), after(0, 0), false));
    w1.done(call, "split.s1", Reply::Lost);
    fixture.processes.push(w1.journal);
    assert_eq!(fixture.check(), Vec::new());
}

const STORES: [StoreKind; 2] = [StoreKind::Nats, StoreKind::DynamoDb];

/// Revisions one key moves through: consecutive on DynamoDB, which counts
/// per item, and spaced on NATS, whose stream sequence every key shares.
fn revs(store: StoreKind) -> [u64; 6] {
    match store {
        StoreKind::DynamoDb => [1, 2, 3, 4, 5, 6],
        StoreKind::Nats => [7, 11, 18, 24, 31, 40],
    }
}

fn kill(instance: &str, pid: u32) -> Event {
    Event::Kill {
        instance: instance.to_owned(),
        pid,
        leader: LeaderAtKill::Unread,
    }
}

fn proxy_fault(fault: &str) -> Event {
    Event::ProxyFault {
        instance: "w0".to_owned(),
        pid: 100,
        fault: fault.to_owned(),
        key: Some("split.s0".to_owned()),
    }
}

/// How [`Handover::build`] passes `split.s0` from `w0` (pid 100), which has
/// written its records up to `o002-r000000`, to a later tenant that replays
/// from `w0`'s committed watermark.
struct Handover {
    store: StoreKind,
    /// The later tenant's instance and pid.
    to: (&'static str, u32),
    /// The time of the later tenant's claim `send`.
    claim_at: u64,
    /// The claim spends a delivery attempt.
    raised: bool,
    /// `w0` releases the split before the claim.
    release: bool,
    /// The watermark `w0` commits.
    committed: Option<i64>,
    /// The records the later tenant writes.
    rows: Vec<String>,
}

impl Handover {
    fn new(store: StoreKind) -> Handover {
        Handover {
            store,
            to: ("w1", 101),
            claim_at: 3_000,
            raised: true,
            release: false,
            committed: after(0, 0),
            rows: vec![id(0, 1), id(2, 0), id(2, 1)],
        }
    }

    fn build(&self) -> Fixture {
        let key = "split.s0";
        let r = revs(self.store);
        let mut fixture = Fixture::empty();
        fixture.store = self.store;
        let mut w0 = Proc::new("w0", 100);
        let s1 = w0.deliver(
            "split.s1",
            &[id(1, 0), id(1, 1)],
            1,
            after(0, 0),
            after(0, 1),
        );
        let s2 = w0.deliver(
            "split.s2",
            &[id(1, 2), id(1, 3)],
            1,
            after(0, 0),
            after(0, 1),
        );
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        w0.rows(&[id(0, 0), id(0, 1)]);
        let committed = value(1, w0.me(), self.committed, false);
        w0.landed(key, r[1], r[2], &committed);
        w0.rows(&[id(2, 0)]);
        let mut prev = (r[2], committed);
        let mut next = 3;
        if self.release {
            let released = value(1, None, self.committed, false);
            w0.landed(key, r[2], r[3], &released);
            prev = (r[3], released);
            next = 4;
        }
        let (instance, pid) = self.to;
        let mut b = Proc::new(instance, pid);
        b.at(self.claim_at - 1);
        b.seen(key, prev.0, &prev.1);
        let mut claimed = value(2, Some(instance), self.committed, false);
        claimed.attempts = prev.1.attempts + u32::from(self.raised);
        b.landed(key, prev.0, r[next], &claimed);
        b.rows(&self.rows);
        let mut completed = value(2, Some(instance), after(1, 1), true);
        completed.attempts = claimed.attempts;
        b.landed(key, r[next], r[next + 1], &completed);
        fixture.processes = vec![w0.journal, b.journal];
        fixture.finals = vec![
            (key.to_owned(), r[next + 1], completed),
            ("split.s1".to_owned(), 4, s1),
            ("split.s2".to_owned(), 4, s2),
        ];
        fixture
    }
}

/// The property-2 violations, each as its key and detail.
fn unexplained(fixture: &Fixture) -> Vec<(Option<String>, String)> {
    fixture
        .check()
        .into_iter()
        .filter(|v| v.check.property() == 2)
        .map(|v| {
            assert_eq!(v.check, Check::UnexplainedDuplicate);
            (v.key, v.detail)
        })
        .collect()
}

/// Asserts one unexplained-duplicate violation on `split.s0` that names
/// exactly the ids in `named` among `o000-r000001`, `o002-r000000` and
/// `o002-r000001`.
#[track_caller]
fn assert_flags(fixture: &Fixture, named: &[String], case: &str) {
    let found = unexplained(fixture);
    assert_eq!(found.len(), 1, "{case} on {:?}: {found:?}", fixture.store);
    let (key, detail) = &found[0];
    assert_eq!(key.as_deref(), Some("split.s0"), "{case}");
    for candidate in [id(0, 1), id(2, 0), id(2, 1)] {
        assert_eq!(
            detail.contains(&candidate),
            named.contains(&candidate),
            "{case} on {:?}, {candidate}: {detail}",
            fixture.store
        );
    }
}

#[track_caller]
fn assert_clean(fixture: &Fixture, case: &str) {
    assert_eq!(fixture.check(), Vec::new(), "{case} on {:?}", fixture.store);
}

/// A replay of the records the previous tenant wrote above its watermark,
/// after a kill on that tenant, is explained, the last record it wrote
/// included.
#[test]
fn p2_accepts_a_duplicate_of_the_last_record_written() {
    for store in STORES {
        let mut fixture = Handover::new(store).build();
        fixture.fault(1_000, kill("w0", 100));
        assert_clean(&fixture, "kill before the claim");
    }
}

/// A duplicate with no fault on the previous tenant is flagged.
#[test]
fn p2_flags_a_duplicate_with_no_fault_before_it() {
    for store in STORES {
        let fixture = Handover::new(store).build();
        assert_flags(&fixture, &[id(0, 1), id(2, 0)], "no fault");
    }
}

/// A fault logged after the claim does not explain the claim's replay; one
/// logged at the claim's own millisecond does.
#[test]
fn p2_flags_a_duplicate_whose_only_fault_comes_after_the_claim() {
    for store in STORES {
        let handover = Handover::new(store);
        let mut fixture = handover.build();
        fixture.fault(handover.claim_at + 1, kill("w0", 100));
        assert_flags(&fixture, &[id(0, 1), id(2, 0)], "kill after the claim");
        let mut fixture = handover.build();
        fixture.fault(handover.claim_at, kill("w0", 100));
        assert_clean(&fixture, "kill at the claim");
    }
}

/// A DynamoDB proxy fault and an `err_after_land` line on the previous tenant
/// each explain a replay as faults of no duration; a `pass` or `delay` answer
/// does not.
#[test]
fn p2_accepts_a_duplicate_after_a_zero_length_logged_fault() {
    for store in STORES {
        let handover = Handover::new(store);
        let before = handover.claim_at - 100;
        let mut fixture = handover.build();
        fixture.fault(before, proxy_fault("throttle"));
        assert_clean(&fixture, "proxy fault");

        let mut fixture = handover.build();
        fixture.processes[0].lines.push(Line {
            t_ms: before,
            event: Event::ErrAfterLand {
                key: "split.s0".to_owned(),
                rev: revs(store)[2],
            },
        });
        assert_clean(&fixture, "err_after_land");

        for benign in ["pass", "delay(50ms)"] {
            let mut fixture = handover.build();
            fixture.fault(before, proxy_fault(benign));
            assert_flags(&fixture, &[id(0, 1), id(2, 0)], benign);
        }
    }
}

/// A duplicate below the watermark the claim replaced is flagged.
#[test]
fn p2_flags_a_duplicate_below_the_replaced_watermark() {
    for store in STORES {
        let mut handover = Handover::new(store);
        handover.committed = after(0, 1);
        let mut fixture = handover.build();
        fixture.fault(1_000, kill("w0", 100));
        assert_flags(&fixture, &[id(0, 1)], "below the replaced watermark");
    }
}

/// A duplicate above the last record the previous tenant wrote is flagged.
#[test]
fn p2_flags_a_duplicate_above_the_previous_tenancys_highest_write() {
    for store in STORES {
        let mut handover = Handover::new(store);
        handover.rows.push(id(2, 1));
        let mut fixture = handover.build();
        fixture.fault(1_000, kill("w0", 100));
        assert_flags(&fixture, &[id(2, 1)], "above the highest write");
    }
}

/// A claim after the previous tenant's release replays with no fault and
/// no attempt spent.
#[test]
fn p2_accepts_a_replay_after_a_forced_release() {
    for store in STORES {
        let mut handover = Handover::new(store);
        handover.release = true;
        handover.raised = false;
        assert_clean(&handover.build(), "release");
    }
}

/// A claim that took over an owned split without spending an attempt does
/// not explain a replay, even inside a fault's window.
#[test]
fn p2_flags_a_duplicate_after_a_claim_that_did_not_raise_attempts() {
    for store in STORES {
        let mut handover = Handover::new(store);
        handover.raised = false;
        let mut fixture = handover.build();
        fixture.fault(1_000, kill("w0", 100));
        assert_flags(&fixture, &[id(0, 1), id(2, 0)], "no attempt spent");
    }
}

/// After a kill, the window runs from the replacement's start, here a
/// replacement under the same instance id that wins a claim race against a
/// peer.
#[test]
fn p2_anchors_the_window_at_the_replacement_start() {
    for store in STORES {
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let (lease, slack) = (tuned.timing().lease_ms, tuned.slack());
        let started = 1_000 + lease;
        for (claim_at, explained) in [(1_000 + slack + 500, true), (started + slack + 1, false)] {
            let mut handover = Handover::new(store);
            handover.to = ("w0", 102);
            handover.claim_at = claim_at;
            let mut fixture = handover.build();
            let mut peer = Proc::new("w1", 101);
            peer.at(claim_at);
            let r = revs(store);
            peer.seen("split.s0", r[2], &value(1, Some("w0"), after(0, 0), false));
            let mut lost = value(2, peer.me(), after(0, 0), false);
            lost.attempts = 1;
            let call = peer.send("split.s0", r[2], &lost);
            peer.done(call, "split.s0", Reply::Lost);
            fixture.processes.push(peer.journal);
            fixture.fault(1_000, kill("w0", 100));
            fixture.fault(
                started,
                Event::Respawn {
                    instance: "w0".to_owned(),
                    pid: 102,
                },
            );
            if explained {
                assert_clean(&fixture, "claim within the replacement's window");
            } else {
                assert_flags(&fixture, &[id(0, 1), id(2, 0)], "claim after the window");
            }
        }
    }
}

/// A SIGSTOP's window runs from its end.
#[test]
fn p2_extends_the_window_by_the_fault_duration() {
    for store in STORES {
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let (lease, slack) = (tuned.timing().lease_ms, tuned.slack());
        let end = 1_000 + 2 * lease;
        for (claim_at, explained) in [(end + slack, true), (end + slack + 1, false)] {
            let mut handover = Handover::new(store);
            handover.claim_at = claim_at;
            let mut fixture = handover.build();
            fixture.fault(
                1_000,
                Event::Sigstop {
                    instance: "w0".to_owned(),
                    pid: 100,
                    duration_ms: 2 * lease,
                },
            );
            if explained {
                assert_clean(&fixture, "claim within the stop's window");
            } else {
                assert_flags(&fixture, &[id(0, 1), id(2, 0)], "claim after the window");
            }
        }
    }
}

/// A `stop` line in the previous tenant's journal opens a window that the
/// harness's next `sigcont` for its pid closes.
#[test]
fn p2_accepts_a_duplicate_during_a_self_raised_stop() {
    for store in STORES {
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let (lease, slack) = (tuned.timing().lease_ms, tuned.slack());
        let resumed = 1_000 + lease * 5 / 4;
        for (claim_at, explained) in [
            (1_000 + lease, true),
            (resumed + slack, true),
            (resumed + slack + 1, false),
        ] {
            let mut handover = Handover::new(store);
            handover.claim_at = claim_at;
            let mut fixture = handover.build();
            fixture.processes[0].lines.push(Line {
                t_ms: 1_000,
                event: Event::Stop {
                    key: "split.s0".to_owned(),
                    expected: revs(store)[2],
                    epoch: 1,
                },
            });
            fixture.fault(
                resumed,
                Event::Sigcont {
                    instance: "w0".to_owned(),
                    pid: 100,
                },
            );
            if explained {
                assert_clean(&fixture, "claim during or after the stop");
            } else {
                assert_flags(&fixture, &[id(0, 1), id(2, 0)], "claim after the window");
            }
        }
    }
}

/// A `leader_stop` line in the previous tenant's journal opens a window that
/// the harness's next `sigcont` for its pid closes.
#[test]
fn p2_accepts_a_duplicate_during_a_leader_stop() {
    for store in STORES {
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let (lease, slack) = (tuned.timing().lease_ms, tuned.slack());
        let resumed = 1_000 + lease * 5 / 4;
        for (claim_at, explained) in [
            (1_000 + lease, true),
            (resumed + slack, true),
            (resumed + slack + 1, false),
        ] {
            let mut handover = Handover::new(store);
            handover.claim_at = claim_at;
            let mut fixture = handover.build();
            fixture.processes[0].lines.push(Line {
                t_ms: 1_000,
                event: Event::LeaderStop {
                    key: "assign.w0".to_owned(),
                    kind: WriteKind::Assign,
                    n: 1,
                    value: serde_json::json!({"splits": ["s0"]}),
                    published: false,
                },
            });
            fixture.fault(
                resumed,
                Event::Sigcont {
                    instance: "w0".to_owned(),
                    pid: 100,
                },
            );
            if explained {
                assert_clean(&fixture, "claim during or after the stop");
            } else {
                assert_flags(&fixture, &[id(0, 1), id(2, 0)], "claim after the window");
            }
        }
    }
}

/// Splits `s0` and `s2` delivered by `w3` (pid 103), for fixtures whose story
/// is on `s1`.
fn others(fixture: &mut Fixture) -> (Progress, Progress) {
    let mut w3 = Proc::new("w3", 103);
    let s0 = w3.deliver(
        "split.s0",
        &[id(0, 0), id(0, 1), id(2, 0), id(2, 1)],
        2,
        after(0, 1),
        after(1, 1),
    );
    let s2 = w3.deliver(
        "split.s2",
        &[id(1, 2), id(1, 3)],
        1,
        after(0, 0),
        after(0, 1),
    );
    fixture.processes.push(w3.journal);
    (s0, s2)
}

/// `w0` (pid 100) claims `split.s1` and commits its first record, and `w1`
/// (pid 101) claims it at epoch 2. `w0` then loses a commit of its second
/// record under epoch 1, reads `w1`'s claim, and re-sends the commit, which
/// lands. Returns the fixture and the stale commit's revision.
fn stale_commit(store: StoreKind) -> (Fixture, u64) {
    let key = "split.s1";
    let r = revs(store);
    let mut fixture = Fixture::empty();
    fixture.store = store;
    let (s0, s2) = others(&mut fixture);
    let mut w0 = Proc::new("w0", 100);
    w0.seen(key, r[0], &value(0, None, None, false));
    w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
    w0.rows(&[id(1, 0)]);
    let committed = value(1, w0.me(), after(0, 0), false);
    w0.landed(key, r[1], r[2], &committed);
    w0.rows(&[id(1, 1)]);
    let mut w1 = Proc::new("w1", 101);
    w1.seen(key, r[2], &committed);
    let mut claimed = value(2, w1.me(), after(0, 0), false);
    claimed.attempts = 1;
    w1.landed(key, r[2], r[3], &claimed);
    let stale = value(1, w0.me(), after(0, 1), false);
    let call = w0.send(key, r[2], &stale);
    w0.done(call, key, Reply::Lost);
    w0.seen(key, r[3], &claimed);
    w0.landed(key, r[3], r[4], &stale);
    fixture.processes.extend([w0.journal, w1.journal]);
    fixture.finals = vec![
        ("split.s0".to_owned(), 4, s0),
        (key.to_owned(), r[4], stale),
        ("split.s2".to_owned(), 4, s2),
    ];
    (fixture, r[4])
}

/// A violation as its check, key, revision and writer pid.
type Found = (Check, Option<String>, Option<u64>, Option<u32>);

/// The property-5 violations.
fn epochs(fixture: &Fixture) -> Vec<Found> {
    fixture
        .check()
        .into_iter()
        .filter(|v| v.check.property() == 5)
        .map(|v| (v.check, v.key, v.rev, v.pid))
        .collect()
}

fn stale_found(rev: u64) -> Vec<Found> {
    let key = Some("split.s1".to_owned());
    vec![
        (Check::EpochRegressed, key.clone(), Some(rev), Some(100)),
        (Check::StaleEpochCommit, key, Some(rev), Some(100)),
    ]
}

/// A commit under epoch 1 that lands above a claim at epoch 2 regresses the
/// epoch and moves the watermark under a stale epoch, both attributed to its
/// writer.
#[test]
fn p5_flags_a_stale_epoch_above_a_newer_claim() {
    for store in STORES {
        let (fixture, rev) = stale_commit(store);
        assert_eq!(epochs(&fixture), stale_found(rev), "{store:?}");
    }
}

/// A claim known only from a `seen` line orders the values after it.
#[test]
fn p5_counts_seen_entries_not_only_won_writes() {
    for store in STORES {
        let (mut fixture, rev) = stale_commit(store);
        let w1 = fixture
            .processes
            .iter_mut()
            .find(|p| p.instance == "w1")
            .unwrap();
        for line in &mut w1.lines {
            if let Event::Done { reply, .. } = &mut line.event {
                *reply = Reply::Cancelled;
            }
        }
        assert_eq!(epochs(&fixture), stale_found(rev), "{store:?}");
    }
}

/// A late `seen` of an older revision does not regress the epoch.
#[test]
fn p5_orders_by_revision_not_journal_time() {
    let mut fixture = Fixture::clean();
    let mut w2 = Proc::new("w2", 102);
    w2.at(9_000);
    w2.seen("split.s1", 1, &value(0, None, None, false));
    fixture.processes.push(w2.journal);
    assert_eq!(fixture.check(), Vec::new());
}

/// A second owner at an epoch another owner holds is flagged against its
/// writer.
#[test]
fn p5_flags_two_owners_in_one_epoch() {
    for store in STORES {
        let key = "split.s1";
        let r = revs(store);
        let mut fixture = Fixture::empty();
        fixture.store = store;
        let (s0, s2) = others(&mut fixture);
        let mut w0 = Proc::new("w0", 100);
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        let mut w1 = Proc::new("w1", 101);
        w1.seen(key, r[1], &value(1, Some("w0"), None, false));
        let mut first = value(2, w1.me(), None, false);
        first.attempts = 1;
        w1.landed(key, r[1], r[2], &first);
        let mut w2 = Proc::new("w2", 102);
        w2.seen(key, r[2], &first);
        let mut second = value(2, w2.me(), None, false);
        second.attempts = 2;
        w2.landed(key, r[2], r[3], &second);
        fixture
            .processes
            .extend([w0.journal, w1.journal, w2.journal]);
        fixture.finals = vec![
            ("split.s0".to_owned(), 4, s0),
            (key.to_owned(), r[3], second),
            ("split.s2".to_owned(), 4, s2),
        ];
        assert_eq!(
            epochs(&fixture),
            vec![(
                Check::TwoOwners,
                Some(key.to_owned()),
                Some(r[3]),
                Some(102)
            )],
            "{store:?}"
        );
    }
}

/// A release that clears the owner within an epoch is no second owner.
#[test]
fn p5_ignores_an_ownerless_value_in_an_epoch() {
    for store in STORES {
        let key = "split.s1";
        let r = revs(store);
        let mut fixture = Fixture::empty();
        fixture.store = store;
        let (s0, s2) = others(&mut fixture);
        let mut w0 = Proc::new("w0", 100);
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        w0.rows(&[id(1, 0)]);
        w0.landed(key, r[1], r[2], &value(1, w0.me(), after(0, 0), false));
        let released = value(1, None, after(0, 0), false);
        w0.landed(key, r[2], r[3], &released);
        let mut w1 = Proc::new("w1", 101);
        w1.seen(key, r[3], &released);
        w1.landed(key, r[3], r[4], &value(2, w1.me(), after(0, 0), false));
        w1.rows(&[id(1, 1)]);
        let completed = value(2, w1.me(), after(0, 1), true);
        w1.landed(key, r[4], r[5], &completed);
        fixture.processes.extend([w0.journal, w1.journal]);
        fixture.finals = vec![
            ("split.s0".to_owned(), 4, s0),
            (key.to_owned(), r[5], completed),
            ("split.s2".to_owned(), 4, s2),
        ];
        assert_clean(&fixture, "release then claim");
    }
}

/// Epochs are ordered per key on a NATS stream sequence, where one key's
/// later epoch can sit at a lower revision than another key's first value.
#[test]
fn p5_keys_revisions_per_split() {
    let mut fixture = Fixture::empty();
    fixture.store = StoreKind::Nats;
    let mut w0 = Proc::new("w0", 100);
    let mut w1 = Proc::new("w1", 101);
    let s0 = "split.s0";
    w0.seen(s0, 3, &value(0, None, None, false));
    w0.landed(s0, 3, 4, &value(1, w0.me(), None, false));
    w0.rows(&[id(0, 0), id(0, 1)]);
    w0.landed(s0, 4, 5, &value(1, w0.me(), after(0, 1), false));
    let released = value(1, None, after(0, 1), false);
    w0.landed(s0, 5, 6, &released);
    w1.seen(s0, 6, &released);
    w1.landed(s0, 6, 7, &value(2, w1.me(), after(0, 1), false));
    w1.rows(&[id(2, 0), id(2, 1)]);
    let s0_done = value(2, w1.me(), after(1, 1), true);
    w1.landed(s0, 7, 8, &s0_done);
    let s1 = "split.s1";
    w0.seen(s1, 9, &value(0, None, None, false));
    w0.landed(s1, 9, 10, &value(1, w0.me(), None, false));
    w0.rows(&[id(1, 0), id(1, 1)]);
    let s1_done = value(1, w0.me(), after(0, 1), true);
    w0.landed(s1, 10, 11, &s1_done);
    let s2 = "split.s2";
    w1.seen(s2, 12, &value(0, None, None, false));
    w1.landed(s2, 12, 13, &value(1, w1.me(), None, false));
    w1.rows(&[id(1, 2), id(1, 3)]);
    let s2_done = value(1, w1.me(), after(0, 1), true);
    w1.landed(s2, 13, 14, &s2_done);
    fixture.processes.extend([w0.journal, w1.journal]);
    fixture.finals = vec![
        (s0.to_owned(), 8, s0_done),
        (s1.to_owned(), 11, s1_done),
        (s2.to_owned(), 14, s2_done),
    ];
    assert_eq!(fixture.check(), Vec::new());
}

/// A watermark moved under an epoch no claim reached is flagged.
#[test]
fn p5_flags_a_commit_under_an_epoch_no_claim_reached() {
    for store in STORES {
        let key = "split.s1";
        let r = revs(store);
        let mut fixture = Fixture::empty();
        fixture.store = store;
        let (s0, s2) = others(&mut fixture);
        let mut w0 = Proc::new("w0", 100);
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        w0.rows(&[id(1, 0), id(1, 1)]);
        let completed = value(2, w0.me(), after(0, 1), true);
        w0.landed(key, r[1], r[2], &completed);
        fixture.processes.push(w0.journal);
        fixture.finals = vec![
            ("split.s0".to_owned(), 4, s0),
            (key.to_owned(), r[2], completed),
            ("split.s2".to_owned(), 4, s2),
        ];
        assert_eq!(
            epochs(&fixture),
            vec![(
                Check::StaleEpochCommit,
                Some(key.to_owned()),
                Some(r[2]),
                Some(100)
            )],
            "{store:?}"
        );
    }
}

/// Two journals that report different values at one revision below the
/// swept one are flagged whichever journal comes first.
#[test]
fn p5_flags_two_values_at_one_revision_in_either_journal_order() {
    let mut fixture = Fixture::clean();
    let mut w1 = Proc::new("w1", 101);
    w1.seen("split.s1", 2, &value(1, Some("w0"), None, false));
    let mut w2 = Proc::new("w2", 102);
    w2.seen("split.s1", 2, &value(2, Some("w2"), None, false));
    fixture.processes.extend([w1.journal, w2.journal]);
    let conflicts = |fixture: &Fixture| -> Vec<Violation> {
        fixture
            .check()
            .into_iter()
            .filter(|v| v.check == Check::ValueConflict)
            .collect()
    };
    let forward = conflicts(&fixture);
    fixture.processes.reverse();
    let backward = conflicts(&fixture);
    assert_eq!(forward.len(), 1, "{forward:?}");
    assert_eq!(
        (forward[0].key.as_deref(), forward[0].rev),
        (Some("split.s1"), Some(2))
    );
    assert_eq!(forward, backward);
}

/// A kill on one pid explains no takeover from that instance's replacement.
#[test]
fn p2_flags_a_takeover_from_an_unfaulted_replacement() {
    for store in STORES {
        let key = "split.s0";
        let r = revs(store);
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let lease = tuned.timing().lease_ms;
        let mut fixture = Fixture::empty();
        fixture.store = store;
        let mut w0 = Proc::new("w0", 100);
        let s1 = w0.deliver(
            "split.s1",
            &[id(1, 0), id(1, 1)],
            1,
            after(0, 0),
            after(0, 1),
        );
        let s2 = w0.deliver(
            "split.s2",
            &[id(1, 2), id(1, 3)],
            1,
            after(0, 0),
            after(0, 1),
        );
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        w0.rows(&[id(0, 0), id(0, 1)]);
        let committed = value(1, w0.me(), after(0, 0), false);
        w0.landed(key, r[1], r[2], &committed);
        let started = 1_000 + lease;
        let mut b = Proc::new("w0", 102);
        b.at(started + 100);
        b.seen(key, r[2], &committed);
        let mut reclaimed = value(2, b.me(), after(0, 0), false);
        reclaimed.attempts = 1;
        b.landed(key, r[2], r[3], &reclaimed);
        b.rows(&[id(0, 1), id(2, 0)]);
        let mut w1 = Proc::new("w1", 101);
        w1.at(started + 2_000);
        w1.seen(key, r[3], &reclaimed);
        let mut claimed = value(3, w1.me(), after(0, 0), false);
        claimed.attempts = 2;
        w1.landed(key, r[3], r[4], &claimed);
        w1.rows(&[id(0, 1), id(2, 0), id(2, 1)]);
        let mut completed = value(3, w1.me(), after(1, 1), true);
        completed.attempts = 2;
        w1.landed(key, r[4], r[5], &completed);
        fixture.processes = vec![w0.journal, b.journal, w1.journal];
        fixture.finals = vec![
            (key.to_owned(), r[5], completed),
            ("split.s1".to_owned(), 4, s1),
            ("split.s2".to_owned(), 4, s2),
        ];
        fixture.fault(1_000, kill("w0", 100));
        fixture.fault(
            started,
            Event::Respawn {
                instance: "w0".to_owned(),
                pid: 102,
            },
        );
        assert_flags(
            &fixture,
            &[id(2, 0)],
            "takeover from an unfaulted replacement",
        );
    }
}

/// After a second kill of an instance, the window runs from the replacement
/// that follows that kill.
#[test]
fn p2_anchors_a_second_kill_at_its_own_replacement() {
    for store in STORES {
        let key = "split.s0";
        let r = revs(store);
        let tuned = Fixture {
            store,
            ..Fixture::empty()
        };
        let (lease, slack) = (tuned.timing().lease_ms, tuned.slack());
        let mut fixture = Fixture::empty();
        fixture.store = store;
        let mut w0 = Proc::new("w0", 100);
        let s1 = w0.deliver(
            "split.s1",
            &[id(1, 0), id(1, 1)],
            1,
            after(0, 0),
            after(0, 1),
        );
        let s2 = w0.deliver(
            "split.s2",
            &[id(1, 2), id(1, 3)],
            1,
            after(0, 0),
            after(0, 1),
        );
        w0.seen(key, r[0], &value(0, None, None, false));
        w0.landed(key, r[0], r[1], &value(1, w0.me(), None, false));
        w0.rows(&[id(0, 0), id(0, 1)]);
        let committed = value(1, w0.me(), after(0, 0), false);
        w0.landed(key, r[1], r[2], &committed);
        let first = 1_000 + lease;
        let mut b = Proc::new("w0", 102);
        b.at(first + 100);
        b.seen(key, r[2], &committed);
        let mut reclaimed = value(2, b.me(), after(0, 0), false);
        reclaimed.attempts = 1;
        b.landed(key, r[2], r[3], &reclaimed);
        b.rows(&[id(0, 1), id(2, 0)]);
        let second_kill = first + 1_000;
        let second = second_kill + lease;
        let mut c = Proc::new("w0", 103);
        c.at(second_kill + slack + 500);
        c.seen(key, r[3], &reclaimed);
        let mut claimed = value(3, c.me(), after(0, 0), false);
        claimed.attempts = 2;
        c.landed(key, r[3], r[4], &claimed);
        c.rows(&[id(0, 1), id(2, 0), id(2, 1)]);
        let mut completed = value(3, c.me(), after(1, 1), true);
        completed.attempts = 2;
        c.landed(key, r[4], r[5], &completed);
        fixture.processes = vec![w0.journal, b.journal, c.journal];
        fixture.finals = vec![
            (key.to_owned(), r[5], completed),
            ("split.s1".to_owned(), 4, s1),
            ("split.s2".to_owned(), 4, s2),
        ];
        let respawn = |instance: &str, pid: u32| Event::Respawn {
            instance: instance.to_owned(),
            pid,
        };
        fixture.fault(1_000, kill("w0", 100));
        fixture.fault(first, respawn("w0", 102));
        fixture.fault(second_kill, kill("w0", 102));
        fixture.fault(second_kill + 50, kill("w3", 104));
        fixture.fault(second_kill + 100, respawn("w3", 105));
        fixture.fault(second, respawn("w0", 103));
        assert_clean(&fixture, "claim within the second replacement's window");
    }
}
