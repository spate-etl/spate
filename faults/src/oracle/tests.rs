use spate_s3::fuzz_seams::encode_position;
use spate_s3::{DescriptorObject, SplitRange};

use super::*;
use crate::journal::{SCHEMA, Source, WriteOp};

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

/// A journal under construction.
struct Proc {
    journal: ProcessJournal,
    calls: u64,
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
        }
    }

    fn push(&mut self, event: Event) {
        let t_ms = self.journal.lines.len() as u64;
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
        let call = self.send(key, expected, value);
        self.done(call, key, Reply::Won(expected + 1));
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
        }
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
