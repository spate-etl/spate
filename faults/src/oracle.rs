//! The delivery oracle: judges a run's journals and the store's final durable
//! state against the delivery properties.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use spate_s3::SplitDescriptor;
use spate_s3::fuzz_seams::decode_position;

use crate::classify::{WriteKind, classify};
use crate::journal::{Event, Line, Progress, Reply, Status};
use crate::outcome::{Check, Violation};

const SPLIT_PREFIX: &str = "split.";
const SPEC_PREFIX: &str = "spec.";
const VERDICT_KEY: &str = "verdict";
/// How many ids a grouped violation names.
const NAMED: usize = 5;
/// Margin added to property 2's time bound for scheduling and clock reads.
const MARGIN_MS: u64 = 2_000;

/// Which coordination store a run used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreKind {
    /// NATS JetStream, a push store.
    Nats,
    /// DynamoDB, a polled store. A worker writes a verdict to it.
    DynamoDb,
}

/// A record the harness generated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedRecord {
    /// The record's id.
    pub id: String,
    /// Byte offset of the record's first byte in its object.
    pub offset: u64,
}

/// An object the harness generated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedObject {
    /// Object key.
    pub key: String,
    /// The object's records in byte order.
    pub records: Vec<GeneratedRecord>,
}

/// The journal of one worker process.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessJournal {
    /// Instance id the process ran as.
    pub instance: String,
    /// The process's pid.
    pub pid: u32,
    /// The journal's lines in file order.
    pub lines: Vec<Line>,
}

/// One durable entry of the final sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SweptEntry {
    /// Store key.
    pub key: String,
    /// Revision of the entry.
    pub rev: u64,
    /// Stored bytes.
    pub value: Vec<u8>,
}

/// The worker tuning that bounds how long after a fault a replay may begin,
/// in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    /// Split lease duration.
    pub lease_ms: u64,
    /// Cooperative drain deadline.
    pub drain_deadline_ms: u64,
    /// Per-call store timeout.
    pub op_timeout_ms: u64,
    /// Watch poll interval; 0 on a push store.
    pub poll_interval_ms: u64,
}

impl Timing {
    /// How long after a fault window closes a claim it caused may come.
    fn slack(self) -> u64 {
        2 * self.lease_ms
            + self.drain_deadline_ms
            + 2 * self.op_timeout_ms
            + 2 * self.poll_interval_ms
            + MARGIN_MS
    }
}

/// Everything [`check`] judges.
#[derive(Clone, Copy, Debug)]
pub struct Inputs<'a> {
    /// The store the workers coordinated through.
    pub store: StoreKind,
    /// The data set, as generated.
    pub generated: &'a [GeneratedObject],
    /// Every worker process's journal, replacements included.
    pub processes: &'a [ProcessJournal],
    /// The durable `spec.`, `split.`, `plan` and `verdict` entries at the end
    /// of the run.
    pub sweep: &'a [SweptEntry],
    /// The faults the harness injected, as it journalled them.
    pub faults: &'a [Line],
    /// The workers' tuning.
    pub timing: Timing,
}

/// Why the oracle could not judge a run.
#[derive(Debug)]
pub struct OracleError(String);

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OracleError {}

/// A record's position in its split: object ordinal and record index, in the
/// order of [`decode_position`].
type Position = (u32, u64);

/// Each swept split's records at their positions, keyed by its `split.` key.
type Splits<'a> = BTreeMap<String, Vec<(Position, &'a str)>>;

/// Every value landed on each `split.` key, by revision.
type Landed<'a> = HashMap<&'a str, BTreeMap<u64, Progress>>;

/// Every `send` line, per key.
type Sends<'a> = HashMap<&'a str, Vec<Send<'a>>>;

/// Checks the five delivery properties and returns every violation found.
///
/// A value landed at `(key, rev)` when a `seen` line, a `won` reply or the
/// sweep shows it there. Where these disagree, the oracle judges the first of
/// the sweep and then the journals in input order, and reports the
/// disagreement as a [`Check::ValueConflict`]. A landed value is attributed to every process whose journal sent that value on that
/// key against the key's next lower landed revision, whether or not a `done`
/// line followed.
///
/// # Errors
///
/// Fails when a swept record or spec does not decode, a descriptor names an
/// object the harness did not generate, or a landed watermark is negative.
pub fn check(inputs: &Inputs<'_>) -> Result<Vec<Violation>, OracleError> {
    let splits = splits(inputs)?;
    let (landed, conflicts) = landed(inputs)?;
    let sends = sends(inputs.processes);
    let written: Vec<HashMap<&str, usize>> = inputs.processes.iter().map(first_rows).collect();

    let swept: HashMap<&str, u64> = inputs
        .sweep
        .iter()
        .map(|e| (e.key.as_str(), e.rev))
        .collect();

    let mut violations = Vec::new();
    arrival(inputs, &mut violations);
    partition(inputs.generated, &splits, &mut violations);
    let none = BTreeMap::new();
    for (key, records) in &splits {
        let revisions = landed.get(key.as_str()).unwrap_or(&none);
        let mut completions = 0;
        let mut prev: Option<(u64, &Progress)> = None;
        for (&rev, value) in revisions {
            let before = prev.map(|(_, p)| p);
            let completes = value.completed && !before.is_some_and(|p| p.completed);
            completions += usize::from(completes);
            if completes || value.watermark != before.and_then(|p| p.watermark) {
                let write = Write {
                    key,
                    rev,
                    expected: prev.map(|(r, _)| r),
                    from: before.and_then(|p| p.watermark),
                    value,
                    completes,
                };
                ahead_of_rows(&write, records, &sends, &written, &mut violations);
            }
            prev = Some((rev, value));
        }
        let last = swept
            .get(key.as_str())
            .and_then(|rev| revisions.get_key_value(rev))
            .map(|(rev, value)| (*rev, value));
        completion(key, completions, last, &mut violations);
    }
    if inputs.store == StoreKind::DynamoDb && !inputs.sweep.iter().any(|e| e.key == VERDICT_KEY) {
        violations.push(violation(
            Check::VerdictMissing,
            None,
            "the store holds no verdict".to_owned(),
        ));
    }
    duplicates(inputs, &splits, &landed, &sends, &mut violations);
    let mut keys: Vec<&&str> = landed.keys().collect();
    keys.sort();
    for key in keys {
        epoch_order(key, &landed[*key], &sends, &mut violations);
    }
    violations.extend(conflicts);
    Ok(violations)
}

/// A landed value that moved the watermark or set `completed`.
struct Write<'a> {
    key: &'a str,
    rev: u64,
    expected: Option<u64>,
    from: Option<i64>,
    value: &'a Progress,
    completes: bool,
}

/// A `send` line: its process, its index in that journal, its time, and what
/// it sent.
struct Send<'a> {
    process: &'a ProcessJournal,
    process_index: usize,
    line: usize,
    t_ms: u64,
    expected: Option<u64>,
    value: &'a Progress,
}

/// Property 1: every generated id reached a sink, and every id a sink wrote
/// was generated.
fn arrival(inputs: &Inputs<'_>, violations: &mut Vec<Violation>) {
    let generated: HashSet<&str> = generated_ids(inputs.generated).collect();
    let mut written = HashSet::new();
    let mut unknown = Vec::new();
    for id in inputs.processes.iter().flat_map(|p| rows(&p.lines)) {
        if written.insert(id) && !generated.contains(id) {
            unknown.push(id);
        }
    }
    let missing: Vec<&str> = generated_ids(inputs.generated)
        .filter(|id| !written.contains(id))
        .collect();
    if !missing.is_empty() {
        let detail = format!("no sink wrote {}", listed(&missing));
        violations.push(violation(Check::RecordMissing, None, detail));
    }
    if !unknown.is_empty() {
        let detail = format!("a sink wrote ids never generated: {}", listed(&unknown));
        violations.push(violation(Check::RecordUnknown, None, detail));
    }
}

/// Property 4: the swept descriptors partition the generated records.
fn partition(generated: &[GeneratedObject], splits: &Splits<'_>, violations: &mut Vec<Violation>) {
    let mut homes: HashMap<&str, usize> = HashMap::new();
    for (_, id) in splits.values().flatten() {
        *homes.entry(id).or_default() += 1;
    }
    let count = |id: &str| homes.get(id).copied().unwrap_or(0);
    let nowhere: Vec<&str> = generated_ids(generated)
        .filter(|id| count(id) == 0)
        .collect();
    let twice: Vec<&str> = generated_ids(generated)
        .filter(|id| count(id) > 1)
        .collect();
    if !nowhere.is_empty() {
        let detail = format!("no descriptor holds {}", listed(&nowhere));
        violations.push(violation(Check::RecordInNoSplit, None, detail));
    }
    if !twice.is_empty() {
        let detail = format!("more than one descriptor holds {}", listed(&twice));
        violations.push(violation(Check::RecordInTwoSplits, None, detail));
    }
}

/// Property 3 for one landed write: each process that sent it had written
/// every record in `[from, watermark)` before the `send`, and a completing
/// value's watermark lies above every record of the split.
fn ahead_of_rows(
    write: &Write<'_>,
    records: &[(Position, &str)],
    sends: &Sends<'_>,
    written: &[HashMap<&str, usize>],
    violations: &mut Vec<Violation>,
) {
    let at = |w: Option<i64>| w.map_or((0, 0), decode_position);
    let (lo, hi) = (at(write.from), at(write.value.watermark));
    let senders: Vec<&Send<'_>> = senders(sends, write.key, write.expected, write.value).collect();
    if senders.is_empty() {
        violations.push(Violation {
            check: Check::AheadOfRows,
            key: Some(write.key.to_owned()),
            rev: Some(write.rev),
            instance: None,
            pid: None,
            detail: "no process journalled a send of the value landed here".to_owned(),
        });
    }
    for send in senders {
        let before = &written[send.process_index];
        let unwritten: Vec<&str> = records
            .iter()
            .filter(|(pos, _)| lo <= *pos && *pos < hi)
            .map(|(_, id)| *id)
            .filter(|id| before.get(id).is_none_or(|line| *line > send.line))
            .collect();
        if !unwritten.is_empty() {
            violations.push(Violation {
                check: Check::AheadOfRows,
                key: Some(write.key.to_owned()),
                rev: Some(write.rev),
                instance: Some(send.process.instance.clone()),
                pid: Some(send.process.pid),
                detail: format!(
                    "the watermark moved past rows this process had not written: {}",
                    listed(&unwritten)
                ),
            });
        }
    }
    if write.completes {
        let above: Vec<&str> = records
            .iter()
            .filter(|(pos, _)| *pos >= hi)
            .map(|(_, id)| *id)
            .collect();
        if !above.is_empty() {
            violations.push(Violation {
                check: Check::AheadOfRows,
                key: Some(write.key.to_owned()),
                rev: Some(write.rev),
                instance: None,
                pid: None,
                detail: format!(
                    "completed with records at or above its watermark: {}",
                    listed(&above)
                ),
            });
        }
    }
}

/// Property 4 for one split: one completion, and a final `Completed` status.
fn completion(
    key: &str,
    completions: usize,
    last: Option<(u64, &Progress)>,
    violations: &mut Vec<Violation>,
) {
    if completions > 1 {
        let detail = format!("{completions} landed values set completed");
        violations.push(violation(Check::CompletedTwice, Some(key), detail));
    }
    match last {
        Some((_, value)) if value.status == Status::Completed => {}
        Some((rev, value)) if value.status == Status::Quarantined => {
            let detail = format!("quarantined at revision {rev}");
            violations.push(violation(Check::Quarantined, Some(key), detail));
        }
        Some((rev, _)) => {
            let detail = format!("still runnable at revision {rev}");
            violations.push(violation(Check::Unfinished, Some(key), detail));
        }
        None => {
            let detail = "the store holds no progress record".to_owned();
            violations.push(violation(Check::Unfinished, Some(key), detail));
        }
    }
}

/// A merged fault window on one process, in wall-clock milliseconds.
struct Window {
    start: u64,
    /// The later of the window's end and, when the window holds a kill or an
    /// abort, the start of the instance's replacement.
    anchor: u64,
}

/// A claim that moved a split to a new tenancy.
struct Change<'s, 'a> {
    /// The sends of the claim that began the previous tenancy.
    from: Vec<&'s Send<'a>>,
    /// The sends of this claim.
    claims: Vec<&'s Send<'a>>,
    /// The watermark this claim replaced.
    replaced: Position,
    /// This claim spent a delivery attempt.
    raised: bool,
    /// Processes whose `Release` this claim replaced.
    released_by: Vec<usize>,
}

/// Property 2: each record written more than once lies in the replay range of
/// a tenancy change that a fault on the previous tenant, or its release,
/// explains.
fn duplicates(
    inputs: &Inputs<'_>,
    splits: &Splits<'_>,
    landed: &Landed<'_>,
    sends: &Sends<'_>,
    violations: &mut Vec<Violation>,
) {
    let mut count: HashMap<&str, usize> = HashMap::new();
    for id in inputs.processes.iter().flat_map(|p| rows(&p.lines)) {
        *count.entry(id).or_default() += 1;
    }
    let mut home: HashMap<&str, (&str, Position)> = HashMap::new();
    for (key, records) in splits {
        for (pos, id) in records {
            home.entry(id).or_insert((key.as_str(), *pos));
        }
    }
    let highest: Vec<HashMap<&str, Position>> = inputs
        .processes
        .iter()
        .map(|p| {
            let mut top: HashMap<&str, Position> = HashMap::new();
            for id in rows(&p.lines) {
                if let Some(&(key, pos)) = home.get(id) {
                    let at = top.entry(key).or_insert(pos);
                    *at = (*at).max(pos);
                }
            }
            top
        })
        .collect();
    let end_of_run = inputs
        .processes
        .iter()
        .flat_map(|p| &p.lines)
        .chain(inputs.faults)
        .map(|l| l.t_ms)
        .max()
        .unwrap_or(0);
    let windows: Vec<Vec<Window>> = inputs
        .processes
        .iter()
        .map(|p| windows(p, inputs.faults, end_of_run))
        .collect();
    let slack = inputs.timing.slack();
    let none = BTreeMap::new();
    let mut changes: HashMap<&str, Vec<Change<'_, '_>>> = HashMap::new();
    let mut unexplained: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in generated_ids(inputs.generated) {
        if count.get(id).copied().unwrap_or(0) < 2 {
            continue;
        }
        let Some(&(key, pos)) = home.get(id) else {
            continue;
        };
        let changes = changes
            .entry(key)
            .or_insert_with(|| tenancy_changes(key, landed.get(key).unwrap_or(&none), sends));
        let explained = changes.iter().any(|c| {
            pos >= c.replaced
                && c.from.iter().any(|a| {
                    let Some(&(ordinal, record)) = highest[a.process_index].get(key) else {
                        return false;
                    };
                    let released = c.released_by.contains(&a.process_index);
                    let faulted = c.raised
                        && c.claims.iter().any(|b| {
                            windows[a.process_index]
                                .iter()
                                .any(|w| w.start <= b.t_ms && b.t_ms <= w.anchor + slack)
                        });
                    pos < (ordinal, record + 1) && (released || faulted)
                })
        });
        if !explained {
            unexplained.entry(key).or_default().push(id);
        }
    }
    for (key, ids) in unexplained {
        let detail = format!(
            "written more than once with no fault or replay to explain it: {}",
            listed(&ids)
        );
        violations.push(violation(Check::UnexplainedDuplicate, Some(key), detail));
    }
}

/// Every claim on `key` in revision order, with the tenancy it replaced.
fn tenancy_changes<'s, 'a>(
    key: &str,
    revisions: &'s BTreeMap<u64, Progress>,
    sends: &'s Sends<'a>,
) -> Vec<Change<'s, 'a>> {
    let at = |w: Option<i64>| w.map_or((0, 0), decode_position);
    let mut changes = Vec::new();
    let mut tenancy: Vec<&Send<'_>> = Vec::new();
    let mut before: Option<(u64, &Progress)> = None;
    let mut prev: Option<(u64, &Progress)> = None;
    for (&rev, value) in revisions {
        if let (Some((q, p)), Some(owner)) = (prev, value.owner.as_deref())
            && classify(p, value, owner) == Some(WriteKind::Claim)
        {
            let claims: Vec<&Send<'_>> = senders(sends, key, Some(q), value).collect();
            let released_by = before
                .map(|(r, b)| {
                    senders(sends, key, Some(r), p)
                        .filter(|s| classify(b, p, &s.process.instance) == Some(WriteKind::Release))
                        .map(|s| s.process_index)
                        .collect()
                })
                .unwrap_or_default();
            changes.push(Change {
                from: std::mem::replace(&mut tenancy, claims.clone()),
                claims,
                replaced: at(p.watermark),
                raised: value.attempts > p.attempts,
                released_by,
            });
        }
        before = prev;
        prev = Some((rev, value));
    }
    changes
}

/// The merged fault windows on one process.
///
/// Kills, aborts, `err_after_land` lines and proxy faults other than `pass`
/// and those named `delay…` last zero time; SIGSTOPs and toxics last their duration. A
/// `stop` or `leader_stop` line lasts until the next `sigcont` for the process, else its next
/// kill, else `end_of_run`.
fn windows(process: &ProcessJournal, faults: &[Line], end_of_run: u64) -> Vec<Window> {
    let mine = |instance: &str, pid: u32| instance == process.instance && pid == process.pid;
    // (start, end, a kill or abort)
    let mut raw: Vec<(u64, u64, bool)> = Vec::new();
    for line in &process.lines {
        let t = line.t_ms;
        match &line.event {
            Event::Abort { .. } => raw.push((t, t, true)),
            Event::ErrAfterLand { .. } => raw.push((t, t, false)),
            Event::Stop { .. } | Event::LeaderStop { .. } => {
                let after = || faults.iter().filter(|l| l.t_ms >= t);
                let sigcont = after().find(
                    |l| matches!(&l.event, Event::Sigcont { instance, pid } if mine(instance, *pid)),
                );
                let kill = || {
                    after().find(
                        |l| matches!(&l.event, Event::Kill { instance, pid, .. } if mine(instance, *pid)),
                    )
                };
                let end = sigcont.or_else(kill).map_or(end_of_run, |l| l.t_ms);
                raw.push((t, end, false));
            }
            _ => {}
        }
    }
    for line in faults {
        let t = line.t_ms;
        match &line.event {
            Event::Kill { instance, pid, .. } if mine(instance, *pid) => raw.push((t, t, true)),
            Event::Sigstop {
                instance,
                pid,
                duration_ms,
            }
            | Event::Toxic {
                instance,
                pid,
                duration_ms,
                ..
            } if mine(instance, *pid) => raw.push((t, t + duration_ms, false)),
            Event::ProxyFault {
                instance,
                pid,
                fault,
                ..
            } if mine(instance, *pid) && fault != "pass" && !fault.starts_with("delay") => {
                raw.push((t, t, false));
            }
            _ => {}
        }
    }
    raw.sort_unstable();
    // (start, end, earliest kill or abort)
    let mut merged: Vec<(u64, u64, Option<u64>)> = Vec::new();
    for (start, end, killed) in raw {
        let killed = killed.then_some(start);
        match merged.last_mut() {
            Some(last) if start <= last.1 => {
                last.1 = last.1.max(end);
                last.2 = last.2.or(killed);
            }
            _ => merged.push((start, end, killed)),
        }
    }
    merged
        .into_iter()
        .map(|(start, end, killed)| {
            let replaced = killed.and_then(|k| {
                faults
                    .iter()
                    .find(|l| {
                        l.t_ms >= k
                            && matches!(&l.event, Event::Respawn { instance, .. } if *instance == process.instance)
                    })
                    .map(|l| l.t_ms)
            });
            Window {
                start,
                anchor: end.max(replaced.unwrap_or(0)),
            }
        })
        .collect()
}

/// Property 5 over one key's landed values in revision order: the epoch never
/// falls, one owner per epoch, and a moved watermark carries the highest epoch
/// at any lower revision.
fn epoch_order(
    key: &str,
    revisions: &BTreeMap<u64, Progress>,
    sends: &Sends<'_>,
    violations: &mut Vec<Violation>,
) {
    let mut highest: Option<u64> = None;
    let mut owners: HashMap<u64, Vec<&str>> = HashMap::new();
    let mut prev: Option<(u64, &Progress)> = None;
    for (&rev, value) in revisions {
        let mut flag = |check: Check, detail: String| {
            let writers: Vec<&Send<'_>> =
                senders(sends, key, prev.map(|(r, _)| r), value).collect();
            by_writer(check, key, rev, &writers, &detail, violations);
        };
        if let (Some(top), Some((_, p))) = (highest, prev) {
            if value.epoch < top {
                flag(
                    Check::EpochRegressed,
                    format!(
                        "epoch {} below epoch {top} at a lower revision",
                        value.epoch
                    ),
                );
            }
            if value.watermark != p.watermark && value.epoch != top {
                flag(
                    Check::StaleEpochCommit,
                    format!(
                        "the watermark moved under epoch {} while the highest earlier epoch is {top}",
                        value.epoch
                    ),
                );
            }
        }
        if let Some(owner) = value.owner.as_deref() {
            let seen = owners.entry(value.epoch).or_default();
            if !seen.contains(&owner) {
                if !seen.is_empty() {
                    flag(
                        Check::TwoOwners,
                        format!(
                            "{owner} owns epoch {} after {}",
                            value.epoch,
                            seen.join(", ")
                        ),
                    );
                }
                seen.push(owner);
            }
        }
        highest = Some(highest.map_or(value.epoch, |h| h.max(value.epoch)));
        prev = Some((rev, value));
    }
}

/// One violation per process that sent the value, or one naming no writer.
fn by_writer(
    check: Check,
    key: &str,
    rev: u64,
    writers: &[&Send<'_>],
    detail: &str,
    violations: &mut Vec<Violation>,
) {
    let base = Violation {
        check,
        key: Some(key.to_owned()),
        rev: Some(rev),
        instance: None,
        pid: None,
        detail: detail.to_owned(),
    };
    for w in writers {
        violations.push(Violation {
            instance: Some(w.process.instance.clone()),
            pid: Some(w.process.pid),
            ..base.clone()
        });
    }
    if writers.is_empty() {
        violations.push(base);
    }
}

fn splits<'a>(inputs: &Inputs<'a>) -> Result<Splits<'a>, OracleError> {
    #[derive(Deserialize)]
    struct Spec {
        descriptor: String,
    }
    let objects: HashMap<&str, &GeneratedObject> = inputs
        .generated
        .iter()
        .map(|o| (o.key.as_str(), o))
        .collect();
    let mut splits = BTreeMap::new();
    for entry in inputs.sweep {
        let Some(id) = entry.key.strip_prefix(SPEC_PREFIX) else {
            continue;
        };
        let fail = |what: String| OracleError(format!("{}: {what}", entry.key));
        let spec: Spec = serde_json::from_slice(&entry.value).map_err(|e| fail(e.to_string()))?;
        let bytes = STANDARD
            .decode(&spec.descriptor)
            .map_err(|e| fail(e.to_string()))?;
        let descriptor = SplitDescriptor::decode(&bytes).map_err(|e| fail(e.to_string()))?;
        let mut records = Vec::new();
        for (ordinal, member) in descriptor.objects.iter().enumerate() {
            let object = objects
                .get(member.key.as_str())
                .ok_or_else(|| fail(format!("{} was never generated", member.key)))?;
            let ordinal = u32::try_from(ordinal).map_err(|e| fail(e.to_string()))?;
            let in_range = |r: &&GeneratedRecord| {
                descriptor
                    .range
                    .is_none_or(|range| (range.start..range.end).contains(&r.offset))
            };
            for (index, record) in (0_u64..).zip(object.records.iter().filter(in_range)) {
                records.push(((ordinal, index), record.id.as_str()));
            }
        }
        splits.insert(format!("{SPLIT_PREFIX}{id}"), records);
    }
    Ok(splits)
}

/// Who reported a landed value.
#[derive(Clone, Copy)]
enum Origin {
    Sweep,
    Process(usize),
}

/// Every value known to have landed, per `split.` key and revision, and a
/// [`Check::ValueConflict`] for each `(key, rev)` observed with two values.
fn landed<'a>(inputs: &Inputs<'a>) -> Result<(Landed<'a>, Vec<Violation>), OracleError> {
    let mut observed: BTreeMap<(&'a str, u64), Vec<(Origin, Progress)>> = BTreeMap::new();
    let mut learn = |key: &'a str, rev: u64, origin: Origin, value: &Progress| {
        if key.starts_with(SPLIT_PREFIX) {
            observed
                .entry((key, rev))
                .or_default()
                .push((origin, value.clone()));
        }
    };
    for entry in inputs.sweep {
        if entry.key.starts_with(SPLIT_PREFIX) {
            let value = Progress::parse(&entry.value)
                .map_err(|e| OracleError(format!("{}: {e}", entry.key)))?;
            learn(&entry.key, entry.rev, Origin::Sweep, &value);
        }
    }
    for (index, process) in inputs.processes.iter().enumerate() {
        let origin = Origin::Process(index);
        let mut sent: HashMap<u64, (&str, &Progress)> = HashMap::new();
        for line in &process.lines {
            match &line.event {
                Event::Seen {
                    key, rev, value, ..
                } => learn(key, *rev, origin, value),
                Event::Send {
                    call, key, value, ..
                } => {
                    sent.insert(*call, (key, value));
                }
                Event::Done {
                    call,
                    reply: Reply::Won(rev),
                    ..
                } => {
                    if let Some((key, value)) = sent.get(call) {
                        learn(key, *rev, origin, value);
                    }
                }
                _ => {}
            }
        }
    }
    let mut landed: Landed<'a> = HashMap::new();
    let mut conflicts = Vec::new();
    for ((key, rev), seen) in observed {
        let value = &seen[0].1;
        if value.watermark.is_some_and(|w| w < 0) {
            return Err(OracleError(format!(
                "{key}: negative watermark at revision {rev}"
            )));
        }
        if seen.iter().any(|(_, v)| v != value) {
            conflicts.push(Violation {
                check: Check::ValueConflict,
                key: Some(key.to_owned()),
                rev: Some(rev),
                instance: None,
                pid: None,
                detail: disagreement(inputs.processes, &seen),
            });
        }
        landed.entry(key).or_default().insert(rev, value.clone());
    }
    Ok((landed, conflicts))
}

/// Names each distinct value observed at one revision and who reported it,
/// in an order that does not depend on the order of the inputs.
fn disagreement(processes: &[ProcessJournal], seen: &[(Origin, Progress)]) -> String {
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for (origin, value) in seen {
        let shown = format!("{value:?}");
        let who = match origin {
            Origin::Sweep => "the sweep".to_owned(),
            Origin::Process(i) => format!("{} (pid {})", processes[*i].instance, processes[*i].pid),
        };
        match groups.iter_mut().find(|(s, _)| *s == shown) {
            Some((_, whos)) => whos.push(who),
            None => groups.push((shown, vec![who])),
        }
    }
    for (_, whos) in &mut groups {
        whos.sort();
        whos.dedup();
    }
    groups.sort();
    let mut detail = format!("{} values observed:", groups.len());
    for (shown, whos) in groups {
        let _ = write!(detail, " [{shown}] from {};", whos.join(", "));
    }
    detail.pop();
    detail
}

/// Every `send` line, per key.
fn sends(processes: &[ProcessJournal]) -> Sends<'_> {
    let mut sends: Sends<'_> = HashMap::new();
    for (process_index, process) in processes.iter().enumerate() {
        for (line, entry) in process.lines.iter().enumerate() {
            if let Event::Send {
                key,
                expected,
                value,
                ..
            } = &entry.event
            {
                sends.entry(key).or_default().push(Send {
                    process,
                    process_index,
                    line,
                    t_ms: entry.t_ms,
                    expected: *expected,
                    value,
                });
            }
        }
    }
    sends
}

/// The sends a value landed on `key` after revision `expected` is attributed
/// to.
fn senders<'s, 'a>(
    sends: &'s Sends<'a>,
    key: &str,
    expected: Option<u64>,
    value: &'s Progress,
) -> impl Iterator<Item = &'s Send<'a>> {
    sends
        .get(key)
        .into_iter()
        .flatten()
        .filter(move |s| s.value == value && s.expected == expected)
}

/// The index of the first `rows` line naming each id in one journal.
fn first_rows(process: &ProcessJournal) -> HashMap<&str, usize> {
    let mut first = HashMap::new();
    for (index, line) in process.lines.iter().enumerate() {
        if let Event::Rows { ids } = &line.event {
            for id in ids {
                first.entry(id.as_str()).or_insert(index);
            }
        }
    }
    first
}

fn rows(lines: &[Line]) -> impl Iterator<Item = &str> {
    lines
        .iter()
        .flat_map(|line| match &line.event {
            Event::Rows { ids } => ids.as_slice(),
            _ => &[],
        })
        .map(String::as_str)
}

fn generated_ids(generated: &[GeneratedObject]) -> impl Iterator<Item = &str> {
    generated
        .iter()
        .flat_map(|o| &o.records)
        .map(|r| r.id.as_str())
}

fn violation(check: Check, key: Option<&str>, detail: String) -> Violation {
    Violation {
        check,
        key: key.map(str::to_owned),
        rev: None,
        instance: None,
        pid: None,
        detail,
    }
}

/// `ids` as a count and the first few, for a violation's detail.
fn listed(ids: &[&str]) -> String {
    let shown = ids[..ids.len().min(NAMED)].join(", ");
    if ids.len() > NAMED {
        format!("{} records ({shown}, ...)", ids.len())
    } else {
        format!("{} records ({shown})", ids.len())
    }
}

#[cfg(test)]
mod tests;
