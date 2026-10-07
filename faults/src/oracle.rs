//! The delivery oracle: judges a run's journals and the store's final durable
//! state against the delivery properties.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use spate_s3::SplitDescriptor;
use spate_s3::fuzz_seams::decode_position;

use crate::journal::{Event, Line, Progress, Reply, Status};
use crate::outcome::{Check, Violation};

const SPLIT_PREFIX: &str = "split.";
const SPEC_PREFIX: &str = "spec.";
const VERDICT_KEY: &str = "verdict";
/// How many ids a grouped violation names.
const NAMED: usize = 5;

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

/// Checks properties 1, 3 and 4 and returns every violation found.
///
/// A value landed at `(key, rev)` when a `seen` line, a `won` reply or the
/// sweep shows it there. It is attributed to every process whose journal sent
/// that value on that key against the key's next lower landed revision,
/// whether or not a `done` line followed.
///
/// # Errors
///
/// Fails when a swept record or spec does not decode, a descriptor names an
/// object the harness did not generate, or a landed watermark is negative.
pub fn check(inputs: &Inputs<'_>) -> Result<Vec<Violation>, OracleError> {
    let splits = splits(inputs)?;
    let landed = landed(inputs)?;
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

/// A `send` line: its process, its index in that journal, and what it sent.
struct Send<'a> {
    process: &'a ProcessJournal,
    process_index: usize,
    line: usize,
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
    sends: &HashMap<&str, Vec<Send<'_>>>,
    written: &[HashMap<&str, usize>],
    violations: &mut Vec<Violation>,
) {
    let at = |w: Option<i64>| w.map_or((0, 0), decode_position);
    let (lo, hi) = (at(write.from), at(write.value.watermark));
    let senders: Vec<&Send<'_>> = sends
        .get(write.key)
        .into_iter()
        .flatten()
        .filter(|s| s.value == write.value && s.expected == write.expected)
        .collect();
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

/// Every value known to have landed, per `split.` key and revision.
fn landed<'a>(
    inputs: &Inputs<'a>,
) -> Result<HashMap<&'a str, BTreeMap<u64, Progress>>, OracleError> {
    let mut landed: HashMap<&str, BTreeMap<u64, Progress>> = HashMap::new();
    let mut learn = |key: &'a str, rev: u64, value: &Progress| {
        if key.starts_with(SPLIT_PREFIX) {
            landed
                .entry(key)
                .or_default()
                .entry(rev)
                .or_insert_with(|| value.clone());
        }
    };
    for entry in inputs.sweep {
        if entry.key.starts_with(SPLIT_PREFIX) {
            let value = Progress::parse(&entry.value)
                .map_err(|e| OracleError(format!("{}: {e}", entry.key)))?;
            learn(&entry.key, entry.rev, &value);
        }
    }
    for process in inputs.processes {
        let mut sent: HashMap<u64, (&str, &Progress)> = HashMap::new();
        for line in &process.lines {
            match &line.event {
                Event::Seen {
                    key, rev, value, ..
                } => learn(key, *rev, value),
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
                        learn(key, *rev, value);
                    }
                }
                _ => {}
            }
        }
    }
    for (key, revisions) in &landed {
        for (rev, value) in revisions {
            if value.watermark.is_some_and(|w| w < 0) {
                return Err(OracleError(format!(
                    "{key}: negative watermark at revision {rev}"
                )));
            }
        }
    }
    Ok(landed)
}

/// Every `send` line, per key.
fn sends(processes: &[ProcessJournal]) -> HashMap<&str, Vec<Send<'_>>> {
    let mut sends: HashMap<&str, Vec<Send<'_>>> = HashMap::new();
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
                    expected: *expected,
                    value,
                });
            }
        }
    }
    sends
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
