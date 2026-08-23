//! Entry points into the parsers that read what the coordination store
//! returns.
//!
//! Follows the rules [`bench_seams`](crate::bench_seams) states.
//!
//! A parser reports its verdict as an [`Option`], dropping the message a
//! rejection carries. Each record parser hands the record back re-encoded, so
//! a caller can parse what this build wrote. The encoders fix the advisory
//! wall-clock stamp at zero, so the same fields always produce the same
//! bytes.

use crate::records::{
    self, PlanFinalityRepr, PlanRecord, SplitProgressRecord, SplitSpecRecord, SplitStatus,
};
use spate_core::coordination::SplitId;

/// Encode the `spec.{id}` record the leader writes once at planning time.
#[must_use]
pub fn encode_spec_record(
    id: &str,
    fp: u64,
    generation: u64,
    weight: u64,
    descriptor: &[u8],
) -> Vec<u8> {
    SplitSpecRecord {
        schema: records::SCHEMA,
        id: id.to_string(),
        fp,
        generation,
        weight,
        descriptor: records::b64_encode(descriptor),
    }
    .encode()
}

/// Encode the `split.{id}` record a claim, a fence or a commit writes.
///
/// `progress` is the committed watermark, resume state and terminal flag, or
/// `None` for a record nothing has committed to. The status follows the
/// terminal flag, as it does on the record a commit writes.
#[must_use]
pub fn encode_progress_record(
    id: &str,
    fp: u64,
    epoch: u64,
    owner: Option<&str>,
    attempts: u32,
    progress: Option<(i64, &[u8], bool)>,
) -> Vec<u8> {
    let completed = progress.is_some_and(|(_, _, completed)| completed);
    SplitProgressRecord {
        schema: records::SCHEMA,
        id: id.to_string(),
        fp,
        epoch,
        status: if completed {
            SplitStatus::Completed
        } else {
            SplitStatus::Runnable
        },
        owner: owner.map(str::to_string),
        attempts,
        watermark: progress.map(|(watermark, _, _)| watermark),
        state: progress.map(|(_, state, _)| records::b64_encode(state)),
        completed,
        written_at_ms: 0,
    }
    .encode()
}

/// Encode the `plan` record a leader publishes.
#[must_use]
pub fn encode_plan_record(
    fingerprint: &str,
    generation: u64,
    is_final: bool,
    planned: u64,
    planner_state: Option<&[u8]>,
) -> Vec<u8> {
    PlanRecord {
        schema: records::SCHEMA,
        fingerprint: fingerprint.to_string(),
        generation,
        finality: if is_final {
            PlanFinalityRepr::Final
        } else {
            PlanFinalityRepr::Open
        },
        planned,
        planner_state: planner_state.map(records::b64_encode),
        updated_at_ms: 0,
    }
    .encode()
}

/// Parse the `spec.{id}` record stored at `key` under the job fingerprint
/// hash `fp`, and re-encode it. `None` when the bytes are not a spec record
/// this build reads at this key for this job.
#[must_use]
pub fn parse_spec_record(key: &str, bytes: &[u8], fp: u64) -> Option<Vec<u8>> {
    SplitSpecRecord::parse(key, bytes, fp)
        .ok()
        .map(|record| record.encode())
}

/// Parse the `split.{id}` record stored at `key` under the job fingerprint
/// hash `fp`, and re-encode it. `None` when the bytes are not a progress
/// record this build reads at this key for this job.
#[must_use]
pub fn parse_progress_record(key: &str, bytes: &[u8], fp: u64) -> Option<Vec<u8>> {
    SplitProgressRecord::parse(key, bytes, fp)
        .ok()
        .map(|record| record.encode())
}

/// Parse the `plan` record for the job `fingerprint`, and re-encode it.
/// `None` when the bytes are not a plan record this build reads for this job.
#[must_use]
pub fn parse_plan_record(bytes: &[u8], fingerprint: &str) -> Option<Vec<u8>> {
    PlanRecord::parse(bytes, fingerprint)
        .ok()
        .map(|record| record.encode())
}

/// The split id and descriptor a stored `spec.{id}` record carries, as the
/// worker starting the split reads them. `None` when the record does not
/// parse, its id is not a valid split id, or its base64 does not decode.
#[must_use]
pub fn spec_payload(key: &str, bytes: &[u8], fp: u64) -> Option<(String, Vec<u8>)> {
    let spec = SplitSpecRecord::parse(key, bytes, fp).ok()?.spec().ok()?;
    Some((spec.id.as_str().to_string(), spec.descriptor))
}

/// The committed progress a stored `split.{id}` record carries, as
/// `(watermark, resume state, terminal flag)`. `None` when the record does
/// not parse, has never been committed to, or its base64 does not decode.
#[must_use]
pub fn committed_progress(key: &str, bytes: &[u8], fp: u64) -> Option<(i64, Vec<u8>, bool)> {
    let progress = SplitProgressRecord::parse(key, bytes, fp)
        .ok()?
        .progress()
        .ok()??;
    Some((progress.watermark, progress.state, progress.completed))
}

/// What each of the five prefixed keyspaces reads out of `key`, in the order
/// `[split, spec, worker, probe, assign]`. A slot is `None` where that
/// keyspace's prefix does not match.
#[must_use]
pub fn parse_keys(key: &str) -> [Option<&str>; 5] {
    [
        records::parse_split_key(key),
        records::parse_spec_key(key),
        records::parse_worker_key(key),
        records::parse_probe_key(key),
        records::parse_assign_key(key),
    ]
}

/// The `split.{id}` and `spec.{id}` keys a split id is written under, in that
/// order. `None` when `id` is not a valid split id.
#[must_use]
pub fn split_keys(id: &str) -> Option<[String; 2]> {
    let id = SplitId::new(id).ok()?;
    Some([records::split_key(&id), records::spec_key(&id)])
}

/// The `worker.{instance}`, `_probe.{instance}` and `assign.{instance}` keys
/// an instance is written under, in that order. `None` when `instance` is not
/// a valid instance id.
#[must_use]
pub fn instance_keys(instance: &str) -> Option<[String; 3]> {
    records::validate_instance_id(instance).ok()?;
    Some([
        records::worker_key(instance),
        records::probe_key(instance),
        records::assign_key(instance),
    ])
}
