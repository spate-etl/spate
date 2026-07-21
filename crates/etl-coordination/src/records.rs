//! On-store record schemas: encode/decode + validation discipline.
//!
//! Everything persisted is schema-versioned compact JSON. Parsing is
//! strict and every failure mode gets a distinct, actionable Fatal
//! message — a corrupt or alien record means two incompatible jobs share
//! a store prefix, and limping past that would corrupt fencing.
//!
//! A split's durable state is deliberately **two** records: the immutable
//! [`SplitSpecRecord`] (descriptor — potentially hundreds of KiB), written
//! once at planning, and the small mutable [`SplitProgressRecord`] (owner,
//! epoch, attempts, watermark), which is the CAS target for every claim,
//! fence, and commit. Commit cost is therefore independent of descriptor
//! size. The leader creates the spec before the progress record, so a
//! progress record implies its spec exists (watch delivery may still show
//! them in either order — consumers buffer).
//!
//! Layout (keys relative to the per-job buckets):
//!
//! | Keyspace  | Key                 | Record        |
//! |-----------|---------------------|---------------|
//! | Durable   | `plan`              | [`PlanRecord`]          — fingerprint, generation, finality, planner cursor |
//! | Durable   | `spec.{id}`         | [`SplitSpecRecord`]     — descriptor, weight (immutable) |
//! | Durable   | `split.{id}`        | [`SplitProgressRecord`] — epoch, status, attempts, progress (the fence) |
//! | Durable   | `assign.{instance}` | [`AssignmentVal`]       — the leader's desired assignment |
//! | Ephemeral | `leader`            | [`LeaderVal`]           — leadership lease |
//! | Ephemeral | `worker.{instance}` | [`WorkerVal`]           — membership presence |
//! | Ephemeral | `split.{id}`        | [`LeaseVal`]            — split lease |

use crate::error::fatal;
use base64::Engine as _;
use etl_core::coordination::{CoordinationError, PlanFinality, SplitId, SplitProgress, SplitSpec};
use serde::{Deserialize, Serialize};
use std::hash::BuildHasher as _;

/// Schema version stamped into every record.
///
/// 3 replaced the peer-to-peer revocation-request key with the leader's
/// `assign.{instance}` records.
///
/// The versions do not interoperate, and they do not *try* to: this pin is
/// checked on the plan record during startup and on every split and spec
/// record afterwards, and a mismatch is [`Fatal`] on both sides. So a
/// schema-3 worker cannot join a store a schema-2 fleet wrote, and vice
/// versa — an upgrade needs a fresh store prefix, not merely a full-fleet
/// restart, and an in-flight bounded job restarts from the beginning.
/// (At-least-once holds through that: the re-run duplicates, it does not
/// lose.) See the "Upgrading" section of the scaling-out guide.
///
/// Failing closed is the point. Ownership transfers on the durable
/// progress-record CAS regardless of vintage, so a mixed fleet could not
/// corrupt anything — but it would rebalance against two different models
/// at once, and diagnosing that is far worse than refusing to start.
///
/// [`Fatal`]: etl_core::coordination::CoordinationErrorKind::Fatal
pub(crate) const SCHEMA: u32 = 3;

/// Key of the plan record in the durable keyspace.
pub(crate) const PLAN_KEY: &str = "plan";
/// Key of the leadership lease in the ephemeral keyspace.
pub(crate) const LEADER_KEY: &str = "leader";
/// Prefix of progress keys (durable) and lease keys (ephemeral). The
/// progress record and the lease of one split share a key name: they are
/// the two faces of the same contended entity.
pub(crate) const SPLIT_PREFIX: &str = "split.";
/// Prefix of immutable spec records in the durable keyspace.
pub(crate) const SPEC_PREFIX: &str = "spec.";
/// Prefix of worker presence keys in the ephemeral keyspace.
pub(crate) const WORKER_PREFIX: &str = "worker.";
/// Prefix of the leader's assignment records in the **durable** keyspace.
/// Durable and not ephemeral on purpose: an assignment must outlive the
/// leadership that wrote it, or every leader gap would blank the fleet's
/// instructions and stall reconciliation until a new leader had planned.
pub(crate) const ASSIGN_PREFIX: &str = "assign.";

/// `split.{id}` key (durable progress record / ephemeral lease).
pub(crate) fn split_key(id: &SplitId) -> String {
    format!("{SPLIT_PREFIX}{id}")
}

/// `split.{id}` key from a raw id string.
pub(crate) fn split_key_str(id: &str) -> String {
    format!("{SPLIT_PREFIX}{id}")
}

/// `spec.{id}` key for the immutable spec record.
pub(crate) fn spec_key(id: &SplitId) -> String {
    format!("{SPEC_PREFIX}{id}")
}

/// `worker.{instance}` presence key.
pub(crate) fn worker_key(instance: &str) -> String {
    format!("{WORKER_PREFIX}{instance}")
}

/// The split id encoded in a `split.{id}` key, if it is one.
pub(crate) fn parse_split_key(key: &str) -> Option<&str> {
    key.strip_prefix(SPLIT_PREFIX)
}

/// The split id encoded in a `spec.{id}` key, if it is one.
pub(crate) fn parse_spec_key(key: &str) -> Option<&str> {
    key.strip_prefix(SPEC_PREFIX)
}

/// The instance encoded in a `worker.{instance}` key, if it is one.
pub(crate) fn parse_worker_key(key: &str) -> Option<&str> {
    key.strip_prefix(WORKER_PREFIX)
}

/// `assign.{instance}` assignment key.
pub(crate) fn assign_key(instance: &str) -> String {
    format!("{ASSIGN_PREFIX}{instance}")
}

/// The instance encoded in an `assign.{instance}` key, if it is one.
pub(crate) fn parse_assign_key(key: &str) -> Option<&str> {
    key.strip_prefix(ASSIGN_PREFIX)
}

/// Stable 64-bit digest of the job fingerprint, stamped into every split
/// record so a record from a differently-configured job is rejected even
/// if it somehow lands under this job's prefix.
pub(crate) fn fingerprint_hash(fingerprint: &str) -> u64 {
    foldhash::fast::FixedState::with_seed(0).hash_one(fingerprint)
}

/// Milliseconds since the Unix epoch — **advisory only** (diagnostics);
/// never compared across machines and never part of any protocol rule.
pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// A split's lifecycle on the durable progress record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SplitStatus {
    /// Claimable (unless a live lease key exists).
    Runnable,
    /// Terminal: fully delivered and committed.
    Completed,
    /// Terminal: out of delivery attempts; never re-offered.
    Quarantined,
}

/// The immutable per-split record: everything a worker needs to *start*
/// the split. Written once by the leader (create-if-absent) and never
/// updated, so its size never taxes the commit path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitSpecRecord {
    pub(crate) schema: u32,
    /// Redundant copy of the key's split id; must match on read.
    pub(crate) id: String,
    /// [`fingerprint_hash`] of the job fingerprint; must match on read.
    pub(crate) fp: u64,
    /// Plan generation that created this record.
    pub(crate) generation: u64,
    /// Planner cost hint, carried for claim ordering.
    pub(crate) weight: u64,
    /// Base64 of the source-defined descriptor.
    pub(crate) descriptor: String,
}

impl SplitSpecRecord {
    pub(crate) fn planned(spec: &SplitSpec, fp: u64, generation: u64) -> SplitSpecRecord {
        SplitSpecRecord {
            schema: SCHEMA,
            id: spec.id.as_str().to_string(),
            fp,
            generation,
            weight: spec.weight.max(1),
            descriptor: b64_encode(&spec.descriptor),
        }
    }

    /// The split spec carried by this record.
    pub(crate) fn spec(&self) -> Result<SplitSpec, CoordinationError> {
        let id = SplitId::new(self.id.clone())?;
        let descriptor = b64_decode(&self.descriptor).map_err(|e| {
            fatal(format!(
                "spec record {}: corrupt descriptor ({e}); the store prefix holds records \
                 this build cannot read",
                self.id
            ))
        })?;
        Ok(SplitSpec::new(id, descriptor).with_weight(self.weight))
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("spec record serializes")
    }

    /// Strict parse + pin validation.
    pub(crate) fn parse(
        key: &str,
        bytes: &[u8],
        fp: u64,
    ) -> Result<SplitSpecRecord, CoordinationError> {
        let record: SplitSpecRecord = serde_json::from_slice(bytes).map_err(|e| {
            fatal(format!(
                "spec record at {key}: not a schema-{SCHEMA} record ({e}); another system \
                 or an incompatible build shares this store prefix"
            ))
        })?;
        validate_pins(key, "spec record", record.schema, record.fp, fp)?;
        if parse_spec_key(key) != Some(record.id.as_str()) {
            return Err(fatal(format!(
                "spec record at {key} claims id {}; the store prefix is corrupt",
                record.id
            )));
        }
        Ok(record)
    }
}

/// The mutable per-split record: fencing state and committed progress.
/// Small by construction — this is what every claim and commit CASes, so
/// its encode cost is the per-commit cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitProgressRecord {
    pub(crate) schema: u32,
    /// Redundant copy of the key's split id; must match on read.
    pub(crate) id: String,
    /// [`fingerprint_hash`] of the job fingerprint; must match on read.
    pub(crate) fp: u64,
    /// Fencing token: bumped on every ownership change. `0` = never owned.
    pub(crate) epoch: u64,
    pub(crate) status: SplitStatus,
    /// Owner of the current/last tenancy. `None` after a graceful release
    /// (or before any claim) — the claim path reads this, not watch
    /// markers, to decide whether a takeover consumes a delivery attempt.
    pub(crate) owner: Option<String>,
    /// Delivery attempts consumed (non-graceful tenancy ends + explicit
    /// failure reports).
    pub(crate) attempts: u32,
    /// Committed watermark; `None` before the first commit.
    pub(crate) watermark: Option<i64>,
    /// Base64 of the source-defined resume state.
    pub(crate) state: Option<String>,
    /// Terminal flag of the committed progress.
    pub(crate) completed: bool,
    /// Advisory wall-clock stamp of the last write.
    pub(crate) written_at_ms: i64,
}

impl SplitProgressRecord {
    /// A freshly planned, never-owned record (optionally seeded).
    pub(crate) fn planned(
        id: &SplitId,
        fp: u64,
        seed: Option<&SplitProgress>,
    ) -> SplitProgressRecord {
        SplitProgressRecord {
            schema: SCHEMA,
            id: id.as_str().to_string(),
            fp,
            epoch: 0,
            status: if seed.is_some_and(|s| s.completed) {
                SplitStatus::Completed
            } else {
                SplitStatus::Runnable
            },
            owner: None,
            attempts: 0,
            watermark: seed.map(|s| s.watermark),
            state: seed.map(|s| b64_encode(&s.state)),
            completed: seed.is_some_and(|s| s.completed),
            written_at_ms: now_ms(),
        }
    }

    /// The committed progress carried by this record, if any.
    pub(crate) fn progress(&self) -> Result<Option<SplitProgress>, CoordinationError> {
        let Some(watermark) = self.watermark else {
            return Ok(None);
        };
        let state = match &self.state {
            Some(encoded) => b64_decode(encoded).map_err(|e| {
                fatal(format!(
                    "split record {}: corrupt resume state ({e}); the store prefix holds \
                     records this build cannot read",
                    self.id
                ))
            })?,
            None => Vec::new(),
        };
        Ok(Some(if self.completed {
            SplitProgress::completed(watermark, state)
        } else {
            SplitProgress::new(watermark, state)
        }))
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("progress record serializes")
    }

    /// Strict parse + pin validation.
    pub(crate) fn parse(
        key: &str,
        bytes: &[u8],
        fp: u64,
    ) -> Result<SplitProgressRecord, CoordinationError> {
        let record: SplitProgressRecord = serde_json::from_slice(bytes).map_err(|e| {
            fatal(format!(
                "split record at {key}: not a schema-{SCHEMA} record ({e}); another system \
                 or an incompatible build shares this store prefix"
            ))
        })?;
        validate_pins(key, "split record", record.schema, record.fp, fp)?;
        if parse_split_key(key) != Some(record.id.as_str()) {
            return Err(fatal(format!(
                "split record at {key} claims id {}; the store prefix is corrupt",
                record.id
            )));
        }
        Ok(record)
    }
}

/// Shared schema/fingerprint pins for the two split-record types.
fn validate_pins(
    key: &str,
    what: &str,
    schema: u32,
    record_fp: u64,
    fp: u64,
) -> Result<(), CoordinationError> {
    if schema != SCHEMA {
        return Err(fatal(format!(
            "{what} at {key}: schema {schema} but this build reads schema {SCHEMA}; \
             upgrade or isolate the store prefix"
        )));
    }
    if record_fp != fp {
        return Err(fatal(format!(
            "{what} at {key}: job fingerprint mismatch — two differently-configured \
             pipelines share this store prefix; give each job its own prefix"
        )));
    }
    Ok(())
}

/// The durable plan record: job identity, leadership generation, finality.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlanRecord {
    pub(crate) schema: u32,
    /// Full job fingerprint (config-derived); byte-equal across workers.
    pub(crate) fingerprint: String,
    /// Leadership generation: bumped by every newly elected leader before
    /// it plans. The plan record's CAS revision is the leader fence.
    pub(crate) generation: u64,
    /// Whether the enumeration is final.
    pub(crate) finality: PlanFinalityRepr,
    /// Splits planned (progress records live in the store), recounted from
    /// an authoritative listing on every *successful* publish.
    ///
    /// Treat it as a **lower bound**, not a census: seeding happens before
    /// the recount, and a publish that loses its CAS or errors leaves the
    /// seeded records behind — a `Final` plan then never replans, so the
    /// count stays short for the life of the job. Terminal detection
    /// therefore uses it only to tell whether a worker has caught up, and
    /// renders its verdict against a fresh listing (see
    /// `Task::check_terminal`).
    pub(crate) planned: u64,
    /// Base64 of the planner's opaque cursor.
    pub(crate) planner_state: Option<String>,
    /// Advisory wall-clock stamp of the last write.
    pub(crate) updated_at_ms: i64,
}

/// Serialized form of [`PlanFinality`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PlanFinalityRepr {
    Open,
    Final,
}

impl From<PlanFinality> for PlanFinalityRepr {
    fn from(f: PlanFinality) -> PlanFinalityRepr {
        match f {
            PlanFinality::Open => PlanFinalityRepr::Open,
            PlanFinality::Final => PlanFinalityRepr::Final,
        }
    }
}

impl PlanRecord {
    pub(crate) fn new(fingerprint: String) -> PlanRecord {
        PlanRecord {
            schema: SCHEMA,
            fingerprint,
            generation: 0,
            finality: PlanFinalityRepr::Open,
            planned: 0,
            planner_state: None,
            updated_at_ms: now_ms(),
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("plan record serializes")
    }

    pub(crate) fn parse(bytes: &[u8], fingerprint: &str) -> Result<PlanRecord, CoordinationError> {
        let record: PlanRecord = serde_json::from_slice(bytes).map_err(|e| {
            fatal(format!(
                "plan record: not a schema-{SCHEMA} record ({e}); another system or an \
                 incompatible build shares this store prefix"
            ))
        })?;
        if record.schema != SCHEMA {
            return Err(fatal(format!(
                "plan record: schema {} but this build reads schema {SCHEMA}; upgrade or \
                 isolate the store prefix",
                record.schema
            )));
        }
        if record.fingerprint != fingerprint {
            return Err(fatal(format!(
                "job fingerprint mismatch: this worker is configured as\n  {fingerprint}\nbut \
                 the store prefix belongs to\n  {}\nDivergent configurations can never share \
                 a coordinated job; fix the configuration or give this job its own prefix",
                record.fingerprint
            )));
        }
        Ok(record)
    }
}

/// The ephemeral split-lease value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LeaseVal {
    pub(crate) schema: u32,
    pub(crate) owner: String,
    /// Random per-process nonce: distinguishes a dead predecessor with
    /// the same stable instance id from a live twin (which is Fatal).
    pub(crate) nonce: String,
    /// The tenancy's fencing epoch (mirrors the record's).
    pub(crate) epoch: u64,
}

/// The ephemeral leadership-lease value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LeaderVal {
    pub(crate) schema: u32,
    pub(crate) owner: String,
    pub(crate) nonce: String,
    /// The generation this leadership planned under.
    pub(crate) generation: u64,
}

/// The ephemeral worker-presence value (explicit membership).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerVal {
    pub(crate) schema: u32,
    pub(crate) nonce: String,
    /// This worker's own lane budget (`max_in_flight`). The leader must
    /// balance against each member's budget rather than its own: nothing
    /// forces the fleet to be homogeneous (the job fingerprint covers
    /// *source* configuration, not coordination tuning), and a leader that
    /// assumed its own value would hand splits to a smaller worker that
    /// then refuses to claim them — permanently, because that worker keeps
    /// looking like the least loaded one.
    ///
    /// `#[serde(default)]` so a presence value written before the field
    /// existed still parses; the leader reads a missing budget as its own.
    #[serde(default)]
    pub(crate) max_in_flight: u32,
}

/// The leader's desired assignment for one instance, at
/// `assign.{instance}` in the durable keyspace.
///
/// **This is an instruction, not a grant of ownership.** A worker named
/// here may claim these splits; it is not thereby the owner, and reading
/// its own name here confers nothing until the durable progress-record CAS
/// succeeds. That separation is the whole reason a stale or split-brained
/// leader is harmless: two leaders can publish contradictory assignments
/// and still not produce two owners, because neither assignment is a
/// fence. Losing, delaying, or duplicating this record costs balance
/// latency only.
///
/// The converse instruction is implicit: a split this instance holds and
/// which is *absent* here must be released. Absence is meaningful, so a
/// worker must distinguish "no record yet" (nothing has been decided —
/// hold what you have) from "a record that omits this split" (give it up).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AssignmentVal {
    pub(crate) schema: u32,
    /// The leadership generation that computed this assignment. A worker
    /// ignores a record older than the newest generation it has seen, so a
    /// deposed leader's late write cannot walk the fleet backwards.
    pub(crate) generation: u64,
    /// The splits this instance should hold. Sorted, so an unchanged
    /// assignment is byte-identical and the leader can skip the write.
    pub(crate) splits: Vec<String>,
}

/// Encode any of the small lease values.
pub(crate) fn encode_val<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("lease value serializes")
}

/// Parse a lease-keyspace value; corrupt values are Fatal like records.
pub(crate) fn parse_val<T: for<'de> Deserialize<'de>>(
    key: &str,
    bytes: &[u8],
) -> Result<T, CoordinationError> {
    serde_json::from_slice(bytes).map_err(|e| {
        fatal(format!(
            "lease value at {key}: unreadable ({e}); another system or an incompatible \
             build shares this store prefix"
        ))
    })
}

/// Validate a stable instance id: same charset as split ids so it embeds
/// in `worker.{instance}` keys on any backend.
pub(crate) fn validate_instance_id(id: &str) -> Result<(), CoordinationError> {
    if id.is_empty() || id.len() > 128 {
        return Err(fatal(format!(
            "instance_id must be 1..=128 bytes, got {} ({id:?})",
            id.len()
        )));
    }
    if let Some(bad) = id
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(fatal(format!(
            "instance_id may only contain [A-Za-z0-9_-], got {bad:?} in {id:?} (pod names \
             with dots: replace them, e.g. with '-')"
        )));
    }
    Ok(())
}

/// Standard-alphabet, padded base64 for the descriptor/state payloads —
/// they must round-trip byte-exactly through JSON strings.
pub(crate) fn b64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub(crate) fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn split_records_round_trip_and_validate() {
        let spec = SplitSpec::new(
            SplitId::new("rows-0-100").unwrap(),
            b"\x00\x01binary descriptor".to_vec(),
        )
        .with_weight(64 << 20);
        let fp = fingerprint_hash("job:v1");

        let spec_record = SplitSpecRecord::planned(&spec, fp, 1);
        let parsed = SplitSpecRecord::parse("spec.rows-0-100", &spec_record.encode(), fp).unwrap();
        assert_eq!(parsed, spec_record);
        assert_eq!(parsed.spec().unwrap(), spec);

        let progress_record = SplitProgressRecord::planned(
            &spec.id,
            fp,
            Some(&SplitProgress::new(7, b"\xffstate".to_vec())),
        );
        let parsed =
            SplitProgressRecord::parse("split.rows-0-100", &progress_record.encode(), fp).unwrap();
        assert_eq!(parsed, progress_record);
        let progress = parsed.progress().unwrap().unwrap();
        assert_eq!(progress.watermark, 7);
        assert_eq!(progress.state, b"\xffstate");
        assert!(!progress.completed);
        assert_eq!(parsed.status, SplitStatus::Runnable);
        assert_eq!(parsed.epoch, 0, "never owned");
    }

    #[test]
    fn parse_failures_are_distinct_and_actionable() {
        let fp = fingerprint_hash("job:v1");
        let spec = SplitSpec::new(SplitId::new("a").unwrap(), vec![]);
        let spec_record = SplitSpecRecord::planned(&spec, fp, 1);
        let progress_record = SplitProgressRecord::planned(&spec.id, fp, None);

        let garbage = SplitProgressRecord::parse("split.a", b"not json", fp).unwrap_err();
        assert!(garbage.to_string().contains("another system"), "{garbage}");

        let mut wrong_schema = progress_record.clone();
        wrong_schema.schema = 99;
        let err = SplitProgressRecord::parse("split.a", &wrong_schema.encode(), fp).unwrap_err();
        assert!(err.to_string().contains("schema 99"), "{err}");

        let err = SplitProgressRecord::parse("split.b", &progress_record.encode(), fp).unwrap_err();
        assert!(err.to_string().contains("claims id a"), "{err}");

        let err =
            SplitProgressRecord::parse("split.a", &progress_record.encode(), fp + 1).unwrap_err();
        assert!(err.to_string().contains("fingerprint mismatch"), "{err}");

        let err = SplitSpecRecord::parse("spec.b", &spec_record.encode(), fp).unwrap_err();
        assert!(err.to_string().contains("claims id a"), "{err}");
        let err = SplitSpecRecord::parse("spec.a", &spec_record.encode(), fp + 1).unwrap_err();
        assert!(err.to_string().contains("fingerprint mismatch"), "{err}");

        let plan = PlanRecord::new("job:v1".into());
        let err = PlanRecord::parse(&plan.encode(), "job:v2").unwrap_err();
        assert!(err.to_string().contains("job:v1"), "{err}");
        assert!(err.to_string().contains("job:v2"), "{err}");
    }

    #[test]
    fn seeded_completed_records_are_terminal() {
        let id = SplitId::new("done").unwrap();
        let record =
            SplitProgressRecord::planned(&id, 0, Some(&SplitProgress::completed(10, vec![])));
        assert_eq!(record.status, SplitStatus::Completed);
        assert!(record.progress().unwrap().unwrap().completed);
    }

    #[test]
    fn assignment_values_round_trip_and_reject_alien_shapes() {
        let val = AssignmentVal {
            schema: SCHEMA,
            generation: 4,
            splits: vec!["split-7".to_string(), "split-9".to_string()],
        };
        let parsed: AssignmentVal = parse_val(&assign_key("worker-a"), &encode_val(&val)).unwrap();
        assert_eq!(parsed, val);

        let err = parse_val::<AssignmentVal>("assign.worker-a", b"not json").unwrap_err();
        assert!(err.to_string().contains("another system"), "{err}");
        let err = parse_val::<AssignmentVal>("assign.worker-a", br#"{"schema":3,"who":"x"}"#)
            .unwrap_err();
        assert!(err.to_string().contains("unreadable"), "{err}");
        // A schema-2 peer wrote `revocation.{victim}` values here instead;
        // they must fail to parse rather than read as an empty assignment,
        // which the caller would act on by releasing everything.
        let err = parse_val::<AssignmentVal>(
            "assign.worker-a",
            br#"{"schema":2,"requester":"b","nonce":"n","granted":[]}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unreadable"), "{err}");

        assert_eq!(assign_key("worker-a"), "assign.worker-a");
        assert_eq!(parse_assign_key("assign.worker-a"), Some("worker-a"));
        assert_eq!(parse_assign_key("worker.worker-a"), None);
        // The two prefixes must not shadow each other in a durable-keyspace
        // watch: `assign.` and `split.`/`spec.` share no prefix.
        assert_eq!(parse_assign_key("split.abc"), None);
        assert_eq!(parse_split_key("assign.worker-a"), None);
    }

    #[test]
    fn instance_ids_validate() {
        validate_instance_id("pipeline-7f9c").unwrap();
        for bad in ["", "a.b", "a b", "π"] {
            assert!(validate_instance_id(bad).is_err(), "{bad:?}");
        }
    }

    proptest! {
        #[test]
        fn base64_round_trips(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let encoded = b64_encode(&bytes);
            prop_assert_eq!(b64_decode(&encoded).unwrap(), bytes);
        }

        #[test]
        fn records_round_trip(
            id in "[A-Za-z0-9_-]{1,32}",
            descriptor in proptest::collection::vec(any::<u8>(), 0..512),
            weight in 1u64..u64::MAX,
            watermark in proptest::option::of(any::<i64>()),
            state in proptest::collection::vec(any::<u8>(), 0..128),
            completed in any::<bool>(),
        ) {
            let spec = SplitSpec::new(SplitId::new(id.clone()).unwrap(), descriptor)
                .with_weight(weight);
            let seed = watermark.map(|w| {
                if completed {
                    SplitProgress::completed(w, state)
                } else {
                    SplitProgress::new(w, state)
                }
            });
            let fp = fingerprint_hash("prop:v1");
            let spec_record = SplitSpecRecord::planned(&spec, fp, 3);
            let parsed = SplitSpecRecord::parse(&spec_key(&spec.id), &spec_record.encode(), fp)
                .unwrap();
            prop_assert_eq!(&parsed, &spec_record);
            prop_assert_eq!(parsed.spec().unwrap(), spec.clone());

            let progress_record = SplitProgressRecord::planned(&spec.id, fp, seed.as_ref());
            let parsed = SplitProgressRecord::parse(
                &split_key(&spec.id),
                &progress_record.encode(),
                fp,
            ).unwrap();
            prop_assert_eq!(&parsed, &progress_record);
            prop_assert_eq!(parsed.progress().unwrap(), seed);
        }
    }
}
