//! Two pipeline instances divide one bounded job — no external
//! infrastructure, no broker, no object store.
//!
//! This is the coordination seam end to end, deliberately **not**
//! S3-shaped: a synthetic "ledger" of 1,000 numbered rows is planned into
//! 8 id-range splits (the kind a database source would emit), and two
//! coordinated instances race to lease, read, and complete them. The
//! moving parts, exactly as a real deployment wires them:
//!
//! - a [`SplitPlanner`] that enumerates the work — run only by whichever
//!   instance holds the leadership lease;
//! - a [`CoordinationDriver`] embedded in the source, translating
//!   ownership events into the controller's assignment protocol;
//! - a [`SplitCoordinator`] backend per instance — here over the shared
//!   in-memory store; in production over NATS
//!   (`NatsCoordinator`, feature `coordination-nats`).
//!
//! Progress carried in the store is checked before it is believed: every
//! commit pins the descriptor it was produced against, and
//! [`SplitSource::validate_resume`] re-reads that pin — and the watermark's
//! place in the range — when a split is handed to its next owner.
//!
//! Both instances exit `Completed` once every split is done, and the
//! union of their sink captures is exactly the 1,000 rows (at-least-once:
//! the union is complete; overlap is possible and fine).
//!
//! ```sh
//! cargo run -p spate --features coordination --example custom_coordinated_source
//! ```

// The examples index renders these four fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  extending
// INDEX-GOAL:  write a coordination-aware source — planner, splits and driver
// INDEX-TECH:  the coordination seam
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::checkpoint::{AckIssuer, AckRef};
use spate::coordination::driver::{CoordinationDriver, SplitOpening, SplitSource};
use spate::coordination::store::memory::MemoryStore;
use spate::coordination::{
    CoordinationConfig, CoordinationError, PlanContext, PlanFinality, PlannedSplit,
    SplitCoordinator, SplitId, SplitPlan, SplitPlanner, SplitProgress, SplitSpec, StoreCoordinator,
};
use spate::error::SourceError;
use spate::prelude::*;
use spate::record::RawPayload;
use spate::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::{TestDeserializer, TestEncoder, capture_sink, decode_rows};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// One value drives both the store's TTL and the coordinator's
/// `lease_duration`: they must be built from the same constant (the
/// coordinator fails fast on a mismatch — heartbeats pace from the
/// config while expiry runs on the store's clock).
const LEASE: Duration = Duration::from_secs(1);

const ROWS: i64 = 1_000;
const SPLITS: i64 = 8;

// ─── The planner (runs on whichever instance leads) ─────────────────────

/// Plans the ledger as balanced id ranges. Deterministic split ids make
/// replanning and leader failover idempotent: re-emitting an
/// already-planned split is a store-side no-op.
// ANCHOR: planner
struct LedgerPlanner;

impl SplitPlanner for LedgerPlanner {
    fn fingerprint(&self) -> String {
        // Config-derived job identity — every instance must agree.
        format!("ledger-demo:v1:rows={ROWS}")
    }

    fn plan(&mut self, _ctx: PlanContext<'_>) -> Result<SplitPlan, CoordinationError> {
        let per_split = ROWS / SPLITS;
        let splits = (0..SPLITS)
            .map(|i| {
                let start = i * per_split;
                let end = if i == SPLITS - 1 {
                    ROWS
                } else {
                    start + per_split
                };
                let id = SplitId::new(format!("rows-{start:06}-{end:06}"))?;
                let descriptor = format!("{start}..{end}").into_bytes();
                Ok(PlannedSplit::new(
                    SplitSpec::new(id, descriptor).with_weight((end - start) as u64),
                ))
            })
            .collect::<Result<_, CoordinationError>>()?;
        // A bounded job: the enumeration is complete — AllComplete fires
        // once every split is done. A discovery source would return
        // `Open` and grow the plan on later replan ticks.
        Ok(SplitPlan::new(splits, PlanFinality::Final))
    }
}
// ANCHOR_END: planner

// ─── The data plane: one lane per in-flight split ───────────────────────

struct LedgerLane {
    lane: LaneId,
    partition: PartitionId,
    issuer: AckIssuer,
    start: i64,
    end: i64,
    /// Offset within the split (0-based), so watermarks are split-local.
    next: i64,
    buf: Vec<Vec<u8>>,
}

struct LedgerBatch<'a> {
    payloads: &'a [Vec<u8>],
    partition: PartitionId,
    base_offset: i64,
    idx: usize,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for LedgerBatch<'a> {
    fn next_payload(&mut self) -> Option<RawPayload<'a>> {
        let bytes = self.payloads.get(self.idx)?;
        let offset = self.base_offset + self.idx as i64;
        self.idx += 1;
        Some(RawPayload {
            bytes,
            key: None,
            partition: self.partition,
            offset,
            timestamp_ms: offset,
        })
    }

    fn ack(&self) -> &AckRef {
        &self.ack
    }
}

impl SourceLane for LedgerLane {
    type Batch<'a> = LedgerBatch<'a>;

    fn id(&self) -> LaneId {
        self.lane
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<Self::Batch<'_>>, SourceError> {
        let len = self.end - self.start;
        if self.next >= len {
            std::thread::sleep(timeout); // exhausted: idle, never busy-spin
            return Ok(None);
        }
        let base = self.next;
        let end = (base + max_records as i64).min(len);
        self.buf.clear();
        self.buf
            .extend((base..end).map(|o| (self.start + o).to_string().into_bytes()));
        self.next = end;
        let ack = self.issuer.issue(self.partition, end - 1);
        Ok(Some(LedgerBatch {
            payloads: &self.buf,
            partition: self.partition,
            base_offset: base,
            idx: 0,
            ack,
        }))
    }
}

// ─── The SplitSource callbacks (the driver's view of this source) ───────

/// Reads a planner descriptor back as its `start..end` bounds. Both
/// [`SplitSource::open_split`] and the drift check below go through here on
/// purpose: a descriptor one accepted and the other rejected would be a lane
/// reading bounds nothing had validated.
fn decode_range(descriptor: &[u8], split: &SplitId) -> Result<(i64, i64), SourceError> {
    let text = String::from_utf8_lossy(descriptor);
    let reject = |reason: String| SourceError::Client {
        class: spate::error::ErrorClass::Fatal,
        reason: format!("split {split}: {reason}"),
    };
    let (start, end) = text
        .split_once("..")
        .and_then(|(a, b)| Some((a.parse::<i64>().ok()?, b.parse::<i64>().ok()?)))
        .ok_or_else(|| reject(format!("undecodable ledger descriptor {text:?}")))?;
    if start < 0 || end < start {
        return Err(reject(format!(
            "ledger descriptor {text:?} is not an ascending non-negative range"
        )));
    }
    Ok((start, end))
}

/// The lane-assembly context: what materializing a split needs, kept as a
/// sibling field of the driver so both borrow disjointly.
// ANCHOR: split_source
struct LedgerCtx {
    issuer: Option<AckIssuer>,
    /// Per live split: (range length, the exact descriptor bytes the split
    /// was opened against) — the length spots completion, and the
    /// descriptor is echoed into every commit as the resume pin that
    /// [`SplitSource::validate_resume`] re-checks under the next owner.
    ranges: BTreeMap<String, (i64, Vec<u8>)>,
}

impl SplitSource for LedgerCtx {
    type Lane = LedgerLane;

    fn open_split(&mut self, opening: SplitOpening<'_>) -> Result<LedgerLane, SourceError> {
        let (start, end) = decode_range(&opening.split.descriptor, &opening.split.id)?;
        // Resume exactly at the committed watermark: rows before it were
        // durably written by a previous tenancy (possibly ours). It is
        // trustworthy without re-checking here — the driver already put it
        // through `validate_resume` before staging this opening.
        let resume = opening.resume.map_or(0, |p| p.watermark);
        self.ranges.insert(
            opening.split.id.as_str().to_string(),
            (end - start, opening.split.descriptor.clone()),
        );
        Ok(LedgerLane {
            lane: opening.lane,
            partition: opening.partition,
            issuer: self.issuer.clone().expect("open() ran first"),
            start,
            end,
            next: resume,
            buf: Vec::new(),
        })
    }

    /// The drift check: carried progress against the split as it exists
    /// *now*, before the driver trusts a byte of it. The two disagree
    /// whenever a replan moves an id's bounds under progress already
    /// committed against them — a job restarted with a different `ROWS`, a
    /// planner whose ranges shift — and the resume point then counts rows
    /// this split no longer covers.
    ///
    /// **Rejecting is terminal, so the class has to be `Fatal`.** The
    /// driver raises this before it records the tenancy or stages the
    /// opening, so the split is never opened: no lane, no partition id, and
    /// the rest of that batch of coordination events goes unapplied. The
    /// error leaves `CoordinationDriver::poll_events` through this source's
    /// `poll_events`, and the controller classifies it — a `Fatal` one
    /// becomes the pipeline's `FatalError` and the run ends
    /// `ExitState::Failed`, which is the only honest outcome for progress
    /// nothing can reconcile. Any other class is logged as a retryable
    /// control-plane error and the gain is simply dropped, leaving a split
    /// this instance neither reads nor hands back.
    // ANCHOR: validate_resume
    fn validate_resume(
        &self,
        split: &SplitSpec,
        progress: &SplitProgress,
    ) -> Result<(), SourceError> {
        let reject = |detail: String| SourceError::Client {
            class: spate::error::ErrorClass::Fatal,
            reason: format!(
                "split {}: carried progress no longer describes this split ({detail}) — \
                 unrecoverable divergence; requeue the split or start a fresh job",
                split.id
            ),
        };
        let (start, end) = decode_range(&split.descriptor, &split.id)?;

        // The pin `encode_commit` wrote: the descriptor bytes the progress
        // was produced against. Same id, different rows behind it is drift
        // an in-range watermark cannot reveal on its own.
        if progress.state != split.descriptor {
            return Err(reject(format!(
                "progress is pinned to {:?}, the descriptor says {start}..{end}",
                String::from_utf8_lossy(&progress.state)
            )));
        }
        // Watermarks are split-local, so the legal band is `[0, len]`: the
        // length itself means the split was read to its end, one past it
        // means the range shrank under a resume point that outran it.
        let len = end - start;
        if !(0..=len).contains(&progress.watermark) {
            return Err(reject(format!(
                "watermark {} is not a position in the {len} row(s) this split covers",
                progress.watermark
            )));
        }
        Ok(())
    }
    // ANCHOR_END: validate_resume

    fn encode_commit(
        &mut self,
        split: &SplitId,
        watermark: i64,
    ) -> Result<SplitProgress, SourceError> {
        let (len, pin) = &self.ranges[split.as_str()];
        // The watermark IS the position, so the state carries no offset —
        // only the descriptor this progress was produced against, which is
        // what makes the check above able to tell drift from a resume.
        Ok(if watermark >= *len {
            SplitProgress::completed(watermark, pin.clone())
        } else {
            SplitProgress::new(watermark, pin.clone())
        })
    }

    fn sweep(&mut self, _split: &SplitId) -> Result<Option<SplitProgress>, SourceError> {
        // Every split ends on a watermark-carrying commit (ranges are
        // never empty), so the tick commits already cover completion.
        Ok(None)
    }

    fn close_split(&mut self, split: &SplitId) {
        // Nothing to detach: the lane owns no background fetcher.
        self.ranges.remove(split.as_str());
    }
}
// ANCHOR_END: split_source

// ─── The source: driver + ctx glued into the Source contract ────────────

// ANCHOR: driver
struct LedgerSource {
    driver: CoordinationDriver,
    ctx: LedgerCtx,
    started: bool,
}

impl LedgerSource {
    fn new(coordinator: Box<dyn SplitCoordinator>) -> LedgerSource {
        LedgerSource {
            driver: CoordinationDriver::new(coordinator),
            ctx: LedgerCtx {
                issuer: None,
                ranges: BTreeMap::new(),
            },
            started: false,
        }
    }
}

impl Source for LedgerSource {
    type Lane = LedgerLane;

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        self.ctx.issuer = Some(ctx.issuer);
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<LedgerLane>, SourceError> {
        if !self.started {
            self.started = true;
            // Joins the job and returns the empty ready signal; splits
            // arrive as later assignments while claims race.
            return self.driver.start(Box::new(LedgerPlanner));
        }
        self.driver.poll_events(&mut self.ctx, timeout)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        self.driver.commit(&mut self.ctx, watermarks)
    }
    // flush_commits stays the default no-op: an Ok commit is durable in
    // the store, or (after a transient store failure) cached by the
    // driver and recommitted next tick — replay can widen, data cannot
    // be lost.
}

impl Drop for LedgerSource {
    fn drop(&mut self) {
        // Graceful shutdown: hand splits back so peers claim them now
        // rather than after the lease TTL.
        self.driver.release();
    }
}
// ANCHOR_END: driver

// ─── Assembly: two instances over one shared store ──────────────────────

// Two pipelines in one process, which costs two things a real deployment
// never pays. Each admin server binds an ephemeral port rather than the
// default, and each pipeline carries the instance in its name — a gauge
// series has exactly one live owner per process (INV-10), so two instances
// under one name is the same series claimed twice. In production each
// instance is its own process and they share a name; here the label has to
// do what the process boundary would.
//
// The planner fingerprint is deliberately *not* instance-scoped: it is how
// the two agree they are planning the same job.
fn config_yaml(instance: &str) -> String {
    format!(
        r#"
pipeline: {{ name: ledger-demo-{instance}, threads: 2 }}
checkpoint: {{ interval: 100ms }}
metrics: {{ listen: 127.0.0.1:0 }}
source: {{ ledger: {{}} }}
sink: {{ capture: {{}} }}
"#
    )
}

fn run_instance(
    instance: &str,
    store: MemoryStore,
) -> Result<(ExitState, Vec<i64>), Box<dyn std::error::Error + Send + Sync>> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(&config_yaml(instance))?)?;

    // ANCHOR: coordinator
    let coordinator = StoreCoordinator::new(
        store,
        CoordinationConfig {
            // Demo-fast takeover; production floors are far higher (see
            // the deployment guide). The store was built from the SAME
            // constant — the coordinator rejects a store whose TTL
            // diverges from lease_duration.
            lease_duration: LEASE,
            op_timeout: Duration::from_millis(250),
            instance_id: Some(instance.to_string()),
            replan_interval: Duration::from_secs(1),
            ..CoordinationConfig::default()
        },
        pipeline.io_handle(),
        None,
    )?;
    let source = LedgerSource::new(Box::new(coordinator));
    // ANCHOR_END: coordinator

    let (sink, script) = capture_sink(1, 1);
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(20);
        cfg
    };
    let report = pipeline
        .sink(sink.with_pool_config(pool_cfg))?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(TestDeserializer::passthrough())
                .with_metrics(ctx.pipeline, "main")
                .sink(
                    TestEncoder,
                    KeyHashRouter,
                    chunk_cfg,
                    ctx.queues,
                    ctx.budget,
                )
                .build()
        })
        .runtime_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        })
        .run(source)?;

    let mut rows = Vec::new();
    for write in script.writes() {
        for row in decode_rows(&write.payload) {
            rows.push(String::from_utf8(row)?.parse::<i64>()?);
        }
    }
    Ok((report.state, rows))
}

/// Drives the drift check both ways, because a healthy run only ever
/// produces one of its two answers. The driver calls it on every gain that
/// carries progress — every takeover in the run below — but this plan is
/// `Final` and its split ids are deterministic, so a descriptor never moves
/// under committed progress and the rejecting branch is unreachable from
/// here. That branch stops a pipeline, which is precisely why it is asserted
/// rather than merely written.
fn check_resume_drift() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = LedgerCtx {
        issuer: None,
        ranges: BTreeMap::new(),
    };
    let descriptor = b"250..375".to_vec();
    let spec = SplitSpec::new(SplitId::new("rows-000250-000375")?, descriptor.clone());
    let len = 125;

    // Accepted — what a takeover actually hands over: a split-local
    // watermark inside the range, pinned to the descriptor it was read
    // against. `len` itself is a resume point too (the split was read to
    // its end and the tail's commit had not landed yet), and so is 0.
    ctx.validate_resume(&spec, &SplitProgress::new(0, descriptor.clone()))?;
    ctx.validate_resume(&spec, &SplitProgress::new(64, descriptor.clone()))?;
    ctx.validate_resume(&spec, &SplitProgress::new(len, descriptor.clone()))?;

    // Rejected — the watermark is not a position in this split. A range
    // that shrank under a resume point that already passed its new end, and
    // a store value no encoding of ours produces.
    assert!(
        ctx.validate_resume(&spec, &SplitProgress::new(len + 1, descriptor.clone()))
            .is_err(),
        "a watermark past the range end must be rejected"
    );
    assert!(
        ctx.validate_resume(&spec, &SplitProgress::new(-1, descriptor.clone()))
            .is_err(),
        "a negative watermark must be rejected"
    );

    // Rejected — the pin disagrees with the descriptor: this id covers a
    // different 125 rows than the ones the watermark counted, and no
    // in-range check would have caught it.
    assert!(
        ctx.validate_resume(&spec, &SplitProgress::new(64, b"500..625".to_vec()))
            .is_err(),
        "progress pinned to another range must be rejected"
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    check_resume_drift()?;

    // The shared store stands in for the NATS cluster both instances
    // would point at in production.
    // ANCHOR: store
    let store = MemoryStore::new(LEASE);
    // ANCHOR_END: store

    let workers: Vec<_> = ["instance-a", "instance-b"]
        .into_iter()
        .map(|instance| {
            let store = store.clone();
            std::thread::spawn(move || run_instance(instance, store))
        })
        .collect();

    let mut union: BTreeSet<i64> = BTreeSet::new();
    let mut total_captured = 0usize;
    for (instance, worker) in ["instance-a", "instance-b"].iter().zip(workers) {
        let (state, rows) = worker
            .join()
            .expect("instance thread")
            .map_err(|e| format!("{instance}: {e}"))?;
        println!("{instance}: exit={state:?}, rows={}", rows.len());
        assert!(
            matches!(state, ExitState::Completed),
            "{instance} must complete, got {state:?}"
        );
        total_captured += rows.len();
        union.extend(rows);
    }

    // At-least-once: the union is exactly the ledger; overlap (replayed
    // rows after a steal or takeover) is possible and fine.
    assert_eq!(union.len() as i64, ROWS, "the union must cover the ledger");
    assert_eq!(*union.first().unwrap(), 0);
    assert_eq!(*union.last().unwrap(), ROWS - 1);
    println!(
        "union covers all {ROWS} rows ({} captured across instances, {} duplicates)",
        total_captured,
        total_captured as i64 - ROWS
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The example is the test. `cargo run --example` still runs `main`;
    /// under `--test` the harness makes `main` an ordinary function and this
    /// its only caller, so the assertions above stop being decorative.
    #[test]
    fn runs_to_completion() {
        super::main().expect("the example must run clean");
    }
}
