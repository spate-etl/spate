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
//! Both instances exit `Completed` once every split is done, and the
//! union of their sink captures is exactly the 1,000 rows (at-least-once:
//! the union is complete; overlap is possible and fine).
//!
//! ```sh
//! cargo run -p spate --features coordination --example coordinated_pipeline
//! ```

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

/// The lane-assembly context: what materializing a split needs, kept as a
/// sibling field of the driver so both borrow disjointly.
struct LedgerCtx {
    issuer: Option<AckIssuer>,
    /// Per live split: (range length, split-local resume offset last
    /// handed to a lane) — enough to encode commits and spot completion.
    ranges: BTreeMap<String, (i64, i64)>,
}

impl SplitSource for LedgerCtx {
    type Lane = LedgerLane;

    fn open_split(&mut self, opening: SplitOpening<'_>) -> Result<LedgerLane, SourceError> {
        let descriptor = String::from_utf8_lossy(&opening.split.descriptor);
        let (start, end) = descriptor
            .split_once("..")
            .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
            .ok_or_else(|| SourceError::Client {
                class: spate::error::ErrorClass::Fatal,
                reason: format!("undecodable ledger descriptor {descriptor:?}"),
            })?;
        // Resume exactly at the committed watermark: rows before it were
        // durably written by a previous tenancy (possibly ours).
        let resume = opening.resume.map_or(0, |p| p.watermark);
        self.ranges
            .insert(opening.split.id.as_str().to_string(), (end - start, resume));
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

    fn encode_commit(
        &mut self,
        split: &SplitId,
        watermark: i64,
    ) -> Result<SplitProgress, SourceError> {
        let (len, _) = self.ranges[split.as_str()];
        // No extra resume state needed: the watermark IS the position.
        Ok(if watermark >= len {
            SplitProgress::completed(watermark, vec![])
        } else {
            SplitProgress::new(watermark, vec![])
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

// ─── The source: driver + ctx glued into the Source contract ────────────

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

// ─── Assembly: two instances over one shared store ──────────────────────

// Two pipelines in one process: each admin server binds an ephemeral
// port (a real deployment runs one pipeline per process on the default).
const CONFIG: &str = r#"
pipeline: { name: ledger-demo, threads: 2 }
checkpoint: { interval: 100ms }
metrics: { listen: 127.0.0.1:0 }
source: { ledger: {} }
sink: { capture: {} }
"#;

fn run_instance(
    instance: &str,
    store: MemoryStore,
) -> Result<(ExitState, Vec<i64>), Box<dyn std::error::Error + Send + Sync>> {
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");

    // The shared store stands in for the NATS cluster both instances
    // would point at in production.
    let store = MemoryStore::new(LEASE);

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
