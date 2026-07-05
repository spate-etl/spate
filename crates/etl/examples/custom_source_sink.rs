//! Writing a source and a sink from scratch — the connector-author tutorial.
//!
//! A source is two pieces: a control plane ([`Source`]: assignment events,
//! commits, pause/resume) and per-thread data-plane lanes ([`SourceLane`]:
//! poll borrowed payload batches, one [`AckRef`] per batch). A sink is also
//! two pieces: a CPU half ([`RowEncoder`]: record → wire bytes, runs on
//! pipeline threads) and an I/O half ([`ShardWriter`]: sealed batch →
//! endpoint, runs on sink workers). The framework owns everything between —
//! batching, retries, replica rotation, acknowledgements, backpressure.
//!
//! Here: a generator source counting to a limit per partition, and a sink
//! printing JSON lines to stdout.
//!
//! ```sh
//! cargo run -p etl --example custom_source_sink
//! ```
//!
//! [`AckRef`]: etl::checkpoint::AckRef

use etl::backpressure::InflightBudget;
use etl::checkpoint::{AckIssuer, AckRef};
use etl::config::PipelineConfig;
use etl::deser::Owned;
use etl::error::{SinkError, SourceError};
use etl::metrics::{ComponentLabels, SinkShardMetrics};
use etl::ops::{ChunkConfig, chain_owned};
use etl::pipeline::{PipelineRuntime, RuntimeOptions, SinkRuntime};
use etl::record::{PartitionId, RawPayload, Record};
use etl::sink::{
    KeyHashRouter, RowEncoder, SealedBatch, ShardWriter, SinkPool, SinkPoolConfig, shard_queues,
};
use etl::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ─── The source ─────────────────────────────────────────────────────────

/// One partition's data plane: yields ASCII numbers `0..limit` in batches.
/// Payloads borrow the lane's buffer — zero copies out of the lane, exactly
/// like a Kafka lane borrowing librdkafka's message memory.
struct CounterLane {
    id: LaneId,
    partition: PartitionId,
    issuer: AckIssuer,
    next: i64,
    limit: i64,
    buf: Vec<Vec<u8>>,
}

/// One poll's worth of payloads, all borrowing `'a` from the lane.
struct CounterBatch<'a> {
    payloads: &'a [Vec<u8>],
    partition: PartitionId,
    base_offset: i64,
    idx: usize,
    ack: AckRef,
}

impl<'a> PayloadBatch<'a> for CounterBatch<'a> {
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

impl SourceLane for CounterLane {
    type Batch<'a> = CounterBatch<'a>;

    fn id(&self) -> LaneId {
        self.id
    }

    fn partition(&self) -> PartitionId {
        self.partition
    }

    fn poll(
        &mut self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<Option<Self::Batch<'_>>, SourceError> {
        if self.next >= self.limit {
            // Exhausted: block briefly like an idle consumer would — a lane
            // must never busy-spin the pipeline thread.
            std::thread::sleep(timeout);
            return Ok(None);
        }
        let base = self.next;
        let end = (base + max_records as i64).min(self.limit);
        self.buf.clear();
        self.buf
            .extend((base..end).map(|n| n.to_string().into_bytes()));
        self.next = end;

        // One acknowledgement handle per batch: the checkpointer commits
        // `end` (one past the last offset) only after every derived record
        // is durably written.
        let ack = self.issuer.issue(self.partition, end - 1);
        Ok(Some(CounterBatch {
            payloads: &self.buf,
            partition: self.partition,
            base_offset: base,
            idx: 0,
            ack,
        }))
    }
}

/// The control plane: hands out its lanes once, then idles. Commits are
/// recorded where the demo (and your tests) can observe them — a real
/// source would store them durably (Kafka: `store_offsets`).
struct CounterSource {
    per_partition: i64,
    partitions: u32,
    issuer: Option<AckIssuer>,
    handed_out: bool,
    commits: Arc<Mutex<BTreeMap<u32, i64>>>,
}

impl Source for CounterSource {
    type Lane = CounterLane;

    fn open(&mut self, ctx: SourceCtx) -> Result<(), SourceError> {
        self.issuer = Some(ctx.issuer);
        Ok(())
    }

    fn poll_events(&mut self, timeout: Duration) -> Result<SourceEvent<CounterLane>, SourceError> {
        if !self.handed_out {
            self.handed_out = true;
            let issuer = self.issuer.as_ref().expect("open() before poll_events");
            let lanes = (0..self.partitions)
                .map(|p| CounterLane {
                    id: LaneId(p),
                    partition: PartitionId(p),
                    issuer: issuer.clone(),
                    next: 0,
                    limit: self.per_partition,
                    buf: Vec::new(),
                })
                .collect();
            return Ok(SourceEvent::LanesAssigned(lanes));
        }
        std::thread::sleep(timeout); // nothing else ever happens
        Ok(SourceEvent::Idle)
    }

    fn commit(&mut self, watermarks: &[(PartitionId, i64)]) -> Result<(), SourceError> {
        let mut commits = self.commits.lock().expect("commits lock");
        for (p, offset) in watermarks {
            commits.insert(p.0, *offset);
        }
        Ok(())
    }
}

// ─── The sink ───────────────────────────────────────────────────────────

/// CPU half: encode each record as a JSON line. Runs on pipeline threads;
/// must not do I/O. Frames are concatenable, so workers merge them into
/// big batches regardless of how many pipeline threads produced them.
#[derive(Clone)]
struct JsonLinesEncoder;

impl RowEncoder<Owned<Vec<u8>>> for JsonLinesEncoder {
    fn encode<'buf>(
        &mut self,
        rec: &Record<Vec<u8>>,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), SinkError> {
        use bytes::BufMut;
        buf.put_slice(b"{\"partition\":");
        buf.put_slice(rec.meta.partition.0.to_string().as_bytes());
        buf.put_slice(b",\"value\":");
        buf.put_slice(&rec.payload);
        buf.put_slice(b"}\n");
        Ok(())
    }
}

/// I/O half: "write" a sealed batch by printing it. Returning `Ok` is the
/// durable-ack point — a real writer returns only after its server
/// confirmed (e.g. ClickHouse `end()`).
struct StdoutWriter;

impl ShardWriter for StdoutWriter {
    type Endpoint = String; // a real sink holds a connected client here

    fn write_batch(
        &self,
        endpoint: &String,
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let mut out = String::new();
        for frame in &batch.frames {
            out.push_str(&String::from_utf8_lossy(frame));
        }
        let header = format!(
            "── batch {} → {endpoint}: {} rows ──\n",
            batch.dedup_token, batch.rows
        );
        async move {
            print!("{header}{out}");
            Ok(())
        }
    }
}

// ─── Assembly ───────────────────────────────────────────────────────────

const CONFIG: &str = r#"
pipeline: { name: counter-demo, threads: 2 }
checkpoint: { interval: 100ms }
source: { counter: {} }
sink: { stdout: {} }
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    etl::telemetry::init(etl::telemetry::LogFormat::Pretty, "info");
    let config = PipelineConfig::from_str(CONFIG)?;

    let per_partition = 100;
    let commits = Arc::new(Mutex::new(BTreeMap::new()));
    let source = CounterSource {
        per_partition,
        partitions: 2,
        issuer: None,
        handed_out: false,
        commits: Arc::clone(&commits),
    };

    // Two shards, one "replica" each. Keyless records route by partition.
    let (queues, receivers) = shard_queues(2, 8);
    let budget = Arc::new(InflightBudget::new());
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let labels = ComponentLabels::new("counter-demo", "sink", "stdout");
    let metrics = (0..2u32)
        .map(|s| SinkShardMetrics::new(&labels, s, &[format!("stdout-{s}")]))
        .collect();
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    };
    let pool = SinkPool::spawn(
        Arc::new(StdoutWriter),
        vec![vec!["shard-0".into()], vec!["shard-1".into()]],
        receivers,
        pool_cfg,
        Arc::clone(&budget),
        metrics,
        "counter-demo",
        io.handle(),
    );
    let sink = SinkRuntime {
        queues: queues.clone(),
        drain: Box::new(move |deadline| {
            Box::pin(async move {
                let r = pool.drain(deadline).await;
                etl::pipeline::DrainReport {
                    flushed_batches: r.flushed,
                    abandoned_batches: r.abandoned,
                }
            })
        }),
        probe: None,
    };

    let chain_queues = queues;
    let chain_budget = Arc::clone(&budget);
    let chains = move |_thread: usize| {
        chain_owned::<Vec<u8>, _>(etl::deser::BytesPassthrough)
            .with_metrics("counter-demo", "main")
            .sink(
                JsonLinesEncoder,
                KeyHashRouter,
                ChunkConfig::default(),
                chain_queues.clone(),
                Arc::clone(&chain_budget),
            )
            .build()
    };

    let runtime =
        PipelineRuntime::new(config, source, chains, sink, budget).with_options(RuntimeOptions {
            handle_signals: false,
            ..RuntimeOptions::default()
        });
    let shutdown = runtime.shutdown_handle();
    let pipeline = std::thread::spawn(move || runtime.run());

    // Wait for the checkpointer to commit both partitions to the end.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let commits = commits.lock().expect("commits lock");
            if (0..2).all(|p| commits.get(&p) == Some(&per_partition)) {
                break;
            }
        }
        assert!(Instant::now() < deadline, "commits not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = pipeline.join().expect("pipeline thread")?;
    println!("\npipeline exit: {:?}", report.state);
    println!("committed: {:?}", commits.lock().expect("commits lock"));
    Ok(())
}
