//! Writing a source and a sink from scratch — the connector-author tutorial.
//!
//! A source is two pieces: a control plane ([`Source`]: assignment events,
//! commits, pause/resume) and per-thread data-plane lanes ([`SourceLane`]:
//! poll borrowed payload batches, one [`AckRef`] per batch). A sink is also
//! two pieces: a CPU half ([`RowEncoder`]: record → wire bytes, runs on
//! pipeline threads) and an I/O half ([`ShardWriter`]: sealed batch →
//! endpoint, runs on sink workers). The framework owns everything between —
//! batching, retries, replica rotation, acknowledgments, backpressure.
//!
//! Here: a generator source counting to a limit per partition, and a sink
//! printing JSON lines to stdout — with a payload-aware [`RecordRouter`]
//! deciding which shard each order line belongs to.
//!
//! ```sh
//! cargo run -p spate --example custom_source_sink
//! ```
//!
//! [`AckRef`]: spate::checkpoint::AckRef
//! [`RecordRouter`]: spate::sink::RecordRouter

// The examples index renders these fields; see crates/spate/tests/examples_index.rs.
// INDEX-TIER:  extending
// INDEX-GOAL:  write a source, a payload-aware router and a sink from scratch
// INDEX-TECH:  no infrastructure
// INDEX-NEEDS: nothing

// Examples talk to their user on stdout/stderr by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use spate::checkpoint::{AckIssuer, AckRef};
use spate::deser::Owned;
use spate::error::{ErrorPolicy, SinkError, SourceError};
use spate::prelude::*;
use spate::record::{RawPayload, Record};
use spate::sink::{RecordRouter, RowEncoder, SealedBatch, ShardWriter};
use spate::source::{LaneId, PayloadBatch, Source, SourceCtx, SourceEvent, SourceLane};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Sink shards, and how many storefront customers the orders belong to.
const SHARDS: usize = 2;
const CUSTOMERS: i64 = 4;
/// Each source record is an upload carrying this many order lines, and
/// consecutive orders belong to different customers — the fan-out below is
/// what makes the routing tier matter.
const LINES_PER_BATCH: i64 = 2;

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
// ANCHOR: batch
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
// ANCHOR_END: batch

// ANCHOR: lane
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

        // One acknowledgment handle per batch: the checkpointer commits
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
// ANCHOR_END: lane

/// The control plane: hands out its lanes once, then idles. Commits are
/// recorded where the demo (and your tests) can observe them — a real
/// source would store them durably (Kafka: `store_offsets`).
// ANCHOR: control
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
// ANCHOR_END: control

// ─── The router ─────────────────────────────────────────────────────────

/// Routes on a field of the decoded record — the **record-aware** tier.
///
/// The chain below fans one upload out into several order lines, and every
/// child of a `flat_map` carries its parent's `RecordMeta` — the shared
/// metadata that lets one parent ack resolve across all its children. A
/// meta-only `ShardRouter` (the tier `KeyHashRouter` sits in) sees that
/// metadata and nothing else, so it places every child of one upload on one
/// shard; reading the payload places them independently.
///
/// The decision is **deterministic across retries**. Delivery is
/// at-least-once, so a record can be replayed after a failure, and a router
/// answering differently the second time writes the same order into two
/// shards — the dedup token is per shard, so nothing downstream collapses
/// them. Keep the hash below explicit rather than reaching for
/// `DefaultHasher`, whose output is seeded and not stable across releases.
// ANCHOR: router
struct ByCustomer;

impl RecordRouter<Owned<Vec<u8>>> for ByCustomer {
    fn route_record<'buf>(&self, rec: &Record<Vec<u8>>, num_shards: usize) -> usize {
        shard_of(customer_field(&rec.payload), num_shards)
    }
}

/// The routing decision itself, so `main` can assert that what each shard
/// received is what this function chose, rather than re-deriving it.
fn shard_of(customer_id: &[u8], num_shards: usize) -> usize {
    (fnv1a(customer_id) % num_shards as u64) as usize
}

/// The `cust-N` prefix of an order line. Total by construction: a line
/// without the separator hashes whole. A router has no per-record error
/// policy — it must return a shard for every record and never panic, since
/// a payload-dependent panic replays into a crash loop on restart.
fn customer_field(line: &[u8]) -> &[u8] {
    match line.iter().position(|b| *b == b'|') {
        Some(sep) => &line[..sep],
        None => line,
    }
}

/// FNV-1a: a few instructions, no allocation, and the same answer in every
/// process and every release — the properties routing needs.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
// ANCHOR_END: router

// ─── The sink ───────────────────────────────────────────────────────────

/// CPU half: encode each record as a JSON line. Runs on pipeline threads;
/// must not do I/O. Frames are concatenable, so workers merge them into
/// big batches regardless of how many pipeline threads produced them.
// ANCHOR: encoder
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
        buf.put_slice(b",\"order_line\":\"");
        buf.put_slice(&rec.payload);
        buf.put_slice(b"\"}\n");
        Ok(())
    }
}
// ANCHOR_END: encoder

/// I/O half: "write" a sealed batch by printing it, and record the rows it
/// carried so `main` can assert on the placement. Returning `Ok` is the
/// durable-ack point — a real writer returns only after its server
/// confirmed (e.g. ClickHouse `end()`).
// ANCHOR: writer
struct StdoutWriter {
    /// Endpoint → the encoded rows it received; a real deployment reads
    /// this back out of the destination instead.
    written: Arc<Mutex<BTreeMap<String, Vec<String>>>>,
}

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
        let endpoint = endpoint.clone();
        async move {
            print!("{header}{out}");
            // Recorded on the path that returns `Ok`, not when the future
            // is built: a future the drain deadline aborts before it
            // resolves wrote nothing.
            let mut written = self.written.lock().expect("written lock");
            written
                .entry(endpoint)
                .or_default()
                .extend(out.lines().map(str::to_owned));
            Ok(())
        }
    }
}
// ANCHOR_END: writer

/// Read `(partition, customer, order)` back out of an encoded row — the
/// assertions' stand-in for querying the destination. The partition
/// identifies which source record an order line came from: upload ids
/// repeat across partitions, so an order id alone does not.
fn parse_order_line(row: &str) -> Option<(u32, i64, i64)> {
    let (_, rest) = row.split_once("\"partition\":")?;
    let (partition, rest) = rest.split_once(',')?;
    let (_, rest) = rest.split_once("\"order_line\":\"")?;
    let (line, _) = rest.split_once('"')?;
    let (customer, order) = line.split_once('|')?;
    let customer = customer.strip_prefix("cust-")?.parse().ok()?;
    Some((partition.parse().ok()?, customer, order.parse().ok()?))
}

// ─── Assembly ───────────────────────────────────────────────────────────

const CONFIG: &str = r#"
pipeline: { name: counter-demo, threads: 2 }
checkpoint: { interval: 100ms }
source: { counter: {} }
sink: { stdout: {} }
"#;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Pretty logs for a demo: first init wins, the builder's JSON default
    // becomes a no-op. The builder then owns the metrics exporter (before
    // any handle can exist) and the shared I/O runtime.
    spate::telemetry::init(spate::telemetry::LogFormat::Pretty, "info");
    let pipeline = Pipeline::from_config(PipelineConfig::from_str(CONFIG)?)?;

    let per_partition = 100;
    let partitions = 2;
    let commits = Arc::new(Mutex::new(BTreeMap::new()));
    let source = CounterSource {
        per_partition,
        partitions,
        issuer: None,
        handed_out: false,
        commits: Arc::clone(&commits),
    };

    // A hand-rolled sink needs no SinkBundle impl of its own: SinkParts is
    // the bundle. `SHARDS` shards, one "replica" each, named for their
    // index — config order is the shard identity. The builder derives
    // labels, per-shard metrics, queues, and workers from it.
    // ANCHOR: bundle
    let pool_cfg = {
        let mut cfg = SinkPoolConfig::default();
        cfg.batch.linger = Duration::from_millis(50);
        cfg
    };
    let written = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = SinkParts::new(
        StdoutWriter {
            written: Arc::clone(&written),
        },
        (0..SHARDS).map(|s| vec![format!("shard-{s}")]).collect(),
        pool_cfg,
    )
    .with_component_type("stdout");
    // ANCHOR_END: bundle

    let runtime = pipeline
        .sink(sink)?
        .chains(|ctx| {
            let chunk_cfg = ctx.chunk();
            chain_owned::<Vec<u8>, _>(spate::deser::BytesPassthrough)
                .with_metrics(ctx.pipeline, "main")
                // Each source record is an upload id; parse it, then fan it
                // out into the order lines it carried. Routing runs once per
                // *emitted* record, after this fan-out and before encoding,
                // so the router sees each order line on its own.
                .try_map(
                    |upload: Vec<u8>| {
                        std::str::from_utf8(&upload)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                            .ok_or("upload id is not an integer")
                    },
                    ErrorPolicy::Fail,
                )
                .flat_map::<Owned<Vec<u8>>, _>(|upload: i64, out| {
                    for line in 0..LINES_PER_BATCH {
                        let order_id = upload * LINES_PER_BATCH + line;
                        let customer_id = order_id % CUSTOMERS;
                        out.emit(format!("cust-{customer_id}|{order_id}").into_bytes());
                    }
                })
                .sink(
                    JsonLinesEncoder,
                    ByCustomer,
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
        .into_runtime(source)?;
    let shutdown = runtime.shutdown_handle();
    let join = std::thread::spawn(move || runtime.run());

    // Wait for the checkpointer to commit both partitions to the end.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let commits = commits.lock().expect("commits lock");
            if (0..partitions).all(|p| commits.get(&p) == Some(&per_partition)) {
                break;
            }
        }
        assert!(Instant::now() < deadline, "commits not observed in time");
        std::thread::sleep(Duration::from_millis(20));
    }
    shutdown.trigger();
    let report = join.join().expect("pipeline thread")?;
    assert_eq!(report.exit_code(), 0, "the pipeline must drain clean");

    // ─── What the router actually did ───────────────────────────────────
    //
    // Commits are gated on durable writes, so every order line is in the
    // log by now; the writer never fails here, so nothing was retried and
    // the counts are exact.
    let written = written.lock().expect("written lock");
    assert_eq!(written.len(), SHARDS, "every shard must have been written");

    let mut shard_of_customer: BTreeMap<i64, usize> = BTreeMap::new();
    // Keyed by the source record the lines came from — `(partition,
    // upload)`, never the upload id alone. Every partition counts from
    // zero, so an upload id names one record per partition, and a set
    // merged across partitions is split by any router that separates
    // partitions at all.
    let mut shards_of_upload: BTreeMap<(u32, i64), BTreeSet<usize>> = BTreeMap::new();
    let mut rows = 0;
    for (endpoint, lines) in written.iter() {
        let shard: usize = endpoint
            .strip_prefix("shard-")
            .and_then(|s| s.parse().ok())
            .expect("endpoints are named for their shard index");
        for line in lines {
            let (partition, customer_id, order_id) =
                parse_order_line(line).expect("every row is one encoded order line");
            rows += 1;
            // Same customer, same shard, always — the determinism
            // `ByCustomer` states as its contract.
            let seen = shard_of_customer.insert(customer_id, shard);
            assert!(
                seen.is_none() || seen == Some(shard),
                "cust-{customer_id} landed on two shards"
            );
            shards_of_upload
                .entry((partition, order_id / LINES_PER_BATCH))
                .or_default()
                .insert(shard);
        }
    }
    assert_eq!(
        rows,
        i64::from(partitions) * per_partition * LINES_PER_BATCH
    );

    // The placement is the router's, not an accident of the shard count.
    for (customer_id, shard) in &shard_of_customer {
        let expected = shard_of(format!("cust-{customer_id}").as_bytes(), SHARDS);
        assert_eq!(*shard, expected, "cust-{customer_id} routed elsewhere");
    }
    // And the payoff: one source record's order lines sit on different
    // shards, which no meta-only router can produce — every child of a
    // `flat_map` carries the same `RecordMeta`, so metadata alone cannot
    // tell them apart.
    assert!(
        shards_of_upload.values().any(|shards| shards.len() > 1),
        "one upload's order lines never split across shards"
    );

    println!("\npipeline exit: {:?}", report.state);
    println!("committed: {:?}", commits.lock().expect("commits lock"));
    println!("customer → shard: {shard_of_customer:?}");
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
