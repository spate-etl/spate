// ---- two-shard Distributed-parity + shard-pruning proof ---------------------
//
// This is the end-to-end payoff of the whole feature. Two real ClickHouse
// nodes on a shared docker network form a cluster `parity` (one shard each,
// native port 9000, resolvable by container hostname). Plain non-replicated
// MergeTree locals; no Keeper. DDL is applied per node over each node's
// host-mapped HTTP port.
//
// Three independent proofs, each on its own fresh cluster. The same two
// nodes host TWO named clusters in one remote_servers config: `parity`
// (equal weights) and `parity_weighted` (weights 9/10). Weights are
// per-cluster config, so no extra containers are needed for the weighted
// proof.
//
//   A. Placement parity. A REAL pipeline (memory source -> flat_map explode
//      -> ClickHouse sink with `sink.router::<_>(sku_key)`) writes rows
//      DIRECTLY into each node's shard-local `lines_local`. The SAME logical
//      rows are then inserted through a `Distributed` table (`lines_twin_dist`,
//      `insert_distributed_sync = 1`), letting ClickHouse place them into
//      `lines_twin`. Per node, `lines_local` must equal `lines_twin`
//      bit-for-bit, proving our router reproduces the engine's placement and
//      that every sku lives on exactly one shard.
//
//   B. Shard pruning. A `SELECT ... WHERE sku = <literal owned by shard 0>`
//      through the `Distributed` table with `optimize_skip_unused_shards = 1`
//      (+ `force_optimize_skip_unused_shards = 2` as a guardrail) must query
//      only the owning shard. Proven via the REMOTE node's `system.query_log`
//      (zero subqueries under the initiator's `initial_query_id`), with a
//      pruning-off negative control that DOES reach the remote shard, so the
//      detection method is shown to detect.
//
//   C. WEIGHTED placement parity: proof A's comparison over cluster
//      `parity_weighted` (weights 9/10). The unit tests pin the router's
//      interval mapping against ClickHouse's *documented* semantics; this is
//      the only place the live engine itself is the oracle for a non-uniform
//      weight split. It also exercises `distributed_check` against real
//      non-default `shard_weight` rows in `system.clusters`: the matching
//      sink config passes, a weight-drifted one fails before writing.
//
// Auth: the image gives `default` a password (CLICKHOUSE_PASSWORD); the
// cluster XML embeds `<user>default</user><password>...</password>` in each
// replica so inter-node subqueries and distributed inserts authenticate.
//
// Container names/network carry a per-run nonce so concurrent runs never
// collide on a docker name. Nextest gives each test its own process, and a
// leftover container from an aborted run outlives both; the cluster XML is
// generated with the same nonced hostnames so peer resolution still works.
// The in-cluster hostnames are therefore `spate-ch0-<nonce>` /
// `spate-ch1-<nonce>` rather than a fixed `ch-shard-0`.

use super::*;
use spate_clickhouse::{ClickHouseEncoder, DistributedRouter, ShardKey};
use spate_core::backpressure::InflightBudget;
use spate_core::checkpoint::{AckRef, Checkpointer};
use spate_core::deser::{Deserializer, EmitRecord, Owned};
use spate_core::error::DeserError;
use spate_core::metrics::{ComponentLabels, E2eBasis, SinkShardMetrics};
use spate_core::ops::{ChunkConfig, Emitter, PushOutcome, chain};
use spate_core::record::{PartitionId, RawPayload, Record};
use spate_core::sink::{DrainReport, SinkPool, shard_queues};
use spate_core::source::{LaneId, Source, SourceCtx, SourceEvent, SourceLane};
use spate_test::memory_source;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---- the user-side record types + their owned families ----------------------

/// One decoded sku batch: a sku and its lines. Owned (the memory
/// source hands us bytes we parse into `String`s), so the family is the
/// stock [`Owned`].
#[derive(Debug)]
struct SkuBatch {
    sku: String,
    lines: Vec<(String, i64)>,
}

/// One exploded order line, in the sink's RowBinary shape and the read-back
/// shape, in the `columns: [sku, unit, qty]` order. `Serialize` drives
/// the sink encoder; `Row + Deserialize` drives the comparison read-back;
/// `Ord` sorts the ground-truth for equality.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    clickhouse::Row,
)]
struct LineRow {
    sku: String,
    unit: String,
    qty: i64,
}

/// Sharding key: the `sku` column, so one sku always lands on one shard,
/// matching a `Distributed` DDL of `xxHash64(sku)`. A fn item, not a
/// closure (the extractor is higher-ranked over the payload lifetime).
fn sku_key(row: &LineRow) -> ShardKey<'_> {
    ShardKey::Str(&row.sku)
}

/// The `flat_map` explode: one SKU's batch fans out into one row per line,
/// each re-keyed by its own `sku`, the record-aware routing that
/// meta-only routing cannot express.
fn explode(batch: SkuBatch, em: &mut Emitter<'_, Owned<LineRow>>) {
    for (unit, qty) in batch.lines {
        // Small data + a generously-sized queue: it never blocks, so
        // (mirroring the spate-avro flat_map template) the Flow is ignored.
        let _ = em.emit(LineRow {
            sku: batch.sku.clone(),
            unit,
            qty,
        });
    }
}

/// Parse one memory-source payload: first line is the sku, each further
/// `unit=qty` line is one of its lines.
fn parse_batch(text: &str) -> SkuBatch {
    let mut text_lines = text.split('\n');
    let sku = text_lines.next().unwrap_or_default().to_string();
    let lines = text_lines
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (unit, qty) = l.split_once('=').expect("a line reads `unit=qty`");
            (unit.to_string(), qty.parse::<i64>().expect("i64 qty"))
        })
        .collect();
    SkuBatch { sku, lines }
}

/// A trivial line-format deserializer for the memory source's byte payloads.
struct BatchDeser;

impl Deserializer<Owned<SkuBatch>> for BatchDeser {
    fn deserialize<'buf>(
        &mut self,
        raw: &RawPayload<'buf>,
        ack: &AckRef,
        out: &mut dyn EmitRecord<'buf, SkuBatch>,
    ) -> Result<(), DeserError> {
        let text = std::str::from_utf8(raw.bytes).map_err(|e| DeserError::Malformed {
            reason: e.to_string(),
        })?;
        let _ = out.emit(Record {
            payload: parse_batch(text),
            meta: raw.meta(),
            ack: ack.clone(),
        });
        Ok(())
    }
}

/// `num_skus` sku batches of `lines_per` lines each. Returns the
/// memory-source payloads and the flat ground-truth rows they encode to.
fn make_data(num_skus: usize, lines_per: usize) -> (Vec<String>, Vec<LineRow>) {
    let mut payloads = Vec::with_capacity(num_skus);
    let mut rows = Vec::with_capacity(num_skus * lines_per);
    for s in 0..num_skus {
        let sku = format!("SKU-{s:02}");
        // The payload's text lines: the SKU header, then one `unit=qty` per
        // row, so this is `lines_per + 1` long, not `lines_per`.
        let mut payload_lines = vec![sku.clone()];
        for e in 0..lines_per {
            let unit = format!("pack-{e}");
            let qty = (s * 100 + e) as i64;
            payload_lines.push(format!("{unit}={qty}"));
            rows.push(LineRow {
                sku: sku.clone(),
                unit,
                qty,
            });
        }
        payloads.push(payload_lines.join("\n"));
    }
    (payloads, rows)
}

/// The shard our router (and, by parity, ClickHouse) assigns `sku` to,
/// for a 2-shard equal-weight cluster.
fn shard_of(sku: &str) -> usize {
    (DistributedRouter::<Owned<LineRow>>::hash_key(ShardKey::Str(sku)) % 2) as usize
}

// ---- the two-node cluster fixture (net-new: nothing else networks two --------
// ---- containers or copies config into them) ---------------------------------

struct Node {
    _container: ContainerAsync<ClickHouse>,
    url: String,
    admin: clickhouse::Client,
    /// In-network hostname (== container name) referenced by the cluster XML.
    host: String,
}

struct Cluster {
    node0: Node,
    node1: Node,
}

/// A docker-safe unique token. The atomic counter guarantees uniqueness
/// across calls within this process (two tests run concurrently and macOS's
/// coarse `SystemTime` resolution can otherwise collide on the same
/// microsecond); pid + time keep it distinct across processes and across
/// leftover containers from a previous aborted run.
fn unique(tag: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{tag}-{}-{}-{n}", std::process::id(), nanos % 1_000_000)
}

/// `remote_servers` XML defining TWO clusters over the same two nodes:
/// `parity` (equal weights) and `parity_weighted` (weights 9/10), two
/// single-replica shards each, native port 9000, addressed by container
/// hostname. Each replica embeds the `default` credentials so node 0
/// authenticates to node 1 for distributed inserts and subqueries.
fn cluster_xml(pw: &str, h0: &str, h1: &str) -> String {
    let replica = |h: &str| {
        format!(
            "<replica><host>{h}</host><port>9000</port>\
             <user>default</user><password>{pw}</password></replica>"
        )
    };
    format!(
        "<clickhouse>\n\
         \x20 <remote_servers>\n\
         \x20   <parity>\n\
         \x20     <shard>\n\
         \x20       <internal_replication>false</internal_replication>\n\
         \x20       {r0}\n\
         \x20     </shard>\n\
         \x20     <shard>\n\
         \x20       <internal_replication>false</internal_replication>\n\
         \x20       {r1}\n\
         \x20     </shard>\n\
         \x20   </parity>\n\
         \x20   <parity_weighted>\n\
         \x20     <shard>\n\
         \x20       <internal_replication>false</internal_replication>\n\
         \x20       <weight>9</weight>\n\
         \x20       {r0}\n\
         \x20     </shard>\n\
         \x20     <shard>\n\
         \x20       <internal_replication>false</internal_replication>\n\
         \x20       <weight>10</weight>\n\
         \x20       {r1}\n\
         \x20     </shard>\n\
         \x20   </parity_weighted>\n\
         \x20 </remote_servers>\n\
         </clickhouse>\n",
        r0 = replica(h0),
        r1 = replica(h1),
    )
}

/// Start one node: pinned 26.3, password auth, joined to `net` under the
/// hostname `host`, with the cluster XML dropped into `config.d` before boot.
async fn start_node(pw: &str, net: &str, host: &str, xml: &str) -> Node {
    let container = started_only(
        ClickHouse::default()
            .with_tag("26.3")
            .with_env_var("CLICKHOUSE_USER", "default")
            .with_env_var("CLICKHOUSE_PASSWORD", pw)
            .with_network(net)
            .with_container_name(host)
            .with_copy_to(
                "/etc/clickhouse-server/config.d/cluster.xml",
                xml.as_bytes().to_vec(),
            ),
    )
    .start()
    .await
    .unwrap_or_else(|e| panic!("start clickhouse node {host}: {e}"));
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let url = format!("http://127.0.0.1:{port}");
    let admin = clickhouse::Client::default()
        .with_url(&url)
        .with_user("default")
        .with_password(pw);
    wait_for_queries(&container, &admin, host).await;
    Node {
        _container: container,
        url,
        admin,
        host: host.to_string(),
    }
}

/// Bring up the two-node `parity` cluster and assert its topology resolves
/// in config order before any real test runs.
async fn two_node_cluster(pw: &str) -> Cluster {
    let net = unique("spate-parity-net");
    let h0 = unique("spate-ch0");
    let h1 = unique("spate-ch1");
    let xml = cluster_xml(pw, &h0, &h1);

    // Node 0 also creates the shared network; node 1 joins it. Node 0 boots
    // fine though h1 is not up yet; cluster hosts resolve lazily, only when
    // a distributed query runs (after both are up and the cache is dropped).
    let node0 = start_node(pw, &net, &h0, &xml).await;
    let node1 = start_node(pw, &net, &h1, &xml).await;
    let cluster = Cluster { node0, node1 };

    for n in [&cluster.node0, &cluster.node1] {
        n.admin
            .query("SYSTEM DROP DNS CACHE")
            .execute()
            .await
            .expect("drop dns cache");
    }

    // Sanity: shard 1 == node 0's host, shard 2 == node 1's host. The whole
    // parity story depends on this ordering (sink shard i == cluster
    // shard_num i+1); if the fixture wired it backwards, every proof below
    // would be meaningless.
    let topology = cluster
        .node0
        .admin
        .query(
            "SELECT shard_num, host_name FROM system.clusters \
             WHERE cluster = 'parity' ORDER BY shard_num",
        )
        .fetch_all::<(u32, String)>()
        .await
        .expect("cluster topology");
    assert_eq!(
        topology,
        vec![
            (1, cluster.node0.host.clone()),
            (2, cluster.node1.host.clone()),
        ],
        "cluster `parity` must list the two shards in remote_servers order"
    );
    cluster
}

/// Create `lines_local`/`lines_twin` on both nodes, and the two
/// `Distributed` tables on node 0 (the initiator).
async fn create_tables(c: &Cluster) {
    let local = |t: &str| {
        format!(
            "CREATE TABLE {t} (sku String, unit String, qty Int64) \
             ENGINE = MergeTree ORDER BY (sku, unit, qty)"
        )
    };
    for n in [&c.node0, &c.node1] {
        for t in ["lines_local", "lines_twin"] {
            n.admin
                .query(&local(t))
                .execute()
                .await
                .unwrap_or_else(|e| panic!("create {t} on {}: {e}", n.host));
        }
    }
    for (dist, local_name) in [
        ("lines_dist", "lines_local"),
        ("lines_twin_dist", "lines_twin"),
    ] {
        c.node0
            .admin
            .query(&format!(
                "CREATE TABLE {dist} AS {local_name} \
                 ENGINE = Distributed('parity', currentDatabase(), '{local_name}', xxHash64(sku))"
            ))
            .execute()
            .await
            .unwrap_or_else(|e| panic!("create {dist}: {e}"));
    }
}

/// The weighted twin of [`create_tables`], over cluster `parity_weighted`:
/// `lines_w_local`/`lines_w_twin` on both nodes, `lines_w_dist` (the
/// sink's check target) and `lines_w_twin_dist` (the engine-placement twin)
/// on node 0.
async fn create_weighted_tables(c: &Cluster) {
    let local = |t: &str| {
        format!(
            "CREATE TABLE {t} (sku String, unit String, qty Int64) \
             ENGINE = MergeTree ORDER BY (sku, unit, qty)"
        )
    };
    for n in [&c.node0, &c.node1] {
        for t in ["lines_w_local", "lines_w_twin"] {
            n.admin
                .query(&local(t))
                .execute()
                .await
                .unwrap_or_else(|e| panic!("create {t} on {}: {e}", n.host));
        }
    }
    for (dist, local_name) in [
        ("lines_w_dist", "lines_w_local"),
        ("lines_w_twin_dist", "lines_w_twin"),
    ] {
        c.node0
            .admin
            .query(&format!(
                "CREATE TABLE {dist} AS {local_name} \
                 ENGINE = Distributed('parity_weighted', currentDatabase(), '{local_name}', \
                 xxHash64(sku))"
            ))
            .execute()
            .await
            .unwrap_or_else(|e| panic!("create {dist}: {e}"));
    }
}

/// A sink over the given `(url, weight)` shards in order, writing the given
/// shard-local table as RowBinary, with an optional `distributed_check:`
/// block appended verbatim.
fn parity_sink(
    table: &str,
    shards: &[(&str, u32)],
    pw: &str,
    check: Option<&str>,
) -> config::ClickHouseSink {
    let shard_lines: String = shards
        .iter()
        .map(|(url, weight)| format!("  - replicas: [\"{url}\"]\n    weight: {weight}\n"))
        .collect();
    let cfg: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "table: {table}\n\
         columns: [sku, unit, qty]\n\
         shards:\n{shard_lines}\
         user: default\n\
         password: {pw}\n\
         batch:\n  max_rows: 100000\n  max_bytes: 64MiB\n  linger: 100ms\n\
         {check}",
        check = check.unwrap_or_default(),
    ))
    .expect("sink config yaml");
    config::build(cfg).expect("valid sink config")
}

/// Drive one full pipeline: memory source -> `BatchDeser` -> `flat_map`
/// explode -> ClickHouse sink (record-routed by `sku`), wired to a real
/// [`SinkPool`] so chunks flow chain -> per-shard queues -> workers ->
/// `ClickHouseWriter` -> the servers. Returns the pool's drain report.
async fn run_pipeline(sink: config::ClickHouseSink, payloads: &[String]) -> DrainReport {
    const P0: PartitionId = PartitionId(0);
    const L0: LaneId = LaneId(0);

    let num_shards = sink.endpoints.len();
    // Read everything that borrows `sink` before the endpoints move out.
    let router = sink.router::<Owned<LineRow>>(sku_key);
    let pool_cfg = sink.pool;
    let writer = Arc::new(sink.writer.clone());
    let endpoints = sink.endpoints; // partial move; the rest of `sink` drops at scope end

    let (queues, receivers) = shard_queues(num_shards, 256);
    let budget = Arc::new(InflightBudget::new());

    let mut driver = chain(BatchDeser)
        .flat_map::<Owned<LineRow>, _>(explode)
        .sink(
            ClickHouseEncoder::<Owned<LineRow>>::new(),
            router,
            ChunkConfig::default(),
            queues,
            Arc::clone(&budget),
        )
        .build();

    // Per-shard metric handles. With no exporter installed they record into
    // the void, since these tests assert on ClickHouse contents, not
    // /metrics.
    let labels = ComponentLabels::new("parity", "clickhouse", "clickhouse");
    let metrics: Vec<SinkShardMetrics> = (0..num_shards)
        .map(|s| SinkShardMetrics::new(&labels, s as u32, &[format!("ch-{s}-0")], E2eBasis::Ingest))
        .collect();

    let pool = SinkPool::spawn(
        writer,
        endpoints,
        receivers,
        pool_cfg,
        budget,
        metrics,
        "parity",
        &tokio::runtime::Handle::current(),
    );

    // Drive the source exactly as the runtime does: one lane, push every
    // sku-batch payload, poll one batch, run it through the chain.
    let mut cp = Checkpointer::new();
    let (mut source, handle) = memory_source();
    source
        .open(SourceCtx::new(cp.handle()))
        .expect("open source");
    cp.begin_epoch(&[P0], 1);
    handle.assign_lanes(&[(L0, P0)]);
    let mut lanes = match source
        .poll_events(Duration::from_millis(200))
        .expect("poll source events")
    {
        SourceEvent::LanesAssigned(lanes) => lanes,
        other => panic!("expected lane assignment, got {other:?}"),
    };
    for payload in payloads {
        handle.push(P0, None, payload.as_bytes());
    }
    let mut batch = lanes[0]
        .poll(payloads.len(), Duration::from_millis(200))
        .expect("poll batch")
        .expect("one batch of all pushed payloads");
    match driver.push_batch(&mut batch, 0) {
        PushOutcome::Done => {}
        other => panic!("chain did not fully process the batch: {other:?}"),
    }
    match driver.flush() {
        PushOutcome::Done => {}
        other => panic!("chain flush did not complete: {other:?}"),
    }

    // Dropping the chain closes the shard queues; only then do the workers
    // enter their drain phase (the pool's documented contract).
    drop(driver);
    pool.drain(Duration::from_secs(30)).await
}

/// All rows of a table in a stable order, for bit-for-bit comparison.
async fn dump(admin: &clickhouse::Client, table: &str) -> Vec<LineRow> {
    admin
        .query(&format!(
            "SELECT ?fields FROM {table} ORDER BY sku, unit, qty"
        ))
        .fetch_all::<LineRow>()
        .await
        .unwrap_or_else(|e| panic!("dump {table}: {e}"))
}

async fn count_where_sku(admin: &clickhouse::Client, table: &str, sku: &str) -> u64 {
    admin
        .query(&format!("SELECT count() FROM {table} WHERE sku = ?"))
        .bind(sku)
        .fetch_one::<u64>()
        .await
        .expect("sku count")
}

async fn flush_logs(c: &Cluster) {
    for n in [&c.node0, &c.node1] {
        n.admin
            .query("SYSTEM FLUSH LOGS")
            .execute()
            .await
            .expect("flush logs");
    }
}

/// How many remote (non-initial) subqueries `node` executed under
/// `initial_query_id`, the robust cross-node evidence that a shard was
/// contacted (or not).
async fn remote_subqueries(admin: &clickhouse::Client, initial_query_id: &str) -> u64 {
    admin
        .query(
            "SELECT count() FROM system.query_log \
             WHERE initial_query_id = ? AND is_initial_query = 0",
        )
        .bind(initial_query_id)
        .fetch_one::<u64>()
        .await
        .expect("query_log")
}

// ---- Proof A: placement parity ----------------------------------------------

/// The sink's direct-to-shard placement equals ClickHouse's own `Distributed`
/// placement, bit-for-bit and per shard, so a `Distributed` table can prune
/// SELECTs over rows this sink wrote directly to the locals.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn distributed_insert_and_sink_place_rows_identically() {
    let pw = "parity-secret";
    let c = two_node_cluster(pw).await;
    create_tables(&c).await;

    let (payloads, rows) = make_data(12, 3);

    // The data must exercise both shards, or parity proves nothing.
    let on_shard0 = rows.iter().filter(|r| shard_of(&r.sku) == 0).count();
    let on_shard1 = rows.len() - on_shard0;
    assert!(
        on_shard0 > 0 && on_shard1 > 0,
        "the fixture data must reach both shards (shard0={on_shard0}, shard1={on_shard1})"
    );

    // Run the real pipeline: rows land directly in each node's lines_local.
    let sink = parity_sink(
        "lines_local",
        &[(&c.node0.url, 1), (&c.node1.url, 1)],
        pw,
        None,
    );
    let report = run_pipeline(sink, &payloads).await;
    assert_eq!(
        report.abandoned, 0,
        "every routed batch must durably land — none may be abandoned"
    );
    assert!(
        report.flushed >= 1,
        "the sink pool must have flushed at least one batch"
    );

    // Insert the SAME logical rows through the Distributed table; ClickHouse
    // itself places them into lines_twin. Synchronous so placement is
    // complete when the insert returns.
    let mut insert = c
        .node0
        .admin
        .insert::<LineRow>("lines_twin_dist")
        .await
        .expect("open twin insert")
        .with_setting("insert_distributed_sync", "1");
    for row in &rows {
        insert.write(row).await.expect("write twin row");
    }
    insert.end().await.expect("finish twin insert");

    // Per node: what the sink placed == what ClickHouse placed, exactly.
    for n in [&c.node0, &c.node1] {
        let sink_placed = dump(&n.admin, "lines_local").await;
        let ch_placed = dump(&n.admin, "lines_twin").await;
        assert!(
            !sink_placed.is_empty(),
            "both shards must receive rows; {} got none",
            n.host
        );
        assert_eq!(
            sink_placed, ch_placed,
            "on {} the sink's placement must equal ClickHouse's Distributed placement",
            n.host
        );
    }

    // And every sku lives on exactly one shard, the one our router chose.
    for s in 0..12 {
        let sku = format!("SKU-{s:02}");
        let c0 = count_where_sku(&c.node0.admin, "lines_local", &sku).await;
        let c1 = count_where_sku(&c.node1.admin, "lines_local", &sku).await;
        assert!(
            (c0 == 0) ^ (c1 == 0),
            "sku {sku} must live on exactly one shard (node0={c0}, node1={c1})"
        );
        let owning = if c0 > 0 { 0 } else { 1 };
        assert_eq!(
            owning,
            shard_of(&sku),
            "sku {sku} landed on shard {owning} but the router chose shard {}",
            shard_of(&sku)
        );
    }
}

// ---- Proof B: shard pruning -------------------------------------------------

/// A SELECT on the sharding key with `optimize_skip_unused_shards = 1`
/// queries only the owning shard, proven on the REMOTE node's query_log,
/// with a pruning-off negative control that does reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn select_on_sharding_key_queries_exactly_one_shard() {
    let pw = "pruning-secret";
    let c = two_node_cluster(pw).await;
    create_tables(&c).await;

    // Populate via a synchronous distributed insert (both shards fill).
    let (_payloads, rows) = make_data(12, 3);
    let mut insert = c
        .node0
        .admin
        .insert::<LineRow>("lines_dist")
        .await
        .expect("open insert")
        .with_setting("insert_distributed_sync", "1");
    for row in &rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("finish insert");

    // A sku OWNED BY SHARD 0, local to the initiator (node 0), so a
    // pruned query must never touch node 1.
    let owned_by_shard0 = (0..12)
        .map(|s| format!("SKU-{s:02}"))
        .find(|s| shard_of(s) == 0)
        .expect("some sku must hash to shard 0");
    assert_eq!(
        shard_of(&owned_by_shard0),
        0,
        "the chosen sku must be owned by shard 0"
    );
    let expected = rows.iter().filter(|r| r.sku == owned_by_shard0).count() as u64;

    // Pruned query: literal `=` on the sharding key; force=2 makes ClickHouse
    // throw if it could NOT prune (a guardrail that the optimizer engaged).
    let pruned_id = unique("pruned-query");
    let count = c
        .node0
        .admin
        .query("SELECT count() FROM lines_dist WHERE sku = ?")
        .bind(&owned_by_shard0)
        .with_setting("optimize_skip_unused_shards", "1")
        .with_setting("force_optimize_skip_unused_shards", "2")
        .with_setting("query_id", pruned_id.as_str())
        .fetch_one::<u64>()
        .await
        .expect("pruned count");
    assert_eq!(
        count, expected,
        "the pruned query must still return the right count for the sku"
    );

    flush_logs(&c).await;
    assert_eq!(
        remote_subqueries(&c.node1.admin, &pruned_id).await,
        0,
        "a pruned SELECT on the sharding key must not reach the non-owning shard"
    );

    // Negative control: pruning off. The initiator contacts BOTH shards, so
    // the remote node DOES log a subquery, proving the query_log method
    // detects remote contact and that the pruned zero above is real.
    let full_id = unique("full-scan-query");
    let _ = c
        .node0
        .admin
        .query("SELECT count() FROM lines_dist WHERE sku = ?")
        .bind(&owned_by_shard0)
        .with_setting("optimize_skip_unused_shards", "0")
        .with_setting("query_id", full_id.as_str())
        .fetch_one::<u64>()
        .await
        .expect("full-scan count");

    flush_logs(&c).await;
    assert!(
        remote_subqueries(&c.node1.admin, &full_id).await >= 1,
        "with pruning disabled the non-owning shard must receive a remote subquery"
    );
}

// ---- Proof C: WEIGHTED placement parity ---------------------------------------

/// The `distributed_check:` block matching cluster `parity_weighted` and the
/// weighted sink configs below.
const WEIGHTED_CHECK: &str = "distributed_check:\n\
                              \x20 cluster: parity_weighted\n\
                              \x20 table: lines_w_dist\n\
                              \x20 sharding_key: sku\n";

/// Proof A's engine-as-oracle comparison under a NON-UNIFORM weight split
/// (9/10): the unit tests pin the interval mapping against ClickHouse's
/// documented semantics, this pins it against the engine itself. Plus the
/// only live exercise of `distributed_check` reading non-default
/// `shard_weight` values: the matching config passes, a drifted one fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn weighted_distributed_insert_and_sink_place_rows_identically() {
    let pw = "weighted-secret";
    let c = two_node_cluster(pw).await;
    create_weighted_tables(&c).await;

    // The live cluster must report the weights the proof depends on.
    let topology = c
        .node0
        .admin
        .query(
            "SELECT shard_num, shard_weight FROM system.clusters \
             WHERE cluster = 'parity_weighted' ORDER BY shard_num",
        )
        .fetch_all::<(u32, u32)>()
        .await
        .expect("weighted cluster topology");
    assert_eq!(
        topology,
        vec![(1, 9), (2, 10)],
        "cluster `parity_weighted` must report weights 9/10 in config order"
    );

    // Weighted placement oracle: the router's own public API over [9, 10].
    let oracle =
        DistributedRouter::<Owned<LineRow>>::new(sku_key, &[9, 10]).expect("oracle router");
    let shard_w = |sku: &str| {
        oracle.shard_for_hash(DistributedRouter::<Owned<LineRow>>::hash_key(
            ShardKey::Str(sku),
        ))
    };

    let (payloads, rows) = make_data(16, 2);
    let skus: Vec<String> = (0..16).map(|s| format!("SKU-{s:02}")).collect();

    // The fixture must exercise both shards AND differ from the equal-weight
    // split for at least one sku; otherwise this proof adds nothing over
    // proof A and a wrong interval model could pass vacuously.
    let on_shard0 = skus.iter().filter(|s| shard_w(s) == 0).count();
    assert!(
        on_shard0 > 0 && on_shard0 < skus.len(),
        "weighted fixture data must reach both shards (shard0={on_shard0}/16)"
    );
    assert!(
        skus.iter().any(|s| shard_w(s) != shard_of(s)),
        "at least one sku must place differently under weights 9/10 than \
         under the equal-weight split"
    );

    // The startup guard against the LIVE weighted cluster: the matching
    // config passes; a weight-drifted config (1/1) fails before writing —
    // system.clusters really carries shard_weight 9/10 and the check reads it.
    let sink = parity_sink(
        "lines_w_local",
        &[(&c.node0.url, 9), (&c.node1.url, 10)],
        pw,
        Some(WEIGHTED_CHECK),
    );
    sink.validate_distributed()
        .await
        .expect("the matching weighted config must pass the startup check");
    let drifted = parity_sink(
        "lines_w_local",
        &[(&c.node0.url, 1), (&c.node1.url, 1)],
        pw,
        Some(WEIGHTED_CHECK),
    );
    let err = drifted
        .validate_distributed()
        .await
        .expect_err("weight drift must fail the startup check");
    assert!(
        err.to_string().contains("weight"),
        "the drift error must name the weight mismatch: {err}"
    );

    // Run the real pipeline with the weighted router: rows land directly in
    // each node's lines_w_local.
    let report = run_pipeline(sink, &payloads).await;
    assert_eq!(report.abandoned, 0, "no batch may be abandoned");
    assert!(report.flushed >= 1, "the pool must have flushed");

    // The SAME logical rows through the weighted Distributed table: the
    // engine itself places them into lines_w_twin.
    let mut insert = c
        .node0
        .admin
        .insert::<LineRow>("lines_w_twin_dist")
        .await
        .expect("open weighted twin insert")
        .with_setting("insert_distributed_sync", "1");
    for row in &rows {
        insert.write(row).await.expect("write weighted twin row");
    }
    insert.end().await.expect("finish weighted twin insert");

    // Per node: the sink's weighted placement == ClickHouse's, bit-for-bit.
    for n in [&c.node0, &c.node1] {
        let sink_placed = dump(&n.admin, "lines_w_local").await;
        let ch_placed = dump(&n.admin, "lines_w_twin").await;
        assert!(
            !sink_placed.is_empty(),
            "both shards must receive rows under weights 9/10; {} got none",
            n.host
        );
        assert_eq!(
            sink_placed, ch_placed,
            "on {} the sink's weighted placement must equal ClickHouse's",
            n.host
        );
    }

    // And each sku lives on exactly the shard the weighted intervals name.
    for sku in &skus {
        let c0 = count_where_sku(&c.node0.admin, "lines_w_local", sku).await;
        let c1 = count_where_sku(&c.node1.admin, "lines_w_local", sku).await;
        assert!(
            (c0 == 0) ^ (c1 == 0),
            "sku {sku} must live on exactly one shard (node0={c0}, node1={c1})"
        );
        let owning = if c0 > 0 { 0 } else { 1 };
        assert_eq!(
            owning,
            shard_w(sku),
            "sku {sku} landed on shard {owning}, not its weighted-interval shard"
        );
    }
}
