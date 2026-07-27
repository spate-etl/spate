// ---- partitioning × deduplication: one batch, many partitions ---------------
//
// The sink seals batches on rows/bytes/linger with no awareness of the target
// table's PARTITION BY column, and stamps one `insert_deduplication_token` per
// batch (reused verbatim on retries). So a single INSERT routinely spans
// several partitions (e.g. two dates) under one token. These tests pin down
// what ClickHouse 26.3 actually does with that, since the at-least-once
// guarantee leans on it. Both pin `26.3` (the LTS the docs target) and use a
// non-replicated MergeTree with an explicit dedup window — the framework's
// documented "you must set the window" case.

use super::*;

/// A row whose `dt` (days since epoch) feeds a ClickHouse `Date` column,
/// so distinct `dt` values land in distinct partitions under `PARTITION BY
/// dt`. Only `Serialize` is needed — the wire path is RowBinary and the
/// tests read back counts, not typed rows.
#[derive(Debug, Clone, Serialize)]
struct DatedRow {
    id: u64,
    dt: u16,
}

async fn table_count(admin: &clickhouse::Client) -> u64 {
    admin
        .query("SELECT count() FROM dated")
        .fetch_one::<u64>()
        .await
        .expect("count")
}

async fn active_partitions(admin: &clickhouse::Client) -> u64 {
    admin
        .query(
            "SELECT count(DISTINCT partition) FROM system.parts WHERE table = 'dated' AND active",
        )
        .fetch_one::<u64>()
        .await
        .expect("partition count")
}

async fn active_parts(admin: &clickhouse::Client) -> u64 {
    admin
        .query("SELECT count() FROM system.parts WHERE table = 'dated' AND active")
        .fetch_one::<u64>()
        .await
        .expect("part count")
}

/// A batch that spans two date partitions under one token must (a) insert
/// without error, (b) land every row across both partitions, and (c) be
/// idempotent when the same token is replayed — ClickHouse deduplicates
/// per partition, so both partitions dedupe independently.
#[tokio::test]
#[ignore = "requires Docker"]
async fn multi_partition_batch_stays_idempotent_per_partition() {
    let srv = bare_server("26.3", "partition-secret").await;
    srv.admin
        .query(
            "CREATE TABLE dated (id UInt64, dt Date) ENGINE = MergeTree \
                 PARTITION BY dt ORDER BY id \
                 SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create dated");

    let sink = sink_with(
        &srv.url,
        "dated",
        &["id", "dt"],
        "off",
        "user: default\npassword: partition-secret\n",
    );

    // 100 rows split across two adjacent dates (interleaved by id parity),
    // shipped as several frames to prove frame boundaries are irrelevant.
    let rows: Vec<DatedRow> = (0..100)
        .map(|id| DatedRow {
            id,
            dt: 20000 + (id % 2) as u16,
        })
        .collect();
    let batch = sealed(&rows, "mp-1", 4);

    // (a) A multi-partition single-token insert does not fail.
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("multi-partition insert must succeed");

    // (b) Every row landed, genuinely across two partitions.
    assert_eq!(table_count(&srv.admin).await, 100, "all rows must land");
    assert_eq!(
        active_partitions(&srv.admin).await,
        2,
        "the batch must have spanned two partitions"
    );

    // (c) Replaying the same token is a per-partition no-op in both
    // partitions: no loss, no doubling.
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("replay must succeed");
    assert_eq!(
        table_count(&srv.admin).await,
        100,
        "same token must deduplicate in every partition"
    );

    // Control: a different token inserts again — dedup is token-driven and
    // works independently per partition (both partitions double).
    let reinsert = sealed(&rows, "mp-2", 4);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &reinsert)
        .await
        .expect("distinct-token insert must succeed");
    assert_eq!(
        table_count(&srv.admin).await,
        200,
        "a different token must insert in every partition"
    );
}

/// Why the "multiple same-partition parts under one token" data-loss case
/// cannot arise from a batch. ClickHouse writes **one part per partition
/// per insert**: even when the parser is told to form ten-row blocks
/// (`max_insert_block_size = 10`, squashing disabled), a single-partition
/// insert of 100 rows coalesces into exactly one part — so a batch's single
/// token maps to exactly one dedup unit per partition it touches, and there
/// is no intra-insert collision to lose data to. (Several parts in one
/// partition would take several *inserts*, i.e. several batches, and each
/// batch carries a distinct sequence token.) Merges are stopped so the part
/// count reflects only what the insert wrote.
#[tokio::test]
#[ignore = "requires Docker"]
async fn single_partition_batch_forms_one_dedup_unit() {
    let srv = bare_server("26.3", "partition-secret").await;
    srv.admin
        .query(
            "CREATE TABLE dated (id UInt64, dt Date) ENGINE = MergeTree \
                 PARTITION BY dt ORDER BY id \
                 SETTINGS non_replicated_deduplication_window = 100",
        )
        .execute()
        .await
        .expect("create dated");
    srv.admin
        .query("SYSTEM STOP MERGES dated")
        .execute()
        .await
        .expect("stop merges");

    // Force the parser to form many small blocks; the point is that they
    // still coalesce into one part per partition.
    let sink = sink_with(
        &srv.url,
        "dated",
        &["id", "dt"],
        "off",
        "user: default\npassword: partition-secret\n\
             settings: { max_insert_block_size: \"10\", min_insert_block_size_rows: \"0\", \
             min_insert_block_size_bytes: \"0\" }",
    );

    // 100 rows, all in one partition (single date), one frame, one token.
    let rows: Vec<DatedRow> = (0..100).map(|id| DatedRow { id, dt: 20000 }).collect();
    let batch = sealed(&rows, "sp-1", 1);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("single-partition insert must succeed");

    assert_eq!(table_count(&srv.admin).await, 100, "all rows must land");
    // One insert into one partition is one part: one dedup unit, so no
    // block within the batch can be dropped as a false duplicate.
    assert_eq!(
        active_parts(&srv.admin).await,
        1,
        "a single-partition insert coalesces into exactly one part"
    );
}
