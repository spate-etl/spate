//! Wall-clock cases for the ClickHouse write path: one sealed batch through
//! `ClickHouseWriter::write_batch`.
//!
//! The batch is the first 1,600 frames of `support/frames.rs`, Native blocks
//! of the `events` corpus sealed at `ChunkConfig`'s default chunk target. The
//! sink is built by `config::build(..).with_row()` against the in-process
//! server in `support/http_server.rs`, as a pipeline builds it, so the writer,
//! its settings and its client are the production ones. The build and the
//! schema fetch run in setup, outside the measured region.
//!
//! One iteration is one `write_batch`: the request is opened, every frame
//! goes through `InsertFormatted::send` (which compresses it under `lz4` and
//! `zstd`), and the call returns once the server answers. The writer runs on
//! a current-thread runtime, so the client's compression and socket writes
//! share one thread, as they share an I/O worker in a pipeline.
//!
//! # Reading these numbers
//!
//! - `wall_ns_per_iter` is the batch's latency on one thread.
//! - `cpu_ns_per_iter` reads the whole process's CPU, so it also carries the
//!   server's runtime reading and discarding the body. Both legs of a
//!   comparison carry the same server cost, so a difference between them is
//!   the writer's.
//! - `records_per_s` counts rows and `bytes_per_s` counts uncompressed frame
//!   bytes.
//!
//! # Cases
//!
//! `write_lz4`, `write_zstd3` and `write_off`, one per `compression` value.
//!
//! Run: `cargo xtask bench ab --ref main --filter write_`

use spate_bench::{Suite, bench_main};
use spate_clickhouse::{ClickHouseEndpoint, ClickHouseWriter};
use spate_core::sink::{SealedBatch, ShardWriter};
use tokio::runtime::Runtime;

#[allow(dead_code, reason = "this target writes the events corpus only")]
#[path = "support/rows.rs"]
mod rows;

#[path = "support/frames.rs"]
mod frames;

#[path = "support/http_server.rs"]
mod http_server;

use frames::BATCH_FRAMES;
use http_server::BenchServer;

/// One case's sink, its server and the batch it writes.
struct Rig {
    runtime: Runtime,
    writer: ClickHouseWriter,
    endpoint: ClickHouseEndpoint,
    batch: SealedBatch,
    _server: BenchServer,
}

fn rig(compression: &str, batch: SealedBatch) -> Rig {
    let server = BenchServer::discarding(&rows::event_columns());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the writer runtime builds");
    let (writer, endpoint) = runtime.block_on(http_server::events_sink(server.url(), compression));
    Rig {
        runtime,
        writer,
        endpoint,
        batch,
        _server: server,
    }
}

fn case(suite: Suite, id: &str, compression: &'static str) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let frames = frames::native_frames(BATCH_FRAMES);
                for f in &frames {
                    corpus.absorb("frame", &f.bytes);
                }
                corpus.absorb("compression", compression.as_bytes());
                rig(compression, frames::sealed(&frames, "bench-write"))
            },
            |b, rig: &Rig| {
                b.iter(|| {
                    rig.runtime
                        .block_on(rig.writer.write_batch(&rig.endpoint, &rig.batch))
                        .map(|()| rig.batch.rows)
                        .expect("the server accepts the batch")
                });
            },
        )
        .items_of(|rig: &Rig| rig.batch.rows)
        .bytes_of(|rig: &Rig| rig.batch.bytes)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-clickhouse");
    let suite = case(suite, "write_lz4", "lz4");
    let suite = case(suite, "write_zstd3", "zstd:3");
    case(suite, "write_off", "off")
}

bench_main!(suite);
