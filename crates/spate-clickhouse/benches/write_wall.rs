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
//! One iteration codes the batch's frames and writes them. Under `lz4` and
//! `zstd` every frame goes through one instance of the sink's frame codec,
//! as a pipeline thread codes each chunk at seal, and `write_batch` then
//! sends the blocks as they are. Under `off` the frames are written as the
//! encoder made them. The writer runs on a current-thread runtime, so the
//! coding and the socket writes share one thread.
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
//! `write_lz4`, `write_zstd3` and `write_off`, one per `compression` value,
//! cost coding and writing together: the total CPU a batch costs, wherever it
//! runs. `write_lz4_precoded` and `write_zstd3_precoded` code the frames in
//! setup and time the write alone, which is the share left to the I/O
//! runtime.
//!
//! Run: `cargo xtask bench ab --ref main --filter write_`

use bytes::Bytes;
use spate_bench::{Suite, bench_main};
use spate_clickhouse::{ClickHouseEndpoint, ClickHouseWriter, Compression};
use spate_core::sink::{FrameCodec, SealedBatch, ShardWriter};
use std::cell::RefCell;
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
    /// The frames as sealed. Under a timed codec they are raw and coded in
    /// each iteration; otherwise they are what the writer sends.
    batch: SealedBatch,
    /// The codec each iteration runs, for the cases that time coding.
    codec: Option<RefCell<Box<dyn FrameCodec>>>,
    _server: BenchServer,
}

impl Rig {
    /// Codes the batch when the case times coding, then writes it.
    fn write(&self) -> u64 {
        let coded;
        let batch = match &self.codec {
            Some(codec) => {
                let mut codec = codec.borrow_mut();
                coded = SealedBatch {
                    frames: self
                        .batch
                        .frames
                        .iter()
                        .map(|f| {
                            Bytes::copy_from_slice(codec.encode_frame(f).expect("the frame codes"))
                        })
                        .collect(),
                    rows: self.batch.rows,
                    bytes: self.batch.bytes,
                    dedup_token: self.batch.dedup_token.clone(),
                };
                &coded
            }
            None => &self.batch,
        };
        self.runtime
            .block_on(self.writer.write_batch(&self.endpoint, batch))
            .map(|()| batch.rows)
            .expect("the server accepts the batch")
    }
}

/// How a case prepares its frames.
#[derive(Clone, Copy)]
enum Coding {
    /// Coded inside the measured iteration (or not at all under `off`).
    Timed,
    /// Coded once in setup; the iteration writes only.
    Precoded,
}

fn rig(compression: &str, frames: &[frames::Frame], coding: Coding) -> Rig {
    let server = BenchServer::discarding(&rows::event_columns());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the writer runtime builds");
    let (writer, endpoint) = runtime.block_on(http_server::events_sink(server.url(), compression));
    let (batch, codec) = match coding {
        Coding::Precoded => (frames::coded(frames, compression, "bench-write"), None),
        Coding::Timed => {
            let parsed: Compression = compression.parse().expect("a compression value");
            let codec = spate_clickhouse::testing::frame_codec(parsed).map(|f| {
                let mut codec = f.make();
                // Warm the scratch, as a chain's codec is after its first seal.
                codec
                    .encode_frame(&frames[0].bytes)
                    .expect("the frame codes");
                RefCell::new(codec)
            });
            (frames::sealed(frames, "bench-write"), codec)
        }
    };
    Rig {
        runtime,
        writer,
        endpoint,
        batch,
        codec,
        _server: server,
    }
}

fn case(suite: Suite, id: &str, compression: &'static str, coding: Coding) -> Suite {
    suite
        .case(
            id,
            move |corpus, _seed| {
                let frames = frames::native_frames(BATCH_FRAMES);
                for f in &frames {
                    corpus.absorb("frame", &f.bytes);
                }
                corpus.absorb("compression", compression.as_bytes());
                rig(compression, &frames, coding)
            },
            |b, rig: &Rig| b.iter(|| rig.write()),
        )
        .items_of(|rig: &Rig| rig.batch.rows)
        .bytes_of(|rig: &Rig| rig.batch.bytes)
        .done()
}

fn suite() -> Suite {
    let suite = spate_bench::suite("spate-clickhouse");
    let suite = case(suite, "write_lz4", "lz4", Coding::Timed);
    let suite = case(suite, "write_zstd3", "zstd:3", Coding::Timed);
    let suite = case(suite, "write_off", "off", Coding::Timed);
    let suite = case(suite, "write_lz4_precoded", "lz4", Coding::Precoded);
    case(suite, "write_zstd3_precoded", "zstd:3", Coding::Precoded)
}

bench_main!(suite);
