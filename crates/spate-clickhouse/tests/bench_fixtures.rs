//! Both encoders accept every bench schema, every bench corpus is the one
//! its recorded numbers were measured against, and the write-path benches'
//! HTTP server serves a sink under every compression.
//!
//! The Native encoder is driven by a column list written beside the row
//! struct, and a mismatch between the two (a column added without its field,
//! or a type that does not match) is a fatal error raised at encode time.
//! `cargo bench --no-run` cannot catch that: it compiles the bench without
//! running it, so the first sign would be a failed valgrind job minutes into
//! CI. These tests run wherever `cargo test` does; the frame pin, the
//! slowest, takes under two seconds in the dev profile.

use bytes::BytesMut;
use spate_clickhouse::{
    ClickHouseEncoder, ClickHouseRow, DistributedRouter, NativeEncoder, NativeSchema, ShardKey,
};
use spate_core::deser::Owned;
use spate_core::sink::{RowEncoder, ShardWriter};
use spate_test::record;

#[path = "../benches/support/frames.rs"]
mod frames;
#[path = "../benches/support/http_server.rs"]
mod http_server;
#[path = "../benches/support/keys.rs"]
mod keys;
#[path = "../benches/support/rows.rs"]
mod rows;

fn drive<E, T>(mut enc: E, payloads: Vec<T>) -> usize
where
    E: RowEncoder<Owned<T>>,
    T: Send + 'static,
{
    let mut buf = BytesMut::new();
    for p in payloads {
        enc.encode(&record(p), &mut buf).expect("row encodes");
    }
    enc.finish_chunk(&mut buf).expect("chunk finishes");
    buf.len()
}

#[test]
fn the_native_schemas_match_their_row_structs() {
    let events = NativeSchema::from_columns(&rows::event_columns()).expect("event schema builds");
    let metrics =
        NativeSchema::from_columns(&rows::metric_columns()).expect("metric schema builds");
    let exotic = NativeSchema::from_columns(&rows::exotic_columns()).expect("exotic schema builds");

    assert!(
        drive(
            NativeEncoder::<Owned<_>>::new(events),
            rows::events(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            NativeEncoder::<Owned<_>>::new(metrics),
            rows::metrics(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            NativeEncoder::<Owned<_>>::new(exotic),
            rows::exotic(rows::ROWS)
        ) > 0
    );
}

#[test]
fn rowbinary_encodes_every_schema() {
    assert!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::events(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::metrics(rows::ROWS)
        ) > 0
    );
    assert!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::exotic(rows::ROWS)
        ) > 0
    );
}

/// The encoded sizes, pinned.
///
/// An instruction count only compares across two legs that encoded identical
/// bytes, and the corpus is generated rather than stored, so a one-character
/// edit to a value formula in `rows.rs` re-baselines every comparison with
/// nothing to say it happened. These numbers make that edit fail here
/// instead. They are the sizes the pull request that added each bench
/// published; changing one means re-recording it and treating every count
/// from before the change as measuring a different corpus.
#[test]
fn the_corpora_are_pinned_across_revisions() {
    let events = NativeSchema::from_columns(&rows::event_columns()).expect("event schema builds");
    let metrics =
        NativeSchema::from_columns(&rows::metric_columns()).expect("metric schema builds");
    let exotic = NativeSchema::from_columns(&rows::exotic_columns()).expect("exotic schema builds");

    assert_eq!(
        drive(
            NativeEncoder::<Owned<_>>::new(events),
            rows::events(rows::ROWS)
        ),
        116_573,
        "native/events"
    );
    assert_eq!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::events(rows::ROWS)
        ),
        125_738,
        "rowbinary/events"
    );
    assert_eq!(
        drive(
            NativeEncoder::<Owned<_>>::new(metrics),
            rows::metrics(rows::ROWS)
        ),
        92_204,
        "native/metrics"
    );
    assert_eq!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::metrics(rows::ROWS)
        ),
        92_000,
        "rowbinary/metrics"
    );
    assert_eq!(
        drive(
            NativeEncoder::<Owned<_>>::new(exotic),
            rows::exotic(rows::ROWS)
        ),
        174_724,
        "native/exotic"
    );
    assert_eq!(
        drive(
            ClickHouseEncoder::<Owned<_>>::unchecked(),
            rows::exotic(rows::ROWS)
        ),
        170_463,
        "rowbinary/exotic"
    );
}

/// The routing corpora, pinned the same way and for the same reason.
///
/// The lengths decide the cost: XXH64's cost is a function of input length
/// alone, and it changes shape at 32 bytes, so
/// `str_short` and `str_long` are named for which side of that they sit on.
/// A key format edited to one byte longer or shorter would keep every case
/// running and quietly re-baseline it.
///
/// The digest pins the bytes themselves. It folds the hash of every key in
/// order, so a changed value, a changed seed, or a reordered corpus all
/// fail, and because it is built from `hash_key`, it is simultaneously a
/// check that the hash the bench measures is the one ClickHouse's
/// `xxHash64(col)` agrees with (the vectors in `router.rs` pin the absolute
/// values against a live server).
#[test]
fn the_routing_corpora_are_pinned_across_revisions() {
    /// Any family works: `hash_key` does not read one.
    type Keys = Owned<String>;

    fn digest(hashes: impl Iterator<Item = u64>) -> u64 {
        hashes.fold(0u64, |acc, h| acc.rotate_left(7) ^ h)
    }

    let short = keys::short_strings(keys::KEYS);
    let long = keys::long_strings(keys::KEYS);
    let blobs = keys::blobs(keys::KEYS);
    let u64s = keys::u64s(keys::KEYS);
    let u32s = keys::u32s(keys::KEYS);

    assert!(
        short.iter().all(|k| k.len() == keys::SHORT_LEN),
        "a short key left XXH64's single-accumulator regime"
    );
    assert!(
        long.iter().all(|k| k.len() == keys::LONG_LEN),
        "a long key left XXH64's four-lane regime"
    );
    assert!(blobs.iter().all(|b| b.len() == keys::BLOB_LEN));

    assert_eq!(
        digest(
            short
                .iter()
                .map(|k| DistributedRouter::<Keys>::hash_key(ShardKey::Str(k)))
        ),
        0x30fe_fce2_a8f4_e239,
        "str_short"
    );
    assert_eq!(
        digest(
            long.iter()
                .map(|k| DistributedRouter::<Keys>::hash_key(ShardKey::Str(k)))
        ),
        0x0cef_330d_2fc7_cb2e,
        "str_long"
    );
    assert_eq!(
        digest(
            blobs
                .iter()
                .map(|b| DistributedRouter::<Keys>::hash_key(ShardKey::Bytes(b)))
        ),
        0x3957_2f43_7510_81a1,
        "blob"
    );
    assert_eq!(
        digest(
            u64s.iter()
                .map(|v| DistributedRouter::<Keys>::hash_key(ShardKey::U64(*v)))
        ),
        0xe556_8701_4100_be0e,
        "u64"
    );
    assert_eq!(
        digest(
            u32s.iter()
                .map(|v| DistributedRouter::<Keys>::hash_key(ShardKey::U32(*v)))
        ),
        0x9dda_6b55_d66b_d743,
        "u32"
    );
}

/// The two weight tables select shards differently, and both are reached.
///
/// The uniform table takes the `hash % N` fast path and the tiered one the
/// interval scan, which is the whole reason the bench carries a case for
/// each. Pinning the total scan depth over a fixed corpus pins what that
/// case measures: if a change made the scan exit sooner on average, this
/// number moves, and the count that moved with it has an explanation.
#[test]
fn the_weight_tables_reach_both_selection_paths() {
    type Keys = Owned<String>;

    let uniform = DistributedRouter::<Keys>::new(first_char, &keys::UNIFORM).expect("uniform");
    let tiered = DistributedRouter::<Keys>::new(first_char, &keys::TIERED).expect("tiered");
    assert_eq!(uniform.shard_count(), keys::SHARDS);
    assert_eq!(tiered.shard_count(), keys::SHARDS);

    let hashes: Vec<u64> = keys::short_strings(keys::KEYS)
        .iter()
        .map(|k| DistributedRouter::<Keys>::hash_key(ShardKey::Str(k)))
        .collect();

    // Every shard index is in range under both tables, and the scan depth
    // (the sum of one-based positions the tiered table walks) is fixed.
    let uniform_total: usize = hashes.iter().map(|&h| uniform.shard_for_hash(h)).sum();
    let tiered_total: usize = hashes.iter().map(|&h| tiered.shard_for_hash(h) + 1).sum();
    // Both are close to what a uniform hash predicts (3.5 and 5.27 per
    // record), which checks that the corpus scatters rather than being the
    // point of the pin.
    assert_eq!(uniform_total, 349_449, "uniform placement");
    assert_eq!(tiered_total, 527_767, "tiered scan depth");
}

// `&String` (not `&str`) is forced: the fn must coerce to
// `KeyExtractor<Owned<String>>`, whose argument is `&'a Rec<'buf>`. Routing
// by hash is what these tests exercise; the extractor is never called.
#[allow(
    clippy::ptr_arg,
    reason = "the KeyExtractor fn-pointer signature fixes the argument type"
)]
fn first_char(rec: &String) -> ShardKey<'_> {
    ShardKey::Str(rec)
}

/// The compression benches' frames, pinned.
///
/// A per-frame figure is only comparable across revisions that compressed the
/// same frames. The frames come from the `events` corpus through the Native
/// encoder's size estimate. The rows per frame and the size range are exact,
/// the lz4 ratio is a band around the measured 2.600, and a digest pins the
/// write-path batch's bytes, so a corpus edit that moves no size still fails.
#[test]
fn the_frame_fixture_is_pinned_across_revisions() {
    let frames = frames::native_frames(frames::FRAMES);
    assert_eq!(frames.len(), frames::FRAMES);

    let per_frame: Vec<u64> = frames.iter().map(|f| f.rows).collect();
    assert_eq!(per_frame, ROWS_PER_FRAME, "rows per frame");

    let sizes = frames.iter().map(|f| f.bytes.len());
    assert_eq!(
        (sizes.clone().min(), sizes.max()),
        (Some(FRAME_BYTES_MIN), Some(FRAME_BYTES_MAX)),
        "frame size range"
    );

    let raw: usize = frames.iter().map(|f| f.bytes.len()).sum();
    let lz4: usize = frames
        .iter()
        .map(|f| {
            clickhouse::_priv::lz4_compress(&f.bytes)
                .expect("lz4 compresses")
                .len()
        })
        .sum();
    let ratio = raw as f64 / lz4 as f64;
    assert!(
        (LZ4_RATIO_MIN..=LZ4_RATIO_MAX).contains(&ratio),
        "lz4 ratio {ratio:.3} outside {LZ4_RATIO_MIN}..={LZ4_RATIO_MAX}"
    );

    // The write-path batch extends the same frames; its row total pins the
    // frames past the eighth.
    let batch = frames::sealed(&frames::native_frames(frames::BATCH_FRAMES), "pin");
    assert_eq!(batch.frames.len(), frames::BATCH_FRAMES);
    assert_eq!(batch.rows, BATCH_ROWS, "rows in the write-path batch");
    assert_eq!(
        batch.frames[..frames::FRAMES],
        frames.iter().map(|f| f.bytes.clone()).collect::<Vec<_>>()[..],
        "the batch opens with the per-frame fixture"
    );

    let digest = batch
        .frames
        .iter()
        .map(|f| DistributedRouter::<Owned<String>>::hash_key(ShardKey::Bytes(&f[..])))
        .fold(0u64, |acc, h| acc.rotate_left(7) ^ h);
    assert_eq!(digest, BATCH_DIGEST, "the write-path batch's bytes");
}

/// Rows in each of the eight frames.
const ROWS_PER_FRAME: [u64; 8] = [448, 448, 448, 432, 432, 432, 432, 432];
/// The smallest and largest of the eight frames, in bytes. The encoder's
/// estimate runs ahead of the bytes it writes, so a frame seals below
/// `ChunkConfig`'s default target.
const FRAME_BYTES_MIN: usize = 51_074;
const FRAME_BYTES_MAX: usize = 52_859;
/// Uncompressed over lz4-compressed bytes across the eight frames: 2.600.
const LZ4_RATIO_MIN: f64 = 2.55;
const LZ4_RATIO_MAX: f64 = 2.65;
/// Rows across the write-path batch's 1,600 frames.
const BATCH_ROWS: u64 = 698_224;
/// XXH64 of each frame in the write-path batch, folded. An edit to a
/// fixed-width column's value formula in `rows.rs` moves no row count or frame
/// size, and fails here.
const BATCH_DIGEST: u64 = 0x18d4_801a_4f0c_7919;

/// The write-path bench's setup works end to end under every compression.
///
/// `write_wall.rs` builds its sink against `http_server.rs` in setup, so a
/// server that answered the schema query in a form the client cannot read
/// would fail minutes into a bench run. This builds the same sink under each
/// `compression` value, writes one batch, and checks what the server received:
/// the schema query, then one insert carrying the frames, raw under `off` and
/// compressed by the client otherwise.
#[test]
fn the_bench_server_serves_the_schema_under_every_compression() {
    assert_eq!(
        <rows::EventRow as ClickHouseRow>::COLUMNS,
        rows::event_columns()
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>(),
        "the fixture's insert columns are the events schema's"
    );
    let frames = frames::native_frames(frames::FRAMES);
    let batch = frames::sealed(&frames, "bench-fixture");
    let raw: Vec<u8> = frames
        .iter()
        .flat_map(|f| f.bytes.iter().copied())
        .collect();

    for compression in ["lz4", "zstd:3", "off"] {
        let server = http_server::BenchServer::recording(&rows::event_columns());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let (writer, endpoint) =
            runtime.block_on(http_server::events_sink(server.url(), compression));
        runtime
            .block_on(writer.write_batch(&endpoint, &batch))
            .unwrap_or_else(|e| panic!("{compression}: the server accepts the batch: {e}"));

        let requests = server.requests();
        assert_eq!(
            requests.len(),
            2,
            "{compression}: the schema query, then the insert"
        );
        let insert = &requests[1];
        assert!(
            insert.param("query") == Some(writer.insert_sql()),
            "{compression}: the second request is the insert"
        );
        if compression == "off" {
            assert_eq!(insert.param("decompress"), None, "off: no decompress");
            assert_eq!(&insert.body[..], &raw[..], "off: the frames, unchanged");
        } else {
            assert_eq!(
                insert.param("decompress"),
                Some("1"),
                "{compression}: decompress"
            );
            assert!(
                !insert.body.is_empty() && insert.body.len() < raw.len(),
                "{compression}: a compressed body, {} bytes against {} raw",
                insert.body.len(),
                raw.len()
            );
            // The client compresses each frame into one block.
            // `_priv::zstd_compress` uses zstd's default level, 3.
            assert_eq!(zstd::DEFAULT_COMPRESSION_LEVEL, 3, "zstd's default level");
            let blocks: Vec<u8> = frames
                .iter()
                .flat_map(|f| {
                    let block = if compression == "lz4" {
                        clickhouse::_priv::lz4_compress(&f.bytes)
                    } else {
                        clickhouse::_priv::zstd_compress(&f.bytes)
                    };
                    block.expect("compresses").to_vec()
                })
                .collect();
            assert!(
                insert.body[..] == blocks[..],
                "{compression}: one block per frame, at the configured level"
            );
        }
    }
}
