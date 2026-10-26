//! Allocation snapshot of this crate's object framing.
//!
//! Each case drives the same rig and the same `bench_seams` call the counted
//! tier in `framing_gungraun.rs` measures, under the same case names, and
//! prints its allocation, reallocation and byte counts for
//! `cargo xtask bench alloc` to compare with `benches/alloc/`. Harness-free, so
//! every case runs on the main thread and nothing else allocates while one is
//! counted. Each case is counted once after one uncounted pass.
//!
//! The `zstd_*` rows count the Rust-side buffers only: libzstd allocates its
//! decoder state through C `malloc`, which the counting allocator never sees.

use spate_s3::bench_seams::{frame_objects, frame_range};
use spate_test_support::alloc::{Counting, main_with};

#[path = "support/framing_rig.rs"]
mod framing_rig;
#[path = "support/ndjson.rs"]
mod ndjson;

use framing_rig::{
    gzip_multi_member, gzip_whole, make_framer, plain_many_small, plain_mid_offset, plain_range,
    plain_whole, zstd_multi_frame, zstd_whole,
};

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        for (name, rig) in [
            ("plain_whole", plain_whole()),
            ("gzip_whole", gzip_whole()),
            ("zstd_whole", zstd_whole()),
            ("gzip_multi_member", gzip_multi_member()),
            ("zstd_multi_frame", zstd_multi_frame()),
            ("plain_mid_offset", plain_mid_offset()),
            ("plain_many_small", plain_many_small()),
        ] {
            report.case(name, || {
                let records = frame_objects(rig.compression, make_framer(), &rig.objects)
                    .expect("the fixture frames cleanly");
                assert_eq!(records, rig.expect_records, "framed record count changed");
            });
        }
        let rig = plain_range();
        report.case("plain_range", || {
            let records = frame_range(
                make_framer(),
                &rig.body,
                rig.start,
                rig.body.len() as u64,
                b'\n',
                ndjson::RANGE_BYTES,
                ndjson::CHUNK_BYTES,
            )
            .expect("the fixture frames cleanly");
            assert_eq!(
                records, rig.expect_records,
                "a range framed a different record count"
            );
        });
    });
}
