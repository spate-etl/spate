//! Shared object-storage staging for the S3 backfill rigs.
//!
//! Both the solo [`s3_backfill`](../bin/s3_backfill.rs) and the coordinated
//! [`s3_backfill_coordinated`](../bin/s3_backfill_coordinated.rs) rigs stage
//! identical NDJSON objects, so the generator lives here once. The bytes on
//! disk are byte-identical across rigs, which keeps their arms comparable.

use std::io::Write as _;

/// One NDJSON line of exactly `payload` bytes (including the newline).
pub fn line(i: usize, payload: usize) -> String {
    let head = format!("{{\"i\":{i},\"pad\":\"");
    let tail = "\"}";
    let pad = payload.saturating_sub(head.len() + tail.len() + 1).max(1);
    format!("{head}{}{tail}", "x".repeat(pad))
}

/// Stages the objects and returns the **decoded** size of one object in bytes.
///
/// The caller reports throughput against this rather than against
/// `records * payload`: [`line`] pads to a fixed width only while there is room
/// for the index, and its `.max(1)` floor silently widens the high-index lines
/// once `PAYLOAD` gets close to the JSON scaffolding. Measuring the body keeps
/// `decoded_mb_per_s` honest if that ever happens; the assertion below keeps it
/// from happening quietly.
pub fn stage(
    dir: &std::path::Path,
    codec: &str,
    objects: usize,
    records: usize,
    payload: usize,
) -> u64 {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).expect("data dir");
    std::fs::create_dir_all(dir.join("state")).expect("state dir");
    let mut body = Vec::new();
    for i in 0..records {
        body.extend_from_slice(line(i, payload).as_bytes());
        body.push(b'\n');
    }
    assert_eq!(
        body.len(),
        records * payload,
        "PAYLOAD={payload} is too small to hold a {records}-record index plus \
         its JSON scaffolding, so the records are not uniformly sized and the \
         arm is not comparable with the others — raise PAYLOAD or lower \
         RECORDS_PER_OBJECT",
    );
    for o in 0..objects {
        match codec {
            "none" => {
                std::fs::write(data.join(format!("part-{o:04}.ndjson")), &body).expect("write");
            }
            "gzip" => {
                let mut enc =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                enc.write_all(&body).expect("gzip");
                std::fs::write(
                    data.join(format!("part-{o:04}.ndjson.gz")),
                    enc.finish().unwrap(),
                )
                .expect("write");
            }
            "zstd" => {
                std::fs::write(
                    data.join(format!("part-{o:04}.ndjson.zst")),
                    zstd::encode_all(&body[..], 3).expect("zstd"),
                )
                .expect("write");
            }
            other => panic!("unknown codec {other}"),
        }
    }
    body.len() as u64
}
