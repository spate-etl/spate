//! The sealed batch a sink test hands the Kafka writer.

use bytes::BytesMut;
use spate_core::deser::Owned;
use spate_core::sink::{RowEncoder, SealedBatch};

/// The batch the runtime would seal from `payloads`, each encoded by
/// `encoder` into a frame of its own.
pub(crate) fn sealed<P: Into<Vec<u8>>>(
    encoder: &mut impl RowEncoder<Owned<Vec<u8>>>,
    payloads: impl IntoIterator<Item = P>,
) -> SealedBatch {
    let frames: Vec<_> = payloads
        .into_iter()
        .map(|payload| {
            let mut frame = BytesMut::new();
            encoder
                .encode(&spate_test::record(payload.into()), &mut frame)
                .expect("encode");
            frame.freeze()
        })
        .collect();
    SealedBatch {
        rows: frames.len() as u64,
        bytes: frames.iter().map(|f| f.len() as u64).sum(),
        frames,
        dedup_token: "unused-by-kafka".to_string(),
    }
}
