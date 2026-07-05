//! The CPU half of the sink: encoding records to RowBinary on pipeline
//! threads.

use crate::rowbinary;
use bytes::BytesMut;
use etl_core::deser::Owned;
use etl_core::error::{ErrorClass, SinkError};
use etl_core::record::Record;
use etl_core::sink::RowEncoder;
use serde::Serialize;
use std::marker::PhantomData;

/// Encodes `T: Serialize` records into RowBinary via this crate's
/// [serializer](crate::rowbinary). Runs inside the chain's terminal stage
/// on pinned pipeline threads; sink workers ship the resulting frames as-is.
///
/// The struct's **field declaration order is the wire contract** — it must
/// match the column list configured for the sink (see the crate docs).
#[derive(Debug)]
pub struct ClickHouseEncoder<T> {
    _row: PhantomData<fn(T)>,
}

impl<T> ClickHouseEncoder<T> {
    /// An encoder for rows of type `T`.
    #[must_use]
    pub fn new() -> Self {
        ClickHouseEncoder { _row: PhantomData }
    }
}

impl<T> Default for ClickHouseEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for ClickHouseEncoder<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T> RowEncoder<Owned<T>> for ClickHouseEncoder<T>
where
    T: Serialize + Send + 'static,
{
    fn encode<'buf>(&mut self, rec: &Record<T>, buf: &mut BytesMut) -> Result<(), SinkError> {
        rowbinary::serialize_row(&rec.payload, buf).map_err(|e| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: format!("rowbinary encoding failed: {e}"),
        })
    }
}

/// Passthrough for records that are **already RowBinary-encoded** rows
/// (`Vec<u8>` payloads): appends the bytes verbatim. For pipelines that
/// encode upstream or replicate pre-encoded data. One record must be
/// exactly one encoded row — the framework counts rows by records.
#[derive(Clone, Copy, Debug, Default)]
pub struct PreEncodedRows;

impl RowEncoder<Owned<Vec<u8>>> for PreEncodedRows {
    fn encode<'buf>(&mut self, rec: &Record<Vec<u8>>, buf: &mut BytesMut) -> Result<(), SinkError> {
        buf.extend_from_slice(&rec.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_core::checkpoint::AckRef;
    use etl_core::record::{PartitionId, RecordMeta};
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        id: u64,
        name: String,
    }

    fn record<T>(
        payload: T,
    ) -> (
        Record<T>,
        crossbeam_channel::Receiver<etl_core::checkpoint::AckMsg>,
    ) {
        let (ack, rx) = AckRef::test_pair();
        (
            Record {
                payload,
                meta: RecordMeta {
                    partition: PartitionId(0),
                    offset: 0,
                    event_time_ms: 0,
                    key_hash: None,
                },
                ack,
            },
            rx,
        )
    }

    #[test]
    fn encodes_serializable_rows() {
        let (rec, _rx) = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut buf = BytesMut::new();
        ClickHouseEncoder::new().encode(&rec, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[7, 0, 0, 0, 0, 0, 0, 0, 1, b'x']);
    }

    #[test]
    fn encoding_failures_are_record_level() {
        #[derive(Serialize)]
        struct Bad {
            c: char,
        }
        let (rec, _rx) = record(Bad { c: 'x' });
        let err = ClickHouseEncoder::new()
            .encode(&rec, &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, .. } => assert_eq!(class, ErrorClass::RecordLevel),
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    #[test]
    fn pre_encoded_rows_pass_through() {
        let (rec, _rx) = record(vec![1u8, 2, 3]);
        let mut buf = BytesMut::new();
        PreEncodedRows.encode(&rec, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[1, 2, 3]);
    }
}
