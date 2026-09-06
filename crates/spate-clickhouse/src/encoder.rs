//! The CPU half of the sink: encoding records to RowBinary on pipeline
//! threads.

use crate::rowbinary;
use crate::schema::{self, RowSchema};
use bytes::BytesMut;
use serde::Serialize;
use spate_core::deser::{Owned, RecFamily};
use spate_core::error::{ErrorClass, SinkError};
use spate_core::record::Record;
use spate_core::sink::RowEncoder;
use std::marker::PhantomData;
use std::sync::Arc;

/// Encodes a record family's `Serialize` rows into RowBinary via this
/// crate's [serializer](crate::rowbinary). Runs inside the chain's terminal
/// stage on pinned pipeline threads; sink workers ship the resulting frames
/// as-is.
///
/// `F` is the **record family**: use `Owned<T>` for plain owned row structs
/// (`ClickHouseEncoder::<Owned<MyRow>>::new()`), or a borrowed family whose
/// `Rec<'buf>` points into the payload buffer for zero-copy pipelines. Any
/// family whose records implement `Serialize` at every lifetime encodes.
///
/// The row struct's **field declaration order is the wire contract**: it is
/// the insert column list `#[derive(ClickHouseRow)]` generates (see the
/// crate docs). The encoder checks that contract against the live table's
/// schema on each pipeline thread's first record.
#[derive(Debug)]
pub struct ClickHouseEncoder<F> {
    expected: Arc<RowSchema>,
    checked: bool,
    _row: PhantomData<fn(F)>,
}

impl<F> ClickHouseEncoder<F> {
    /// An encoder that validates the row struct against `expected` (the
    /// schema from [`ClickHouseSink::schema`](crate::ClickHouseSink::schema))
    /// on the first record it encodes: field names, order and type classes.
    /// A mismatch is an [`ErrorClass::Fatal`] error, which stops the pipeline
    /// before any misaligned batch is sent. Steady-state cost after the first
    /// record is one predictable branch.
    #[must_use]
    pub fn with_schema(expected: Arc<RowSchema>) -> Self {
        ClickHouseEncoder {
            expected,
            checked: false,
            _row: PhantomData,
        }
    }

    /// An encoder that skips the first-record check, for the encode
    /// benchmarks, whose subject is the steady-state row loop.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[must_use]
    pub fn unchecked() -> Self {
        ClickHouseEncoder {
            expected: Arc::new(RowSchema::empty()),
            checked: true,
            _row: PhantomData,
        }
    }
}

impl<F> Clone for ClickHouseEncoder<F> {
    fn clone(&self) -> Self {
        // Each pipeline thread's clone re-validates its own first record:
        // the check is cheap, and threads must not race a shared flag.
        ClickHouseEncoder {
            expected: Arc::clone(&self.expected),
            checked: self.checked,
            _row: PhantomData,
        }
    }
}

impl<F> RowEncoder<F> for ClickHouseEncoder<F>
where
    F: RecFamily,
    for<'b> F::Rec<'b>: Serialize,
{
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        if !self.checked {
            let fields = schema::probe::probe_row(&rec.payload).map_err(|e| SinkError::Client {
                class: ErrorClass::Fatal,
                reason: format!("schema validation could not probe the row struct: {e}"),
            })?;
            schema::check_first_record(&self.expected, &fields).map_err(|diff| {
                SinkError::Client {
                    class: ErrorClass::Fatal,
                    reason: diff,
                }
            })?;
            self.checked = true;
        }
        rowbinary::serialize_row(&rec.payload, buf).map_err(|e| SinkError::Client {
            class: ErrorClass::RecordLevel,
            reason: format!("rowbinary encoding failed: {e}"),
        })
    }
}

/// Passthrough for records that are **already RowBinary-encoded** rows
/// (`Vec<u8>` payloads): appends the bytes verbatim. For pipelines that
/// encode upstream or replicate pre-encoded data. One record must be
/// exactly one encoded row; the framework counts rows by records.
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
    use crate::schema::typeparse;
    use serde::Serialize;
    use spate_core::checkpoint::AckRef;
    use spate_core::record::{PartitionId, RecordMeta};

    /// A schema as `with_row` would fetch it from a live table.
    fn schema(cols: &[(&str, &str)]) -> Arc<RowSchema> {
        Arc::new(RowSchema {
            wire: crate::schema::Wire::RowBinary,
            table: "`orders`".into(),
            columns: cols
                .iter()
                .map(|(n, t)| ((*n).to_string(), typeparse::parse(t), (*t).to_string()))
                .collect(),
        })
    }

    fn id_name() -> Arc<RowSchema> {
        schema(&[("id", "UInt64"), ("name", "String")])
    }

    #[derive(Serialize)]
    struct Row {
        id: u64,
        name: String,
    }

    fn record<T>(
        payload: T,
    ) -> (
        Record<T>,
        crossbeam_channel::Receiver<spate_core::checkpoint::AckMsg>,
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
        ClickHouseEncoder::<Owned<Row>>::with_schema(id_name())
            .encode(&rec, &mut buf)
            .unwrap();
        assert_eq!(buf.as_ref(), &[7, 0, 0, 0, 0, 0, 0, 0, 1, b'x']);
    }

    #[test]
    fn encodes_borrowed_rows_identically_to_owned() {
        // A borrowed family encodes byte-for-byte like its owned equivalent:
        // RowBinary only needs `Serialize`, never an owned or 'static row.
        #[derive(Serialize)]
        struct RowRef<'a> {
            id: u64,
            name: &'a str,
        }
        struct RowRefFam;
        impl RecFamily for RowRefFam {
            type Rec<'buf> = RowRef<'buf>;
        }

        let name = String::from("x");
        let (rec, _rx) = record(RowRef { id: 7, name: &name });
        let mut buf = BytesMut::new();
        ClickHouseEncoder::<RowRefFam>::with_schema(id_name())
            .encode(&rec, &mut buf)
            .unwrap();
        assert_eq!(buf.as_ref(), &[7, 0, 0, 0, 0, 0, 0, 0, 1, b'x']);
    }

    #[test]
    fn encoding_failures_are_record_level() {
        #[derive(Serialize)]
        struct Bad {
            c: char,
        }
        let (rec, _rx) = record(Bad { c: 'x' });
        let err = ClickHouseEncoder::<Owned<Bad>>::unchecked()
            .encode(&rec, &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, .. } => assert_eq!(class, ErrorClass::RecordLevel),
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    /// A field type the probe cannot describe stops the pipeline at the first
    /// record, rather than failing every record for the life of the run.
    #[test]
    fn an_unprobeable_row_is_fatal_at_the_first_record() {
        #[derive(Serialize)]
        struct Bad {
            c: char,
        }
        let (rec, _rx) = record(Bad { c: 'x' });
        let err = ClickHouseEncoder::<Owned<Bad>>::with_schema(schema(&[("c", "String")]))
            .encode(&rec, &mut BytesMut::new())
            .unwrap_err();
        match err {
            SinkError::Client { class, reason } => {
                assert_eq!(class, ErrorClass::Fatal);
                assert!(
                    reason.contains("could not probe the row struct"),
                    "{reason}"
                );
            }
            other => panic!("unexpected error shape: {other:?}"),
        }
    }

    /// The struct check runs once per encoder, and a clone re-runs it, so a
    /// pipeline thread never inherits another thread's verdict.
    #[test]
    fn a_clone_revalidates_its_own_first_record() {
        let (rec, _rx) = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut enc = ClickHouseEncoder::<Owned<Row>>::with_schema(id_name());
        enc.encode(&rec, &mut BytesMut::new()).unwrap();

        let mut wrong =
            ClickHouseEncoder::<Owned<Row>>::with_schema(schema(&[("id", "UInt64")])).clone();
        let err = wrong.encode(&rec, &mut BytesMut::new()).unwrap_err();
        match err {
            SinkError::Client { class, .. } => assert_eq!(class, ErrorClass::Fatal),
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
