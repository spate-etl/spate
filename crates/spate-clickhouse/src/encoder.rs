//! The CPU half of the sink: encoding records on pipeline threads.

use crate::ClickHouseFormat;
use crate::native::{ClickHouseNativeEncoder, NativeSchema};
use crate::rowbinary;
use crate::schema::{self, RowSchema};
use bytes::BytesMut;
use serde::Serialize;
use spate_core::deser::RecFamily;
use spate_core::error::SinkError;
use spate_core::record::Record;
use spate_core::sink::RowEncoder;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

/// The sink's `RowEncoder`: writes each record in the wire format the sink's
/// `format` names. [`ClickHouseSink::encoder`](crate::ClickHouseSink::encoder)
/// hands it out.
///
/// `F` is the **record family**: `Owned<T>` for plain owned row structs, or a
/// borrowed family whose `Rec<'buf>` points into the payload buffer for
/// zero-copy pipelines. Any family whose records implement `Serialize` at
/// every lifetime encodes.
///
/// The row struct's **field declaration order is the wire contract** (see the
/// crate docs). On each pipeline thread's first record the encoder checks the
/// struct's field names, order and type classes against the live table, and a
/// mismatch is an [`ErrorClass::Fatal`](spate_core::error::ErrorClass::Fatal)
/// error, which stops the pipeline before any misaligned batch is sent.
///
/// Cloning gives an encoder with nothing buffered, which checks its own first
/// record. The terminal stage clones one per shard.
pub struct ClickHouseFormatEncoder<F> {
    inner: Inner<F>,
}

enum Inner<F> {
    RowBinary(ClickHouseRowBinaryEncoder<F>),
    Native(ClickHouseNativeEncoder<F>),
}

impl<F> ClickHouseFormatEncoder<F> {
    pub(crate) fn row_binary(expected: Arc<RowSchema>) -> Self {
        Self::from_row_binary(ClickHouseRowBinaryEncoder::new(expected))
    }

    pub(crate) fn native(schema: Arc<NativeSchema>) -> Self {
        Self::from_native(ClickHouseNativeEncoder::new(schema))
    }

    pub(crate) fn from_row_binary(enc: ClickHouseRowBinaryEncoder<F>) -> Self {
        ClickHouseFormatEncoder {
            inner: Inner::RowBinary(enc),
        }
    }

    pub(crate) fn from_native(enc: ClickHouseNativeEncoder<F>) -> Self {
        ClickHouseFormatEncoder {
            inner: Inner::Native(enc),
        }
    }

    /// The wire format this encoder writes.
    pub(crate) fn format(&self) -> ClickHouseFormat {
        match self.inner {
            Inner::RowBinary(_) => ClickHouseFormat::RowBinary,
            Inner::Native(_) => ClickHouseFormat::Native,
        }
    }
}

impl<F> Clone for ClickHouseFormatEncoder<F> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            Inner::RowBinary(enc) => Inner::RowBinary(enc.clone()),
            Inner::Native(enc) => Inner::Native(enc.clone()),
        };
        ClickHouseFormatEncoder { inner }
    }
}

impl<F> fmt::Debug for ClickHouseFormatEncoder<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseFormatEncoder")
            .field("format", &self.format())
            .finish_non_exhaustive()
    }
}

impl<F> RowEncoder<F> for ClickHouseFormatEncoder<F>
where
    F: RecFamily,
    for<'b> F::Rec<'b>: Serialize,
{
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        match &mut self.inner {
            Inner::RowBinary(enc) => enc.encode(rec, buf),
            Inner::Native(enc) => enc.encode(rec, buf),
        }
    }

    fn buffered_bytes(&self) -> usize {
        match &self.inner {
            Inner::RowBinary(enc) => enc.buffered_bytes(),
            Inner::Native(enc) => enc.buffered_bytes(),
        }
    }

    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        match &mut self.inner {
            Inner::RowBinary(enc) => enc.finish_chunk(buf),
            Inner::Native(enc) => enc.finish_chunk(buf),
        }
    }
}

/// Encodes a record family's `Serialize` rows into RowBinary, one row per
/// record, appended in place.
///
/// The row struct's **field declaration order is the wire contract**: it is
/// the insert column list `#[derive(ClickHouseRow)]` generates (see the
/// crate docs). The encoder checks that contract against the live table's
/// schema on each pipeline thread's first record.
#[derive(Debug)]
pub struct ClickHouseRowBinaryEncoder<F> {
    expected: Arc<RowSchema>,
    /// False only for [`Self::unchecked`]; a clone starts from it.
    check: bool,
    checked: bool,
    _row: PhantomData<fn(F)>,
}

impl<F> ClickHouseRowBinaryEncoder<F> {
    /// An encoder that validates the row struct against `expected` on the
    /// first record it encodes. Steady-state cost after the first record is
    /// one predictable branch.
    #[must_use]
    pub(crate) fn new(expected: Arc<RowSchema>) -> Self {
        ClickHouseRowBinaryEncoder {
            expected,
            check: true,
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
        ClickHouseRowBinaryEncoder {
            expected: Arc::new(RowSchema::empty()),
            check: false,
            checked: true,
            _row: PhantomData,
        }
    }
}

impl<F> Clone for ClickHouseRowBinaryEncoder<F> {
    fn clone(&self) -> Self {
        // Each pipeline thread's clone re-validates its own first record:
        // the check is cheap, and threads must not race a shared flag.
        ClickHouseRowBinaryEncoder {
            expected: Arc::clone(&self.expected),
            check: self.check,
            checked: !self.check,
            _row: PhantomData,
        }
    }
}

impl<F> RowEncoder<F> for ClickHouseRowBinaryEncoder<F>
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
            let fields = schema::probe::probe_row(&rec.payload).map_err(|e| {
                SinkError::fatal(format!(
                    "schema validation could not probe the row struct: {e}"
                ))
            })?;
            schema::check_first_record(&self.expected, &fields).map_err(SinkError::fatal)?;
            self.checked = true;
        }
        rowbinary::serialize_row(&rec.payload, buf)
            .map_err(|e| SinkError::record_level(format!("rowbinary encoding failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::typeparse;
    use serde::Serialize;
    use spate_core::deser::Owned;
    use spate_core::error::ErrorClass;
    use spate_test::record;

    /// A schema as `ClickHouseSinkBuilder::connect` would fetch it from a live table.
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

    #[test]
    fn encodes_serializable_rows() {
        let rec = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut buf = BytesMut::new();
        ClickHouseRowBinaryEncoder::<Owned<Row>>::new(id_name())
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
        let rec = record(RowRef { id: 7, name: &name });
        let mut buf = BytesMut::new();
        ClickHouseRowBinaryEncoder::<RowRefFam>::new(id_name())
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
        let rec = record(Bad { c: 'x' });
        let err = ClickHouseRowBinaryEncoder::<Owned<Bad>>::unchecked()
            .encode(&rec, &mut BytesMut::new())
            .unwrap_err();
        assert_eq!(err.class(), ErrorClass::RecordLevel, "{err}");
        assert!(
            err.reason().starts_with("rowbinary encoding failed: "),
            "{err}"
        );
    }

    /// A field type the probe cannot describe stops the pipeline at the first
    /// record, rather than failing every record for the life of the run.
    #[test]
    fn an_unprobeable_row_is_fatal_at_the_first_record() {
        #[derive(Serialize)]
        struct Bad {
            c: char,
        }
        let rec = record(Bad { c: 'x' });
        let err = ClickHouseRowBinaryEncoder::<Owned<Bad>>::new(schema(&[("c", "String")]))
            .encode(&rec, &mut BytesMut::new())
            .unwrap_err();
        let reason = err.reason();
        assert_eq!(err.class(), ErrorClass::Fatal);
        assert!(
            reason.contains("could not probe the row struct"),
            "{reason}"
        );
    }

    /// The struct check runs once per encoder, and a clone of an encoder that
    /// has already passed it runs it again, so a pipeline thread never
    /// inherits another thread's verdict.
    #[test]
    fn a_clone_revalidates_its_own_first_record() {
        #[derive(Serialize)]
        struct MaybeRow {
            id: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<String>,
        }
        let mut enc = ClickHouseRowBinaryEncoder::<Owned<MaybeRow>>::new(schema(&[
            ("id", "UInt64"),
            ("name", "Nullable(String)"),
        ]));
        let full = record(MaybeRow {
            id: 1,
            name: Some("x".into()),
        });
        enc.encode(&full, &mut BytesMut::new()).unwrap();

        // Serde skips the field, so the row is one column short.
        let short = record(MaybeRow { id: 2, name: None });
        let err = enc
            .clone()
            .encode(&short, &mut BytesMut::new())
            .unwrap_err();
        assert_eq!(err.class(), ErrorClass::Fatal, "{err}");
    }

    /// A clone of the bench encoder stays unchecked.
    #[test]
    fn a_clone_of_the_unchecked_encoder_skips_the_check() {
        let rec = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut enc = ClickHouseRowBinaryEncoder::<Owned<Row>>::unchecked().clone();
        enc.encode(&rec, &mut BytesMut::new()).unwrap();
    }

    /// The format encoder hands the terminal stage the Native encoder's
    /// buffered size and block, and a per-shard clone starts empty.
    #[test]
    fn the_format_encoder_delegates_to_native() {
        let schema = NativeSchema::from_columns(&[("id", "UInt64"), ("name", "String")])
            .expect("schema builds");
        let mut enc = ClickHouseFormatEncoder::<Owned<Row>>::native(schema);
        assert_eq!(
            format!("{enc:?}"),
            "ClickHouseFormatEncoder { format: Native, .. }"
        );
        let rec = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut buf = BytesMut::new();
        enc.encode(&rec, &mut buf).unwrap();
        assert!(buf.is_empty(), "Native buffers rows until the chunk ends");
        assert!(enc.buffered_bytes() > 0);
        assert_eq!(enc.clone().buffered_bytes(), 0);

        enc.finish_chunk(&mut buf).unwrap();
        // 2 columns, 1 row, then each column's name, type and value.
        let mut block = vec![2, 1];
        block.extend([2, b'i', b'd', 6]);
        block.extend(b"UInt64");
        block.extend(7u64.to_le_bytes());
        block.extend([4]);
        block.extend(b"name");
        block.extend([6]);
        block.extend(b"String");
        block.extend([1, b'x']);
        assert_eq!(buf.as_ref(), block.as_slice());
        assert_eq!(enc.buffered_bytes(), 0);
    }

    #[test]
    fn the_format_encoder_writes_rowbinary_rows_in_place() {
        let mut enc = ClickHouseFormatEncoder::<Owned<Row>>::row_binary(id_name());
        assert_eq!(
            format!("{enc:?}"),
            "ClickHouseFormatEncoder { format: RowBinary, .. }"
        );
        let rec = record(Row {
            id: 7,
            name: "x".into(),
        });
        let mut buf = BytesMut::new();
        enc.encode(&rec, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[7, 0, 0, 0, 0, 0, 0, 0, 1, b'x']);
        assert_eq!(enc.buffered_bytes(), 0);
        enc.finish_chunk(&mut buf).unwrap();
        assert_eq!(buf.as_ref(), &[7, 0, 0, 0, 0, 0, 0, 0, 1, b'x']);
    }
}
