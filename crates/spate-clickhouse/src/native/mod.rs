//! ClickHouse **Native** (columnar, block-framed) encoder.
//!
//! Where [`crate::rowbinary`] streams rows, Native transposes a chunk of rows
//! into per-column buffers and emits one self-describing **block**:
//! ```text
//! [VarUInt num_columns][VarUInt num_rows]
//! ( [String name][String type][column prefix][column data] ) × num_columns
//! ```
//! A `FORMAT Native` insert body over HTTP is a *stream* of such blocks (they
//! concatenate), so the sink's per-chunk framing is unchanged: each
//! [`crate::native::NativeEncoder`]-produced chunk is one complete block.
//!
//! The encoder is **type-driven**: it needs each column's ClickHouse type to
//! lay bytes out columnar, so it is built from a fetched [`RowSchema`]. The
//! per-column type-name string emitted on the wire is the *raw*
//! `system.columns.type` text (so `Enum8('a'=1,…)`, `Decimal(P,S)`,
//! `DateTime64(3,'UTC')` etc. round-trip verbatim). Layout decisions come
//! from the parsed `ChType`.
//!
//! Targets `INSERT … FORMAT Native` over HTTP, which is hardcoded to
//! revision 0 / default serialization: no `BlockInfo`, no
//! `has_custom_serialization` byte, no sparse/detached columns.

mod column;
mod dispatch;
mod leaf;
mod lowcard;

use crate::schema::RowSchema;
use crate::schema::typeparse::ChType;
use bytes::BytesMut;
use column::ColumnWriter;
use dispatch::RowDispatchSer;
use leaf::{put_leb128, put_string};
use serde::Serialize;
use spate_core::deser::RecFamily;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::record::Record;
use spate_core::sink::RowEncoder;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

/// A Native encoding failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NativeError {
    /// A column's ClickHouse type is not supported by the Native encoder
    /// (fatal, surfaced at construction before any row is sent).
    #[error("column `{column}`: type `{ch_type}` is not supported by the Native encoder")]
    UnsupportedColumn {
        /// The column name.
        column: String,
        /// The offending (possibly nested) type description.
        ch_type: String,
    },
    /// The row struct's value did not match the column's type class.
    #[error("value does not match column type (expected {expected})")]
    TypeMismatch {
        /// A short label for the column type the value should have matched.
        expected: &'static str,
    },
    /// A fixed-width blob column (`UUID`/`IPv6`/`Int256`/`UInt256`) got the
    /// wrong number of bytes.
    #[error("fixed-width column expected {expected} bytes, got {got}")]
    RawWidth {
        /// The column's fixed width.
        expected: usize,
        /// The bytes the value produced.
        got: usize,
    },
    /// A `FixedString(N)` value was longer than `N`.
    #[error("FixedString({width}) value is {got} bytes (too long)")]
    FixedTooLong {
        /// The column width.
        width: usize,
        /// The value length.
        got: usize,
    },
    /// The row serialized a different number of fields than the schema has
    /// columns (positional mismatch).
    #[error("row has {got} field(s) but the table has {expected} column(s)")]
    RowArity {
        /// Columns in the schema.
        expected: usize,
        /// Fields the row serialized.
        got: usize,
    },
    /// The first record's probed struct does not match the configured
    /// columns (pre-formatted multi-line diff). Field names and order are
    /// checked whenever the schema was fetched; under `validate_schema:
    /// full` this also rejects class-incompatible types per position —
    /// including a wire-wrapper scale that disagrees with the column
    /// (`DateTime64Millis` into `DateTime64(6)`).
    #[error("{0}")]
    FirstRecord(String),
    /// A tuple/geo value serialized more elements than its column has.
    #[error("tuple value has more elements than the column type")]
    TupleArity,
    /// The row did not serialize as a struct/tuple of columns.
    #[error("row must serialize as a struct or tuple of columns")]
    NotAStruct,
    /// An internal invariant was violated (a bug).
    #[error("internal Native encoder error: {0}")]
    Internal(&'static str),
    /// An error raised by a `Serialize` implementation.
    #[error("serialize error: {0}")]
    Custom(String),
}

impl serde::ser::Error for NativeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        NativeError::Custom(msg.to_string())
    }
}

impl NativeError {
    /// Map onto the framework's error taxonomy. A columnar encode failure is
    /// **fatal**: it poisons the pending block (columns of unequal length),
    /// and a type-driven encoder only fails when the row type disagrees with
    /// the schema, which recurs for every record. Record-level skipping is
    /// not possible without a mid-block rollback, which the happy path does
    /// not pay for.
    fn into_sink_error(self) -> SinkError {
        SinkError::Client {
            class: ErrorClass::Fatal,
            reason: format!("native encoding failed: {self}"),
        }
    }
}

/// The immutable, `clickhouse`-free column template a [`NativeEncoder`] is
/// built from: the validated, config-ordered `(name, parsed type, raw type
/// string)` columns plus the validation mode they were fetched under, which
/// the encoder's first-record struct check applies.
#[derive(Debug)]
pub struct NativeSchema {
    expected: RowSchema,
}

impl NativeSchema {
    /// Build from a validated [`RowSchema`] (fetched from `system.columns`),
    /// failing fatally for any column whose type the encoder cannot lay out.
    ///
    /// The schema's validation mode carries over to the encoder's
    /// first-record check: under `validate_schema: full` each field's type
    /// class is checked against the live column type, so a wire-wrapper
    /// scale that disagrees with the table's declared precision
    /// (`DateTime64Millis` into `DateTime64(6)`) fails on the first record
    /// instead of silently landing wrong timestamps.
    pub fn from_row_schema(schema: &RowSchema) -> Result<Arc<NativeSchema>, NativeError> {
        Self::from_expected(RowSchema {
            mode: schema.mode,
            table: schema.table.clone(),
            columns: schema.columns.clone(),
        })
    }

    fn from_expected(expected: RowSchema) -> Result<Arc<NativeSchema>, NativeError> {
        for (name, ty, _) in &expected.columns {
            // Validate the writer can be built (fail fast, before any row).
            ColumnWriter::build(ty).map_err(|ch_type| NativeError::UnsupportedColumn {
                column: name.clone(),
                ch_type,
            })?;
        }
        Ok(Arc::new(NativeSchema { expected }))
    }

    /// Build a schema from `(column name, ClickHouse type string)` pairs,
    /// e.g. `[("id", "UInt64"), ("tags", "Array(LowCardinality(String))")]`.
    ///
    /// The normal path is [`ClickHouseSink::native_schema`](crate::ClickHouseSink::native_schema),
    /// which fetches the real types from `system.columns`. Use this only when
    /// the schema is known statically; the type strings must match the
    /// server's column types exactly, or the server will reject the block.
    /// The first-record check runs at name level only (there is no fetched
    /// truth to compare type classes against).
    pub fn from_columns(specs: &[(&str, &str)]) -> Result<Arc<NativeSchema>, NativeError> {
        Self::from_expected(RowSchema {
            mode: crate::config::SchemaValidation::Names,
            table: "<static schema>".into(),
            columns: specs
                .iter()
                .map(|(n, t)| {
                    (
                        (*n).to_string(),
                        crate::schema::typeparse::parse(t),
                        (*t).to_string(),
                    )
                })
                .collect(),
        })
    }

    fn columns(&self) -> &[(String, ChType, String)] {
        &self.expected.columns
    }

    fn fresh_columns(&self) -> Vec<ColumnWriter> {
        self.columns()
            .iter()
            // build() succeeded at construction, so it cannot fail here.
            .map(|(_, ty, _)| {
                ColumnWriter::build(ty).expect("column type validated at construction")
            })
            .collect()
    }
}

/// Encodes a record family's `Serialize` rows into the ClickHouse Native
/// format. Runs on pipeline threads inside the terminal stage; buffers rows
/// columnar in [`encode`](RowEncoder::encode) and emits one block per chunk
/// in [`finish_chunk`](RowEncoder::finish_chunk).
///
/// `F` is the **record family**: `Owned<T>` for plain owned row structs
/// (`NativeEncoder::<Owned<MyRow>>::new(schema)`), or a borrowed family for
/// zero-copy pipelines. Any family whose records implement `Serialize` at
/// every lifetime encodes.
///
/// Cloning mints a fresh, empty encoder over the same schema; the terminal
/// stage clones one per shard.
pub struct NativeEncoder<F> {
    schema: Arc<NativeSchema>,
    columns: Vec<ColumnWriter>,
    rows: u32,
    /// Set if a row failed mid-encode, leaving columns of unequal length. The
    /// pending block is unemittable; [`finish_chunk`](RowEncoder::finish_chunk)
    /// refuses it so the buffered rows fail and replay rather than producing a
    /// corrupt block. A failed row is fatal anyway (a type-driven encoder only
    /// fails on a schema/struct mismatch, which recurs), so the pipeline stops.
    poisoned: bool,
    /// Whether the first-record field-name check has run. Cleared on clone so
    /// each per-shard encoder re-validates its own first record (cheap, and a
    /// mismatch is the same fatal error on any shard).
    checked: bool,
    /// Cached approximate buffered size, refreshed periodically in `encode`
    /// so `buffered_bytes` (called by the terminal stage on every record) is
    /// O(1) instead of an O(columns) walk per row.
    approx_bytes: usize,
    _row: PhantomData<fn(F)>,
}

impl<F> NativeEncoder<F> {
    /// A Native encoder for the columns described by `schema`.
    #[must_use]
    pub fn new(schema: Arc<NativeSchema>) -> Self {
        let columns = schema.fresh_columns();
        NativeEncoder {
            schema,
            columns,
            rows: 0,
            poisoned: false,
            checked: false,
            approx_bytes: 0,
            _row: PhantomData,
        }
    }

    /// Sum the current buffered bytes across all columns (O(columns)).
    fn compute_buffered(&self) -> usize {
        self.columns
            .iter()
            .map(ColumnWriter::byte_len)
            .sum::<usize>()
            + self.columns.len() * 16
    }

    /// Build directly from a validated [`RowSchema`].
    pub fn from_row_schema(schema: &RowSchema) -> Result<Self, NativeError> {
        Ok(Self::new(NativeSchema::from_row_schema(schema)?))
    }

    fn finalize_block(&mut self, buf: &mut BytesMut) {
        // One up-front reservation for the whole block: a cheap overestimate
        // (compute_buffered over-counts LowCard keys ×8 and adds 16 B/column
        // slack) plus each column's name/type-name bytes and a constant for
        // the two leading VarUInts. Not exact: the overestimate is what
        // guarantees a single output allocation.
        let reserve = self.compute_buffered()
            + self
                .schema
                .columns()
                .iter()
                .map(|(name, _, type_name)| name.len() + type_name.len())
                .sum::<usize>()
            + 20;
        buf.reserve(reserve);
        put_leb128(buf, self.columns.len() as u64);
        put_leb128(buf, u64::from(self.rows));
        for (col, (name, _, type_name)) in self.columns.iter().zip(self.schema.columns()) {
            put_string(buf, name.as_bytes());
            put_string(buf, type_name.as_bytes());
            // Per column: state prefix (inner-first), then data streams.
            col.write_prefix(buf);
            col.write_data(buf);
        }
        for col in &mut self.columns {
            col.reset();
        }
        self.rows = 0;
        self.approx_bytes = 0;
    }
}

impl<F> Clone for NativeEncoder<F> {
    fn clone(&self) -> Self {
        // A per-shard clone is a fresh, empty encoder over the same schema —
        // never a copy of another shard's buffered rows.
        NativeEncoder::new(Arc::clone(&self.schema))
    }
}

impl<F> fmt::Debug for NativeEncoder<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeEncoder")
            .field("columns", &self.schema.columns().len())
            .field("rows", &self.rows)
            .finish()
    }
}

impl<F> RowEncoder<F> for NativeEncoder<F>
where
    F: RecFamily,
    for<'b> F::Rec<'b>: Serialize,
{
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        _buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        // First record only: validate the row's probed struct against the
        // configured columns off the per-row path. Positional dispatch would
        // otherwise silently mis-column a same-wire-class field/column swap,
        // and (in `full` mode) a wire wrapper whose scale disagrees with the
        // column's declared precision would silently land wrong values.
        // Best-effort safety, not a gate: `probe_row` errors for tuple/seq rows
        // (no field names) and other exotic-but-encodable shapes; skip the
        // check there rather than reject a row the encoder would accept. Only a
        // positive mismatch is fatal. Same first-record pattern as the
        // RowBinary encoder, except a probe failure is fatal there (RowBinary's
        // check requires a named struct) and skipped here (Native also accepts
        // tuple rows).
        if !self.checked {
            if let Ok(fields) = crate::schema::probe::probe_row(&rec.payload) {
                crate::schema::check_first_record(&self.schema.expected, &fields)
                    .map_err(|diff| NativeError::FirstRecord(diff).into_sink_error())?;
            }
            self.checked = true;
        }
        // No per-row buffer bookkeeping on the happy path: route the row's
        // fields straight into the column buffers. A failure part-way leaves
        // columns unequal, so poison the block. The error is fatal (a
        // type-driven encoder only fails on schema/struct mismatch, which
        // recurs), the pipeline stops, and the buffered rows fail and replay.
        match rec.payload.serialize(RowDispatchSer {
            columns: &mut self.columns,
        }) {
            Ok(()) => {
                self.rows += 1;
                // Refresh the cached size cheaply: every row for the first 16
                // (small blocks stay accurate), then every 16th (amortizing
                // the O(columns) walk). The seal threshold tolerates the
                // ≤16-row lag; blocks are thousands of rows.
                if self.rows <= 16 || self.rows.is_multiple_of(16) {
                    self.approx_bytes = self.compute_buffered();
                }
                Ok(())
            }
            Err(e) => {
                self.poisoned = true;
                Err(e.into_sink_error())
            }
        }
    }

    #[inline]
    fn buffered_bytes(&self) -> usize {
        // O(1): the cache is refreshed in `encode` (see above). Used only to
        // decide when a block reaches the target size, so a small lag is fine.
        self.approx_bytes
    }

    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        if self.poisoned {
            // A prior row failed mid-encode; the columns are unequal length.
            // Refuse rather than emit a corrupt block; the stage treats this
            // as fatal and the buffered rows fail (replay). Defensive: encode
            // already returned fatal, which should stop the pipeline first.
            return Err(SinkError::Client {
                class: ErrorClass::Fatal,
                reason: "native block abandoned after a failed row".into(),
            });
        }
        if self.rows > 0 {
            self.finalize_block(buf);
        }
        Ok(())
    }
}

#[cfg(test)]
impl<T> NativeEncoder<T> {
    /// Encode `rows` into a single Native block and return the bytes. Panics
    /// on encode failure; tests supply matching schema and rows.
    pub(crate) fn block_of<R: Serialize>(
        schema: Arc<NativeSchema>,
        rows: &[R],
    ) -> Result<Vec<u8>, NativeError> {
        let mut enc = NativeEncoder::<T>::new(schema);
        for r in rows {
            r.serialize(RowDispatchSer {
                columns: &mut enc.columns,
            })?;
            enc.rows += 1;
        }
        let mut buf = BytesMut::new();
        if enc.rows > 0 {
            enc.finalize_block(&mut buf);
        }
        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests;
