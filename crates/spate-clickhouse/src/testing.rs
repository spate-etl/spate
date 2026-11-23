//! Fixtures for testing a pipeline's ClickHouse sink against the `clickhouse`
//! crate's mock server, and the sink's frame codec for benches and tests.
//!
//! Building a sink sends each replica two requests: it reads
//! `system.columns`, then sends a `SELECT 1` carrying the insert settings. A
//! mock answers both before it sees an insert. [`system_columns`] builds the
//! rows that answer the first, for `clickhouse::test::handlers::provide`, and
//! any row answers the second.
//!
//! It also exposes the encoders under the sink's format encoder, and the
//! RowBinary serializer, for benches, tests and fuzzing that encode without a
//! table.
//!
//! Behind the off-by-default `testing` feature: the rows implement a
//! `clickhouse` 0.x trait, which this crate's public API otherwise avoids
//! entirely, and no stability promise attaches to them.

use spate_core::sink::FrameCodecFactory;

pub use crate::encoder::ClickHouseRowBinaryEncoder;
pub use crate::native::{ClickHouseNativeEncoder, NativeError, NativeSchema};
pub use crate::rowbinary::{RowBinaryError, serialize_row};
pub use crate::schema::ColumnRow;
use crate::{ClickHouseFormatEncoder, Compression};

/// The format encoder over a Native encoder, as the sink hands it out under
/// `format: native`.
#[must_use]
pub fn native_format_encoder<F>(enc: ClickHouseNativeEncoder<F>) -> ClickHouseFormatEncoder<F> {
    ClickHouseFormatEncoder::from_native(enc)
}

/// The format encoder over a RowBinary encoder, as the sink hands it out under
/// `format: rowbinary`.
#[must_use]
pub fn rowbinary_format_encoder<F>(
    enc: ClickHouseRowBinaryEncoder<F>,
) -> ClickHouseFormatEncoder<F> {
    ClickHouseFormatEncoder::from_row_binary(enc)
}

/// The frame codec for `compression`, or `None` under
/// `off`.
///
/// Each instance turns one sealed chunk into one compressed block in
/// ClickHouse's native framing, the form an insert body sent with
/// `decompress=1` carries.
#[must_use]
pub fn frame_codec(compression: Compression) -> Option<FrameCodecFactory> {
    crate::codec::BlockCodec::factory(compression)
}

/// Rows answering the sink's `system.columns` query, from
/// `(name, type, default_kind)` triples in table order. `default_kind` is
/// `""` for an ordinary column, or `DEFAULT`, `MATERIALIZED` or `ALIAS`.
///
/// ```ignore
/// mock.add(handlers::provide(system_columns(&[
///     ("id", "UInt64", ""),
///     ("name", "String", ""),
/// ])));
/// ```
#[must_use]
pub fn system_columns(specs: &[(&str, &str, &str)]) -> Vec<ColumnRow> {
    specs
        .iter()
        .map(|(name, type_, default_kind)| ColumnRow {
            name: (*name).to_string(),
            type_: (*type_).to_string(),
            default_kind: (*default_kind).to_string(),
        })
        .collect()
}

/// The names the `settings` passthrough refuses.
#[must_use]
pub fn reserved_settings() -> Vec<&'static str> {
    crate::writer::reserved_names().collect()
}

/// The server exception code in an error's `reason`, as the sink reads it to
/// classify the error.
#[must_use]
pub fn exception_code(reason: &str) -> Option<u32> {
    crate::writer::exception_code(reason)
}

/// The settings the sink sets on an insert carrying `token`, with
/// `decompress` when the body is `coded`.
#[must_use]
pub fn insert_settings(token: &str, coded: bool) -> Vec<(&'static str, String)> {
    crate::writer::owned_settings(token, coded).collect()
}
