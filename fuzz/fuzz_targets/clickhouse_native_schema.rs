//! ClickHouse type-expression parsing behind the Native encoder's schema.
//!
//! `NativeSchema::from_columns` parses each column's ClickHouse type string,
//! a recursive grammar over `Nullable`, `LowCardinality`, `Array`, `Map`,
//! `Tuple`, enums and the parameterized date, time and decimal types. The
//! schema constructor promises to reject every column it cannot lay out
//! before any row is encoded, so `NativeEncoder::new` builds a column writer
//! per column without a fallible path. The target asserts that promise by
//! constructing an encoder from every schema that builds, and asserts that a
//! rejection names one of the columns it was given.

#![no_main]

use libfuzzer_sys::fuzz_target;
use spate_clickhouse::{NativeEncoder, NativeError, NativeSchema};

fuzz_target!(|specs: Vec<(String, String)>| {
    let columns: Vec<(&str, &str)> = specs
        .iter()
        .map(|(name, ty)| (name.as_str(), ty.as_str()))
        .collect();

    match NativeSchema::from_columns(&columns) {
        Ok(schema) => {
            // `<()>` stands in for a record family: the constructor is generic
            // over it and touches only the schema.
            let _ = NativeEncoder::<()>::new(schema);
        }
        Err(NativeError::UnsupportedColumn { column, .. }) => assert!(
            columns.iter().any(|(name, _)| *name == column),
            "the rejection names `{column}`, which is not one of {columns:?}"
        ),
        Err(_) => {}
    }
});
