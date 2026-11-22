**Breaking:** **The ClickHouse sink hands out an encoder that writes its configured `format`** (`spate-clickhouse`)

`ClickHouseSink::encoder::<F>()` returns a `ClickHouseFormatEncoder<F>`, which writes RowBinary or Native as the sink's `format` names. It replaces `ClickHouseEncoder::with_schema(sink.schema())` and `NativeEncoder::new(sink.native_schema()?)`. An encoder that did not match `format` could insert garbage rows, acknowledge a batch with nothing inserted, or fail every retry. Switching `format` needs no code change.

These leave the public API: `ClickHouseEncoder`, `NativeEncoder`, `NativeSchema`, `NativeError`, `RowSchema`, `serialize_row`, `RowBinaryError`, the `native` and `rowbinary` modules, `ClickHouseSink::schema()` and `ClickHouseSink::native_schema()`. `PreEncodedRows` is removed; a pipeline of pre-encoded RowBinary rows writes a `RowEncoder` that appends each record's bytes. The type mapping the `rowbinary` module documented is in the crate docs, under Row types.

Under `format: native`, `ClickHouseSinkBuilder::connect` refuses a declared column whose type the Native format does not write, with `SchemaError::Mismatch` and the full schema diff. In 0.5, `native_schema()` refused it, one column at a time.
