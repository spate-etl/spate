**An out-of-range narrowing Avro promotion is rejected instead of truncating
silently** (`spate-avro`). A `reader_schema` declaring `int` for a field a
writer wrote as `long` used to wrap the value: a quantity of `5_000_000_000`
was delivered as `705_032_704`, with no error, nothing logged and no metric
moved. The record now fails to decode and takes the deserializer's
`ErrorPolicy` — dropped and counted on
`spate_deser_records_dropped_total{reason="skip_policy"}` under `Skip`, fatal
under `Fail`. A pipeline quietly writing wrong numbers starts reporting them
instead.

Only the range is checked, not the direction: a `long` that fits the reader's
`int` still resolves, `double`→`float` is not checked at all and saturates to
infinity, and `long`→`float`/`double` loses precision. The Avro connector page
carries the table.
