**A named value for a record with no event time** (`spate-core`)

`NO_EVENT_TIME_MS` is `i64::MAX`, and a source reports it as `RecordMeta::event_time_ms` or `RawPayload::timestamp_ms` for a record that has no event time. The event basis of `metrics.e2e_basis` ages a batch from ingest when no record in it has an event time, and a record carrying this value does not change the minimum of the others. A custom source that reports 0 for a record with no event time is still read as an event at the epoch, so it should report `NO_EVENT_TIME_MS` instead.
