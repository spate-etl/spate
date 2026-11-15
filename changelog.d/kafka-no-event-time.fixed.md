**The Kafka source reports no event time for a message without a timestamp** (`spate-kafka`)

A message whose timestamp is absent or -1 reports `NO_EVENT_TIME_MS` as its `RawPayload::timestamp_ms` and `RecordMeta::event_time_ms`. Under `metrics.e2e_basis: event`, a batch in which no record has an event time is aged from ingest. In previous versions such a message reported 0, and its batch's end-to-end latency counted from 1970. An operator that reads `RecordMeta::event_time_ms` sees `NO_EVENT_TIME_MS` for these records.
