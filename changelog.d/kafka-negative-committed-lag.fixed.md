**A negative committed offset leaves the Kafka source's lag unknown** (`spate-kafka`)

A partition whose committed offset, as the group coordinator returns it, is negative has no `spate_source_lag_records` series until librdkafka reports a lag. In previous versions the lag was computed from it, so `spate_source_lag_records` read high by the offset's size, and with a high watermark near the largest offset a build with overflow checks panicked.
