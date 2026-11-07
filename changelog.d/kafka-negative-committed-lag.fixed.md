**A negative committed offset leaves the Kafka source's lag unknown** (`spate-kafka`)

A partition whose committed offset, as the group coordinator returns it, is negative gets no lag figure until librdkafka reports one. Previously the lag was computed from it, which with a high watermark near the largest offset panicked a build with overflow checks and wrapped to a negative lag in a release build.
