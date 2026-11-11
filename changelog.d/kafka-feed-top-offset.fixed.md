**Kafka feed and a message at offset `i64::MAX`** (`spate-kafka`)

The Kafka feed now stops with a fatal error when a broker returns a message at offset `i64::MAX`, the top of the offset range. The position that commits past a message is one above its offset, and none exists above this one. In previous versions the feed's thread panicked on such a message in a build with overflow checks on. In a release build the committable position wrapped to `i64::MIN`. The feed now stops again at the same message on each restart, until the group's committed offset is moved past it.
