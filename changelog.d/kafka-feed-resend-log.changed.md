**Kafka feed resend warning** (`spate-kafka`)

The Kafka feed's warning for a write that failed with a retryable error now reads `writing <what> failed; queued to send again after the backoff`. In previous versions it read `sending it again`. A write queued this way is not sent when its partition is revoked, the feed stops or a fatal error is recorded during the backoff, so the old text reported a resend that did not always happen. No unit is lost in those cases: its offset stays unfinished and the unit is delivered again. Update any log search or alert that matches the old text.
