**Breaking:** **The SQS feed's `connect` returns `spate_core::feed::ConnectError`** (`spate-sqs`)

`SqsFeed::connect` returns `spate_core::feed::ConnectError`, and `spate_sqs::SqsError` is removed. The three variants keep their meaning and their text. A match on `SqsError::Refused(reason)` becomes `ConnectError::Refused { reason, .. }`, and the same for `Unavailable`.
