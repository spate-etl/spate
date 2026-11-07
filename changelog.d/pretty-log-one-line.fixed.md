**`LogFormat::Pretty` keeps each event on one log line** (`spate-core`)

A line feed or carriage return in a message or field, such as a broker's refused login text or an object store's error body, is written as `\n` or `\r`. It could previously start a log line of its own.
