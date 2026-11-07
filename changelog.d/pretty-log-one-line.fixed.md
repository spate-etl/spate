**`LogFormat::Pretty` keeps each event on one log line** (`spate-core`)

A control character or line separator in a message or field, such as a line break in a broker's refused login text or an escape sequence in an object key, is written as an escape like `\n` or `\u{1b}`. It could previously start a log line of its own, or move a terminal's cursor to look like one.
