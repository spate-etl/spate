**`NdjsonFramer` frames a CRLF record at the size cap** (`spate-json`)

`NdjsonFramer` no longer counts a line's trailing `\r` against `max_record_bytes`. In previous versions a record of exactly `max_record_bytes` failed the stream when its line ended in `\r\n`, and framed when it ended in `\n`. Streams that framed before frame the same records at the same indexes.
