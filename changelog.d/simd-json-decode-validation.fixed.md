**`simd` decoding rejects input it used to misread** (`spate-json`)

Under the `simd` feature, the inputs below are now malformed decode errors. In previous versions each one decoded to a value the document does not hold, and the record reached the sink.

- A high surrogate escape with no low surrogate escape after it decoded as a NUL character, so `"\ud800"` became `"\0"`.
- A very large exponent wrapped around, so `1e4294967296` decoded as `1.0` and `1e4294967295` as `0.1`.
- An array longer than the tuple, tuple struct, fixed-size array, tuple variant or struct it decodes into lost its extra elements.
- An integer map key with a leading `+` or a leading zero, such as `"+1"`, `"01"` or `"-01"`, decoded as a number, and so did `"-0"`.

Two inputs that failed now decode: an `i128` or `u128` map key, and a number with an exponent of many digits whose value is or rounds to zero, such as `0e99999999999` or `1e-99999999999`.

Each case now matches the default `serde_json` backend, except an `i128` map key `"-0"`, which `serde_json` decodes as `0`. Under `on_error: skip` the record is dropped, or the whole payload with `array` framing, and under `on_error: fail` decoding fails.
