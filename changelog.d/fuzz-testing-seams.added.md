**Decoder access for fuzz testing** (`spate-s3`, `spate-coordination`)

The optional `testing` feature now exposes a `fuzz_seams` module for testing
private decoders. These functions cover the composite offset codec, object
framing with gzip and zstd decompression, and coordination record and key
parsing. The `fuzz/` harness uses them to test malformed input. The feature is
disabled by default, is not enabled by the `spate` facade, and has no semantic
versioning compatibility guarantee.
