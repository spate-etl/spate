**NDJSON line splitting uses a vectorised search** (`spate-json`)

`NdjsonFramer` and the deserializer's `ndjson` framing find each line end with the `memchr` crate, which uses SIMD on x86_64 and aarch64. Previously both checked one byte at a time. Records, blank-line handling and record indexes are unchanged, so a source that resumes by record index resumes at the same record. Framing an S3 object takes fewer instructions per byte, most on uncompressed objects.
