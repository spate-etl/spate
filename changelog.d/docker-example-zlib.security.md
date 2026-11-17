**The Docker example links a patched zlib** (`spate`)

The Docker example in `examples/docker/Dockerfile` links zlib 1.4, bundled with `libz-sys` 1.1.33. In previous versions it linked zlib 1.3.2 from `libz-sys` 1.1.31, which has a heap buffer overflow in `gzprintf` and `gzvprintf` (RUSTSEC-2026-0544, CVE-2026-85091). librdkafka does not call those functions, so the example was not exposed. The image links the bundled zlib because its runtime base carries none.
